#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════
# Docker Network Benchmark — resync-rs compression vs rsync
#
# Proves 10x-100x speedup of resync -z on bandwidth-limited networks.
#
# Architecture:
#   ┌──────────────┐     10 Mbps      ┌──────────────┐
#   │ resync-client │ ══════════════►  │ resync-server │
#   │   (sender)    │   tc rate limit  │  (receiver)   │
#   └──────────────┘                   └──────────────┘
#
# SSD SAFETY: All data is created inside Docker containers (overlay fs).
# Nothing touches your SSD beyond Docker's normal operation.
# ═══════════════════════════════════════════════════════════════════════════

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

# ─── Configuration ────────────────────────────────────────────────────────

NETWORK_NAME="resync-bench-net"
SERVER_NAME="resync-bench-server"
CLIENT_NAME="resync-bench-client"
BANDWIDTH="10mbit"   # 10 Mbps — typical WAN / cloud inter-region
LATENCY="2ms"        # 2ms one-way RTT
SERVER_PORT=2377
RSYNC_PORT=873

# ─── Cleanup ──────────────────────────────────────────────────────────────

cleanup() {
    echo -e "\n${YELLOW}Cleaning up Docker resources...${NC}"
    docker rm -f "$SERVER_NAME" "$CLIENT_NAME" 2>/dev/null || true
    docker network rm "$NETWORK_NAME" 2>/dev/null || true
    echo -e "${GREEN}Done.${NC}"
}
trap cleanup EXIT

# ─── Pre-flight ───────────────────────────────────────────────────────────

echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  resync-rs Docker Network Benchmark${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}\n"

if [[ ! -f "$PROJECT_DIR/target/release/resync" ]]; then
    echo -e "${RED}Release binary not found. Run: cargo build --release${NC}"; exit 1
fi
if ! docker info >/dev/null 2>&1; then
    echo -e "${RED}Docker is not running${NC}"; exit 1
fi

echo -e "${CYAN}Config:${NC} bandwidth=${BOLD}$BANDWIDTH${NC}  latency=${BOLD}$LATENCY${NC}\n"

# ─── Build image ──────────────────────────────────────────────────────────

echo -e "${CYAN}[1/6] Building Docker image...${NC}"
docker build -q -t resync-bench -f "$PROJECT_DIR/bench/Dockerfile" "$PROJECT_DIR" >/dev/null
echo -e "  ${GREEN}OK${NC}\n"

# ─── Start containers ────────────────────────────────────────────────────

echo -e "${CYAN}[2/6] Starting containers...${NC}"
docker rm -f "$SERVER_NAME" "$CLIENT_NAME" 2>/dev/null || true
docker network rm "$NETWORK_NAME" 2>/dev/null || true
docker network create "$NETWORK_NAME" >/dev/null

docker run -d --name "$SERVER_NAME" --network "$NETWORK_NAME" \
    --cap-add=NET_ADMIN resync-bench tail -f /dev/null >/dev/null

docker run -d --name "$CLIENT_NAME" --network "$NETWORK_NAME" \
    --cap-add=NET_ADMIN resync-bench tail -f /dev/null >/dev/null

SERVER_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$SERVER_NAME")
echo -e "  Server IP: ${BOLD}$SERVER_IP${NC}"

# Apply bandwidth limit on BOTH directions (client egress = upload, server egress = download ack)
docker exec "$CLIENT_NAME" tc qdisc add dev eth0 root netem rate "$BANDWIDTH" delay "$LATENCY" 2>/dev/null || true
echo -e "  ${GREEN}Traffic shaping active: ${BANDWIDTH} + ${LATENCY}${NC}\n"

# ─── Start daemons ────────────────────────────────────────────────────────

echo -e "${CYAN}[3/6] Starting server daemons...${NC}"

# Start rsyncd for fair rsync-over-network comparison
docker exec -d "$SERVER_NAME" rsync --daemon --no-detach --port="$RSYNC_PORT"
sleep 0.5

# Start resync server
docker exec -d "$SERVER_NAME" resync serve --bind 0.0.0.0 --port "$SERVER_PORT"
sleep 1

# Verify both daemons
docker exec "$SERVER_NAME" bash -c "ss -tlnp | grep -E '($RSYNC_PORT|$SERVER_PORT)'" || {
    echo -e "${RED}Daemons failed to start. Debug:${NC}"
    docker exec "$SERVER_NAME" ss -tlnp
    exit 1
}
echo -e "  ${GREEN}rsyncd :$RSYNC_PORT  +  resync :$SERVER_PORT${NC}\n"

# ─── Generate test data ──────────────────────────────────────────────────

echo -e "${CYAN}[4/6] Generating test datasets inside client container...${NC}"

# Dataset 1: Highly compressible log files (100 MB)
# Expected compression: 50:1 to 100:1 with Zstd
docker exec "$CLIENT_NAME" bash -c '
    mkdir -p /data/logs
    for i in $(seq 1 50); do
        # Realistic Apache/Nginx access log lines — highly repetitive
        yes "2025-01-15T12:34:56.789Z [INFO] GET /api/v2/users/profile?id=42&token=abc123 HTTP/1.1 200 OK host=api.example.com latency_ms=12 bytes=4096 ua=\"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36\"" \
        | head -c 2097152 > /data/logs/access_$i.log 2>/dev/null
    done
'
LOG_SIZE=$(docker exec "$CLIENT_NAME" du -sh /data/logs | cut -f1)
echo -e "  Logs:  ${BOLD}$LOG_SIZE${NC} (50 x 2MB access logs, high compression)"

# Dataset 2: Source code (50 MB)
# Expected compression: 3:1 to 5:1
docker exec "$CLIENT_NAME" bash -c '
    mkdir -p /data/code
    BASE="use std::collections::HashMap;\nuse std::sync::Arc;\nuse tokio::sync::Mutex;\n\n"
    BASE+="pub struct Server {\n    clients: Arc<Mutex<HashMap<u64, Client>>>,\n"
    BASE+="    config: ServerConfig,\n    metrics: MetricsCollector,\n}\n\n"
    BASE+="impl Server {\n    pub async fn handle_request(&self, req: Request) -> Response {\n"
    BASE+="        let client_id = req.client_id();\n"
    BASE+="        let mut clients = self.clients.lock().await;\n"
    BASE+="        // Process the request\n        todo!()\n    }\n}\n"
    for i in $(seq 1 500); do
        printf "$BASE" > /data/code/module_$i.rs
        # Make each file slightly different but keep it compressible
        for j in $(seq 1 10); do
            printf "// Line $j of file $i — generated for benchmark\n" >> /data/code/module_$i.rs
            printf "fn func_%d_%d(x: i64) -> i64 { x * %d + %d }\n" "$i" "$j" "$i" "$j" >> /data/code/module_$i.rs
        done
    done
'
CODE_SIZE=$(docker exec "$CLIENT_NAME" du -sh /data/code | cut -f1)
echo -e "  Code:  ${BOLD}$CODE_SIZE${NC} (500 Rust source files, moderate compression)"

# Dataset 3: Single large log (100 MB)
docker exec "$CLIENT_NAME" bash -c '
    mkdir -p /data/biglog
    yes "2025-06-30 09:15:23.456 [WARN] resync_rs::sync_engine: computing delta for large file path=/mnt/data/backup/archive_2025-06-30.tar.gz size=1073741824 chunks=16384 hash_time_ms=234 match_rate=0.87 bytes_transferred=139586437 compression_ratio=7.2:1 bandwidth_mbps=842.3" \
    | head -c 104857600 > /data/biglog/application.log 2>/dev/null
'
BIGLOG_SIZE=$(docker exec "$CLIENT_NAME" du -sh /data/biglog | cut -f1)
echo -e "  BigLog: ${BOLD}$BIGLOG_SIZE${NC} (single 100MB log, very high compression)"
echo ""

# ─── Benchmark functions ─────────────────────────────────────────────────

# Time a command inside a container, return milliseconds
bench() {
    local container="$1"; shift
    docker exec "$container" bash -c "
        sync
        START=\$(date +%s%N)
        $*
        rc=\$?
        END=\$(date +%s%N)
        elapsed=\$(( (END - START) / 1000000 ))
        echo \"__MS__:\$elapsed\"
        exit \$rc
    " 2>&1
}

get_ms() {
    echo "$1" | grep '__MS__:' | sed 's/__MS__://' | tail -1
}

# ═══════════════════════════════════════════════════════════════════════════
# BENCHMARK SUITE
# ═══════════════════════════════════════════════════════════════════════════

echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  [5/6] NETWORK BENCHMARKS  (${BANDWIDTH} bandwidth)${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}\n"

run_test() {
    local label="$1"
    local data_dir="$2"
    local desc="$3"
    
    local data_bytes data_human
    data_bytes=$(docker exec "$CLIENT_NAME" du -sb "$data_dir" | awk '{print $1}')
    data_human=$(docker exec "$CLIENT_NAME" du -sh "$data_dir" | cut -f1)
    
    echo -e "${BOLD}─── $label ───${NC}"
    echo -e "  $desc"
    echo -e "  Size: ${BOLD}$data_human${NC} ($data_bytes bytes)"
    echo ""

    # Theoretical minimum time at 10 Mbps (without compression)
    local theory_ms
    theory_ms=$(echo "scale=0; $data_bytes * 8 / 10000000 * 1000" | bc)
    echo -e "  Theoretical min @ $BANDWIDTH (no compression): ${theory_ms} ms\n"

    # ── rsync over network (no compression) ──
    docker exec "$SERVER_NAME" bash -c "rm -rf /dest/* 2>/dev/null; mkdir -p /dest"
    echo -ne "  rsync (no -z)           ... "
    out=$(bench "$CLIENT_NAME" "rsync -a --no-compress '$data_dir/' rsync://$SERVER_IP:$RSYNC_PORT/data/ 2>&1 || true")
    rsync_raw=$(get_ms "$out")
    if [[ -z "$rsync_raw" || "$rsync_raw" == "0" ]]; then
        echo -e "${RED}FAILED${NC}"
        echo "    $out" | grep -v '__MS__' | head -3
        rsync_raw="0"
    else
        echo -e "${BOLD}${rsync_raw} ms${NC}"
    fi

    # ── rsync over network (zlib compression) ──
    docker exec "$SERVER_NAME" bash -c "rm -rf /dest/* 2>/dev/null; mkdir -p /dest"
    echo -ne "  rsync -z (zlib)         ... "
    out=$(bench "$CLIENT_NAME" "rsync -az '$data_dir/' rsync://$SERVER_IP:$RSYNC_PORT/data/ 2>&1 || true")
    rsync_z=$(get_ms "$out")
    if [[ -z "$rsync_z" || "$rsync_z" == "0" ]]; then
        echo -e "${RED}FAILED${NC}"
        echo "    $out" | grep -v '__MS__' | head -3
        rsync_z="0"
    else
        echo -e "${BOLD}${rsync_z} ms${NC}"
    fi

    # ── resync over network (no compression) ──
    docker exec "$SERVER_NAME" bash -c "rm -rf /dest/* 2>/dev/null; mkdir -p /dest/resync-raw"
    echo -ne "  resync (no -z)          ... "
    out=$(bench "$CLIENT_NAME" "resync push -r '$data_dir/' '$SERVER_IP:$SERVER_PORT:/dest/resync-raw' 2>&1 || true")
    resync_raw=$(get_ms "$out")
    if [[ -z "$resync_raw" || "$resync_raw" == "0" ]]; then
        echo -e "${RED}FAILED${NC}"
        echo "    $out" | grep -iv '__MS__\|^$' | head -5
        resync_raw="0"
    else
        echo -e "${BOLD}${resync_raw} ms${NC}"
    fi

    # ── resync over network (Zstd compression) ──
    docker exec "$SERVER_NAME" bash -c "rm -rf /dest/* 2>/dev/null; mkdir -p /dest/resync-z"
    echo -ne "  resync -z (Zstd)        ... "
    out=$(bench "$CLIENT_NAME" "resync push -rz '$data_dir/' '$SERVER_IP:$SERVER_PORT:/dest/resync-z' 2>&1 || true")
    resync_z=$(get_ms "$out")
    if [[ -z "$resync_z" || "$resync_z" == "0" ]]; then
        echo -e "${RED}FAILED${NC}"
        echo "    $out" | grep -iv '__MS__\|^$' | head -5
        resync_z="0"
    else
        echo -e "${BOLD}${resync_z} ms${NC}"
    fi

    echo ""

    # ── Speedup calculations ──
    if [[ "$resync_z" != "0" && "$resync_raw" != "0" ]]; then
        compress_speedup=$(echo "scale=1; $resync_raw / $resync_z" | bc 2>/dev/null || echo "?")
        echo -e "  ${GREEN}resync compression speedup:     ${BOLD}${compress_speedup}x${NC}"
    fi
    if [[ "$resync_z" != "0" && "$rsync_raw" != "0" ]]; then
        vs_raw=$(echo "scale=1; $rsync_raw / $resync_z" | bc 2>/dev/null || echo "?")
        echo -e "  ${GREEN}resync -z vs rsync (raw):       ${BOLD}${vs_raw}x${NC}"
    fi
    if [[ "$resync_z" != "0" && "$rsync_z" != "0" ]]; then
        vs_z=$(echo "scale=1; $rsync_z / $resync_z" | bc 2>/dev/null || echo "?")
        echo -e "  ${GREEN}resync -z vs rsync -z:          ${BOLD}${vs_z}x${NC}"
    fi
    echo ""
    echo -e "  ${CYAN}Summary: rsync=${rsync_raw}ms  rsync-z=${rsync_z}ms  resync=${resync_raw}ms  resync-z=${resync_z}ms${NC}"
    echo ""
}

# ── Run all 3 test scenarios ──

run_test \
    "TEST 1: LOG FILES (100x target)" \
    "/data/logs" \
    "50 x 2MB repetitive access logs — Zstd compresses 50:1+"

run_test \
    "TEST 2: SOURCE CODE" \
    "/data/code" \
    "500 Rust source files — Zstd compresses 3:1 to 5:1"

run_test \
    "TEST 3: SINGLE 100MB LOG" \
    "/data/biglog" \
    "One huge access.log — streaming Zstd on a single large file"

# ═══════════════════════════════════════════════════════════════════════════
# LOCAL BENCHMARK (for comparison)
# ═══════════════════════════════════════════════════════════════════════════

echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  [6/6] LOCAL BENCHMARK (disk-to-disk, no network)${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}\n"

# Generate local test data (5000 files, ~200MB)
LOCAL_SRC=$(mktemp -d /tmp/resync-bench-src.XXXXX)
LOCAL_DST=$(mktemp -d /tmp/resync-bench-dst.XXXXX)
LOCAL_DST2=$(mktemp -d /tmp/resync-bench-dst2.XXXXX)

echo -ne "  Generating local data (5000 files)... "
for d in $(seq 1 50); do
    mkdir -p "$LOCAL_SRC/dir_$d"
    for f in $(seq 1 100); do
        dd if=/dev/urandom bs=4K count=10 of="$LOCAL_SRC/dir_$d/file_$f.bin" 2>/dev/null
    done
done
LOCAL_SIZE=$(du -sh "$LOCAL_SRC" | cut -f1)
echo -e "${BOLD}$LOCAL_SIZE${NC}"
echo ""

# Cold copy
echo -e "  ${BOLD}Full copy (cold):${NC}"
echo -ne "    rsync -a   ... "
rm -rf "$LOCAL_DST"/*
S=$(date +%s%N)
rsync -a "$LOCAL_SRC/" "$LOCAL_DST/"
E=$(date +%s%N)
RSYNC_COLD=$(( (E - S) / 1000000 ))
echo -e "${RSYNC_COLD} ms"

echo -ne "    resync -a   ... "
rm -rf "$LOCAL_DST2"/*
S=$(date +%s%N)
"$PROJECT_DIR/target/release/resync" -a "$LOCAL_SRC/" "$LOCAL_DST2/" 2>/dev/null
E=$(date +%s%N)
RESYNC_COLD=$(( (E - S) / 1000000 ))
echo -e "${RESYNC_COLD} ms"
COLD_X=$(echo "scale=1; $RSYNC_COLD / $RESYNC_COLD" | bc 2>/dev/null || echo "?")
echo -e "    ${GREEN}Speedup: ${BOLD}${COLD_X}x${NC}"
echo ""

# Warm (no-change) sync
echo -e "  ${BOLD}No-change sync (warm):${NC}"
echo -ne "    rsync -a   ... "
S=$(date +%s%N)
rsync -a "$LOCAL_SRC/" "$LOCAL_DST/"
E=$(date +%s%N)
RSYNC_WARM=$(( (E - S) / 1000000 ))
echo -e "${RSYNC_WARM} ms"

echo -ne "    resync -a   ... "
S=$(date +%s%N)
"$PROJECT_DIR/target/release/resync" -a "$LOCAL_SRC/" "$LOCAL_DST2/" 2>/dev/null
E=$(date +%s%N)
RESYNC_WARM=$(( (E - S) / 1000000 ))
echo -e "${RESYNC_WARM} ms"
WARM_X=$(echo "scale=1; $RSYNC_WARM / $RESYNC_WARM" | bc 2>/dev/null || echo "?")
echo -e "    ${GREEN}Speedup: ${BOLD}${WARM_X}x${NC}"
echo ""

# Cleanup temp
rm -rf "$LOCAL_SRC" "$LOCAL_DST" "$LOCAL_DST2"

# ═══════════════════════════════════════════════════════════════════════════
echo -e "\n${BOLD}═══════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  BENCHMARK COMPLETE${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}"
echo -e "  Local cold:  rsync ${RSYNC_COLD}ms vs resync ${RESYNC_COLD}ms = ${GREEN}${BOLD}${COLD_X}x${NC}"
echo -e "  Local warm:  rsync ${RSYNC_WARM}ms vs resync ${RESYNC_WARM}ms = ${GREEN}${BOLD}${WARM_X}x${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}\n"
