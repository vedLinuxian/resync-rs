#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════
# Docker Network Benchmark — 1 Gbps "Supercar" Edition
#
# At 1 Gbps, compression CPU overhead matters more than ratio.
# I/O speed, batching, and zero-copy are king.
#
# Tests:
#   1. 50 × 2MB log files (100 MB)  — high compression
#   2. 500 Rust source files (2 MB)  — small file batching
#   3. Single 100MB log              — streaming throughput
#   4. 10,000 tiny files (10 MB)     — extreme small-file stress
#   5. 1GB mixed dataset             — real-world simulation
#
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
BANDWIDTH="1gbit"    # 1 Gbps — LAN / intra-DC
LATENCY="0.5ms"      # 0.5ms — same-rack latency
SERVER_PORT=2377
RSYNC_PORT=873

# At 1 Gbps, Zstd level 1 is optimal: ~1.5 GB/s compress speed
# (faster than the link) with reasonable ratio.  Level 3 compresses
# at ~500 MB/s which is only 4 Gbps — can become a bottleneck.
ZSTD_LEVEL=1

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
echo -e "${BOLD}  resync-rs 1 Gbps Docker Network Benchmark${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}\n"

if [[ ! -f "$PROJECT_DIR/target/release/resync" ]]; then
    echo -e "${RED}Release binary not found. Run: cargo build --release${NC}"; exit 1
fi
if ! docker info >/dev/null 2>&1; then
    echo -e "${RED}Docker is not running${NC}"; exit 1
fi

echo -e "${CYAN}Config:${NC} bandwidth=${BOLD}$BANDWIDTH${NC}  latency=${BOLD}$LATENCY${NC}  zstd_level=${BOLD}$ZSTD_LEVEL${NC}\n"

# ─── Build image ──────────────────────────────────────────────────────────

echo -e "${CYAN}[1/7] Building Docker image...${NC}"
docker build -q -t resync-bench -f "$PROJECT_DIR/bench/Dockerfile" "$PROJECT_DIR" >/dev/null
echo -e "  ${GREEN}OK${NC}\n"

# ─── Start containers ────────────────────────────────────────────────────

echo -e "${CYAN}[2/7] Starting containers...${NC}"
docker rm -f "$SERVER_NAME" "$CLIENT_NAME" 2>/dev/null || true
docker network rm "$NETWORK_NAME" 2>/dev/null || true
docker network create "$NETWORK_NAME" >/dev/null

docker run -d --name "$SERVER_NAME" --network "$NETWORK_NAME" \
    --cap-add=NET_ADMIN resync-bench tail -f /dev/null >/dev/null

docker run -d --name "$CLIENT_NAME" --network "$NETWORK_NAME" \
    --cap-add=NET_ADMIN resync-bench tail -f /dev/null >/dev/null

SERVER_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$SERVER_NAME")
echo -e "  Server IP: ${BOLD}$SERVER_IP${NC}"

# Traffic shaping: 1 Gbps with 0.5ms latency
docker exec "$CLIENT_NAME" tc qdisc add dev eth0 root netem rate "$BANDWIDTH" delay "$LATENCY" 2>/dev/null || true
echo -e "  ${GREEN}Traffic shaping active: ${BANDWIDTH} + ${LATENCY}${NC}"

# Increase TCP buffer sizes for 1 Gbps (BDP = 1Gbps × 1ms ≈ 125KB)
docker exec "$CLIENT_NAME" bash -c 'sysctl -qw net.core.rmem_max=16777216 net.core.wmem_max=16777216 net.ipv4.tcp_rmem="4096 262144 16777216" net.ipv4.tcp_wmem="4096 262144 16777216"' 2>/dev/null || true
docker exec "$SERVER_NAME" bash -c 'sysctl -qw net.core.rmem_max=16777216 net.core.wmem_max=16777216 net.ipv4.tcp_rmem="4096 262144 16777216" net.ipv4.tcp_wmem="4096 262144 16777216"' 2>/dev/null || true
echo -e "  ${GREEN}TCP buffers tuned for 1 Gbps${NC}\n"

# ─── Start daemons ────────────────────────────────────────────────────────

echo -e "${CYAN}[3/7] Starting server daemons...${NC}"

docker exec -d "$SERVER_NAME" rsync --daemon --no-detach --port="$RSYNC_PORT"
sleep 0.5
docker exec -d "$SERVER_NAME" resync serve --bind 0.0.0.0 --port "$SERVER_PORT"
sleep 1

docker exec "$SERVER_NAME" bash -c "ss -tlnp | grep -E '($RSYNC_PORT|$SERVER_PORT)'" || {
    echo -e "${RED}Daemons failed to start${NC}"
    docker exec "$SERVER_NAME" ss -tlnp
    exit 1
}
echo -e "  ${GREEN}rsyncd :$RSYNC_PORT  +  resync :$SERVER_PORT${NC}\n"

# ─── Generate test data ──────────────────────────────────────────────────

echo -e "${CYAN}[4/7] Generating test datasets...${NC}"

# Dataset 1: Highly compressible log files (100 MB)
docker exec "$CLIENT_NAME" bash -c '
    mkdir -p /data/logs
    for i in $(seq 1 50); do
        yes "2025-01-15T12:34:56.789Z [INFO] GET /api/v2/users/profile?id=42&token=abc123 HTTP/1.1 200 OK host=api.example.com latency_ms=12 bytes=4096 ua=\"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36\"" \
        | head -c 2097152 > /data/logs/access_$i.log 2>/dev/null
    done
'
echo -e "  Logs:  ${BOLD}100M${NC} (50 × 2MB access logs)"

# Dataset 2: Source code (500 Rust files)
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
        for j in $(seq 1 10); do
            printf "// Line $j of file $i — generated for benchmark\n" >> /data/code/module_$i.rs
            printf "fn func_%d_%d(x: i64) -> i64 { x * %d + %d }\n" "$i" "$j" "$i" "$j" >> /data/code/module_$i.rs
        done
    done
'
echo -e "  Code:  ${BOLD}2M${NC} (500 Rust source files)"

# Dataset 3: Single large log (100 MB)
docker exec "$CLIENT_NAME" bash -c '
    mkdir -p /data/biglog
    yes "2025-06-30 09:15:23.456 [WARN] resync_rs::sync_engine: computing delta for large file path=/mnt/data/backup/archive_2025-06-30.tar.gz size=1073741824 chunks=16384 hash_time_ms=234 match_rate=0.87 bytes_transferred=139586437 compression_ratio=7.2:1 bandwidth_mbps=842.3" \
    | head -c 104857600 > /data/biglog/application.log 2>/dev/null
'
echo -e "  BigLog: ${BOLD}100M${NC} (single file)"

# Dataset 4: Extreme small-file stress (10,000 tiny files)
docker exec "$CLIENT_NAME" bash -c '
    mkdir -p /data/tiny
    for d in $(seq 1 100); do
        mkdir -p /data/tiny/dir_$d
        for f in $(seq 1 100); do
            printf "// File %d/%d\nfn main() { println!(\"hello %d.%d\"); }\n" "$d" "$f" "$d" "$f" > /data/tiny/dir_$d/file_$f.rs
        done
    done
'
echo -e "  Tiny:  ${BOLD}~5M${NC} (10,000 files × ~60 bytes)"

# Dataset 5: 1GB mixed (500 medium files + 5000 small files)
docker exec "$CLIENT_NAME" bash -c '
    mkdir -p /data/mixed/large /data/mixed/small
    # 500 × 2MB = 1GB of pseudo-random data (compressible)
    for i in $(seq 1 500); do
        dd if=/dev/urandom bs=4K count=512 2>/dev/null | base64 > /data/mixed/large/data_$i.b64
    done
    # 5000 small config/code files
    for i in $(seq 1 5000); do
        printf "[settings]\nname = worker_%d\nthreads = 8\nmax_connections = 1024\nbuffer_size = 65536\nlog_level = info\n" "$i" > /data/mixed/small/config_$i.toml
    done
'
MIXED_SIZE=$(docker exec "$CLIENT_NAME" du -sh /data/mixed | cut -f1)
echo -e "  Mixed: ${BOLD}$MIXED_SIZE${NC} (500 × 2MB + 5000 small)"
echo ""

# ─── Benchmark functions ─────────────────────────────────────────────────

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

echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  [5/7] NETWORK BENCHMARKS  (${BANDWIDTH} bandwidth)${NC}"
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

    # Theoretical min at 1 Gbps
    local theory_ms
    theory_ms=$(echo "scale=0; $data_bytes * 8 / 1000000000 * 1000" | bc)
    echo -e "  Theoretical min @ 1Gbps: ${theory_ms} ms\n"

    # rsync (no compression)
    docker exec "$SERVER_NAME" bash -c "rm -rf /dest/* 2>/dev/null; mkdir -p /dest"
    echo -ne "  rsync (no -z)           ... "
    out=$(bench "$CLIENT_NAME" "rsync -a --no-compress '$data_dir/' rsync://$SERVER_IP:$RSYNC_PORT/data/ 2>&1 || true")
    rsync_raw=$(get_ms "$out")
    [[ -z "$rsync_raw" || "$rsync_raw" == "0" ]] && { echo -e "${RED}FAILED${NC}"; rsync_raw="1"; } || echo -e "${BOLD}${rsync_raw} ms${NC}"

    # rsync -z
    docker exec "$SERVER_NAME" bash -c "rm -rf /dest/* 2>/dev/null; mkdir -p /dest"
    echo -ne "  rsync -z (zlib)         ... "
    out=$(bench "$CLIENT_NAME" "rsync -az '$data_dir/' rsync://$SERVER_IP:$RSYNC_PORT/data/ 2>&1 || true")
    rsync_z=$(get_ms "$out")
    [[ -z "$rsync_z" || "$rsync_z" == "0" ]] && { echo -e "${RED}FAILED${NC}"; rsync_z="1"; } || echo -e "${BOLD}${rsync_z} ms${NC}"

    # resync (no compression)
    docker exec "$SERVER_NAME" bash -c "rm -rf /dest/* 2>/dev/null; mkdir -p /dest/resync-raw"
    echo -ne "  resync (no -z)          ... "
    out=$(bench "$CLIENT_NAME" "resync push -r '$data_dir/' '$SERVER_IP:$SERVER_PORT:/dest/resync-raw' 2>&1 || true")
    resync_raw=$(get_ms "$out")
    [[ -z "$resync_raw" || "$resync_raw" == "0" ]] && { echo -e "${RED}FAILED${NC}"; resync_raw="1"; } || echo -e "${BOLD}${resync_raw} ms${NC}"

    # resync -z (Zstd level 1 for 1 Gbps)
    docker exec "$SERVER_NAME" bash -c "rm -rf /dest/* 2>/dev/null; mkdir -p /dest/resync-z"
    echo -ne "  resync -z (Zstd L$ZSTD_LEVEL)     ... "
    out=$(bench "$CLIENT_NAME" "resync push -rz --compress-level $ZSTD_LEVEL '$data_dir/' '$SERVER_IP:$SERVER_PORT:/dest/resync-z' 2>&1 || true")
    resync_z=$(get_ms "$out")
    [[ -z "$resync_z" || "$resync_z" == "0" ]] && { echo -e "${RED}FAILED${NC}"; resync_z="1"; } || echo -e "${BOLD}${resync_z} ms${NC}"

    # resync -z (Zstd level 3 for comparison)
    docker exec "$SERVER_NAME" bash -c "rm -rf /dest/* 2>/dev/null; mkdir -p /dest/resync-z3"
    echo -ne "  resync -z (Zstd L3)     ... "
    out=$(bench "$CLIENT_NAME" "resync push -rz --compress-level 3 '$data_dir/' '$SERVER_IP:$SERVER_PORT:/dest/resync-z3' 2>&1 || true")
    resync_z3=$(get_ms "$out")
    [[ -z "$resync_z3" || "$resync_z3" == "0" ]] && { echo -e "${RED}FAILED${NC}"; resync_z3="1"; } || echo -e "${BOLD}${resync_z3} ms${NC}"

    echo ""

    # Best resync time
    local best_resync=$resync_raw
    local best_label="raw"
    if [[ "$resync_z" -lt "$best_resync" ]]; then best_resync=$resync_z; best_label="zstd-L$ZSTD_LEVEL"; fi
    if [[ "$resync_z3" -lt "$best_resync" ]]; then best_resync=$resync_z3; best_label="zstd-L3"; fi

    # Speedups
    if [[ "$best_resync" != "0" && "$rsync_raw" != "0" ]]; then
        local vs_raw=$(echo "scale=1; $rsync_raw / $best_resync" | bc 2>/dev/null || echo "?")
        echo -e "  ${GREEN}resync ($best_label) vs rsync (raw):  ${BOLD}${vs_raw}x${NC}"
    fi
    if [[ "$best_resync" != "0" && "$rsync_z" != "0" ]]; then
        local vs_z=$(echo "scale=1; $rsync_z / $best_resync" | bc 2>/dev/null || echo "?")
        echo -e "  ${GREEN}resync ($best_label) vs rsync -z:     ${BOLD}${vs_z}x${NC}"
    fi

    echo ""
    echo -e "  ${CYAN}rsync=${rsync_raw}ms  rsync-z=${rsync_z}ms  resync=${resync_raw}ms  resync-zL${ZSTD_LEVEL}=${resync_z}ms  resync-zL3=${resync_z3}ms${NC}"
    echo ""
}

# ── Run test scenarios ──

run_test \
    "TEST 1: LOG FILES (50 × 2MB)" \
    "/data/logs" \
    "Repetitive access logs — high compression ratio"

run_test \
    "TEST 2: SOURCE CODE (500 files)" \
    "/data/code" \
    "Rust source files — BatchFiles batching test"

run_test \
    "TEST 3: SINGLE 100MB LOG" \
    "/data/biglog" \
    "Streaming throughput — single large file"

run_test \
    "TEST 4: TINY FILES (10,000 × 60B)" \
    "/data/tiny" \
    "Extreme small-file stress — latency & batching test"

run_test \
    "TEST 5: MIXED 1GB DATASET" \
    "/data/mixed" \
    "Real-world: 500 × 2MB + 5000 small files"

# ═══════════════════════════════════════════════════════════════════════════
# LOCAL BENCHMARK
# ═══════════════════════════════════════════════════════════════════════════

echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  [6/7] LOCAL BENCHMARK (disk-to-disk, no network)${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}\n"

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
echo -e "${BOLD}$LOCAL_SIZE${NC}\n"

echo -e "  ${BOLD}Full copy (cold):${NC}"
echo -ne "    rsync -a   ... "
rm -rf "$LOCAL_DST"/*
S=$(date +%s%N); rsync -a "$LOCAL_SRC/" "$LOCAL_DST/"; E=$(date +%s%N)
RSYNC_COLD=$(( (E - S) / 1000000 ))
echo -e "${RSYNC_COLD} ms"

echo -ne "    resync -a   ... "
rm -rf "$LOCAL_DST2"/*
S=$(date +%s%N); "$PROJECT_DIR/target/release/resync" -a "$LOCAL_SRC/" "$LOCAL_DST2/" 2>/dev/null; E=$(date +%s%N)
RESYNC_COLD=$(( (E - S) / 1000000 ))
echo -e "${RESYNC_COLD} ms"
COLD_X=$(echo "scale=1; $RSYNC_COLD / $RESYNC_COLD" | bc 2>/dev/null || echo "?")
echo -e "    ${GREEN}Speedup: ${BOLD}${COLD_X}x${NC}\n"

echo -e "  ${BOLD}No-change sync (warm):${NC}"
echo -ne "    rsync -a   ... "
S=$(date +%s%N); rsync -a "$LOCAL_SRC/" "$LOCAL_DST/"; E=$(date +%s%N)
RSYNC_WARM=$(( (E - S) / 1000000 ))
echo -e "${RSYNC_WARM} ms"

echo -ne "    resync -a   ... "
S=$(date +%s%N); "$PROJECT_DIR/target/release/resync" -a "$LOCAL_SRC/" "$LOCAL_DST2/" 2>/dev/null; E=$(date +%s%N)
RESYNC_WARM=$(( (E - S) / 1000000 ))
echo -e "${RESYNC_WARM} ms"
WARM_X=$(echo "scale=1; $RSYNC_WARM / $RESYNC_WARM" | bc 2>/dev/null || echo "?")
echo -e "    ${GREEN}Speedup: ${BOLD}${WARM_X}x${NC}\n"

rm -rf "$LOCAL_SRC" "$LOCAL_DST" "$LOCAL_DST2"

# ═══════════════════════════════════════════════════════════════════════════

echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  [7/7] SUMMARY${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}"
echo -e "  Bandwidth: ${BOLD}$BANDWIDTH${NC}  Latency: ${BOLD}$LATENCY${NC}"
echo -e "  Zstd Levels tested: L${ZSTD_LEVEL} (fast) + L3 (balanced)"
echo -e "  Local cold: rsync ${RSYNC_COLD}ms vs resync ${RESYNC_COLD}ms = ${GREEN}${BOLD}${COLD_X}x${NC}"
echo -e "  Local warm: rsync ${RSYNC_WARM}ms vs resync ${RESYNC_WARM}ms = ${GREEN}${BOLD}${WARM_X}x${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}\n"
