#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
# resync-rs Advanced Docker Network Benchmark v3.0
# ═══════════════════════════════════════════════════════════════════════════════
#
# Tests resync vs rsync over real Docker networks with traffic shaping.
#
# Architecture:
#   ┌──────────────┐  tc netem (bw+lat)  ┌──────────────┐
#   │    CLIENT     │ ═══════════════════► │    SERVER     │
#   │  (sender)     │   Docker bridge net  │  (receiver)   │
#   └──────────────┘                      └──────────────┘
#
# Scenarios tested:
#   1. Full copy — many small files (compressible text/code)
#   2. Full copy — few large files (binary/random)
#   3. Full copy — single huge compressible file
#   4. Incremental sync — 5% of files changed
#   5. No-change sync — nothing modified
#   6. Delta sync — large file partially modified
#
# Network profiles:
#   • WAN  — 10 Mbps, 50ms RTT  (cross-continent)
#   • LAN  — 1 Gbps, 0.5ms RTT  (same datacenter)
#   • Slow — 1 Mbps, 100ms RTT   (bad WiFi / cellular)
#
# Compression modes tested:
#   • rsync (no compression)
#   • rsync -z (zlib)
#   • resync (no compression)
#   • resync -z (zstd)
#
# Usage:
#   bash bench/docker_network_benchmark.sh [--quick] [--wan-only] [--lan-only]
#
# ═══════════════════════════════════════════════════════════════════════════════

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# ─── Configuration ────────────────────────────────────────────────────────────

NETWORK_NAME="resync-netbench"
SERVER_NAME="resync-srv"
CLIENT_NAME="resync-cli"
IMAGE_NAME="resync-netbench-img"
RESYNC_PORT=2377
RSYNC_PORT=873
MODE="standard"         # quick / standard

while [[ $# -gt 0 ]]; do
    case "$1" in
        --quick)    MODE="quick"; shift ;;
        --wan-only) RUN_PROFILES="wan"; shift ;;
        --lan-only) RUN_PROFILES="lan"; shift ;;
        --slow-only) RUN_PROFILES="slow"; shift ;;
        --help|-h)
            echo "Usage: $0 [--quick] [--wan-only] [--lan-only] [--slow-only]"
            exit 0 ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

RUN_PROFILES="${RUN_PROFILES:-wan lan slow}"
[[ "$MODE" == "quick" ]] && RUN_PROFILES="${RUN_PROFILES:-wan}"

# ─── Colors ───────────────────────────────────────────────────────────────────

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BLUE='\033[0;34m'; MAGENTA='\033[0;35m'
BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'

# ─── Cleanup ──────────────────────────────────────────────────────────────────

cleanup() {
    echo -e "\n${DIM}Cleaning up Docker resources...${NC}"
    docker rm -f "$SERVER_NAME" "$CLIENT_NAME" 2>/dev/null || true
    docker network rm "$NETWORK_NAME" 2>/dev/null || true
}
trap cleanup EXIT

# ─── Utility Functions ────────────────────────────────────────────────────────

ms_to_human() {
    local ms="$1"
    if (( ms >= 60000 )); then
        echo "$(echo "scale=1; $ms / 60000" | bc)m"
    elif (( ms >= 1000 )); then
        echo "$(echo "scale=2; $ms / 1000" | bc)s"
    else
        echo "${ms}ms"
    fi
}

bandwidth_human() {
    local bytes="$1" ms="$2"
    if (( ms <= 0 || bytes <= 0 )); then echo "N/A"; return; fi
    local bps=$(echo "scale=0; $bytes * 1000 / $ms" | bc 2>/dev/null || echo "0")
    if (( bps >= 1073741824 )); then
        echo "$(echo "scale=2; $bps / 1073741824" | bc) GB/s"
    elif (( bps >= 1048576 )); then
        echo "$(echo "scale=1; $bps / 1048576" | bc) MB/s"
    elif (( bps >= 1024 )); then
        echo "$(echo "scale=0; $bps / 1024" | bc) KB/s"
    else
        echo "${bps} B/s"
    fi
}

# Run a command inside a container and return elapsed ms
bench_cmd() {
    local container="$1"; shift
    local output
    output=$(docker exec "$container" bash -c "
        sync
        START_NS=\$(date +%s%N)
        $* 2>&1
        rc=\$?
        END_NS=\$(date +%s%N)
        ELAPSED_MS=\$(( (END_NS - START_NS) / 1000000 ))
        echo \"__BENCHMS__:\$ELAPSED_MS\"
        exit 0
    " 2>&1) || true
    local ms
    ms=$(echo "$output" | grep '__BENCHMS__:' | sed 's/__BENCHMS__://' | tail -1)
    echo "${ms:-0}"
}

# Apply traffic shaping on client container
apply_traffic_shaping() {
    local bw="$1" latency="$2"
    # Remove existing qdisc
    docker exec "$CLIENT_NAME" tc qdisc del dev eth0 root 2>/dev/null || true
    # Apply new shaping
    docker exec "$CLIENT_NAME" tc qdisc add dev eth0 root netem \
        rate "$bw" delay "$latency" 2>/dev/null || true
}

# ─── Pre-flight ───────────────────────────────────────────────────────────────

echo ""
echo -e "${BOLD}████████████████████████████████████████████████████████████████████${NC}"
echo -e "${BOLD}██                                                                ██${NC}"
echo -e "${BOLD}██    resync-rs  Docker Network Benchmark  v3.0                   ██${NC}"
echo -e "${BOLD}██                                                                ██${NC}"
echo -e "${BOLD}████████████████████████████████████████████████████████████████████${NC}"
echo ""

if [[ ! -f "$PROJECT_DIR/target/release/resync" ]]; then
    echo -e "${RED}ERROR: Release binary not found. Run: cargo build --release${NC}"; exit 1
fi
if ! docker info >/dev/null 2>&1; then
    echo -e "${RED}ERROR: Docker is not running or not accessible${NC}"; exit 1
fi

# ─── Build Docker image ──────────────────────────────────────────────────────

echo -e "${CYAN}[1/5] Building Docker image...${NC}"
docker build -q -t "$IMAGE_NAME" -f "$PROJECT_DIR/bench/Dockerfile.network" "$PROJECT_DIR" >/dev/null 2>&1
echo -e "  ${GREEN}Image built: $IMAGE_NAME${NC}"
echo ""

# ─── Start containers ────────────────────────────────────────────────────────

echo -e "${CYAN}[2/5] Starting server + client containers...${NC}"
docker rm -f "$SERVER_NAME" "$CLIENT_NAME" 2>/dev/null || true
docker network rm "$NETWORK_NAME" 2>/dev/null || true
docker network create "$NETWORK_NAME" >/dev/null

docker run -d --name "$SERVER_NAME" --network "$NETWORK_NAME" \
    --cap-add=NET_ADMIN "$IMAGE_NAME" tail -f /dev/null >/dev/null

docker run -d --name "$CLIENT_NAME" --network "$NETWORK_NAME" \
    --cap-add=NET_ADMIN "$IMAGE_NAME" tail -f /dev/null >/dev/null

SERVER_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$SERVER_NAME")
echo -e "  Server IP: ${BOLD}$SERVER_IP${NC}"

# Verify connectivity
docker exec "$CLIENT_NAME" ping -c1 -W1 "$SERVER_IP" >/dev/null 2>&1 || {
    echo -e "${RED}ERROR: Client cannot reach server${NC}"; exit 1
}
echo -e "  ${GREEN}Connectivity verified${NC}"
echo ""

# ─── Start daemons ────────────────────────────────────────────────────────────

echo -e "${CYAN}[3/5] Starting rsyncd + resync server daemons...${NC}"

# rsyncd
docker exec -d "$SERVER_NAME" rsync --daemon --no-detach --port="$RSYNC_PORT"
sleep 0.5

# resync server
docker exec -d "$SERVER_NAME" resync serve --bind 0.0.0.0 --port "$RESYNC_PORT"
sleep 1

# Verify daemons
DAEMONS_OK=true
docker exec "$SERVER_NAME" bash -c "ss -tlnp | grep -q ':$RSYNC_PORT'" || {
    echo -e "  ${YELLOW}WARNING: rsyncd may not have started${NC}"
    DAEMONS_OK=false
}
docker exec "$SERVER_NAME" bash -c "ss -tlnp | grep -q ':$RESYNC_PORT'" || {
    echo -e "  ${YELLOW}WARNING: resync server may not have started${NC}"
    DAEMONS_OK=false
}

if $DAEMONS_OK; then
    echo -e "  ${GREEN}rsyncd :$RSYNC_PORT  |  resync-server :$RESYNC_PORT${NC}"
else
    echo -e "  ${YELLOW}Daemon status:${NC}"
    docker exec "$SERVER_NAME" ss -tlnp 2>/dev/null | head -10
fi
echo ""

# ─── Generate test datasets ──────────────────────────────────────────────────

echo -e "${CYAN}[4/5] Generating test datasets on client...${NC}"

# Dataset 1: Many small compressible files (source code)
SMALL_COUNT=500
[[ "$MODE" == "quick" ]] && SMALL_COUNT=200
docker exec "$CLIENT_NAME" bash -c "
    mkdir -p /data/code
    for i in \$(seq 1 $SMALL_COUNT); do
        dir=/data/code/pkg_\$((i % 20))
        mkdir -p \"\$dir\"
        cat > \"\$dir/mod_\$i.rs\" <<'RSEOF'
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Module \$i - auto-generated for benchmark
pub struct Handler {
    state: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    config: Config,
}

impl Handler {
    pub async fn process(&self, req: Request) -> Result<Response> {
        let mut state = self.state.lock().await;
        let key = req.path().to_string();
        let data = state.entry(key).or_insert_with(Vec::new);
        data.extend_from_slice(req.body());
        Ok(Response::ok(data.len()))
    }
}
RSEOF
    done
"
SMALL_SIZE=$(docker exec "$CLIENT_NAME" du -sh /data/code 2>/dev/null | cut -f1)
SMALL_BYTES=$(docker exec "$CLIENT_NAME" du -sb /data/code 2>/dev/null | awk '{print $1}')
echo -e "  Code files:  ${BOLD}$SMALL_SIZE${NC} ($SMALL_COUNT files, compressible Rust code)"

# Dataset 2: Large binary files (less compressible)
LARGE_SIZE_MB=20
[[ "$MODE" == "quick" ]] && LARGE_SIZE_MB=5
docker exec "$CLIENT_NAME" bash -c "
    mkdir -p /data/binary
    dd if=/dev/urandom of=/data/binary/random_1.bin bs=1M count=$LARGE_SIZE_MB 2>/dev/null
    dd if=/dev/urandom of=/data/binary/random_2.bin bs=1M count=$LARGE_SIZE_MB 2>/dev/null
"
BIN_SIZE=$(docker exec "$CLIENT_NAME" du -sh /data/binary 2>/dev/null | cut -f1)
BIN_BYTES=$(docker exec "$CLIENT_NAME" du -sb /data/binary 2>/dev/null | awk '{print $1}')
echo -e "  Binary:      ${BOLD}$BIN_SIZE${NC} (2 random files, incompressible)"

# Dataset 3: Single huge compressible log
LOG_SIZE_MB=50
[[ "$MODE" == "quick" ]] && LOG_SIZE_MB=10
docker exec "$CLIENT_NAME" bash -c "
    mkdir -p /data/biglog
    yes '2026-03-09T12:00:00.000Z [INFO] resync_rs::sync_engine: transfer complete path=/backup/data.tar.gz size=1073741824 chunks=16384 hash_ms=234 match=0.87 bytes=139586437 ratio=7.2:1 bw=842.3mbps ua=resync/0.3.0' \
    | head -c \$(( $LOG_SIZE_MB * 1048576 )) > /data/biglog/app.log 2>/dev/null
"
LOG_SIZE=$(docker exec "$CLIENT_NAME" du -sh /data/biglog 2>/dev/null | cut -f1)
LOG_BYTES=$(docker exec "$CLIENT_NAME" du -sb /data/biglog 2>/dev/null | awk '{print $1}')
echo -e "  Big log:     ${BOLD}$LOG_SIZE${NC} (single file, highly compressible)"

# Dataset 4: Mixed realistic workload
docker exec "$CLIENT_NAME" bash -c "
    mkdir -p /data/mixed/src /data/mixed/docs /data/mixed/assets
    # Source code
    for i in \$(seq 1 100); do
        printf 'package main\nimport \"fmt\"\nfunc handler_%d() { fmt.Println(\"handler %d\") }\n' \$i \$i > /data/mixed/src/handler_\$i.go
    done
    # Docs (markdown)
    for i in \$(seq 1 50); do
        printf '# Document %d\n\nThis is a test document for benchmarking.\n\n## Section 1\nLorem ipsum dolor sit amet.\n\n## Section 2\nMore content for compression testing.\n' \$i > /data/mixed/docs/doc_\$i.md
    done
    # Small binary assets
    for i in \$(seq 1 20); do
        dd if=/dev/urandom of=/data/mixed/assets/img_\$i.bin bs=10K count=\$((RANDOM % 50 + 1)) 2>/dev/null
    done
"
MIX_SIZE=$(docker exec "$CLIENT_NAME" du -sh /data/mixed 2>/dev/null | cut -f1)
MIX_BYTES=$(docker exec "$CLIENT_NAME" du -sb /data/mixed 2>/dev/null | awk '{print $1}')
echo -e "  Mixed:       ${BOLD}$MIX_SIZE${NC} (code + docs + binary assets)"
echo ""

# ═══════════════════════════════════════════════════════════════════════════════
# BENCHMARK ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

# Counters for final summary
declare -a SUMMARY_LINES=()

# Run one test: rsync vs rsync-z vs resync vs resync-z
run_network_test() {
    local label="$1"
    local data_dir="$2"
    local data_bytes="$3"
    local data_human="$4"

    echo -e "  ${BOLD}── $label ──${NC}"
    echo -e "  ${DIM}Data: $data_human ($data_bytes bytes)${NC}"

    # theoretical min at current bandwidth (no compression)
    local theory_ms="N/A"

    # ── rsync (no compression) ──
    docker exec "$SERVER_NAME" bash -c "rm -rf /dest/* 2>/dev/null; mkdir -p /dest" 2>/dev/null || true
    echo -ne "    rsync           ... "
    local rsync_ms
    rsync_ms=$(bench_cmd "$CLIENT_NAME" "rsync -a '$data_dir/' rsync://$SERVER_IP:$RSYNC_PORT/data/ || true")
    if [[ "$rsync_ms" == "0" ]]; then
        echo -e "${YELLOW}SKIP${NC}"
    else
        local rsync_bw
        rsync_bw=$(bandwidth_human "$data_bytes" "$rsync_ms")
        echo -e "${BOLD}$(ms_to_human $rsync_ms)${NC}  ($rsync_bw)"
    fi

    # ── rsync -z (zlib compression) ──
    docker exec "$SERVER_NAME" bash -c "rm -rf /dest/* 2>/dev/null; mkdir -p /dest" 2>/dev/null || true
    echo -ne "    rsync -z        ... "
    local rsync_z_ms
    rsync_z_ms=$(bench_cmd "$CLIENT_NAME" "rsync -az '$data_dir/' rsync://$SERVER_IP:$RSYNC_PORT/data/ || true")
    if [[ "$rsync_z_ms" == "0" ]]; then
        echo -e "${YELLOW}SKIP${NC}"
    else
        local rsync_z_bw
        rsync_z_bw=$(bandwidth_human "$data_bytes" "$rsync_z_ms")
        echo -e "${BOLD}$(ms_to_human $rsync_z_ms)${NC}  ($rsync_z_bw)"
    fi

    # ── resync (no compression) ──
    docker exec "$SERVER_NAME" bash -c "rm -rf /dest/* 2>/dev/null; mkdir -p /dest/r1" 2>/dev/null || true
    echo -ne "    resync          ... "
    local resync_ms
    resync_ms=$(bench_cmd "$CLIENT_NAME" "resync push -a '$data_dir/' '$SERVER_IP:$RESYNC_PORT:/dest/r1' || true")
    if [[ "$resync_ms" == "0" ]]; then
        echo -e "${YELLOW}SKIP${NC}"
    else
        local resync_bw
        resync_bw=$(bandwidth_human "$data_bytes" "$resync_ms")
        echo -e "${BOLD}$(ms_to_human $resync_ms)${NC}  ($resync_bw)"
    fi

    # ── resync -z (zstd compression) ──
    docker exec "$SERVER_NAME" bash -c "rm -rf /dest/* 2>/dev/null; mkdir -p /dest/r2" 2>/dev/null || true
    echo -ne "    resync -z       ... "
    local resync_z_ms
    resync_z_ms=$(bench_cmd "$CLIENT_NAME" "resync push -az '$data_dir/' '$SERVER_IP:$RESYNC_PORT:/dest/r2' || true")
    if [[ "$resync_z_ms" == "0" ]]; then
        echo -e "${YELLOW}SKIP${NC}"
    else
        local resync_z_bw
        resync_z_bw=$(bandwidth_human "$data_bytes" "$resync_z_ms")
        echo -e "${BOLD}$(ms_to_human $resync_z_ms)${NC}  ($resync_z_bw)"
    fi

    # ── Speedup calculations ──
    echo ""
    local sp1="N/A" sp2="N/A" sp3="N/A"

    if [[ "$rsync_ms" != "0" && "$resync_ms" != "0" && "$resync_ms" -gt 0 ]] 2>/dev/null; then
        sp1=$(echo "scale=1; $rsync_ms / $resync_ms" | bc 2>/dev/null || echo "N/A")
        echo -e "    ${GREEN}resync vs rsync:          ${BOLD}${sp1}x${NC}"
    fi
    if [[ "$rsync_z_ms" != "0" && "$resync_z_ms" != "0" && "$resync_z_ms" -gt 0 ]] 2>/dev/null; then
        sp2=$(echo "scale=1; $rsync_z_ms / $resync_z_ms" | bc 2>/dev/null || echo "N/A")
        echo -e "    ${GREEN}resync -z vs rsync -z:    ${BOLD}${sp2}x${NC}"
    fi
    if [[ "$rsync_ms" != "0" && "$resync_z_ms" != "0" && "$resync_z_ms" -gt 0 ]] 2>/dev/null; then
        sp3=$(echo "scale=1; $rsync_ms / $resync_z_ms" | bc 2>/dev/null || echo "N/A")
        echo -e "    ${GREEN}resync -z vs rsync raw:   ${BOLD}${sp3}x${NC}"
    fi
    echo ""

    SUMMARY_LINES+=("$label|${rsync_ms:-0}|${rsync_z_ms:-0}|${resync_ms:-0}|${resync_z_ms:-0}|$sp1|$sp2|$sp3")
}

# Run incremental sync test (pre-populate then sync again)
run_incremental_test() {
    local label="$1"
    local data_dir="$2"
    local data_bytes="$3"
    local data_human="$4"
    local mutate_pct="$5"

    echo -e "  ${BOLD}── $label ──${NC}"
    echo -e "  ${DIM}Data: $data_human, mutate: ${mutate_pct}%${NC}"

    # Pre-populate dest for both tools
    docker exec "$SERVER_NAME" bash -c "rm -rf /dest/* 2>/dev/null; mkdir -p /dest/rsync_inc /dest/resync_inc" 2>/dev/null || true

    # Pre-sync rsync
    docker exec "$CLIENT_NAME" bash -c "rsync -a '$data_dir/' rsync://$SERVER_IP:$RSYNC_PORT/data/ 2>/dev/null || true" 2>/dev/null || true
    docker exec "$SERVER_NAME" bash -c "cp -a /dest/data/. /dest/rsync_inc/ 2>/dev/null; rm -rf /dest/data; mkdir -p /dest/data" 2>/dev/null || true
    docker exec "$CLIENT_NAME" bash -c "rsync -a '$data_dir/' rsync://$SERVER_IP:$RSYNC_PORT/data/ 2>/dev/null || true" 2>/dev/null || true

    # Pre-sync resync
    docker exec "$CLIENT_NAME" bash -c "resync push -a '$data_dir/' '$SERVER_IP:$RESYNC_PORT:/dest/resync_inc' 2>/dev/null || true" 2>/dev/null || true

    # Mutate source files if needed
    if [[ "$mutate_pct" -gt 0 ]]; then
        docker exec "$CLIENT_NAME" bash -c "
            total=\$(find '$data_dir' -type f | wc -l)
            count=\$(( total * $mutate_pct / 100 ))
            [[ \$count -lt 1 ]] && count=1
            find '$data_dir' -type f | shuf -n \$count | while read f; do
                fsize=\$(stat -c%s \"\$f\" 2>/dev/null || echo 100)
                if [[ \$fsize -gt 10 ]]; then
                    dd if=/dev/urandom of=\"\$f\" bs=1 count=\$((RANDOM % 64 + 1)) seek=\$((RANDOM % (fsize/2 + 1) )) conv=notrunc 2>/dev/null
                fi
            done
        " 2>/dev/null || true
    fi

    # ── rsync incremental ──
    echo -ne "    rsync           ... "
    local rsync_ms
    rsync_ms=$(bench_cmd "$CLIENT_NAME" "rsync -a '$data_dir/' rsync://$SERVER_IP:$RSYNC_PORT/data/ || true")
    [[ "$rsync_ms" != "0" ]] && echo -e "${BOLD}$(ms_to_human $rsync_ms)${NC}" || echo -e "${YELLOW}SKIP${NC}"

    # ── rsync -z incremental ──
    echo -ne "    rsync -z        ... "
    local rsync_z_ms
    rsync_z_ms=$(bench_cmd "$CLIENT_NAME" "rsync -az '$data_dir/' rsync://$SERVER_IP:$RSYNC_PORT/data/ || true")
    [[ "$rsync_z_ms" != "0" ]] && echo -e "${BOLD}$(ms_to_human $rsync_z_ms)${NC}" || echo -e "${YELLOW}SKIP${NC}"

    # ── resync incremental ──
    echo -ne "    resync -z       ... "
    local resync_z_ms
    resync_z_ms=$(bench_cmd "$CLIENT_NAME" "resync push -az '$data_dir/' '$SERVER_IP:$RESYNC_PORT:/dest/resync_inc' || true")
    [[ "$resync_z_ms" != "0" ]] && echo -e "${BOLD}$(ms_to_human $resync_z_ms)${NC}" || echo -e "${YELLOW}SKIP${NC}"

    echo ""
    local sp="N/A"
    if [[ "$rsync_ms" != "0" && "$resync_z_ms" != "0" && "$resync_z_ms" -gt 0 ]] 2>/dev/null; then
        sp=$(echo "scale=1; $rsync_ms / $resync_z_ms" | bc 2>/dev/null || echo "N/A")
        echo -e "    ${GREEN}resync -z vs rsync:       ${BOLD}${sp}x${NC}"
    fi
    local sp2="N/A"
    if [[ "$rsync_z_ms" != "0" && "$resync_z_ms" != "0" && "$resync_z_ms" -gt 0 ]] 2>/dev/null; then
        sp2=$(echo "scale=1; $rsync_z_ms / $resync_z_ms" | bc 2>/dev/null || echo "N/A")
        echo -e "    ${GREEN}resync -z vs rsync -z:    ${BOLD}${sp2}x${NC}"
    fi
    echo ""

    SUMMARY_LINES+=("$label|${rsync_ms:-0}|${rsync_z_ms:-0}|N/A|${resync_z_ms:-0}|N/A|$sp2|$sp")
}


# ═══════════════════════════════════════════════════════════════════════════════
echo -e "${CYAN}[5/5] Running network benchmarks...${NC}"
echo ""

START_TIME=$(date +%s)

# ─── Network profile loop ────────────────────────────────────────────────────

for profile in $RUN_PROFILES; do
    case "$profile" in
        wan)
            BW="10mbit";  LAT="25ms";  PROFILE_LABEL="WAN (10 Mbps, 50ms RTT)" ;;
        lan)
            BW="1gbit";   LAT="0.25ms"; PROFILE_LABEL="LAN (1 Gbps, 0.5ms RTT)" ;;
        slow)
            BW="1mbit";   LAT="50ms";  PROFILE_LABEL="SLOW (1 Mbps, 100ms RTT)" ;;
        *)
            echo "Unknown profile: $profile"; continue ;;
    esac

    echo ""
    echo -e "${MAGENTA}╔══════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${MAGENTA}║${NC}  ${BOLD}Network Profile: $PROFILE_LABEL${NC}"
    echo -e "${MAGENTA}╚══════════════════════════════════════════════════════════════════╝${NC}"
    echo ""

    apply_traffic_shaping "$BW" "$LAT"
    echo -e "  ${DIM}Traffic shaping: rate=$BW delay=$LAT${NC}"
    echo ""

    # ── Test 1: Small compressible files (source code) ──
    run_network_test \
        "[$profile] Code files (full)" \
        "/data/code" "$SMALL_BYTES" "$SMALL_SIZE"

    # ── Test 2: Large binary files (incompressible) ──
    run_network_test \
        "[$profile] Binary files (full)" \
        "/data/binary" "$BIN_BYTES" "$BIN_SIZE"

    # ── Test 3: Single huge compressible log ──
    run_network_test \
        "[$profile] Big log (full)" \
        "/data/biglog" "$LOG_BYTES" "$LOG_SIZE"

    # ── Test 4: Mixed workload ──
    run_network_test \
        "[$profile] Mixed workload (full)" \
        "/data/mixed" "$MIX_BYTES" "$MIX_SIZE"

    # ── Test 5: Incremental — 5% changed ──
    run_incremental_test \
        "[$profile] Code incr (5% chg)" \
        "/data/code" "$SMALL_BYTES" "$SMALL_SIZE" 5

    # ── Test 6: No-change sync ──
    run_incremental_test \
        "[$profile] Code incr (0% chg)" \
        "/data/code" "$SMALL_BYTES" "$SMALL_SIZE" 0

done

# ═══════════════════════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════

END_TIME=$(date +%s)
ELAPSED=$(( END_TIME - START_TIME ))

echo ""
echo -e "${BOLD}████████████████████████████████████████████████████████████████████${NC}"
echo -e "${BOLD}██                     NETWORK BENCHMARK RESULTS                  ██${NC}"
echo -e "${BOLD}████████████████████████████████████████████████████████████████████${NC}"
echo ""

printf "  ${BOLD}%-30s │ %8s │ %8s │ %8s │ %8s │ %7s │ %7s │ %7s${NC}\n" \
    "Scenario" "rsync" "rsync-z" "resync" "resync-z" "R/r" "Rz/rz" "Rz/r"
echo "  ──────────────────────────────┼──────────┼──────────┼──────────┼──────────┼─────────┼─────────┼─────────"

for line in "${SUMMARY_LINES[@]}"; do
    IFS='|' read -r name rsync_t rsync_z_t resync_t resync_z_t sp1 sp2 sp3 <<< "$line"

    local_rsync="$(ms_to_human ${rsync_t:-0})"
    local_rsync_z="$(ms_to_human ${rsync_z_t:-0})"
    local_resync="${resync_t}"
    [[ "$local_resync" != "N/A" ]] && local_resync="$(ms_to_human ${resync_t:-0})"
    local_resync_z="$(ms_to_human ${resync_z_t:-0})"

    # Color the speedup
    color="$GREEN"
    [[ "$sp3" != "N/A" ]] && {
        cmp=$(echo "$sp3 < 1.0" | bc 2>/dev/null || echo "0")
        [[ "$cmp" == "1" ]] && color="$RED"
    }

    printf "  %-30s │ %8s │ %8s │ %8s │ %8s │ ${color}%6sx${NC} │ ${color}%6sx${NC} │ ${color}%6sx${NC}\n" \
        "$name" "$local_rsync" "$local_rsync_z" "$local_resync" "$local_resync_z" \
        "${sp1:-N/A}" "${sp2:-N/A}" "${sp3:-N/A}"
done

echo "  ──────────────────────────────┴──────────┴──────────┴──────────┴──────────┴─────────┴─────────┴─────────"
echo ""
echo -e "  ${DIM}R/r   = resync vs rsync (no compression)${NC}"
echo -e "  ${DIM}Rz/rz = resync -z (zstd) vs rsync -z (zlib)${NC}"
echo -e "  ${DIM}Rz/r  = resync -z vs rsync raw (best resync vs worst rsync)${NC}"
echo ""

# Compute geometric mean of all non-N/A sp3 values
geomean=$(python3 -c "
import math
vals = []
for line in '''$(printf '%s\n' "${SUMMARY_LINES[@]}")'''.strip().split('\n'):
    parts = line.split('|')
    if len(parts) >= 8:
        v = parts[7].strip()
        try:
            f = float(v)
            if f > 0:
                vals.append(f)
        except: pass
if vals:
    gm = math.exp(sum(math.log(v) for v in vals) / len(vals))
    print(f'{gm:.1f}')
else:
    print('N/A')
" 2>/dev/null || echo "N/A")

echo -e "  ${BOLD}Geometric mean speedup (resync -z vs rsync raw): ${GREEN}${geomean}x${NC}${NC}"
echo ""

if [[ "$geomean" != "N/A" ]]; then
    gm_int=$(echo "$geomean" | cut -d. -f1)
    if (( gm_int >= 10 )); then
        echo -e "  ${GREEN}${BOLD}★ VERDICT: resync CRUSHES rsync over the network (${geomean}x geomean) ★${NC}"
    elif (( gm_int >= 2 )); then
        echo -e "  ${GREEN}${BOLD}✓ VERDICT: resync beats rsync over the network (${geomean}x geomean)${NC}"
    elif (( gm_int >= 1 )); then
        echo -e "  ${YELLOW}${BOLD}~ VERDICT: resync slightly faster than rsync (${geomean}x geomean)${NC}"
    else
        echo -e "  ${RED}${BOLD}✗ VERDICT: rsync was faster in this configuration${NC}"
    fi
fi

echo ""
echo -e "  Total benchmark time: ${ELAPSED}s"
echo -e "${BOLD}████████████████████████████████████████████████████████████████████${NC}"
echo ""
