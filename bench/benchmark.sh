#!/usr/bin/env bash
#
# resync-rs vs rsync benchmark suite
#
# Tests real-world scenarios with actual timing.
# Drops page cache between runs for cold-cache accuracy.
# Reports speedup factors.

set -euo pipefail

RESYNC="$(cd "$(dirname "$0")/.." && pwd)/target/release/resync"
RSYNC="/usr/bin/rsync"
BENCH_DIR="/tmp/resync-bench-$$"
RESULTS_FILE="/tmp/resync-bench-results-$$.txt"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

cleanup() {
    rm -rf "$BENCH_DIR"
}
trap cleanup EXIT

drop_cache() {
    # Try to drop page cache. Needs sudo, skip silently if not available.
    sync
    if sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null; then
        true
    fi
}

# Time a command, return wall-clock seconds (with millisecond precision)
time_cmd() {
    local start end elapsed
    start=$(date +%s%N)
    "$@" >/dev/null 2>&1
    end=$(date +%s%N)
    elapsed=$(( (end - start) ))
    # Return nanoseconds, caller divides
    echo "$elapsed"
}

print_header() {
    echo ""
    echo -e "${BOLD}================================================================${NC}"
    echo -e "${BOLD}  resync-rs vs rsync   --   Benchmark Suite${NC}"
    echo -e "${BOLD}================================================================${NC}"
    echo -e "  CPU     : $(cat /proc/cpuinfo | grep 'model name' | head -1 | sed 's/model name.*: //')"
    echo -e "  Cores   : $(nproc)"
    echo -e "  RAM     : $(free -h | awk '/Mem:/{print $2}')"
    echo -e "  Disk    : NVMe ($(df -Th /tmp | tail -1 | awk '{print $2}'))"
    echo -e "  rsync   : $(rsync --version | head -1 | awk '{print $3}')"
    echo -e "  resync  : $($RESYNC --version | awk '{print $2}')"
    echo -e "${BOLD}================================================================${NC}"
    echo ""
}

print_result() {
    local scenario="$1" rsync_ns="$2" resync_ns="$3"
    local rsync_ms=$(( rsync_ns / 1000000 ))
    local resync_ms=$(( resync_ns / 1000000 ))
    local rsync_s=$(echo "scale=3; $rsync_ns / 1000000000" | bc)
    local resync_s=$(echo "scale=3; $resync_ns / 1000000000" | bc)

    local speedup
    if [ "$resync_ns" -gt 0 ]; then
        speedup=$(echo "scale=1; $rsync_ns / $resync_ns" | bc)
    else
        speedup="INF"
    fi

    printf "  %-32s  %8ss  %8ss  ${GREEN}%6sx${NC}\n" "$scenario" "$rsync_s" "$resync_s" "$speedup"
    echo "$scenario|$rsync_s|$resync_s|$speedup" >> "$RESULTS_FILE"
}

# ─── Generate test data ──────────────────────────────────────────────────────

generate_many_small_files() {
    local dir="$1" count="$2"
    echo -ne "  Generating $count small files... "
    mkdir -p "$dir"
    # Use parallel generation for speed
    seq 1 "$count" | xargs -P 8 -I{} sh -c '
        subdir=$(( {} % 100 ))
        mkdir -p "'"$dir"'/d$subdir"
        dd if=/dev/urandom of="'"$dir"'/d$subdir/f{}.dat" bs=1K count=$(( (RANDOM % 10) + 1 )) 2>/dev/null
    '
    echo "done ($(du -sh "$dir" | awk '{print $1}'))"
}

generate_medium_files() {
    local dir="$1" count="$2"
    echo -ne "  Generating $count medium files (100KB-1MB)... "
    mkdir -p "$dir"
    for i in $(seq 1 "$count"); do
        local size=$(( (RANDOM % 900 + 100) ))  # 100-999 KB
        dd if=/dev/urandom of="$dir/medium_$i.dat" bs=1K count=$size 2>/dev/null
    done
    echo "done ($(du -sh "$dir" | awk '{print $1}'))"
}

generate_large_files() {
    local dir="$1" count="$2" size_mb="$3"
    echo -ne "  Generating $count x ${size_mb}MB files... "
    mkdir -p "$dir"
    for i in $(seq 1 "$count"); do
        dd if=/dev/urandom of="$dir/large_$i.dat" bs=1M count=$size_mb 2>/dev/null
    done
    echo "done ($(du -sh "$dir" | awk '{print $1}'))"
}

mutate_files() {
    local dir="$1" pct="$2"
    local total=$(find "$dir" -type f | wc -l)
    local count=$(( total * pct / 100 ))
    echo -ne "  Mutating $count files ($pct%)... "
    find "$dir" -type f | shuf -n "$count" | while read -r f; do
        # Overwrite a random portion of the file
        local fsize=$(stat -c%s "$f" 2>/dev/null || echo 100)
        if [ "$fsize" -gt 10 ]; then
            local offset=$(( RANDOM % (fsize / 2) ))
            dd if=/dev/urandom of="$f" bs=1 count=64 seek=$offset conv=notrunc 2>/dev/null
        fi
    done
    echo "done"
}

# ─── Benchmark scenarios ──────────────────────────────────────────────────────

run_benchmarks() {
    mkdir -p "$BENCH_DIR"
    > "$RESULTS_FILE"

    echo -e "${CYAN}[1/6] Many small files — initial full copy (10,000 files)${NC}"
    echo ""
    generate_many_small_files "$BENCH_DIR/src_small" 10000
    mkdir -p "$BENCH_DIR/dst_rsync_small" "$BENCH_DIR/dst_resync_small"
    drop_cache

    echo -ne "  Running rsync... "
    local t_rsync=$(time_cmd $RSYNC -a "$BENCH_DIR/src_small/" "$BENCH_DIR/dst_rsync_small/")
    echo "done"
    drop_cache

    echo -ne "  Running resync... "
    local t_resync=$(time_cmd $RESYNC -a "$BENCH_DIR/src_small/" "$BENCH_DIR/dst_resync_small/")
    echo "done"

    print_result "Small files: full copy (10K)" "$t_rsync" "$t_resync"

    # ── Scenario 2: small files incremental (no changes) ─────────────────
    echo ""
    echo -e "${CYAN}[2/6] Many small files — no changes (10,000 files)${NC}"
    echo ""
    drop_cache

    echo -ne "  Running rsync (no changes)... "
    t_rsync=$(time_cmd $RSYNC -a "$BENCH_DIR/src_small/" "$BENCH_DIR/dst_rsync_small/")
    echo "done"
    drop_cache

    echo -ne "  Running resync (no changes)... "
    t_resync=$(time_cmd $RESYNC -a "$BENCH_DIR/src_small/" "$BENCH_DIR/dst_resync_small/")
    echo "done"

    print_result "Small files: no change (10K)" "$t_rsync" "$t_resync"

    # ── Scenario 3: small files incremental (5% changed) ─────────────────
    echo ""
    echo -e "${CYAN}[3/6] Many small files — 5% changed${NC}"
    echo ""
    mutate_files "$BENCH_DIR/src_small" 5
    drop_cache

    echo -ne "  Running rsync (5% changed)... "
    t_rsync=$(time_cmd $RSYNC -a "$BENCH_DIR/src_small/" "$BENCH_DIR/dst_rsync_small/")
    echo "done"
    drop_cache

    echo -ne "  Running resync (5% changed)... "
    t_resync=$(time_cmd $RESYNC -a "$BENCH_DIR/src_small/" "$BENCH_DIR/dst_resync_small/")
    echo "done"

    print_result "Small files: 5% changed" "$t_rsync" "$t_resync"

    # ── Scenario 4: medium files (500 files, 100KB-1MB) ──────────────────
    echo ""
    echo -e "${CYAN}[4/6] Medium files — initial copy (500 files, 100KB-1MB)${NC}"
    echo ""
    generate_medium_files "$BENCH_DIR/src_medium" 500
    mkdir -p "$BENCH_DIR/dst_rsync_medium" "$BENCH_DIR/dst_resync_medium"
    drop_cache

    echo -ne "  Running rsync... "
    t_rsync=$(time_cmd $RSYNC -a "$BENCH_DIR/src_medium/" "$BENCH_DIR/dst_rsync_medium/")
    echo "done"
    drop_cache

    echo -ne "  Running resync... "
    t_resync=$(time_cmd $RESYNC -a "$BENCH_DIR/src_medium/" "$BENCH_DIR/dst_resync_medium/")
    echo "done"

    print_result "Medium files: full copy (500)" "$t_rsync" "$t_resync"

    # ── Scenario 5: large files (5 x 100MB) ──────────────────────────────
    echo ""
    echo -e "${CYAN}[5/6] Large files — initial copy (5 x 100MB)${NC}"
    echo ""
    generate_large_files "$BENCH_DIR/src_large" 5 100
    mkdir -p "$BENCH_DIR/dst_rsync_large" "$BENCH_DIR/dst_resync_large"
    drop_cache

    echo -ne "  Running rsync... "
    t_rsync=$(time_cmd $RSYNC -a "$BENCH_DIR/src_large/" "$BENCH_DIR/dst_rsync_large/")
    echo "done"
    drop_cache

    echo -ne "  Running resync... "
    t_resync=$(time_cmd $RESYNC -a "$BENCH_DIR/src_large/" "$BENCH_DIR/dst_resync_large/")
    echo "done"

    print_result "Large files: full copy (5x100M)" "$t_rsync" "$t_resync"

    # ── Scenario 6: large files with delta (10% changed) ─────────────────
    echo ""
    echo -e "${CYAN}[6/6] Large files — 10% of bytes changed${NC}"
    echo ""
    # Mutate ~10% of each large file
    for f in "$BENCH_DIR/src_large"/*.dat; do
        local fsize=$(stat -c%s "$f")
        local mutate_bytes=$(( fsize / 10 ))
        dd if=/dev/urandom of="$f" bs=1M count=10 seek=5 conv=notrunc 2>/dev/null
    done
    echo "  Mutated 10MB in each large file"
    drop_cache

    echo -ne "  Running rsync... "
    t_rsync=$(time_cmd $RSYNC -a "$BENCH_DIR/src_large/" "$BENCH_DIR/dst_rsync_large/")
    echo "done"
    drop_cache

    echo -ne "  Running resync... "
    t_resync=$(time_cmd $RESYNC -a "$BENCH_DIR/src_large/" "$BENCH_DIR/dst_resync_large/")
    echo "done"

    print_result "Large files: delta (10% changed)" "$t_rsync" "$t_resync"

    # ── Summary ──────────────────────────────────────────────────────────
    echo ""
    echo -e "${BOLD}================================================================${NC}"
    echo -e "${BOLD}  Results Summary${NC}"
    echo -e "${BOLD}================================================================${NC}"
    printf "  %-32s  %9s  %9s  %7s\n" "Scenario" "rsync" "resync" "Speedup"
    echo "  ---------------------------------------------------------------"
    while IFS='|' read -r scenario rsync_s resync_s speedup; do
        printf "  %-32s  %8ss  %8ss  ${GREEN}%6sx${NC}\n" "$scenario" "$rsync_s" "$resync_s" "$speedup"
    done < "$RESULTS_FILE"
    echo -e "${BOLD}================================================================${NC}"
    echo ""
}

# ─── Main ─────────────────────────────────────────────────────────────────────

print_header
run_benchmarks
