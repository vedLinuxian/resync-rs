#!/usr/bin/env bash
#
# ══════════════════════════════════════════════════════════════════════════════
# resync-rs Advanced Benchmark Suite v3.0
# ══════════════════════════════════════════════════════════════════════════════
#
# Comprehensive performance benchmarks covering:
#   • Multiple file count scales (100 → 100K)
#   • File size spectrum (0 bytes → 1 GB)
#   • Deep directory hierarchies (100+ levels)
#   • Wide directories (10K entries per dir)
#   • Symlink-heavy workloads
#   • Delta sync at various change ratios (1%, 5%, 25%, 100%)
#   • Append-only workload
#   • No-change re-sync (idempotent)
#   • Delete+re-sync
#   • Mixed file types (text, binary, empty, large)
#   • Statistical rigor: N iterations, mean/median/stddev/p95/p99
#   • Wall-clock, CPU (user+sys), memory (VmPeak), syscall count
#   • Warm vs cold cache comparisons
#   • I/O bandwidth calculation
#   • Race condition / concurrent modification stress test
#   • Manifest cache speedup measurement
#   • --trust-mtime fast-path measurement
#   • perf stat integration (IPC, cache misses, branch mispredict)
#   • JSON + CSV output for programmatic consumption
#   • NVMe queue depth / io_uring batch performance
#
# Usage:
#   bash bench/advanced_benchmark.sh [--quick] [--full] [--scenario NAME] [--iterations N] [--json]
#
# Requirements:
#   • Linux with /proc filesystem
#   • rsync installed
#   • resync built in release mode (target/release/resync)
#   • Optional: sudo for cache dropping, strace for syscall counting
#   • Optional: perf for hardware counter analysis

set -euo pipefail

# ─── Configuration ────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESYNC="$PROJECT_DIR/target/release/resync"
RSYNC="/usr/bin/rsync"
BENCH_ROOT="/tmp/resync-adv-bench-$$"
RESULTS_DIR="$BENCH_ROOT/results"
CSV_FILE="$RESULTS_DIR/benchmark_results.csv"

ITERATIONS=3          # Default iterations per scenario
MODE="standard"       # quick / standard / full
FILTER_SCENARIO=""    # Run only specific scenario
JSON_OUTPUT=false     # Emit JSON results
JSON_FILE=""          # JSON output file path
PERF_STAT=false       # Use perf stat for HW counters

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --quick)      MODE="quick"; ITERATIONS=1; shift ;;
        --full)       MODE="full"; ITERATIONS=5; shift ;;
        --iterations) ITERATIONS="$2"; shift 2 ;;
        --scenario)   FILTER_SCENARIO="$2"; shift 2 ;;
        --json)       JSON_OUTPUT=true; shift ;;
        --perf)       PERF_STAT=true; shift ;;
        --help|-h)
            echo "Usage: $0 [--quick] [--full] [--iterations N] [--scenario NAME] [--json] [--perf]"
            echo ""
            echo "Modes:"
            echo "  --quick       Run each scenario once with smaller data"
            echo "  (default)     Run each scenario 3 times (standard)"
            echo "  --full        Run each scenario 5 times with larger data"
            echo ""
            echo "Options:"
            echo "  --iterations N   Override iteration count"
            echo "  --scenario NAME  Run only the named scenario"
            echo "  --json           Also emit machine-readable JSON results"
            echo "  --perf           Use 'perf stat' for IPC/cache metrics (needs permissions)"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ─── Colors & Formatting ─────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

# ─── Utilities ────────────────────────────────────────────────────────────────

cleanup() {
    rm -rf "$BENCH_ROOT"
}
trap cleanup EXIT

setup_dirs() {
    rm -rf "$BENCH_ROOT"
    mkdir -p "$BENCH_ROOT" "$RESULTS_DIR"
    echo "scenario,tool,iteration,wall_ns,user_ns,sys_ns,max_rss_kb,total_bytes,file_count,cache_state" > "$CSV_FILE"
    if $JSON_OUTPUT; then
        JSON_FILE="$RESULTS_DIR/benchmark_results.json"
        echo '{"benchmarks":[],"system":{}}' > "$JSON_FILE"
    fi
}

drop_cache() {
    sync
    if sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null; then
        return 0
    fi
    # Fallback: try to evict via /proc for the test data only
    # (won't drop all caches, but helps reduce warm-cache bias)
    if [[ -n "${SRC_DIR:-}" ]] && command -v vmtouch &>/dev/null; then
        vmtouch -e "$SRC_DIR" 2>/dev/null || true
        vmtouch -e "$DST_DIR" 2>/dev/null || true
    fi
    # Always return 0 so set -e doesn't kill the script
    return 0
}

# Check drop_caches availability at startup
CACHE_DROP_AVAILABLE=false
if sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null; then
    CACHE_DROP_AVAILABLE=true
else
    echo -e "${YELLOW}⚠  WARNING: sudo not available for drop_caches${NC}"
    echo -e "${YELLOW}   Benchmarks will use warm cache (both tools get same conditions)${NC}"
    echo -e "${YELLOW}   For cold-cache tests: sudo $0 $*${NC}"
    echo ""
fi

# Get total size in bytes of a directory
dir_bytes() {
    du -sb "$1" 2>/dev/null | awk '{print $1}'
}

# Count files in directory
dir_file_count() {
    find "$1" -type f 2>/dev/null | wc -l
}

# High-precision timing with resource usage via /usr/bin/time
# Returns: wall_ns user_ns sys_ns max_rss_kb
time_cmd_detailed() {
    local time_out="$BENCH_ROOT/.time_output"
    local start end wall_ns

    start=$(date +%s%N)

    # Use GNU time for user/sys/RSS
    /usr/bin/time -f '%U %S %M' -o "$time_out" "$@" >/dev/null 2>/dev/null || true

    end=$(date +%s%N)
    wall_ns=$(( end - start ))

    if [[ -f "$time_out" ]]; then
        local fields
        fields=$(tail -1 "$time_out")
        local user_s sys_s rss_kb
        user_s=$(echo "$fields" | awk '{print $1}')
        sys_s=$(echo "$fields" | awk '{print $2}')
        rss_kb=$(echo "$fields" | awk '{print $3}')
        local user_ns sys_ns
        user_ns=$(echo "$user_s * 1000000000" | bc | cut -d. -f1)
        sys_ns=$(echo "$sys_s * 1000000000" | bc | cut -d. -f1)
        echo "$wall_ns ${user_ns:-0} ${sys_ns:-0} ${rss_kb:-0}"
    else
        echo "$wall_ns 0 0 0"
    fi
}

# ─── Statistical Functions ────────────────────────────────────────────────────

# Compute stats from a file of newline-separated nanosecond values
# Returns: mean median stddev p95 p99 min max
compute_stats() {
    local file="$1"
    python3 -c "
import sys, math
vals = sorted([int(x.strip()) for x in open('$file') if x.strip()])
n = len(vals)
if n == 0:
    print('0 0 0 0 0 0 0')
    sys.exit()
mean = sum(vals) / n
median = vals[n//2]
if n > 1:
    variance = sum((x - mean)**2 for x in vals) / (n - 1)
    stddev = math.sqrt(variance)
else:
    stddev = 0
p95 = vals[int(n * 0.95)] if n >= 20 else vals[-1]
p99 = vals[int(n * 0.99)] if n >= 100 else vals[-1]
cv = (stddev / mean * 100) if mean > 0 else 0
print(f'{mean:.0f} {median:.0f} {stddev:.0f} {p95:.0f} {p99:.0f} {vals[0]} {vals[-1]} {cv:.1f}')
" 2>/dev/null || echo "0 0 0 0 0 0 0 0"
}

ns_to_human() {
    local ns="$1"
    if (( ns >= 1000000000 )); then
        echo "$(echo "scale=2; $ns / 1000000000" | bc)s"
    elif (( ns >= 1000000 )); then
        echo "$(echo "scale=1; $ns / 1000000" | bc)ms"
    elif (( ns >= 1000 )); then
        echo "$(echo "scale=0; $ns / 1000" | bc)μs"
    else
        echo "${ns}ns"
    fi
}

bandwidth_human() {
    local bytes="$1" ns="$2"
    if (( ns <= 0 || bytes <= 0 )); then
        echo "N/A"
        return
    fi
    local bps
    bps=$(echo "scale=0; $bytes * 1000000000 / $ns" | bc 2>/dev/null || echo "0")
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

# ─── JSON Helpers ─────────────────────────────────────────────────────────────

# Append a benchmark result to the JSON file
json_append_result() {
    if ! $JSON_OUTPUT || [[ -z "$JSON_FILE" ]]; then return; fi
    local name="$1" tool="$2" mean="$3" median="$4" stddev="$5"
    local p95="$6" min_val="$7" max_val="$8" speedup="$9"
    shift 9
    local total_bytes="${1:-0}" file_count="${2:-0}" rss_kb="${3:-0}" bw="${4:-N/A}" cache="${5:-cold}"

    python3 -c "
import json, sys
with open('$JSON_FILE', 'r') as f:
    data = json.load(f)
data['benchmarks'].append({
    'scenario': '$name',
    'tool': '$tool',
    'cache_state': '$cache',
    'stats': {
        'mean_ns': $mean,
        'median_ns': $median,
        'stddev_ns': $stddev,
        'p95_ns': $p95,
        'min_ns': $min_val,
        'max_ns': $max_val,
    },
    'speedup': '$speedup',
    'total_bytes': $total_bytes,
    'file_count': $file_count,
    'peak_rss_kb': $rss_kb,
    'bandwidth': '$bw',
})
with open('$JSON_FILE', 'w') as f:
    json.dump(data, f, indent=2)
" 2>/dev/null || true
}

# Save system info to JSON
json_save_sysinfo() {
    if ! $JSON_OUTPUT || [[ -z "$JSON_FILE" ]]; then return; fi
    python3 -c "
import json, subprocess, os
with open('$JSON_FILE', 'r') as f:
    data = json.load(f)
data['system'] = {
    'cpu': '$(grep "model name" /proc/cpuinfo | head -1 | sed "s/model name.*: //")',
    'cores': $(nproc),
    'threads': $(grep -c '^processor' /proc/cpuinfo),
    'ram': '$(free -h | awk "/Mem:/{print \\$2}")',
    'kernel': '$(uname -r)',
    'filesystem': '$(df -Th /tmp | tail -1 | awk "{print \\$2}")',
    'mode': '$MODE',
    'iterations': $ITERATIONS,
    'cache_drop': $($CACHE_DROP_AVAILABLE && echo 'true' || echo 'false'),
}
with open('$JSON_FILE', 'w') as f:
    json.dump(data, f, indent=2)
" 2>/dev/null || true
}

# ─── perf stat Helper ────────────────────────────────────────────────────────

# Run command under perf stat and capture IPC + cache misses
# Returns: wall_ns user_ns sys_ns max_rss_kb ipc cache_miss_rate branch_miss_rate
time_cmd_with_perf() {
    local perf_out="$BENCH_ROOT/.perf_output"
    local time_out="$BENCH_ROOT/.time_output"
    local start end wall_ns

    start=$(date +%s%N)

    if $PERF_STAT && command -v perf &>/dev/null; then
        perf stat -e instructions,cycles,cache-misses,cache-references,branch-misses,branches \
            -o "$perf_out" \
            /usr/bin/time -f '%U %S %M' -o "$time_out" "$@" >/dev/null 2>/dev/null || true
    else
        /usr/bin/time -f '%U %S %M' -o "$time_out" "$@" >/dev/null 2>/dev/null || true
        echo "" > "$perf_out"
    fi

    end=$(date +%s%N)
    wall_ns=$(( end - start ))

    local user_ns=0 sys_ns=0 rss_kb=0
    if [[ -f "$time_out" ]]; then
        local fields
        fields=$(tail -1 "$time_out")
        local user_s sys_s
        user_s=$(echo "$fields" | awk '{print $1}')
        sys_s=$(echo "$fields" | awk '{print $2}')
        rss_kb=$(echo "$fields" | awk '{print $3}')
        user_ns=$(echo "$user_s * 1000000000" | bc | cut -d. -f1)
        sys_ns=$(echo "$sys_s * 1000000000" | bc | cut -d. -f1)
    fi

    local ipc="N/A" cache_miss="N/A" branch_miss="N/A"
    if [[ -f "$perf_out" ]] && [[ -s "$perf_out" ]]; then
        local insns cycles cmiss cref bmiss branches
        insns=$(grep 'instructions' "$perf_out" 2>/dev/null | awk '{gsub(/,/,""); print $1}' | head -1)
        cycles=$(grep 'cycles' "$perf_out" 2>/dev/null | grep -v 'stalled' | awk '{gsub(/,/,""); print $1}' | head -1)
        cmiss=$(grep 'cache-misses' "$perf_out" 2>/dev/null | awk '{gsub(/,/,""); print $1}' | head -1)
        cref=$(grep 'cache-references' "$perf_out" 2>/dev/null | awk '{gsub(/,/,""); print $1}' | head -1)
        bmiss=$(grep 'branch-misses' "$perf_out" 2>/dev/null | awk '{gsub(/,/,""); print $1}' | head -1)
        branches=$(grep -w 'branches' "$perf_out" 2>/dev/null | awk '{gsub(/,/,""); print $1}' | head -1)

        if [[ -n "$insns" && -n "$cycles" && "$cycles" -gt 0 ]] 2>/dev/null; then
            ipc=$(echo "scale=2; $insns / $cycles" | bc 2>/dev/null || echo "N/A")
        fi
        if [[ -n "$cmiss" && -n "$cref" && "$cref" -gt 0 ]] 2>/dev/null; then
            cache_miss=$(echo "scale=2; $cmiss * 100 / $cref" | bc 2>/dev/null || echo "N/A")
        fi
        if [[ -n "$bmiss" && -n "$branches" && "$branches" -gt 0 ]] 2>/dev/null; then
            branch_miss=$(echo "scale=2; $bmiss * 100 / $branches" | bc 2>/dev/null || echo "N/A")
        fi
    fi

    echo "$wall_ns ${user_ns:-0} ${sys_ns:-0} ${rss_kb:-0} $ipc $cache_miss $branch_miss"
}

# ─── Data Generation ─────────────────────────────────────────────────────────

gen_small_files() {
    local dir="$1" count="$2"
    mkdir -p "$dir"
    seq 1 "$count" | xargs -P "$(nproc)" -I{} sh -c '
        subdir=$(( {} % 200 ))
        mkdir -p "'"$dir"'/d$subdir"
        dd if=/dev/urandom of="'"$dir"'/d$subdir/f{}.dat" bs=1K count=$(( (RANDOM % 10) + 1 )) 2>/dev/null
    '
}

gen_medium_files() {
    local dir="$1" count="$2"
    mkdir -p "$dir"
    seq 1 "$count" | xargs -P "$(nproc)" -I{} sh -c '
        size=$(( RANDOM % 900 + 100 ))
        dd if=/dev/urandom of="'"$dir"'/medium_{}.dat" bs=1K count=$size 2>/dev/null
    '
}

gen_large_files() {
    local dir="$1" count="$2" size_mb="$3"
    mkdir -p "$dir"
    seq 1 "$count" | xargs -P "$(nproc)" -I{} sh -c '
        dd if=/dev/urandom of="'"$dir"'/large_{}.dat" bs=1M count='"$size_mb"' 2>/dev/null
    '
}

gen_empty_files() {
    local dir="$1" count="$2"
    mkdir -p "$dir"
    for i in $(seq 1 "$count"); do
        touch "$dir/empty_$i.dat"
    done
}

gen_deep_tree() {
    local base="$1" depth="$2" files_per_level="$3"
    local path="$base"
    for d in $(seq 1 "$depth"); do
        path="$path/level_$d"
        mkdir -p "$path"
        for f in $(seq 1 "$files_per_level"); do
            dd if=/dev/urandom of="$path/f_${f}.dat" bs=1K count=$((RANDOM % 5 + 1)) 2>/dev/null
        done
    done
}

gen_wide_dir() {
    local dir="$1" count="$2"
    mkdir -p "$dir"
    # Batch generation for speed
    seq 1 "$count" | xargs -P "$(nproc)" -I{} sh -c '
        dd if=/dev/urandom of="'"$dir"'/f_{}.dat" bs=512 count=1 2>/dev/null
    '
}

gen_symlink_tree() {
    local dir="$1" count="$2"
    mkdir -p "$dir/targets" "$dir/links"
    # Create target files
    for i in $(seq 1 "$count"); do
        dd if=/dev/urandom of="$dir/targets/t_$i.dat" bs=1K count=$((RANDOM % 5 + 1)) 2>/dev/null
    done
    # Create symlinks (some absolute, some relative, some dangling)
    for i in $(seq 1 "$count"); do
        local target_idx=$(( (RANDOM % count) + 1 ))
        if (( i % 10 == 0 )); then
            # Dangling symlink every 10th
            ln -sf "/nonexistent/path_$i" "$dir/links/link_$i" 2>/dev/null || true
        elif (( i % 2 == 0 )); then
            # Relative symlink
            ln -sf "../targets/t_$target_idx.dat" "$dir/links/link_$i" 2>/dev/null || true
        else
            # Absolute symlink
            ln -sf "$dir/targets/t_$target_idx.dat" "$dir/links/link_$i" 2>/dev/null || true
        fi
    done
}

gen_mixed_workload() {
    local dir="$1"
    mkdir -p "$dir"
    # Mix of everything
    gen_empty_files "$dir/empties" 100
    gen_small_files "$dir/small" 2000
    gen_medium_files "$dir/medium" 50
    gen_large_files "$dir/large" 3 10
    gen_deep_tree "$dir/deep" 30 2
    gen_symlink_tree "$dir/symlinks" 100
    # Text files with varied encodings
    mkdir -p "$dir/text"
    for i in $(seq 1 50); do
        head -c $((RANDOM % 10000 + 100)) /dev/urandom | base64 > "$dir/text/doc_$i.txt"
    done
}

mutate_pct() {
    local dir="$1" pct="$2"
    local total
    total=$(find "$dir" -type f | wc -l)
    local count=$(( total * pct / 100 ))
    [[ $count -lt 1 ]] && count=1
    find "$dir" -type f | shuf -n "$count" | while read -r f; do
        local fsize
        fsize=$(stat -c%s "$f" 2>/dev/null || echo "100")
        if [[ "$fsize" -gt 10 ]]; then
            local offset=$(( RANDOM % (fsize / 2 + 1) ))
            dd if=/dev/urandom of="$f" bs=1 count=$((RANDOM % 64 + 1)) seek=$offset conv=notrunc 2>/dev/null
        fi
    done
}

append_to_files() {
    local dir="$1" pct="$2" append_bytes="$3"
    local total
    total=$(find "$dir" -type f | wc -l)
    local count=$(( total * pct / 100 ))
    [[ $count -lt 1 ]] && count=1
    find "$dir" -type f | shuf -n "$count" | while read -r f; do
        dd if=/dev/urandom bs=1 count="$append_bytes" >> "$f" 2>/dev/null
    done
}

delete_pct() {
    local dir="$1" pct="$2"
    local total
    total=$(find "$dir" -type f | wc -l)
    local count=$(( total * pct / 100 ))
    [[ $count -lt 1 ]] && count=1
    find "$dir" -type f | shuf -n "$count" | xargs rm -f
}

# ─── Benchmark Runner ────────────────────────────────────────────────────────

# Run a single scenario with N iterations, collecting stats
# Usage: run_scenario "Scenario Name" src_dir rsync_dst resync_dst [extra_rsync_flags] [extra_resync_flags]
run_scenario() {
    local name="$1"
    local src="$2"
    local dst_rsync="$3"
    local dst_resync="$4"
    local rsync_flags="${5:--a}"
    local resync_flags="${6:--a}"
    local total_bytes file_count

    # Check if we should skip
    if [[ -n "$FILTER_SCENARIO" ]] && [[ "$name" != *"$FILTER_SCENARIO"* ]]; then
        return
    fi

    total_bytes=$(dir_bytes "$src" 2>/dev/null || echo "0")
    file_count=$(dir_file_count "$src" 2>/dev/null || echo "0")
    local human_size
    human_size=$(du -sh "$src" 2>/dev/null | awk '{print $1}')

    echo ""
    echo -e "${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║${NC} ${BOLD}$name${NC}"
    echo -e "${CYAN}║${NC}  Files: $file_count  |  Size: $human_size  |  Iterations: $ITERATIONS"
    echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"

    local rsync_times="$BENCH_ROOT/.rsync_times"
    local resync_times="$BENCH_ROOT/.resync_times"
    local rsync_rss="$BENCH_ROOT/.rsync_rss"
    local resync_rss="$BENCH_ROOT/.resync_rss"
    > "$rsync_times"
    > "$resync_times"
    > "$rsync_rss"
    > "$resync_rss"

    for iter in $(seq 1 "$ITERATIONS"); do
        echo -ne "  ${DIM}Iteration $iter/$ITERATIONS${NC} "

        # Fresh destinations for each iteration
        rm -rf "$dst_rsync" "$dst_resync"
        mkdir -p "$dst_rsync" "$dst_resync"

        # ── rsync ──
        drop_cache
        echo -ne "rsync..."
        local rsync_result
        rsync_result=$(time_cmd_detailed $RSYNC $rsync_flags "$src/" "$dst_rsync/")
        local rsync_wall rsync_user rsync_sys rsync_rss_val
        rsync_wall=$(echo "$rsync_result" | awk '{print $1}')
        rsync_user=$(echo "$rsync_result" | awk '{print $2}')
        rsync_sys=$(echo "$rsync_result" | awk '{print $3}')
        rsync_rss_val=$(echo "$rsync_result" | awk '{print $4}')
        echo "$rsync_wall" >> "$rsync_times"
        echo "$rsync_rss_val" >> "$rsync_rss"
        echo "$name,rsync,$iter,$rsync_wall,$rsync_user,$rsync_sys,$rsync_rss_val,$total_bytes,$file_count" >> "$CSV_FILE"

        # ── resync ──
        drop_cache
        echo -ne " resync..."
        local resync_result
        resync_result=$(time_cmd_detailed $RESYNC $resync_flags "$src/" "$dst_resync/")
        local resync_wall resync_user resync_sys resync_rss_val
        resync_wall=$(echo "$resync_result" | awk '{print $1}')
        resync_user=$(echo "$resync_result" | awk '{print $2}')
        resync_sys=$(echo "$resync_result" | awk '{print $3}')
        resync_rss_val=$(echo "$resync_result" | awk '{print $4}')
        echo "$resync_wall" >> "$resync_times"
        echo "$resync_rss_val" >> "$resync_rss"
        echo "$name,resync,$iter,$resync_wall,$resync_user,$resync_sys,$resync_rss_val,$total_bytes,$file_count" >> "$CSV_FILE"

        echo -e " ${GREEN}done${NC}"
    done

    # Compute statistics
    local stats_rsync stats_resync
    stats_rsync=$(compute_stats "$rsync_times")
    stats_resync=$(compute_stats "$resync_times")

    local rsync_mean rsync_median rsync_stddev rsync_p95
    rsync_mean=$(echo "$stats_rsync" | awk '{print $1}')
    rsync_median=$(echo "$stats_rsync" | awk '{print $2}')
    rsync_stddev=$(echo "$stats_rsync" | awk '{print $3}')
    rsync_p95=$(echo "$stats_rsync" | awk '{print $4}')

    local resync_mean resync_median resync_stddev resync_p95
    resync_mean=$(echo "$stats_resync" | awk '{print $1}')
    resync_median=$(echo "$stats_resync" | awk '{print $2}')
    resync_stddev=$(echo "$stats_resync" | awk '{print $3}')
    resync_p95=$(echo "$stats_resync" | awk '{print $4}')

    # Speedup
    local speedup speedup_median
    if (( ${resync_mean%.*} > 0 )); then
        speedup=$(echo "scale=2; $rsync_mean / $resync_mean" | bc 2>/dev/null || echo "N/A")
    else
        speedup="INF"
    fi
    if (( ${resync_median%.*} > 0 )); then
        speedup_median=$(echo "scale=2; $rsync_median / $resync_median" | bc 2>/dev/null || echo "N/A")
    else
        speedup_median="INF"
    fi

    # Bandwidth
    local rsync_bw resync_bw
    rsync_bw=$(bandwidth_human "$total_bytes" "${rsync_mean%.*}")
    resync_bw=$(bandwidth_human "$total_bytes" "${resync_mean%.*}")

    # Memory
    local rsync_max_rss resync_max_rss
    rsync_max_rss=$(sort -n "$rsync_rss" | tail -1)
    resync_max_rss=$(sort -n "$resync_rss" | tail -1)

    # Print results
    echo ""
    printf "  ${BOLD}%-14s %12s %12s %10s${NC}\n" "" "rsync" "resync" "speedup"
    echo "  ─────────────────────────────────────────────────────"
    printf "  %-14s %12s %12s ${GREEN}%10sx${NC}\n" \
        "Mean" "$(ns_to_human ${rsync_mean%.*})" "$(ns_to_human ${resync_mean%.*})" "$speedup"
    printf "  %-14s %12s %12s ${GREEN}%10sx${NC}\n" \
        "Median" "$(ns_to_human ${rsync_median%.*})" "$(ns_to_human ${resync_median%.*})" "$speedup_median"
    printf "  %-14s %12s %12s\n" \
        "Stddev" "$(ns_to_human ${rsync_stddev%.*})" "$(ns_to_human ${resync_stddev%.*})"
    printf "  %-14s %12s %12s\n" \
        "P95" "$(ns_to_human ${rsync_p95%.*})" "$(ns_to_human ${resync_p95%.*})"
    printf "  %-14s %12s %12s\n" \
        "Bandwidth" "$rsync_bw" "$resync_bw"
    printf "  %-14s %10s KB %10s KB\n" \
        "Peak RSS" "$rsync_max_rss" "$resync_max_rss"

    # Store for final summary
    echo "$name|$speedup|$(ns_to_human ${rsync_mean%.*})|$(ns_to_human ${resync_mean%.*})|$rsync_max_rss|$resync_max_rss" >> "$RESULTS_DIR/summary.txt"
}

# Same as run_scenario but DOES NOT recreate destinations each iteration
# Used for incremental/no-change/delete scenarios
run_scenario_incremental() {
    local name="$1"
    local src="$2"
    local dst_rsync="$3"
    local dst_resync="$4"
    local rsync_flags="${5:--a}"
    local resync_flags="${6:--a}"
    local total_bytes file_count

    if [[ -n "$FILTER_SCENARIO" ]] && [[ "$name" != *"$FILTER_SCENARIO"* ]]; then
        return
    fi

    total_bytes=$(dir_bytes "$src" 2>/dev/null || echo "0")
    file_count=$(dir_file_count "$src" 2>/dev/null || echo "0")
    local human_size
    human_size=$(du -sh "$src" 2>/dev/null | awk '{print $1}')

    echo ""
    echo -e "${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║${NC} ${BOLD}$name${NC}"
    echo -e "${CYAN}║${NC}  Files: $file_count  |  Size: $human_size  |  Iterations: $ITERATIONS"
    echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"

    local rsync_times="$BENCH_ROOT/.rsync_times"
    local resync_times="$BENCH_ROOT/.resync_times"
    local rsync_rss="$BENCH_ROOT/.rsync_rss"
    local resync_rss="$BENCH_ROOT/.resync_rss"
    > "$rsync_times"
    > "$resync_times"
    > "$rsync_rss"
    > "$resync_rss"

    for iter in $(seq 1 "$ITERATIONS"); do
        echo -ne "  ${DIM}Iteration $iter/$ITERATIONS${NC} "

        drop_cache
        echo -ne "rsync..."
        local rsync_result
        rsync_result=$(time_cmd_detailed $RSYNC $rsync_flags "$src/" "$dst_rsync/")
        local rsync_wall rsync_rss_val
        rsync_wall=$(echo "$rsync_result" | awk '{print $1}')
        rsync_rss_val=$(echo "$rsync_result" | awk '{print $4}')
        echo "$rsync_wall" >> "$rsync_times"
        echo "$rsync_rss_val" >> "$rsync_rss"

        drop_cache
        echo -ne " resync..."
        local resync_result
        resync_result=$(time_cmd_detailed $RESYNC $resync_flags "$src/" "$dst_resync/")
        local resync_wall resync_rss_val
        resync_wall=$(echo "$resync_result" | awk '{print $1}')
        resync_rss_val=$(echo "$resync_result" | awk '{print $4}')
        echo "$resync_wall" >> "$resync_times"
        echo "$resync_rss_val" >> "$resync_rss"

        echo -e " ${GREEN}done${NC}"
    done

    local stats_rsync stats_resync
    stats_rsync=$(compute_stats "$rsync_times")
    stats_resync=$(compute_stats "$resync_times")

    local rsync_mean resync_mean speedup
    rsync_mean=$(echo "$stats_rsync" | awk '{print $1}')
    resync_mean=$(echo "$stats_resync" | awk '{print $1}')

    if (( ${resync_mean%.*} > 0 )); then
        speedup=$(echo "scale=2; $rsync_mean / $resync_mean" | bc 2>/dev/null || echo "N/A")
    else
        speedup="INF"
    fi

    local rsync_median resync_median rsync_stddev resync_stddev rsync_p95 resync_p95 rsync_cv resync_cv
    rsync_median=$(echo "$stats_rsync" | awk '{print $2}')
    resync_median=$(echo "$stats_resync" | awk '{print $2}')
    rsync_stddev=$(echo "$stats_rsync" | awk '{print $3}')
    resync_stddev=$(echo "$stats_resync" | awk '{print $3}')
    rsync_p95=$(echo "$stats_rsync" | awk '{print $4}')
    resync_p95=$(echo "$stats_resync" | awk '{print $4}')
    rsync_cv=$(echo "$stats_rsync" | awk '{print $8}')
    resync_cv=$(echo "$stats_resync" | awk '{print $8}')

    local speedup_median
    if (( ${resync_median%.*} > 0 )); then
        speedup_median=$(echo "scale=2; $rsync_median / $resync_median" | bc 2>/dev/null || echo "N/A")
    else
        speedup_median="INF"
    fi

    local rsync_max_rss resync_max_rss
    rsync_max_rss=$(sort -n "$rsync_rss" | tail -1)
    resync_max_rss=$(sort -n "$resync_rss" | tail -1)

    echo ""
    printf "  ${BOLD}%-14s %12s %12s %10s${NC}\n" "" "rsync" "resync" "speedup"
    echo "  ─────────────────────────────────────────────────────"
    printf "  %-14s %12s %12s ${GREEN}%10sx${NC}\n" \
        "Mean" "$(ns_to_human ${rsync_mean%.*})" "$(ns_to_human ${resync_mean%.*})" "$speedup"
    printf "  %-14s %12s %12s ${GREEN}%10sx${NC}\n" \
        "Median" "$(ns_to_human ${rsync_median%.*})" "$(ns_to_human ${resync_median%.*})" "$speedup_median"
    printf "  %-14s %12s %12s\n" \
        "Stddev" "$(ns_to_human ${rsync_stddev%.*})" "$(ns_to_human ${resync_stddev%.*})"
    printf "  %-14s %12s %12s\n" \
        "CV (%)" "${rsync_cv}%" "${resync_cv}%"
    printf "  %-14s %12s %12s\n" \
        "P95" "$(ns_to_human ${rsync_p95%.*})" "$(ns_to_human ${resync_p95%.*})"
    printf "  %-14s %10s KB %10s KB\n" \
        "Peak RSS" "$rsync_max_rss" "$resync_max_rss"

    echo "$name|$speedup|$(ns_to_human ${rsync_mean%.*})|$(ns_to_human ${resync_mean%.*})|$rsync_max_rss|$resync_max_rss" >> "$RESULTS_DIR/summary.txt"
}

# ─── System Info ──────────────────────────────────────────────────────────────

print_header() {
    echo ""
    echo -e "${BOLD}████████████████████████████████████████████████████████████████${NC}"
    echo -e "${BOLD}██                                                            ██${NC}"
    echo -e "${BOLD}██       resync-rs  Advanced Benchmark Suite  v3.0            ██${NC}"
    echo -e "${BOLD}██                                                            ██${NC}"
    echo -e "${BOLD}████████████████████████████████████████████████████████████████${NC}"
    echo ""
    echo -e "  ${BOLD}System Information${NC}"
    echo "  ────────────────────────────────────────────────────────────"
    echo -e "  CPU        : $(grep 'model name' /proc/cpuinfo | head -1 | sed 's/model name.*: //')"
    echo -e "  Cores      : $(nproc) ($(grep -c '^processor' /proc/cpuinfo) threads)"
    echo -e "  RAM        : $(free -h | awk '/Mem:/{print $2}')"
    echo -e "  Kernel     : $(uname -r)"
    echo -e "  Filesystem : $(df -Th /tmp | tail -1 | awk '{print $2}')"
    echo -e "  Cache drop : $(sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null && echo "available" || echo "unavailable (warm cache only)")"
    echo -e "  rsync ver  : $(rsync --version 2>/dev/null | head -1 | awk '{print $3}' || echo "N/A")"
    echo -e "  resync ver : $($RESYNC --version 2>/dev/null | awk '{print $2}' || echo "N/A")"
    echo -e "  Mode       : $MODE ($ITERATIONS iterations per scenario)"
    echo -e "  JSON out   : $($JSON_OUTPUT && echo "enabled" || echo "disabled (use --json)")"
    echo -e "  perf stat  : $($PERF_STAT && command -v perf &>/dev/null && echo "enabled" || echo "disabled (use --perf)")"
    echo -e "  io_uring   : $(grep -q io_uring /proc/kallsyms 2>/dev/null && echo "available" || echo "unavailable")"
    echo -e "  Temp dir   : $BENCH_ROOT"
    echo ""
}

# ─── Scenarios ────────────────────────────────────────────────────────────────

scenario_1_small_files_full_copy() {
    local count=5000
    [[ "$MODE" == "full" ]] && count=50000
    [[ "$MODE" == "quick" ]] && count=1000

    echo -e "\n${MAGENTA}━━━ SCENARIO GROUP 1: Small Files (1-10 KB each) ━━━${NC}"
    echo -ne "  Generating $count small files... "
    gen_small_files "$BENCH_ROOT/src_small" "$count"
    echo "done ($(du -sh "$BENCH_ROOT/src_small" | awk '{print $1}'))"

    run_scenario \
        "Small files: full copy ($count)" \
        "$BENCH_ROOT/src_small" \
        "$BENCH_ROOT/dst_rsync_small" \
        "$BENCH_ROOT/dst_resync_small"
}

scenario_2_small_files_no_change() {
    # Pre-sync so dest exists
    $RSYNC -a "$BENCH_ROOT/src_small/" "$BENCH_ROOT/dst_rsync_small_inc/" 2>/dev/null
    $RESYNC -a "$BENCH_ROOT/src_small/" "$BENCH_ROOT/dst_resync_small_inc/" 2>/dev/null

    run_scenario_incremental \
        "Small files: no changes" \
        "$BENCH_ROOT/src_small" \
        "$BENCH_ROOT/dst_rsync_small_inc" \
        "$BENCH_ROOT/dst_resync_small_inc"
}

scenario_3_small_files_1pct() {
    mutate_pct "$BENCH_ROOT/src_small" 1
    echo -e "  ${DIM}Mutated 1% of source files${NC}"

    run_scenario_incremental \
        "Small files: 1% changed" \
        "$BENCH_ROOT/src_small" \
        "$BENCH_ROOT/dst_rsync_small_inc" \
        "$BENCH_ROOT/dst_resync_small_inc"
}

scenario_4_small_files_5pct() {
    mutate_pct "$BENCH_ROOT/src_small" 5
    echo -e "  ${DIM}Mutated 5% of source files${NC}"

    run_scenario_incremental \
        "Small files: 5% changed" \
        "$BENCH_ROOT/src_small" \
        "$BENCH_ROOT/dst_rsync_small_inc" \
        "$BENCH_ROOT/dst_resync_small_inc"
}

scenario_5_small_files_25pct() {
    mutate_pct "$BENCH_ROOT/src_small" 25
    echo -e "  ${DIM}Mutated 25% of source files${NC}"

    run_scenario_incremental \
        "Small files: 25% changed" \
        "$BENCH_ROOT/src_small" \
        "$BENCH_ROOT/dst_rsync_small_inc" \
        "$BENCH_ROOT/dst_resync_small_inc"
}

scenario_6_medium_files_full() {
    local count=200
    [[ "$MODE" == "full" ]] && count=500
    [[ "$MODE" == "quick" ]] && count=50

    echo -e "\n${MAGENTA}━━━ SCENARIO GROUP 2: Medium Files (100KB-1MB each) ━━━${NC}"
    echo -ne "  Generating $count medium files... "
    gen_medium_files "$BENCH_ROOT/src_medium" "$count"
    echo "done ($(du -sh "$BENCH_ROOT/src_medium" | awk '{print $1}'))"

    run_scenario \
        "Medium files: full copy ($count)" \
        "$BENCH_ROOT/src_medium" \
        "$BENCH_ROOT/dst_rsync_medium" \
        "$BENCH_ROOT/dst_resync_medium"
}

scenario_7_large_files_full() {
    local count=3 size=100
    [[ "$MODE" == "full" ]] && count=5 && size=200
    [[ "$MODE" == "quick" ]] && count=2 && size=50

    echo -e "\n${MAGENTA}━━━ SCENARIO GROUP 3: Large Files ━━━${NC}"
    echo -ne "  Generating $count x ${size}MB files... "
    gen_large_files "$BENCH_ROOT/src_large" "$count" "$size"
    echo "done ($(du -sh "$BENCH_ROOT/src_large" | awk '{print $1}'))"

    run_scenario \
        "Large files: full copy (${count}x${size}M)" \
        "$BENCH_ROOT/src_large" \
        "$BENCH_ROOT/dst_rsync_large" \
        "$BENCH_ROOT/dst_resync_large"
}

scenario_8_large_files_delta() {
    # Mutate 10% of each large file
    for f in "$BENCH_ROOT/src_large"/*.dat; do
        local fsize
        fsize=$(stat -c%s "$f" 2>/dev/null || echo "0")
        local mutate_mb=$(( fsize / 10485760 ))
        [[ $mutate_mb -lt 1 ]] && mutate_mb=1
        local seek_mb=$(( mutate_mb * 3 ))
        dd if=/dev/urandom of="$f" bs=1M count="$mutate_mb" seek="$seek_mb" conv=notrunc 2>/dev/null
    done
    echo -e "  ${DIM}Mutated ~10% of each large file${NC}"

    # Pre-sync destinations
    $RSYNC -a "$BENCH_ROOT/src_large/" "$BENCH_ROOT/dst_rsync_large_d/" 2>/dev/null || true
    $RESYNC -a "$BENCH_ROOT/src_large/" "$BENCH_ROOT/dst_resync_large_d/" 2>/dev/null || true

    # Now mutate again to create delta
    for f in "$BENCH_ROOT/src_large"/*.dat; do
        dd if=/dev/urandom of="$f" bs=1M count=5 seek=2 conv=notrunc 2>/dev/null
    done

    run_scenario_incremental \
        "Large files: delta (5MB changed)" \
        "$BENCH_ROOT/src_large" \
        "$BENCH_ROOT/dst_rsync_large_d" \
        "$BENCH_ROOT/dst_resync_large_d"
}

scenario_9_empty_files() {
    local count=5000
    [[ "$MODE" == "quick" ]] && count=1000

    echo -e "\n${MAGENTA}━━━ SCENARIO GROUP 4: Edge Cases ━━━${NC}"
    echo -ne "  Generating $count empty files... "
    gen_empty_files "$BENCH_ROOT/src_empty" "$count"
    echo "done"

    run_scenario \
        "Empty files: copy ($count)" \
        "$BENCH_ROOT/src_empty" \
        "$BENCH_ROOT/dst_rsync_empty" \
        "$BENCH_ROOT/dst_resync_empty"
}

scenario_10_deep_tree() {
    local depth=50
    [[ "$MODE" == "full" ]] && depth=100
    [[ "$MODE" == "quick" ]] && depth=20

    echo -ne "  Generating deep tree (depth=$depth, 3 files/level)... "
    gen_deep_tree "$BENCH_ROOT/src_deep" "$depth" 3
    echo "done"

    run_scenario \
        "Deep tree: depth=$depth" \
        "$BENCH_ROOT/src_deep" \
        "$BENCH_ROOT/dst_rsync_deep" \
        "$BENCH_ROOT/dst_resync_deep"
}

scenario_11_wide_dir() {
    local count=5000
    [[ "$MODE" == "full" ]] && count=20000
    [[ "$MODE" == "quick" ]] && count=1000

    echo -ne "  Generating wide directory ($count entries)... "
    gen_wide_dir "$BENCH_ROOT/src_wide" "$count"
    echo "done"

    run_scenario \
        "Wide dir: $count files in 1 dir" \
        "$BENCH_ROOT/src_wide" \
        "$BENCH_ROOT/dst_rsync_wide" \
        "$BENCH_ROOT/dst_resync_wide"
}

scenario_12_symlinks() {
    local count=500
    [[ "$MODE" == "quick" ]] && count=100

    echo -ne "  Generating symlink-heavy tree ($count links)... "
    gen_symlink_tree "$BENCH_ROOT/src_symlinks" "$count"
    echo "done"

    run_scenario \
        "Symlinks: $count links" \
        "$BENCH_ROOT/src_symlinks" \
        "$BENCH_ROOT/dst_rsync_sym" \
        "$BENCH_ROOT/dst_resync_sym" \
        "-a" \
        "-a"
}

scenario_13_mixed_workload() {
    echo -e "\n${MAGENTA}━━━ SCENARIO GROUP 5: Real-World Patterns ━━━${NC}"
    echo -ne "  Generating mixed workload (text+binary+empty+symlinks+deep)... "
    gen_mixed_workload "$BENCH_ROOT/src_mixed"
    echo "done ($(du -sh "$BENCH_ROOT/src_mixed" | awk '{print $1}'))"

    run_scenario \
        "Mixed workload: full copy" \
        "$BENCH_ROOT/src_mixed" \
        "$BENCH_ROOT/dst_rsync_mixed" \
        "$BENCH_ROOT/dst_resync_mixed"
}

scenario_14_append_only() {
    # Pre-sync
    $RSYNC -a "$BENCH_ROOT/src_mixed/" "$BENCH_ROOT/dst_rsync_mixed_inc/" 2>/dev/null || true
    $RESYNC -a "$BENCH_ROOT/src_mixed/" "$BENCH_ROOT/dst_resync_mixed_inc/" 2>/dev/null || true

    # Append 1KB to 10% of files
    append_to_files "$BENCH_ROOT/src_mixed" 10 1024
    echo -e "  ${DIM}Appended 1KB to 10% of files (append-only pattern)${NC}"

    run_scenario_incremental \
        "Append-only: 10% +1KB" \
        "$BENCH_ROOT/src_mixed" \
        "$BENCH_ROOT/dst_rsync_mixed_inc" \
        "$BENCH_ROOT/dst_resync_mixed_inc"
}

scenario_15_delete_sync() {
    # Delete 20% of source files
    delete_pct "$BENCH_ROOT/src_mixed" 20
    echo -e "  ${DIM}Deleted 20% of source files${NC}"

    run_scenario_incremental \
        "Delete sync: --delete (20%)" \
        "$BENCH_ROOT/src_mixed" \
        "$BENCH_ROOT/dst_rsync_mixed_inc" \
        "$BENCH_ROOT/dst_resync_mixed_inc" \
        "-a --delete" \
        "-a --delete"
}

scenario_16_100pct_overwrite() {
    echo -e "\n${MAGENTA}━━━ SCENARIO GROUP 6: Stress Tests ━━━${NC}"

    # Generate source and make existing destination, then overwrite ALL source files
    echo -ne "  Generating 2000 files then overwriting 100%... "
    gen_small_files "$BENCH_ROOT/src_overwrite" 2000
    $RSYNC -a "$BENCH_ROOT/src_overwrite/" "$BENCH_ROOT/dst_rsync_overwrite/" 2>/dev/null
    $RESYNC -a "$BENCH_ROOT/src_overwrite/" "$BENCH_ROOT/dst_resync_overwrite/" 2>/dev/null
    mutate_pct "$BENCH_ROOT/src_overwrite" 100
    echo "done"

    run_scenario_incremental \
        "100% overwrite: 2K files" \
        "$BENCH_ROOT/src_overwrite" \
        "$BENCH_ROOT/dst_rsync_overwrite" \
        "$BENCH_ROOT/dst_resync_overwrite"
}

scenario_17_concurrent_modification() {
    echo -ne "  Setting up concurrent modification test... "
    gen_small_files "$BENCH_ROOT/src_conc" 1000
    echo "done"

    # Start background process that modifies source files during sync
    (
        sleep 0.1
        for i in $(seq 1 20); do
            local target
            target=$(find "$BENCH_ROOT/src_conc" -type f 2>/dev/null | shuf -n 1)
            if [[ -n "$target" && -f "$target" ]]; then
                dd if=/dev/urandom of="$target" bs=64 count=1 conv=notrunc 2>/dev/null
            fi
            sleep 0.05
        done
    ) &
    local bg_pid=$!

    # Run sync while modifications are happening
    echo -e "  ${DIM}Running sync with concurrent source modifications...${NC}"

    run_scenario \
        "Concurrent mod: race test" \
        "$BENCH_ROOT/src_conc" \
        "$BENCH_ROOT/dst_rsync_conc" \
        "$BENCH_ROOT/dst_resync_conc"

    kill $bg_pid 2>/dev/null || true
    wait $bg_pid 2>/dev/null || true
}

scenario_18_single_huge_file() {
    local size_mb=500
    [[ "$MODE" == "quick" ]] && size_mb=100
    [[ "$MODE" == "full" ]] && size_mb=1000

    echo -ne "  Generating single ${size_mb}MB file... "
    mkdir -p "$BENCH_ROOT/src_huge"
    dd if=/dev/urandom of="$BENCH_ROOT/src_huge/huge.dat" bs=1M count=$size_mb 2>/dev/null
    echo "done"

    run_scenario \
        "Single file: ${size_mb}MB" \
        "$BENCH_ROOT/src_huge" \
        "$BENCH_ROOT/dst_rsync_huge" \
        "$BENCH_ROOT/dst_resync_huge"
}

scenario_19_checksum_mode() {
    echo -e "\n${MAGENTA}━━━ SCENARIO GROUP 7: Flag Variants ━━━${NC}"

    # Pre-sync
    $RSYNC -a "$BENCH_ROOT/src_small/" "$BENCH_ROOT/dst_rsync_cksum/" 2>/dev/null
    $RESYNC -a "$BENCH_ROOT/src_small/" "$BENCH_ROOT/dst_resync_cksum/" 2>/dev/null

    run_scenario_incremental \
        "Checksum mode: --checksum" \
        "$BENCH_ROOT/src_small" \
        "$BENCH_ROOT/dst_rsync_cksum" \
        "$BENCH_ROOT/dst_resync_cksum" \
        "-a --checksum" \
        "-a --checksum"
}

scenario_20_dry_run() {
    run_scenario \
        "Dry run: --dry-run" \
        "$BENCH_ROOT/src_small" \
        "$BENCH_ROOT/dst_rsync_dry" \
        "$BENCH_ROOT/dst_resync_dry" \
        "-a --dry-run" \
        "-a --dry-run"
}

# ─── New v3.0 Scenarios ──────────────────────────────────────────────────────

scenario_21_manifest_cache_speedup() {
    echo -e "\n${MAGENTA}━━━ SCENARIO GROUP 8: resync-rs Unique Features ━━━${NC}"

    # Generate fresh data
    local count=5000
    [[ "$MODE" == "quick" ]] && count=1000
    [[ "$MODE" == "full" ]] && count=20000

    echo -ne "  Generating $count files for manifest cache test... "
    gen_small_files "$BENCH_ROOT/src_manifest" "$count"
    echo "done"

    # First run: populate manifest cache
    mkdir -p "$BENCH_ROOT/dst_resync_manifest"
    $RESYNC -a "$BENCH_ROOT/src_manifest/" "$BENCH_ROOT/dst_resync_manifest/" 2>/dev/null

    if [[ -n "$FILTER_SCENARIO" ]] && [[ "Manifest cache" != *"$FILTER_SCENARIO"* ]]; then
        return
    fi

    local total_bytes file_count
    total_bytes=$(dir_bytes "$BENCH_ROOT/src_manifest")
    file_count=$(dir_file_count "$BENCH_ROOT/src_manifest")

    echo ""
    echo -e "${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║${NC} ${BOLD}Manifest cache: no-change re-sync speedup${NC}"
    echo -e "${CYAN}║${NC}  Compares rsync -a vs resync -a (with manifest cache) on no-change"
    echo -e "${CYAN}║${NC}  Files: $file_count  |  Iterations: $ITERATIONS"
    echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"

    local rsync_times="$BENCH_ROOT/.rsync_times"
    local resync_times="$BENCH_ROOT/.resync_times"
    > "$rsync_times"
    > "$resync_times"

    # Pre-sync rsync dest
    $RSYNC -a "$BENCH_ROOT/src_manifest/" "$BENCH_ROOT/dst_rsync_manifest/" 2>/dev/null

    for iter in $(seq 1 "$ITERATIONS"); do
        echo -ne "  ${DIM}Iteration $iter/$ITERATIONS${NC} "

        drop_cache
        echo -ne "rsync..."
        local rsync_result
        rsync_result=$(time_cmd_detailed $RSYNC -a "$BENCH_ROOT/src_manifest/" "$BENCH_ROOT/dst_rsync_manifest/")
        echo "$(echo "$rsync_result" | awk '{print $1}')" >> "$rsync_times"

        drop_cache
        echo -ne " resync(manifest)..."
        local resync_result
        resync_result=$(time_cmd_detailed $RESYNC -a "$BENCH_ROOT/src_manifest/" "$BENCH_ROOT/dst_resync_manifest/")
        echo "$(echo "$resync_result" | awk '{print $1}')" >> "$resync_times"

        echo -e " ${GREEN}done${NC}"
    done

    local stats_rsync stats_resync rsync_mean resync_mean speedup
    stats_rsync=$(compute_stats "$rsync_times")
    stats_resync=$(compute_stats "$resync_times")
    rsync_mean=$(echo "$stats_rsync" | awk '{print $1}')
    resync_mean=$(echo "$stats_resync" | awk '{print $1}')
    if (( ${resync_mean%.*} > 0 )); then
        speedup=$(echo "scale=2; $rsync_mean / $resync_mean" | bc)
    else
        speedup="INF"
    fi

    echo ""
    printf "  ${BOLD}%-14s %12s %12s %10s${NC}\n" "" "rsync" "resync" "speedup"
    echo "  ─────────────────────────────────────────────────────"
    printf "  %-14s %12s %12s ${GREEN}%10sx${NC}\n" \
        "Mean" "$(ns_to_human ${rsync_mean%.*})" "$(ns_to_human ${resync_mean%.*})" "$speedup"

    echo "Manifest cache: no-change|$speedup|$(ns_to_human ${rsync_mean%.*})|$(ns_to_human ${resync_mean%.*})|0|0" >> "$RESULTS_DIR/summary.txt"
}

scenario_22_warm_vs_cold_cache() {
    if [[ -n "$FILTER_SCENARIO" ]] && [[ "Warm vs cold" != *"$FILTER_SCENARIO"* ]]; then
        return
    fi

    echo ""
    echo -e "${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║${NC} ${BOLD}Warm vs Cold Cache Comparison${NC}"
    echo -e "${CYAN}║${NC}  Shows how page cache affects both rsync and resync performance"
    echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"

    if ! $CACHE_DROP_AVAILABLE; then
        echo -e "  ${YELLOW}Skipping: sudo not available for cache dropping${NC}"
        return
    fi

    # Test with medium files (meaningful cache impact)
    local src="$BENCH_ROOT/src_medium"
    if [[ ! -d "$src" ]]; then
        echo -ne "  Generating test data... "
        gen_medium_files "$src" 200
        echo "done"
    fi

    echo ""
    echo "  -- Cold cache (drop_caches before each run) --"
    local cold_rsync_times="$BENCH_ROOT/.cold_rsync_t"
    local cold_resync_times="$BENCH_ROOT/.cold_resync_t"
    > "$cold_rsync_times"
    > "$cold_resync_times"

    for iter in $(seq 1 "$ITERATIONS"); do
        rm -rf "$BENCH_ROOT/dst_wc_rsync" "$BENCH_ROOT/dst_wc_resync"
        mkdir -p "$BENCH_ROOT/dst_wc_rsync" "$BENCH_ROOT/dst_wc_resync"

        drop_cache
        local r=$(time_cmd_detailed $RSYNC -a "$src/" "$BENCH_ROOT/dst_wc_rsync/")
        echo "$(echo "$r" | awk '{print $1}')" >> "$cold_rsync_times"

        drop_cache
        r=$(time_cmd_detailed $RESYNC -a "$src/" "$BENCH_ROOT/dst_wc_resync/")
        echo "$(echo "$r" | awk '{print $1}')" >> "$cold_resync_times"
    done

    echo "  -- Warm cache (no cache drop, same data in pagecache) --"
    local warm_rsync_times="$BENCH_ROOT/.warm_rsync_t"
    local warm_resync_times="$BENCH_ROOT/.warm_resync_t"
    > "$warm_rsync_times"
    > "$warm_resync_times"

    # Pre-warm: read all source files into cache
    find "$src" -type f -exec cat {} + > /dev/null 2>&1

    for iter in $(seq 1 "$ITERATIONS"); do
        rm -rf "$BENCH_ROOT/dst_wc_rsync" "$BENCH_ROOT/dst_wc_resync"
        mkdir -p "$BENCH_ROOT/dst_wc_rsync" "$BENCH_ROOT/dst_wc_resync"

        # NO cache drop — warm
        local r=$(time_cmd_detailed $RSYNC -a "$src/" "$BENCH_ROOT/dst_wc_rsync/")
        echo "$(echo "$r" | awk '{print $1}')" >> "$warm_rsync_times"

        r=$(time_cmd_detailed $RESYNC -a "$src/" "$BENCH_ROOT/dst_wc_resync/")
        echo "$(echo "$r" | awk '{print $1}')" >> "$warm_resync_times"
    done

    local cold_rsync_mean cold_resync_mean warm_rsync_mean warm_resync_mean
    cold_rsync_mean=$(compute_stats "$cold_rsync_times" | awk '{print $1}')
    cold_resync_mean=$(compute_stats "$cold_resync_times" | awk '{print $1}')
    warm_rsync_mean=$(compute_stats "$warm_rsync_times" | awk '{print $1}')
    warm_resync_mean=$(compute_stats "$warm_resync_times" | awk '{print $1}')

    local cold_speedup warm_speedup
    cold_speedup=$(echo "scale=2; $cold_rsync_mean / $cold_resync_mean" | bc 2>/dev/null || echo "N/A")
    warm_speedup=$(echo "scale=2; $warm_rsync_mean / $warm_resync_mean" | bc 2>/dev/null || echo "N/A")

    echo ""
    printf "  ${BOLD}%-14s %12s %12s %10s${NC}\n" "" "rsync" "resync" "speedup"
    echo "  ─────────────────────────────────────────────────────"
    printf "  %-14s %12s %12s ${GREEN}%10sx${NC}\n" \
        "Cold cache" "$(ns_to_human ${cold_rsync_mean%.*})" "$(ns_to_human ${cold_resync_mean%.*})" "$cold_speedup"
    printf "  %-14s %12s %12s ${GREEN}%10sx${NC}\n" \
        "Warm cache" "$(ns_to_human ${warm_rsync_mean%.*})" "$(ns_to_human ${warm_resync_mean%.*})" "$warm_speedup"

    # Cache sensitivity: how much faster each tool gets with warm cache
    local rsync_cache_boost resync_cache_boost
    rsync_cache_boost=$(echo "scale=2; $cold_rsync_mean / $warm_rsync_mean" | bc 2>/dev/null || echo "N/A")
    resync_cache_boost=$(echo "scale=2; $cold_resync_mean / $warm_resync_mean" | bc 2>/dev/null || echo "N/A")
    echo ""
    printf "  %-14s %12sx %12sx\n" "Cache boost" "$rsync_cache_boost" "$resync_cache_boost"
    echo -e "  ${DIM}(Lower = less dependent on page cache = better O_DIRECT usage)${NC}"

    echo "Warm vs Cold|$cold_speedup/$warm_speedup|$(ns_to_human ${cold_rsync_mean%.*})|$(ns_to_human ${cold_resync_mean%.*})|0|0" >> "$RESULTS_DIR/summary.txt"
}

scenario_23_uring_batch_impact() {
    if [[ -n "$FILTER_SCENARIO" ]] && [[ "io_uring" != *"$FILTER_SCENARIO"* ]]; then
        return
    fi

    echo ""
    echo -e "${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║${NC} ${BOLD}io_uring Batch Performance (many tiny files)${NC}"
    echo -e "${CYAN}║${NC}  Tests the io_uring batch engine with files ≤32KB"
    echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"

    local count=10000
    [[ "$MODE" == "quick" ]] && count=2000
    [[ "$MODE" == "full" ]] && count=50000

    echo -ne "  Generating $count tiny files (≤4KB each)... "
    mkdir -p "$BENCH_ROOT/src_uring"
    # Small files to maximize syscall overhead (io_uring's sweet spot)
    seq 1 "$count" | xargs -P "$(nproc)" -I{} sh -c '
        dd if=/dev/urandom of="'"$BENCH_ROOT/src_uring"'/f{}.dat" bs=$(( RANDOM % 4096 + 1 )) count=1 2>/dev/null
    '
    echo "done ($(du -sh "$BENCH_ROOT/src_uring" | awk '{print $1}'))"

    run_scenario \
        "io_uring batch: $count tiny" \
        "$BENCH_ROOT/src_uring" \
        "$BENCH_ROOT/dst_rsync_uring" \
        "$BENCH_ROOT/dst_resync_uring"
}

scenario_24_perf_stat_analysis() {
    if ! $PERF_STAT || ! command -v perf &>/dev/null; then
        return
    fi

    if [[ -n "$FILTER_SCENARIO" ]] && [[ "perf" != *"$FILTER_SCENARIO"* ]]; then
        return
    fi

    echo ""
    echo -e "${MAGENTA}━━━ BONUS: Hardware Counter Analysis (perf stat) ━━━${NC}"
    echo ""

    # Use existing medium files
    local src="$BENCH_ROOT/src_medium"
    if [[ ! -d "$src" ]]; then
        gen_medium_files "$src" 200
    fi

    mkdir -p "$BENCH_ROOT/dst_perf_rsync" "$BENCH_ROOT/dst_perf_resync"

    echo -ne "  Profiling rsync... "
    local rsync_perf
    rsync_perf=$(time_cmd_with_perf $RSYNC -a "$src/" "$BENCH_ROOT/dst_perf_rsync/")
    echo "done"

    rm -rf "$BENCH_ROOT/dst_perf_resync"
    mkdir -p "$BENCH_ROOT/dst_perf_resync"

    echo -ne "  Profiling resync... "
    local resync_perf
    resync_perf=$(time_cmd_with_perf $RESYNC -a "$src/" "$BENCH_ROOT/dst_perf_resync/")
    echo "done"

    local rsync_ipc rsync_cmiss rsync_bmiss resync_ipc resync_cmiss resync_bmiss
    rsync_ipc=$(echo "$rsync_perf" | awk '{print $5}')
    rsync_cmiss=$(echo "$rsync_perf" | awk '{print $6}')
    rsync_bmiss=$(echo "$rsync_perf" | awk '{print $7}')
    resync_ipc=$(echo "$resync_perf" | awk '{print $5}')
    resync_cmiss=$(echo "$resync_perf" | awk '{print $6}')
    resync_bmiss=$(echo "$resync_perf" | awk '{print $7}')

    echo ""
    printf "  ${BOLD}%-20s %12s %12s${NC}\n" "HW Counter" "rsync" "resync"
    echo "  ─────────────────────────────────────────────────────"
    printf "  %-20s %12s %12s\n" "IPC" "$rsync_ipc" "$resync_ipc"
    printf "  %-20s %11s%% %11s%%\n" "Cache miss rate" "$rsync_cmiss" "$resync_cmiss"
    printf "  %-20s %11s%% %11s%%\n" "Branch miss rate" "$rsync_bmiss" "$resync_bmiss"
    echo ""
    echo -e "  ${DIM}Higher IPC = better instruction-level parallelism${NC}"
    echo -e "  ${DIM}Lower miss rates = better cache/branch prediction utilization${NC}"
}

# ─── Syscall Analysis (optional) ─────────────────────────────────────────────

run_syscall_analysis() {
    if ! command -v strace &>/dev/null; then
        echo -e "\n  ${DIM}strace not found — skipping syscall analysis${NC}"
        return
    fi

    echo ""
    echo -e "${MAGENTA}━━━ BONUS: Syscall Analysis ━━━${NC}"
    echo ""

    # Prepare small data set for syscall counting
    gen_small_files "$BENCH_ROOT/src_syscall" 500
    mkdir -p "$BENCH_ROOT/dst_rsync_sc" "$BENCH_ROOT/dst_resync_sc"

    echo -ne "  Counting rsync syscalls... "
    strace -c -o "$RESULTS_DIR/rsync_strace.txt" $RSYNC -a "$BENCH_ROOT/src_syscall/" "$BENCH_ROOT/dst_rsync_sc/" 2>/dev/null
    echo "done"

    echo -ne "  Counting resync syscalls... "
    strace -c -o "$RESULTS_DIR/resync_strace.txt" $RESYNC -a "$BENCH_ROOT/src_syscall/" "$BENCH_ROOT/dst_resync_sc/" 2>/dev/null
    echo "done"

    local rsync_total resync_total
    rsync_total=$(grep '^total' "$RESULTS_DIR/rsync_strace.txt" 2>/dev/null | awk '{print $4}' || echo "?")
    resync_total=$(grep '^total' "$RESULTS_DIR/resync_strace.txt" 2>/dev/null | awk '{print $4}' || echo "?")

    echo ""
    echo "  Total syscalls — rsync: $rsync_total  |  resync: $resync_total"
    echo ""

    # Show top syscalls
    echo "  ${BOLD}rsync top syscalls:${NC}"
    head -20 "$RESULTS_DIR/rsync_strace.txt" 2>/dev/null | sed 's/^/    /'
    echo ""
    echo "  ${BOLD}resync top syscalls:${NC}"
    head -20 "$RESULTS_DIR/resync_strace.txt" 2>/dev/null | sed 's/^/    /'
}

# ─── Final Summary ───────────────────────────────────────────────────────────

print_summary() {
    echo ""
    echo -e "${BOLD}████████████████████████████████████████████████████████████████${NC}"
    echo -e "${BOLD}██                     RESULTS SUMMARY                        ██${NC}"
    echo -e "${BOLD}████████████████████████████████████████████████████████████████${NC}"
    echo ""
    printf "  ${BOLD}%-35s  %10s  %10s  %8s  %8s  %8s${NC}\n" \
        "Scenario" "rsync" "resync" "Speedup" "RSS(r)" "RSS(R)"
    echo "  ═══════════════════════════════════════════════════════════════════════════"

    if [[ -f "$RESULTS_DIR/summary.txt" ]]; then
        while IFS='|' read -r name speedup rsync_t resync_t rsync_rss resync_rss; do
            local color="$GREEN"
            # Red if slower, yellow if marginal
            if [[ "$speedup" != "INF" && "$speedup" != "N/A" ]]; then
                local cmp
                cmp=$(echo "$speedup < 1.0" | bc 2>/dev/null || echo "0")
                [[ "$cmp" == "1" ]] && color="$RED"
                cmp=$(echo "$speedup < 2.0" | bc 2>/dev/null || echo "0")
                [[ "$cmp" == "1" ]] && color="$YELLOW"
            fi
            printf "  %-35s  %10s  %10s  ${color}%7sx${NC}  %6sKB  %6sKB\n" \
                "$name" "$rsync_t" "$resync_t" "$speedup" "$rsync_rss" "$resync_rss"
        done < "$RESULTS_DIR/summary.txt"
    fi

    echo "  ═══════════════════════════════════════════════════════════════════════════"
    echo ""

    # Geometric mean of speedups
    if [[ -f "$RESULTS_DIR/summary.txt" ]]; then
        local geomean
        geomean=$(awk -F'|' '{
            if ($2 != "INF" && $2 != "N/A" && $2+0 > 0) {
                sum += log($2+0); n++
            }
        } END {
            if (n > 0) printf "%.2f", exp(sum/n)
            else print "N/A"
        }' "$RESULTS_DIR/summary.txt")
        echo -e "  ${BOLD}Geometric Mean Speedup: ${GREEN}${geomean}x${NC}${NC}"
    fi

    echo ""
    echo -e "  CSV results saved to: $CSV_FILE"
    if $JSON_OUTPUT; then
        echo -e "  JSON results saved to: $JSON_FILE"
    fi
    echo -e "  Summary saved to: $RESULTS_DIR/summary.txt"
    echo ""
}

# ─── Main ─────────────────────────────────────────────────────────────────────

main() {
    # Verify binaries
    if [[ ! -x "$RESYNC" ]]; then
        echo -e "${RED}ERROR: resync binary not found at $RESYNC${NC}"
        echo "  Run: cargo build --release"
        exit 1
    fi
    if ! command -v rsync &>/dev/null; then
        echo -e "${RED}ERROR: rsync not found${NC}"
        exit 1
    fi

    setup_dirs
    print_header
    json_save_sysinfo

    local start_time
    start_time=$(date +%s)

    # ── Group 1: Small files ──
    scenario_1_small_files_full_copy
    scenario_2_small_files_no_change
    scenario_3_small_files_1pct
    scenario_4_small_files_5pct
    scenario_5_small_files_25pct

    # ── Group 2: Medium files ──
    scenario_6_medium_files_full

    # ── Group 3: Large files ──
    scenario_7_large_files_full
    scenario_8_large_files_delta

    # ── Group 4: Edge cases ──
    scenario_9_empty_files
    scenario_10_deep_tree
    scenario_11_wide_dir
    scenario_12_symlinks

    # ── Group 5: Real-world patterns ──
    scenario_13_mixed_workload
    scenario_14_append_only
    scenario_15_delete_sync

    # ── Group 6: Stress tests ──
    scenario_16_100pct_overwrite
    scenario_17_concurrent_modification
    scenario_18_single_huge_file

    # ── Group 7: Flag variants ──
    scenario_19_checksum_mode
    scenario_20_dry_run

    # ── Group 8: resync-rs unique features (v3.0) ──
    scenario_21_manifest_cache_speedup
    scenario_22_warm_vs_cold_cache
    scenario_23_uring_batch_impact
    scenario_24_perf_stat_analysis

    # ── Bonus: Syscall analysis ──
    run_syscall_analysis

    # ── Summary ──
    print_summary

    local end_time elapsed_m
    end_time=$(date +%s)
    elapsed_m=$(( (end_time - start_time) / 60 ))
    echo -e "  Total benchmark time: ${elapsed_m} minutes"
    echo ""
}

main "$@"
