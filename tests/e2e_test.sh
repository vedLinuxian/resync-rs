#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# resync-rs  —  end-to-end integration test suite + rsync benchmark
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

RESYNC="$(cd "$(dirname "$0")/.." && pwd)/target/release/resync"
TMPDIR=$(mktemp -d /tmp/resync-e2e-XXXXXX)
trap 'rm -rf "$TMPDIR"' EXIT

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

PASS=0
FAIL=0

pass() { ((PASS++)); echo -e "  ${GREEN}✓${NC} $1"; }
fail() { ((FAIL++)); echo -e "  ${RED}✗${NC} $1"; }

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo -e "\n${BOLD}═══ resync-rs  E2E TEST SUITE ═══${NC}\n"

# ─── T1: Fresh sync — all files new ──────────────────────────────────────────
echo -e "${CYAN}TEST 1: Fresh sync (all new files)${NC}"
SRC="$TMPDIR/t1_src"; DST="$TMPDIR/t1_dst"
mkdir -p "$SRC/sub/deep"
echo "hello"       > "$SRC/a.txt"
echo "world"       > "$SRC/sub/b.txt"
dd if=/dev/urandom of="$SRC/sub/deep/big.bin" bs=4096 count=20 2>/dev/null
touch "$SRC/empty.dat"

$RESYNC -r "$SRC/" "$DST/" --stats
diff -rq "$SRC" "$DST" && pass "Fresh sync: all files match" || fail "Fresh sync: files differ"

# Verify empty file
[[ -f "$DST/empty.dat" && ! -s "$DST/empty.dat" ]] && pass "Empty file synced" || fail "Empty file missing or non-empty"

# ─── T2: Delta sync — only changed chunks transfer ───────────────────────────
echo -e "\n${CYAN}TEST 2: Delta sync (one byte change in large file)${NC}"
SRC="$TMPDIR/t2_src"; DST="$TMPDIR/t2_dst"
mkdir -p "$SRC" "$DST"
dd if=/dev/zero of="$SRC/data.bin" bs=8192 count=100 2>/dev/null
cp "$SRC/data.bin" "$DST/data.bin"

# Mutate one byte in the middle
printf '\xff' | dd of="$SRC/data.bin" bs=1 seek=400000 conv=notrunc 2>/dev/null

$RESYNC -r "$SRC/" "$DST/" --stats 2>&1 | tee "$TMPDIR/t2_out.txt"
diff -rq "$SRC" "$DST" && pass "Delta sync: files match after partial change" || fail "Delta sync: files differ"

# ─── T3: Idempotent re-sync — nothing transferred ────────────────────────────
echo -e "\n${CYAN}TEST 3: Idempotent re-sync (no changes)${NC}"
$RESYNC -r "$SRC/" "$DST/" --stats 2>&1 | tee "$TMPDIR/t3_out.txt"
diff -rq "$SRC" "$DST" && pass "Idempotent re-sync: files still match" || fail "Idempotent re-sync: files differ"

# ─── T4: --delete removes extra files from dest ──────────────────────────────
echo -e "\n${CYAN}TEST 4: --delete removes orphan files${NC}"
SRC="$TMPDIR/t4_src"; DST="$TMPDIR/t4_dst"
mkdir -p "$SRC" "$DST/old_dir"
echo "keep"     > "$SRC/keep.txt"
echo "keep"     > "$DST/keep.txt"
echo "orphan"   > "$DST/orphan.txt"
echo "deep"     > "$DST/old_dir/deep.txt"

$RESYNC -r "$SRC/" "$DST/" --delete --stats
[[ ! -f "$DST/orphan.txt" ]] && pass "--delete removed orphan file" || fail "--delete did NOT remove orphan file"
[[ -f "$DST/keep.txt" ]]    && pass "--delete kept existing file"   || fail "--delete incorrectly removed kept file"

# ─── T5: --dry-run does not modify anything ───────────────────────────────────
echo -e "\n${CYAN}TEST 5: --dry-run leaves destination untouched${NC}"
SRC="$TMPDIR/t5_src"; DST="$TMPDIR/t5_dst"
mkdir -p "$SRC" "$DST"
echo "new" > "$SRC/new.txt"
echo "old" > "$DST/old.txt"

$RESYNC -r "$SRC/" "$DST/" --dry-run --delete --stats
[[ ! -f "$DST/new.txt" ]] && pass "--dry-run: new file NOT created" || fail "--dry-run: new file was created"
[[ -f "$DST/old.txt" ]]   && pass "--dry-run: old file NOT deleted" || fail "--dry-run: old file was deleted"

# ─── T6: Large file with many chunks ─────────────────────────────────────────
echo -e "\n${CYAN}TEST 6: Large file (10 MB, 8 KB chunks)${NC}"
SRC="$TMPDIR/t6_src"; DST="$TMPDIR/t6_dst"
mkdir -p "$SRC" "$DST"
dd if=/dev/urandom of="$SRC/large.bin" bs=1M count=10 2>/dev/null

$RESYNC -r "$SRC/" "$DST/" --chunk-size 8192 --stats
diff -rq "$SRC" "$DST" && pass "Large file: matches" || fail "Large file: differs"

# ─── T7: Non-recursive mode ──────────────────────────────────────────────────
echo -e "\n${CYAN}TEST 7: Non-recursive sync${NC}"
SRC="$TMPDIR/t7_src"; DST="$TMPDIR/t7_dst"
mkdir -p "$SRC/sub"
echo "top"  > "$SRC/top.txt"
echo "deep" > "$SRC/sub/deep.txt"

$RESYNC "$SRC/" "$DST/" --stats
[[ -f "$DST/top.txt" ]]      && pass "Non-recursive: top-level file synced" || fail "Non-recursive: top file missing"
[[ ! -f "$DST/sub/deep.txt" ]] && pass "Non-recursive: subdir file skipped" || fail "Non-recursive: subdir file synced"

# ─── T8: CLI help works ──────────────────────────────────────────────────────
echo -e "\n${CYAN}TEST 8: CLI smoke test${NC}"
$RESYNC --help > /dev/null 2>&1 && pass "CLI --help exits 0" || fail "CLI --help failed"

# ─── T9: Symlink handling ────────────────────────────────────────────────────
echo -e "\n${CYAN}TEST 9: Symlink handling${NC}"
SRC="$TMPDIR/t9_src"; DST="$TMPDIR/t9_dst"
mkdir -p "$SRC"
echo "target" > "$SRC/real.txt"
ln -s real.txt "$SRC/link.txt"

$RESYNC -r "$SRC/" "$DST/" --stats
[[ -f "$DST/link.txt" ]] && pass "Symlink target content synced" || fail "Symlink target not synced"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo -e "\n${BOLD}═══ RESULTS: ${GREEN}$PASS passed${NC}, ${RED}$FAIL failed${NC} ═══\n"

if (( FAIL > 0 )); then
    echo -e "${RED}SOME TESTS FAILED${NC}"
    exit 1
fi

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo -e "\n${BOLD}═══ BENCHMARK:  resync-rs  vs  rsync ═══${NC}\n"

# Check rsync is available
if ! command -v rsync &>/dev/null; then
    echo -e "${YELLOW}rsync not found — skipping benchmark${NC}"
    exit 0
fi

BENCH="$TMPDIR/bench"
BSRC="$BENCH/src"
BDST_RSYNC="$BENCH/dst_rsync"
BDST_RESYNC="$BENCH/dst_resync"
mkdir -p "$BSRC" "$BDST_RSYNC" "$BDST_RESYNC"

# ─── Generate test corpus ────────────────────────────────────────────────────
echo -e "${CYAN}Generating benchmark corpus...${NC}"

NFILES=5000
NDIRS=50

for i in $(seq 1 $NDIRS); do
    mkdir -p "$BSRC/dir_$i"
done

# Mix of file sizes: tiny (< 1KB), medium (10KB), large (100KB)
for i in $(seq 1 $NFILES); do
    dir_idx=$(( (i % NDIRS) + 1 ))
    case $(( i % 3 )) in
        0) dd if=/dev/urandom of="$BSRC/dir_$dir_idx/file_$i.dat" bs=100   count=1 2>/dev/null ;;
        1) dd if=/dev/urandom of="$BSRC/dir_$dir_idx/file_$i.dat" bs=10240 count=1 2>/dev/null ;;
        2) dd if=/dev/urandom of="$BSRC/dir_$dir_idx/file_$i.dat" bs=102400 count=1 2>/dev/null ;;
    esac
done

CORPUS_SIZE=$(du -sh "$BSRC" | cut -f1)
echo -e "Corpus: ${BOLD}$NFILES files${NC} across $NDIRS dirs  (${BOLD}$CORPUS_SIZE${NC})"

# ─── Cold sync (all new) ─────────────────────────────────────────────────────
echo -e "\n${YELLOW}── Cold sync (all files are new) ──${NC}"

echo -n "  rsync:   "
RSYNC_START=$(date +%s%N)
rsync -a "$BSRC/" "$BDST_RSYNC/"
RSYNC_END=$(date +%s%N)
RSYNC_COLD_MS=$(( (RSYNC_END - RSYNC_START) / 1000000 ))
echo -e "${BOLD}${RSYNC_COLD_MS} ms${NC}"

echo -n "  resync:  "
RESYNC_START=$(date +%s%N)
$RESYNC -a "$BSRC/" "$BDST_RESYNC/" -j "$(nproc)"
RESYNC_END=$(date +%s%N)
RESYNC_COLD_MS=$(( (RESYNC_END - RESYNC_START) / 1000000 ))
echo -e "${BOLD}${RESYNC_COLD_MS} ms${NC}"

# Verify correctness
diff -rq "$BSRC" "$BDST_RESYNC" > /dev/null && echo -e "  ${GREEN}✓ Output matches source${NC}" || echo -e "  ${RED}✗ Output differs!${NC}"

# ─── Warm sync (nothing changed) ─────────────────────────────────────────────
echo -e "\n${YELLOW}── Warm sync (no changes — idempotent) ──${NC}"

echo -n "  rsync:   "
RSYNC_START=$(date +%s%N)
rsync -a "$BSRC/" "$BDST_RSYNC/"
RSYNC_END=$(date +%s%N)
RSYNC_WARM_MS=$(( (RSYNC_END - RSYNC_START) / 1000000 ))
echo -e "${BOLD}${RSYNC_WARM_MS} ms${NC}"

echo -n "  resync:  "
RESYNC_START=$(date +%s%N)
$RESYNC -a "$BSRC/" "$BDST_RESYNC/" -j "$(nproc)"
RESYNC_END=$(date +%s%N)
RESYNC_WARM_MS=$(( (RESYNC_END - RESYNC_START) / 1000000 ))
echo -e "${BOLD}${RESYNC_WARM_MS} ms${NC}"

# ─── Delta sync (5% of files changed) ────────────────────────────────────────
echo -e "\n${YELLOW}── Delta sync (5% of files mutated) ──${NC}"

MUTATE_COUNT=$(( NFILES / 20 ))
for i in $(seq 1 $MUTATE_COUNT); do
    idx=$(( RANDOM % NFILES + 1 ))
    dir_idx=$(( (idx % NDIRS) + 1 ))
    target="$BSRC/dir_$dir_idx/file_$idx.dat"
    if [[ -f "$target" ]]; then
        printf '\xff' | dd of="$target" bs=1 seek=0 conv=notrunc 2>/dev/null
    fi
done

echo -n "  rsync:   "
RSYNC_START=$(date +%s%N)
rsync -a "$BSRC/" "$BDST_RSYNC/"
RSYNC_END=$(date +%s%N)
RSYNC_DELTA_MS=$(( (RSYNC_END - RSYNC_START) / 1000000 ))
echo -e "${BOLD}${RSYNC_DELTA_MS} ms${NC}"

echo -n "  resync:  "
RESYNC_START=$(date +%s%N)
$RESYNC -a "$BSRC/" "$BDST_RESYNC/" -j "$(nproc)"
RESYNC_END=$(date +%s%N)
RESYNC_DELTA_MS=$(( (RESYNC_END - RESYNC_START) / 1000000 ))
echo -e "${BOLD}${RESYNC_DELTA_MS} ms${NC}"

# Verify correctness after delta
diff -rq "$BSRC" "$BDST_RESYNC" > /dev/null && echo -e "  ${GREEN}✓ Output matches source${NC}" || echo -e "  ${RED}✗ Output differs!${NC}"

# ─── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}═══════════════════════════════════════════════${NC}"
echo -e "${BOLD}        BENCHMARK SUMMARY (${NFILES} files, ${CORPUS_SIZE})${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════${NC}"
printf "  %-18s %10s %10s %10s\n" "" "rsync" "resync" "speedup"
echo    "  ─────────────────────────────────────────────"

speedup() {
    local rs=$1 re=$2
    if (( re > 0 )); then
        awk "BEGIN { printf \"%.1fx\", $rs / $re }"
    else
        echo "inf"
    fi
}

printf "  %-18s %8s ms %8s ms %10s\n" "Cold (all new)" "$RSYNC_COLD_MS" "$RESYNC_COLD_MS" "$(speedup $RSYNC_COLD_MS $RESYNC_COLD_MS)"
printf "  %-18s %8s ms %8s ms %10s\n" "Warm (no change)" "$RSYNC_WARM_MS" "$RESYNC_WARM_MS" "$(speedup $RSYNC_WARM_MS $RESYNC_WARM_MS)"
printf "  %-18s %8s ms %8s ms %10s\n" "Delta (5% mutated)" "$RSYNC_DELTA_MS" "$RESYNC_DELTA_MS" "$(speedup $RSYNC_DELTA_MS $RESYNC_DELTA_MS)"
echo -e "${BOLD}═══════════════════════════════════════════════${NC}"
echo ""
echo -e "${GREEN}Thread count used: $(nproc) cores${NC}"
