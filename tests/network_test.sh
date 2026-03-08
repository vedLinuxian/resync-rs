#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# resync-rs  —  Phase 2 Network Integration Tests
#
# Starts a local resync server, pushes files over TCP, validates correctness.
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

RESYNC="$(cd "$(dirname "$0")/.." && pwd)/target/release/resync"
TMPDIR=$(mktemp -d /tmp/resync-net-e2e-XXXXXX)
trap 'kill $SERVER_PID 2>/dev/null || true; rm -rf "$TMPDIR"' EXIT

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

PASS=0
FAIL=0
PORT=23770

pass() { ((PASS++)); echo -e "  ${GREEN}✓${NC} $1"; }
fail() { ((FAIL++)); echo -e "  ${RED}✗${NC} $1"; }

# ── Start the server ──────────────────────────────────────────────────────────
echo -e "\n${BOLD}═══ resync-rs  NETWORK TEST SUITE  (Phase 2) ═══${NC}\n"
echo -e "${CYAN}Starting server on port $PORT...${NC}"

$RESYNC serve --port $PORT --bind 127.0.0.1 &
SERVER_PID=$!
sleep 0.5

if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo -e "${RED}Server failed to start!${NC}"
    exit 1
fi
echo -e "  ${GREEN}✓${NC} Server running (PID: $SERVER_PID)"

# ─── N1: Fresh network sync — all files new ──────────────────────────────────
echo -e "\n${CYAN}NET TEST 1: Fresh push (all new files over TCP)${NC}"
SRC="$TMPDIR/n1_src"; DST="$TMPDIR/n1_dst"
mkdir -p "$SRC/sub/deep"
echo "hello network"  > "$SRC/a.txt"
echo "deep file"      > "$SRC/sub/deep/b.txt"
dd if=/dev/urandom of="$SRC/sub/big.bin" bs=4096 count=50 2>/dev/null
touch "$SRC/empty.dat"

$RESYNC push -r --stats "$SRC" "127.0.0.1:$PORT:$DST"
diff -rq "$SRC" "$DST" && pass "Fresh network push: all files match" || fail "Fresh push: files differ"
[[ -f "$DST/empty.dat" && ! -s "$DST/empty.dat" ]] && pass "Empty file synced over network" || fail "Empty file missing"

# ─── N2: Delta network sync — one byte change ────────────────────────────────
echo -e "\n${CYAN}NET TEST 2: Delta push (one byte change in large file)${NC}"
SRC="$TMPDIR/n2_src"; DST="$TMPDIR/n2_dst"
mkdir -p "$SRC" "$DST"
dd if=/dev/zero of="$SRC/data.bin" bs=8192 count=50 2>/dev/null
cp "$SRC/data.bin" "$DST/data.bin"
# Copy preserves mtime so server will see identical mtime+size → skip.
# We need to mutate so mtime changes:
sleep 0.1
printf '\xff' | dd of="$SRC/data.bin" bs=1 seek=100000 conv=notrunc 2>/dev/null

$RESYNC push -a --stats "$SRC" "127.0.0.1:$PORT:$DST"
diff -rq "$SRC" "$DST" && pass "Delta network push: files match" || fail "Delta push: files differ"

# ─── N3: Idempotent re-push — nothing changed ────────────────────────────────
echo -e "\n${CYAN}NET TEST 3: Idempotent re-push (no changes)${NC}"
$RESYNC push -a --stats "$SRC" "127.0.0.1:$PORT:$DST"
diff -rq "$SRC" "$DST" && pass "Idempotent network push: still matches" || fail "Idempotent push: differs"

# ─── N4: Network push with --delete ──────────────────────────────────────────
echo -e "\n${CYAN}NET TEST 4: Network push with --delete${NC}"
SRC="$TMPDIR/n4_src"; DST="$TMPDIR/n4_dst"
mkdir -p "$SRC" "$DST"
echo "keep" > "$SRC/keep.txt"
echo "keep" > "$DST/keep.txt"
echo "orphan" > "$DST/orphan.txt"

$RESYNC push -r --delete --stats "$SRC" "127.0.0.1:$PORT:$DST"
[[ ! -f "$DST/orphan.txt" ]] && pass "--delete removed orphan over network" || fail "--delete did NOT remove orphan"
[[ -f "$DST/keep.txt" ]]     && pass "--delete kept existing file"          || fail "--delete removed kept file"

# ─── N5: Large file over network ─────────────────────────────────────────────
echo -e "\n${CYAN}NET TEST 5: Large file (5 MB over TCP)${NC}"
SRC="$TMPDIR/n5_src"; DST="$TMPDIR/n5_dst"
mkdir -p "$SRC"
dd if=/dev/urandom of="$SRC/large.bin" bs=1M count=5 2>/dev/null

$RESYNC push -r --stats "$SRC" "127.0.0.1:$PORT:$DST"
diff -rq "$SRC" "$DST" && pass "Large file: matches over network" || fail "Large file: differs over network"

# ─── N6: Many small files over network ────────────────────────────────────────
echo -e "\n${CYAN}NET TEST 6: Many small files (200 files over TCP)${NC}"
SRC="$TMPDIR/n6_src"; DST="$TMPDIR/n6_dst"
mkdir -p "$SRC"
for i in $(seq 1 200); do echo "file content $i" > "$SRC/file_$i.txt"; done

$RESYNC push -r --stats "$SRC" "127.0.0.1:$PORT:$DST"
FILE_COUNT=$(find "$DST" -type f | wc -l)
[[ "$FILE_COUNT" -eq 200 ]] && pass "200 files transferred over network" || fail "Expected 200 files, got $FILE_COUNT"
diff -rq "$SRC" "$DST" && pass "All 200 files content-match" || fail "Some files differ"

# ─── N7: Server --help and push --help ────────────────────────────────────────
echo -e "\n${CYAN}NET TEST 7: CLI help for subcommands${NC}"
$RESYNC serve --help > /dev/null 2>&1 && pass "serve --help works" || fail "serve --help failed"
$RESYNC push --help > /dev/null 2>&1  && pass "push --help works"  || fail "push --help failed"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo -e "\n${BOLD}═══ NETWORK RESULTS: ${GREEN}$PASS passed${NC}, ${RED}$FAIL failed${NC} ═══\n"

kill $SERVER_PID 2>/dev/null || true

if (( FAIL > 0 )); then
    echo -e "${RED}SOME NETWORK TESTS FAILED${NC}"
    exit 1
fi

echo -e "${GREEN}Phase 2 TCP client-server architecture is working!${NC}"
