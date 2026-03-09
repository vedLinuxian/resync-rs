#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════
# AI Dataset Generator — Creates realistic large-scale datasets for
# benchmarking resync-rs under extreme conditions (millions of files).
#
# Usage:
#   bash bench/generate_dataset.sh /path/to/dataset [num_files]
#
# Default: 100,000 files (~400 MB).  Pass num_files=1000000 for 1M files.
#
# Safety:
#   - Skips generation if dataset already exists (idempotent)
#   - Auto-adds the target dir to .gitignore
#   - Uses /dev/urandom for semi-realistic binary data
# ═══════════════════════════════════════════════════════════════════════════

set -euo pipefail

TARGET_DIR="${1:-/tmp/resync-bench-dataset}"
NUM_FILES="${2:-100000}"
FILES_PER_DIR=1000
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

GREEN='\033[0;32m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

# ─── Skip if already exists ──────────────────────────────────────────────

if [[ -d "$TARGET_DIR" ]]; then
    EXISTING=$(find "$TARGET_DIR" -type f 2>/dev/null | head -1)
    if [[ -n "$EXISTING" ]]; then
        ACTUAL_COUNT=$(find "$TARGET_DIR" -type f | wc -l)
        SIZE=$(du -sh "$TARGET_DIR" | cut -f1)
        echo -e "${GREEN}✅ Dataset already exists: $TARGET_DIR ($ACTUAL_COUNT files, $SIZE)${NC}"
        echo -e "   Delete it to regenerate: rm -rf $TARGET_DIR"
        exit 0
    fi
fi

# ─── Generate ────────────────────────────────────────────────────────────

echo -e "${BOLD}🚀 Generating $NUM_FILES files in $TARGET_DIR${NC}"
echo -e "${CYAN}   Files per directory: $FILES_PER_DIR${NC}"

NUM_DIRS=$(( (NUM_FILES + FILES_PER_DIR - 1) / FILES_PER_DIR ))
echo -e "${CYAN}   Directories: $NUM_DIRS${NC}\n"

mkdir -p "$TARGET_DIR"

GENERATED=0
START=$(date +%s)

for d in $(seq 0 $((NUM_DIRS - 1))); do
    DIR_PATH="$TARGET_DIR/shard_$(printf '%04d' $d)"
    mkdir -p "$DIR_PATH"

    for f in $(seq 1 $FILES_PER_DIR); do
        if (( GENERATED >= NUM_FILES )); then
            break 2
        fi

        FILE_IDX=$GENERATED
        # Mix of file types for realistic workload:
        case $((FILE_IDX % 5)) in
            0)
                # Small text config (200-500 bytes, highly compressible)
                printf "[config]\nname = worker_%d\nthreads = 8\nmax_connections = 1024\nbuffer_size = 65536\nlog_level = info\ntimeout_ms = 30000\nretry_count = 3\nendpoint = http://api.example.com/v2\nauth_token = abc123def456\n" \
                    "$FILE_IDX" > "$DIR_PATH/config_$f.toml"
                ;;
            1)
                # Source code (1-2 KB, moderate compression)
                printf "use std::collections::HashMap;\n\npub struct Worker_%d {\n    id: u64,\n    name: String,\n    tasks: Vec<Task>,\n}\n\nimpl Worker_%d {\n    pub fn new(id: u64) -> Self {\n        Self { id, name: format!(\"worker_{}\", id), tasks: Vec::new() }\n    }\n\n    pub fn process(&mut self) -> Result<(), Error> {\n        for task in &self.tasks {\n            task.execute()?;\n        }\n        Ok(())\n    }\n}\n" \
                    "$FILE_IDX" "$FILE_IDX" > "$DIR_PATH/module_$f.rs"
                ;;
            2)
                # JSON data (2-4 KB, good compression)
                printf '{"id":%d,"type":"measurement","timestamp":"2025-06-30T12:00:00Z","sensor":{"name":"thermal_%d","location":"rack_%d","unit":"celsius"},"values":[%.1f,%.1f,%.1f,%.1f,%.1f],"metadata":{"batch":%d,"quality":"good"}}\n' \
                    "$FILE_IDX" "$FILE_IDX" $((FILE_IDX % 100)) \
                    $(echo "scale=1; 20 + $FILE_IDX % 30" | bc) \
                    $(echo "scale=1; 21 + $FILE_IDX % 30" | bc) \
                    $(echo "scale=1; 22 + $FILE_IDX % 30" | bc) \
                    $(echo "scale=1; 23 + $FILE_IDX % 30" | bc) \
                    $(echo "scale=1; 24 + $FILE_IDX % 30" | bc) \
                    $((FILE_IDX / 1000)) > "$DIR_PATH/data_$f.json"
                ;;
            3)
                # Binary blob (1 KB, low compression)
                dd if=/dev/urandom bs=1024 count=1 of="$DIR_PATH/blob_$f.bin" 2>/dev/null
                ;;
            4)
                # Log line (500 bytes - 1 KB, high compression)
                printf "2025-06-30T09:15:23.%03dZ [INFO] worker_%d: processed batch %d items=%d latency_ms=%d status=OK endpoint=/api/v2/process bytes_in=%d bytes_out=%d compression=%.1f cache_hit=%s\n" \
                    $((FILE_IDX % 1000)) "$FILE_IDX" $((FILE_IDX / 100)) \
                    $((50 + FILE_IDX % 200)) $((1 + FILE_IDX % 50)) \
                    $((1024 + FILE_IDX % 8192)) $((512 + FILE_IDX % 4096)) \
                    $(echo "scale=1; 2 + $FILE_IDX % 8" | bc) \
                    $( (( FILE_IDX % 3 == 0 )) && echo "true" || echo "false" ) \
                    > "$DIR_PATH/log_$f.txt"
                ;;
        esac

        GENERATED=$((GENERATED + 1))
    done

    # Progress every 10,000 files
    if (( GENERATED % 10000 == 0 )); then
        ELAPSED=$(( $(date +%s) - START ))
        RATE=$(( GENERATED / (ELAPSED + 1) ))
        echo -e "  [$GENERATED / $NUM_FILES] ${RATE} files/sec"
    fi
done

END=$(date +%s)
ELAPSED=$((END - START + 1))
SIZE=$(du -sh "$TARGET_DIR" | cut -f1)

echo -e "\n${GREEN}✅ Generated $GENERATED files in ${ELAPSED}s ($SIZE)${NC}"
echo -e "   Path: $TARGET_DIR\n"

# ─── Auto-add to .gitignore ──────────────────────────────────────────────

GITIGNORE="$PROJECT_DIR/.gitignore"
if [[ -f "$GITIGNORE" ]]; then
    # Only add if target is inside the project
    REL_PATH="${TARGET_DIR#$PROJECT_DIR/}"
    if [[ "$REL_PATH" != "$TARGET_DIR" ]] && ! grep -qF "$REL_PATH" "$GITIGNORE" 2>/dev/null; then
        echo "" >> "$GITIGNORE"
        echo "# Auto-generated benchmark dataset" >> "$GITIGNORE"
        echo "$REL_PATH/" >> "$GITIGNORE"
        echo -e "${CYAN}   Added '$REL_PATH/' to .gitignore${NC}"
    fi
fi
