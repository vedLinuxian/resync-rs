# Changelog

All notable changes to resync-rs will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0-beta.1] - 2025-07-17

### Added

- **Content-Defined Chunking on network path** — server and client now use
  `Hasher::with_cdc()` for network delta computation. Previously the network
  path used fixed-size chunks, causing the "shift-byte problem": inserting
  one byte at position 0 would invalidate every chunk and force full
  retransmission. CDC preserves 95%+ of chunk hashes on prepend edits.
- **Hash-set delta matching** — `compute_network_delta()` now uses a
  `HashSet<Hash256>` for O(1) lookups instead of index-based comparison,
  correctly identifying unchanged chunks regardless of position.
- **`--energy-saved` flag** — prints CPU-seconds saved, watt-hours, and
  kg CO₂ avoided compared to an equivalent rsync run.
- **Docker network benchmarks** — new `bench/docker_network_benchmark.sh`
  and `bench/Dockerfile.network` for reproducible, isolated network
  benchmarks with `tc netem` traffic shaping (WAN/LAN/Slow profiles).
- **Page cache eviction** — `posix_fadvise(DONTNEED)` after O_DIRECT
  reads to avoid polluting the page cache during large syncs.
- **Parallel deletes** — `--delete` now runs deletions on the rayon pool.
- **Stack buffer for tiny files** — files ≤ 8 KB use a stack-allocated
  buffer instead of mmap, avoiding kernel overhead for small I/O.

### Performance

- **LAN (1 Gbps):** 4.6× geometric mean over rsync 3.2.7
  - Compressible data: 55.7×
  - Incremental sync: 11.8×
  - No-change: 12.2×
- **WAN (50 Mbps, 40 ms RTT, 0.5% loss):** 4.2× geometric mean
  - Compressible data: 56.0×
  - No-change: 15.6×
  - Incremental sync: 12.5×
- **Local NVMe:** 2.1–2.6× full copy, ~120× with --trust-mtime

### Fixed

- Network delta used fixed-size chunks instead of CDC (critical correctness
  and performance bug for network transfers).
- Client delta comparison used index-based matching that missed reordered
  chunks.

## [0.3.0] - 2025-07-16

### Added

- **Rust 2024 edition** -- upgraded from Rust 2021 to 2024 edition, requiring
  rustc 1.85+. Updated all dependencies to latest versions.
- **Two-tier manifest cache** -- persistent `.resync-manifest` (zstd-compressed
  bincode) and `.resync-toc` (tiny dir-mtime index). On repeated syncs the
  destination is never re-scanned; the manifest provides all metadata.
- **Incremental source scanning** -- uses cached directory mtimes to skip
  `readdir()` for unchanged directories. Still does live `stat()` on every
  file for correctness. Cuts source scan time proportionally to unchanged dirs.
- **`--trust-mtime` flag** -- opt-in ultra-fast mode. If directory mtimes
  match the TOC, skips the entire sync pipeline (~160x faster at 100K files).
  Safe for deployments and CI; not safe when files are edited in-place.
- **`--xattrs` / `-X` flag** -- preserve extended attributes during sync,
  using the `xattr` crate. Copies all user/system xattrs from source to dest.
- **`--files-from FILE`** -- read file list from FILE (one path per line),
  syncing only those files. rsync-compatible.
- **`--chmod PERMS`** -- override destination permissions using chmod-style
  strings (e.g. `u+rw,go+r`).
- **`--chown USER:GROUP`** -- override destination owner and group.
- **`--checksum-verify`** -- BLAKE3 integrity check after every file transfer.
  Reads the destination back and compares against the source hash.
- **`--reflink`** -- use CoW reflinks on btrfs/XFS for instant, zero-copy file
  copies via `copy_file_range()` with FICLONE ioctl.
- **`resync pull` subcommand** -- pull files from a remote resync server to
  local destination (reverse of `push`). Supports `--tls` and `--tls-verify`.
- **Parallel directory walking** -- scanner now uses BFS with parallel readdir
  via rayon's work-stealing pool, saturating NVMe IOPS.

### Security

- **Path traversal protection** -- server and client validate all relative
  paths, rejecting `..` components and absolute paths. Server refuses writes
  to system directories (/etc, /proc, /sys, /dev, /boot, /root, /sbin, /bin,
  /usr).
- **Zstd decompression bomb limit** -- compressed messages are capped at
  256 MiB decompressed. Oversized payloads are rejected with an error.
- **setuid/setgid stripping** -- non-root users automatically strip setuid
  and setgid bits from synced files, preventing privilege escalation.
- **Pull-mode streaming** -- file data is streamed to disk via temp files
  instead of accumulating in memory, preventing OOM on large transfers.
- **SIMD-accelerated zero detection** -- `is_all_zeros()` uses u128 alignment
  for 16x throughput in sparse file detection.

### Fixed

- **12 dead/no-op CLI flags** -- `--hard-links`, `--devices`, `--specials`,
  `--max-delete`, `--force`, `--backup-dir`, `--suffix`, `--partial`,
  `--modify-window`, `--log-file`, `--itemize-changes`, `--bwlimit` were
  accepted but silently ignored. All now emit a "not yet implemented" warning
  or have been wired into the engine.
- **TOC correctness bug** -- directory mtimes don't change when file contents
  are modified in-place. The TOC fast path and decision-phase skip are now
  gated behind `--trust-mtime` to prevent silent data loss.
- **Incremental scan cached-stat bug** -- unchanged directories were using
  cached file metadata (old mtimes/sizes) instead of live `stat()`. Fixed to
  always do live stat on files; only readdir is skipped.

### Performance

- **Lock-free parallel scanner** -- replaced Mutex-based shared-state scanner
  with per-directory `par_iter().map().collect()` pattern. Eliminates lock
  contention during BFS directory walking.
- **`#[inline]` annotations** -- added to 16 hot-path functions in delta,
  hasher, filter, and CDC modules for better inlining across crate boundaries.
- **HashSet<&Path> optimization** -- source path set uses borrowed references
  instead of cloned PathBuf, eliminating ~100K allocations at scale.
- **Native CPU targeting** -- `.cargo/config.toml` enables `-Ctarget-cpu=native`
  for AVX-512/AVX2/SSE4.1 auto-vectorization.
- **Dependency upgrades** -- blake3 1.5→1.8, thiserror 1→2, added xattr 1,
  io-uring 0.7.
- **Full copy**: 2.5-4.3x faster across workloads
- **No-change**: 3.2x at 10K, 15x at 100K files
- **No-change (--trust-mtime)**: 160x at 100K files

## [0.2.0] - 2025-07-15

### Added

- **`--one-file-system` / `-x` implementation** -- scanner now tracks root
  device ID and skips directories on different filesystems. Previously the
  CLI flag was accepted but silently ignored.
- **`--sparse` implementation** -- applier detects all-zero 4 KB pages and
  uses `lseek(SEEK_CUR)` to create holes, producing truly sparse output
  files for VM images and database files.
- **`--append` implementation** -- applier skips writing bytes already
  present at the destination and only appends new data. Previously the CLI
  flag was accepted but never wired into the applier.
- **`fallocate(2)` pre-allocation** -- new files and inplace writes
  pre-allocate the full target size, eliminating filesystem metadata updates
  during write and reducing fragmentation to zero on ext4/xfs/btrfs.
- **`MADV_HUGEPAGE` for large file mmaps** -- files >= 2 MB receive a
  transparent huge page hint via `madvise()`, reducing TLB misses and
  improving hash throughput on large files.
- **Nanosecond timestamp preservation** -- switched from `OpenOptions::write`
  mtime hack to `utimensat(2)` via libc. Sets nanosecond-precision mtime
  without opening the file, preserves atime with `UTIME_OMIT`.
- **Professional homepage** -- complete docs/index.html rewrite (1386 lines).
  Dark theme, animated benchmarks, comparison table, architecture diagram,
  terminal-styled install box. No external CDN dependencies, fully responsive.

### Fixed

- **Inplace mode data corruption** -- `write_delta_inplace` was ignoring
  the `dst_data` parameter entirely. Copy operations that referenced
  existing destination data would produce all-zeros or wrong content.
  Now properly reads and seeks within the destination file for Copy ops.
- **Silent seek error swallowing** -- inplace mode used `.ok()` on seek
  results, hiding I/O errors. Now uses explicit `flush()` + `seek()` with
  proper error propagation.
- **Unnecessary file open for mtime** -- timestamp preservation was opening
  files in write mode just to set mtime. Replaced with `utimensat(2)` which
  operates on the path without opening the file descriptor.

### Performance

- 2x-6x faster than rsync 3.2.7 across measured scenarios (warm cache, NVMe)
- fallocate pre-allocation eliminates fragmentation and metadata overhead
- MADV_HUGEPAGE reduces TLB misses for files >= 2 MB
- utimensat avoids unnecessary file open/close syscalls for mtime preservation
- Sparse zero-skip avoids writing and allocating blocks for all-zero regions

## [0.1.0] - 2025-03-08

### Added

- **Parallel directory scanning** using adaptive sequential/parallel strategy
- **BLAKE3 hashing** with SIMD acceleration and multi-threaded batching (128 chunks/task)
- **Content-Defined Chunking (CDC)** with Gear-hash rolling function
- **Delta sync engine** -- only transfers changed chunks (96%+ savings)
- **Atomic file applier** -- zero-copy mmap reads with 256 KB buffered writes
- **Archive mode** (`-a`) -- recursive, preserves timestamps and permissions
- **`--delete` flag** -- removes orphan files at destination (respects exclude rules)
- **`--dry-run` flag** -- preview changes without modifying destination
- **Include/exclude filters** -- rsync-compatible glob patterns with `**` support
- **Symlink handling** -- follows or preserves based on `-L` flag
- **Backup support** (`-B`) -- hard-links originals before overwriting
- **Rich progress bars** -- real-time throughput, ETA, and delta stats via indicatif
- **TCP client-server mode** -- `resync serve` / `resync push` for network sync
- **TLS 1.3 support** -- optional encryption via tokio-rustls
- **Binary protocol** -- efficient bincode serialization for network messages
- **Comprehensive test suite** -- 53 unit tests, 13 E2E tests, 11 network tests

### Performance

- 2x-6x faster than rsync 3.2.7 across measured benchmark scenarios
- Up to 6.5x faster no-change detection on large files
- 4.5 MB release binary (LTO + strip)

### Fixed (pre-release audit)

- Batched rayon tasks (128/batch) to eliminate scheduling overhead
- Zero-copy `FileData` enum in applier (eliminated `mmap.to_vec()` copies)
- Backup uses `hard_link` instead of `rename` (was breaking delta Copy ops)
- `--delete` now respects exclude filter rules
- Directory entries filtered by exclude rules
- Pre-epoch timestamp roundtrip handling in protocol
- Glob `**` recursion depth limit (prevented exponential blowup)
- Network client uses mmap instead of `fs::read()` for large sends
- Server uses 256 KB `BufWriter` with `final_size` validation
