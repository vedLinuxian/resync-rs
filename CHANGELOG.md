# Changelog

All notable changes to resync-rs will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
