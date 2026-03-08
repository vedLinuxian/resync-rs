# Changelog

All notable changes to resync-rs will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-03-08

### Added

- **Parallel directory scanning** using jwalk for multi-core tree traversal
- **BLAKE3 hashing** with SIMD acceleration and multi-threaded batching (128 chunks/task)
- **Content-Defined Chunking (CDC)** with Gear-hash rolling function
- **Delta sync engine** — only transfers changed chunks (96%+ savings)
- **Atomic file applier** — zero-copy mmap reads with 256 KB buffered writes
- **Archive mode** (`-a`) — recursive, preserves timestamps and permissions
- **`--delete` flag** — removes orphan files at destination (respects exclude rules)
- **`--dry-run` flag** — preview changes without modifying destination
- **Include/exclude filters** — rsync-compatible glob patterns with `**` support
- **Symlink handling** — follows or preserves based on `-L` flag
- **Backup support** (`-B`) — hard-links originals before overwriting
- **Rich progress bars** — real-time throughput, ETA, and delta stats via indicatif
- **TCP client-server mode** — `resync serve` / `resync push` for network sync
- **TLS 1.3 support** — optional encryption via tokio-rustls
- **Binary protocol** — efficient bincode serialization for network messages
- **Comprehensive test suite** — 52 unit tests, 13 E2E tests, 11 network tests

### Performance

- 2.4x–6.0x faster than rsync 3.2.7 across all benchmark scenarios
- 5.9x faster warm (no-change) detection on large files
- 47% less energy consumption per sync operation
- 4.7 MB statically-linked release binary (LTO + strip)

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
