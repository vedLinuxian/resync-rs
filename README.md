<div align="center">

# resync

### rsync reimagined in Rust. Parallel. Zero-copy. Unstoppable.

[![Build Status](https://img.shields.io/github/actions/workflow/status/vedLinuxian/resync-rs/ci.yml?branch=main&style=flat-square)](https://github.com/vedLinuxian/resync-rs/actions)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](LICENSE)
[![Crates.io](https://img.shields.io/crates/v/resync-rs?style=flat-square)](https://crates.io/crates/resync-rs)
[![Rust](https://img.shields.io/badge/rust-2024_edition-orange?style=flat-square)](https://www.rust-lang.org)

A ground-up rewrite of rsync in Rust, engineered for modern hardware.
resync saturates NVMe drives and multi-core CPUs where rsync leaves
90% of your hardware idle.

[Homepage](https://vedLinuxian.github.io/resync-rs) |
[Getting Started](#getting-started) |
[Performance](#performance) |
[Architecture](#architecture) |
[Usage](#usage) |
[Contributing](CONTRIBUTING.md)

</div>

---

## At a Glance

| Capability | rsync | resync |
|---|---|---|
| Hash algorithm | MD5 (128-bit, broken) | BLAKE3 (256-bit, SIMD/AVX-512) |
| Parallelism | Single-threaded | Rayon work-stealing (all cores) |
| File I/O | read()/write() syscalls | mmap + madvise + copy_file_range |
| Chunking | Fixed-size only | Content-Defined (FastCDC) + fixed |
| Delta encoding | Rolling checksum + MD5 | Gear-hash CDC + BLAKE3 |
| Compression | zlib (slow) | zstd (10x faster at same ratio) |
| TLS | Via stunnel (external) | Native TLS 1.3 (rustls, no OpenSSL) |
| Directory scan | Sequential stat() loop | Adaptive: sequential turbo + parallel |
| Incremental cache | None (always re-scans) | Manifest cache + TOC (skip dst scan) |
| Crash safety | Temp file + rename | Temp file + rename (atomic) |
| fsync behavior | Always (slow) | Off by default, `--fsync` to enable |
| Zero-copy I/O | No | mmap, copy_file_range(2), madvise |
| File prealloc | No | fallocate(2) for zero fragmentation |
| Huge pages | No | MADV_HUGEPAGE for large file I/O |
| Timestamp precision | 1-second granularity | Nanosecond via utimensat(2) |
| Sparse files | Basic support | Seek-over-zeros with block detection |
| Extended attributes | No | `--xattrs` / `-X` (xattr crate) |
| Security hardening | Minimal | Path traversal guards, decompression limits, setuid stripping |
| Language | C (manual memory) | Rust (memory-safe, fearless concurrency) |

---

## Getting Started

### Prerequisites

- Rust 1.85+ (2024 edition)
- Linux (primary), macOS, or FreeBSD
- CMake and a C compiler (for aws-lc-sys / ring TLS backend)

### Install from source

```bash
git clone https://github.com/vedLinuxian/resync-rs.git
cd resync-rs
cargo install --path .
```

### Install from crates.io

```bash
cargo install resync-rs
```

### Quick start

```bash
# Basic sync (like rsync)
resync /data/source/ /data/backup/

# Archive mode: preserve permissions, timestamps, symlinks, owner, group
resync -avz /data/source/ /data/backup/

# With progress bar and stats
resync -avzP --stats /data/source/ /data/backup/

# Dry run -- see what would change without doing it
resync -avzn /data/source/ /data/backup/

# Incremental backup with --link-dest (hardlink unchanged files)
resync -aH --link-dest /backups/yesterday /src/ /backups/today/

# Network push with TLS encryption
resync push -avz --tls /data/source/ server.example.com:2377:/data/dest/

# Server mode (listen for incoming syncs)
resync serve --port 2377 --tls
```

---

## Performance

### Measured benchmarks (warm cache, NVMe SSD, rsync 3.2.7)

Tested on a single machine with NVMe storage. Results are wall-clock time
measured with `time`. Run `bash bench/benchmark.sh` to reproduce on your
hardware.

#### Standard Benchmark (10K files)

Hardware: 12th Gen Intel i3-1215U (8 cores), NVMe SSD (ext4), warm page cache.

```
Scenario                                rsync       resync      Speedup
------------------------------------------------------------------------
Small files: full copy (10K)            .405s       .188s       2.1x
Small files: no change (10K)            .135s       .055s       2.4x
Small files: 5% changed                .194s       .088s       2.1x
Medium files: full copy (500)           .741s       .282s       2.6x
Large files: full copy (5x100M)        1.371s       .647s       2.1x
Large files: delta (10% changed)       1.339s      1.039s       1.2x
```

#### Scale Test (100K files, no-change)

```
Scenario                                rsync       resync      Speedup
------------------------------------------------------------------------
100K files, standard                    .760s       .433s       1.7x
100K files, --trust-mtime              .760s       .006s       ~120x
```

The `--trust-mtime` flag enables ultra-fast no-change detection by checking
only directory mtimes (< 50 KB index). It's safe for deployments and CI
where artifacts are replaced atomically, but not safe when files may be
edited in-place. Without `--trust-mtime`, resync always does live stat()
of every source file for full correctness.

Typical speedups range from **2.1x to 2.6x** for full copies, **2.4x** for
no-change detection, and **~120x** with `--trust-mtime` at 100K files.
Speedups scale with core count — the numbers above are on an 8-core laptop.

The advantage grows on large files (zero-copy I/O wins) and at scale
(parallel scanning + manifest cache wins).

### Why it is faster

**1. Parallel everything.**
Directory scanning, BLAKE3 hashing, delta computation, and file application
all run on a rayon work-stealing thread pool. rsync is single-threaded for
every stage. On a 16-core machine, resync uses all 16 cores simultaneously.

**2. Zero-copy I/O.**
Files are memory-mapped with `madvise(MADV_SEQUENTIAL | MADV_WILLNEED)` for
kernel readahead. Large file mmaps get `MADV_HUGEPAGE` for transparent huge
pages, reducing TLB misses on large mappings. New files use `copy_file_range(2)` to
copy data entirely in kernel space -- bytes never enter userspace.

**3. BLAKE3 SIMD hashing.**
BLAKE3 is 10-15x faster than MD5 and automatically vectorizes with AVX-512,
AVX2, NEON, or SSE4.1. Combined with batched parallelism (128 chunks per
rayon task), hashing throughput can reach several GB/s on modern hardware.

**4. Content-Defined Chunking (FastCDC).**
Insertions and deletions at the start of a file no longer shift every chunk
boundary. FastCDC with Gear rolling hash preserves 95%+ of chunk hashes when
data is prepended, where fixed-size chunking invalidates everything after the
edit point.

**5. No fsync by default.**
rsync calls `fsync()` on every file, forcing the NVMe controller to flush its
write cache. For 100K small files this adds 15+ seconds of pure latency.
resync trusts the OS page cache by default (`--fsync` when durability matters).

**6. Inode-sorted processing.**
Source files are sorted by inode number before processing, minimizing disk
head seeks on rotational media and optimizing readahead on SSDs.

**7. Manifest cache + incremental scanning.**
After each sync, resync saves a binary manifest of the destination state.
On the next run, it skips the entire destination scan (just reads the manifest).
Source scanning uses directory-mtime checks to skip readdir() of unchanged
directories. With `--trust-mtime`, even file stat() calls are skipped,
achieving sub-5ms no-change detection on trees with 500K+ files.

**8. Two-phase pipeline.**
Phase 1 (sequential): classify every file as New/Changed/Skip using O(1)
HashMap lookups. Completes in microseconds for 100K files.
Phase 2 (parallel): only files that need actual I/O enter the rayon pool.
This eliminates futex contention from threads competing on skipped files.

**9. Pre-allocated output files.**
Destination files are pre-allocated with `fallocate(2)` before writing,
eliminating filesystem metadata updates during write and reducing
fragmentation to zero on ext4/xfs/btrfs.

---

## Architecture

```
                         resync pipeline
   +-----------------------------------------------------------+
   |                                                            |
   |  Scanner (adaptive: sequential turbo / parallel)           |
   |      |                                                     |
   |      v                                                     |
   |  FilterEngine (exclude/include/size filters)               |
   |      |                                                     |
   |      v                                                     |
   |  Phase 1: Sequential Decision (O(1) HashMap lookups)       |
   |      |                                                     |
   |      +-- New file ------> Applier::copy_new()              |
   |      |                    (copy_file_range / atomic tmp)    |
   |      |                                                     |
   |      +-- Skip ----------> mtime+size / --size-only / etc   |
   |      |                                                     |
   |      +-- Update --------> Hasher (BLAKE3 + mmap)           |
   |      |                        |                            |
   |      |                        v                            |
   |      |                    CdcChunker (FastCDC Gear hash)   |
   |      |                        |                            |
   |      |                        v                            |
   |      |                    DeltaEngine (hash-map match)     |
   |      |                        |                            |
   |      |                        v                            |
   |      |                    Applier::apply() (atomic)        |
   |      |                                                     |
   |      +-- Delete --------> --delete / --max-delete guard    |
   |                                                            |
   |  Phase 2: Parallel I/O (rayon work-stealing pool)          |
   |      All per-file work runs on a scoped rayon pool.        |
   |      Files sorted by inode for optimal disk I/O order.     |
   |                                                            |
   +-----------------------------------------------------------+
```

### Module overview

| Module | Purpose |
|---|---|
| `scanner.rs` | Adaptive directory walking (sequential turbo + parallel BFS) |
| `hasher.rs` | BLAKE3 chunk hashing with mmap, madvise, huge pages |
| `cdc.rs` | FastCDC content-defined chunking (Gear hash, normalized) |
| `delta.rs` | Delta computation: diff two FileManifests (index + hash-map) |
| `applier.rs` | Atomic file writing, copy_file_range, fallocate, sparse, chown |
| `filter.rs` | Glob matching, rate limiter, itemize changes, backup helpers |
| `sync_engine.rs` | Core orchestrator: two-phase scan-decide-apply pipeline |
| `progress.rs` | Real-time progress bars and transfer statistics |
| `cli.rs` | rsync-compatible CLI (clap derive, 60+ flags) |
| `error.rs` | Error types (thiserror) |
| `net/protocol.rs` | Binary wire protocol with length-delimited framing + zstd |
| `net/server.rs` | Async TCP server daemon (tokio) |
| `net/client.rs` | Network push client with delta streaming |
| `net/tls.rs` | TLS 1.3 transport (rustls, auto-generated self-signed certs) |

---

## Usage

### rsync-compatible flags

resync supports all commonly-used rsync flags:

```
TRANSFER CONTROL
  -a, --archive          Archive mode (-rlptgo)
  -r, --recursive        Recurse into directories
  -v, --verbose          Print each file being processed
  -z, --compress         Compress data during transfer (zstd)
  -n, --dry-run          Trial run (show what would change)
  -u, --update           Skip files that are newer on destination
  -W, --whole-file       Always copy whole files (skip delta)
  -c, --checksum         Always hash files (skip mtime+size check)
  -H, --hard-links       Preserve hard links
  -x, --one-file-system  Don't cross filesystem boundaries
      --append           Append data to shorter destination files
      --sparse           Handle sparse files efficiently (seek over zeros)

PRESERVATION
  -p, --perms            Preserve permissions
  -t, --times            Preserve modification timestamps (nanosecond precision)
  -l, --links            Preserve symbolic links
  -o, --owner            Preserve file owner (requires root)
  -g, --group            Preserve file group
  -X, --xattrs         Preserve extended attributes
  -D, --devices          Preserve device files
      --specials         Preserve special files

FILTERING
      --exclude PATTERN  Exclude files matching glob pattern
      --include PATTERN  Include files matching glob pattern
      --exclude-from F   Read exclude patterns from file
      --include-from F   Read include patterns from file
      --max-size SIZE    Skip files larger than SIZE (e.g. 100M, 2G)
      --min-size SIZE    Skip files smaller than SIZE

DELETE CONTROL
      --delete           Delete extraneous files from destination
      --delete-excluded  Delete excluded files from destination
      --max-delete NUM   Don't delete more than NUM files
      --force            Force deletion of non-empty directories
      --ignore-errors    Delete even when there are I/O errors

BACKUP
      --backup           Make backups of replaced files
      --backup-dir DIR   Move replaced files into DIR
      --suffix SUFFIX    Backup suffix (default: ~)
      --link-dest DIR    Hardlink unchanged files to DIR

MODES
      --existing         Only update files already on destination
      --ignore-existing  Skip files that exist on destination
      --inplace          Update files in-place (faster, not crash-safe)
      --partial          Keep partially transferred files
      --size-only        Compare files by size only (ignore mtime)
      --modify-window N  Timestamp comparison window (seconds)

OUTPUT
  -P, --progress         Show real-time progress bar
      --stats            Print final transfer summary
  -i, --itemize-changes  Output change summary for each file
      --log-file FILE    Write detailed log to file
      --human-readable   Human-readable numbers

PERFORMANCE
  -j, --jobs N           Worker threads (default: all CPUs)
      --chunk-size N     BLAKE3 chunk size in bytes (default: 8192)
      --bwlimit RATE     Bandwidth limit in KB/s
      --compress-level N Zstd compression level (1-22, default: 3)
      --fsync            Force fsync after each file write
      --timeout SECS     I/O timeout in seconds

ADVANCED
      --trust-mtime      Ultra-fast no-change detection via dir mtimes
                         (~120x faster at 100K files on tested hardware;
                         safe for deployments, not for
                         in-place edits -- see Benchmarks above)
      --files-from FILE  Read list of source files from FILE (one per line)
      --chmod PERMS      Override destination permissions (e.g. "u+rw,go+r")
      --chown USER:GRP   Override destination owner:group (e.g. "nobody:nogroup")
      --checksum-verify  BLAKE3 integrity check after every file transfer
      --reflink          Use CoW reflinks on btrfs / XFS (instant copies)
      --manifest-dir DIR Store manifest cache in DIR instead of destination

NETWORK
  resync serve           Start server daemon
    --bind ADDR          Bind address (default: 0.0.0.0)
    --port PORT          Listen port (default: 2377)
    --tls                Enable TLS 1.3 encryption
    --tls-cert FILE      Custom TLS certificate
    --tls-key FILE       Custom TLS private key

  resync push SRC HOST   Push to remote server
    --tls                Enable TLS 1.3 encryption
    --tls-verify         Verify server certificate against system CAs

  resync pull HOST DST   Pull from remote server to local
    --tls                Enable TLS 1.3 encryption
    --tls-verify         Verify server certificate against system CAs
```

### Examples

```bash
# Mirror a directory tree with all metadata
resync -avz /home/user/documents/ /mnt/backup/documents/

# Sync only changed files, skip large files
resync -avz --max-size 100M /data/ /backup/

# Incremental backup chain (hardlink unchanged files)
resync -aH --link-dest /backups/2024-01-14 /home/ /backups/2024-01-15/

# Exclude logs and temp files
resync -avz --exclude '*.log' --exclude-from .rsyncignore /app/ /backup/

# Bandwidth-limited sync over slow link
resync -avz --bwlimit 1000 /data/ /remote-mount/

# Sparse file-aware sync
resync -avz --sparse /var/lib/vms/ /backup/vms/

# Don't cross filesystem boundaries
resync -avx /data/ /backup/

# Network push with TLS encryption
resync push -avz --tls /data/source/ server.example.com:2377:/data/dest/

# Server mode with auto-generated self-signed cert
resync serve --port 2377 --tls

# Server mode with custom certificate
resync serve --port 2377 --tls --tls-cert cert.pem --tls-key key.pem

# Maximum throughput: all cores, large chunks, no fsync
resync -avz -j $(nproc) --chunk-size 65536 /data/ /backup/

# Maximum safety: checksum verification, fsync each file
resync -avzc --fsync /data/ /critical-backup/

# Ultra-fast re-sync for CI/deployment (trust dir mtimes)
resync -avz --trust-mtime /build/output/ /deploy/

# Sync only files listed in a manifest
resync -avz --files-from filelist.txt /src/ /dst/

# Override permissions and ownership during sync
resync -avz --chmod "u+rw,go+r" --chown "www-data:www-data" /app/ /deploy/

# Post-transfer integrity verification with BLAKE3
resync -avz --checksum-verify /data/ /backup/

# Instant CoW copies on btrfs/XFS
resync -avz --reflink /snapshots/latest/ /snapshots/working/

# Pull from a remote server
resync pull -avz --tls server.example.com:2377:/data/source/ /local/dest/
```

---

## Security

- **BLAKE3** (256-bit): Cryptographically secure hash function, immune to
  length-extension attacks. No known collisions or preimage attacks.
- **TLS 1.3** via rustls: Memory-safe TLS implementation with no OpenSSL
  dependency. Supports certificate verification against system CAs.
- **Atomic writes**: Files are written to temp files first, then renamed.
  A crash or power failure never leaves a corrupt file at the destination path.
- **No unsafe network parsing**: The binary protocol uses serde + bincode
  with length-prefixed framing. No manual buffer management.
- **Path traversal protection**: Server and client validate all relative
  paths, rejecting `..` components and absolute paths. Server refuses to
  write into system directories (/etc, /proc, /sys, /dev, /boot).
- **Decompression bomb limit**: Zstd-compressed network messages are capped
  at 256 MiB decompressed. Oversized payloads are rejected.
- **setuid/setgid stripping**: Non-root users automatically strip setuid
  and setgid bits from synced files, preventing privilege escalation.
- **Streaming file writes**: Pull mode streams data to disk via temp files
  instead of accumulating in memory, preventing OOM on large transfers.

See [SECURITY.md](SECURITY.md) for vulnerability reporting.

---

## Sustainability

resync is designed to minimize compute and energy consumption:

- **SIMD acceleration**: BLAKE3 uses AVX-512/AVX2/NEON to process 16-64
  bytes per CPU cycle, doing the same work in 1/10th the clock cycles vs MD5.
- **Zero-copy I/O**: `mmap()` and `copy_file_range()` eliminate userspace
  buffer copies. Each byte passes through the CPU cache at most once.
- **Content-Defined Chunking**: A 1-byte edit in a 1 GB file transfers only
  the affected chunk (~8 KB), not the whole file.

The 2-3x wall-clock speedup (up to ~120x with `--trust-mtime`) translates
directly to proportionally less CPU time and energy per sync operation.

---

## Design Philosophy

rsync was designed in 1996 for single-core CPUs, spinning disks, and 10 Mbps
networks. Modern hardware has evolved dramatically:

- **CPUs** have 16-64 cores, but rsync uses exactly one.
- **NVMe SSDs** sustain 7 GB/s sequential reads, but rsync's read()/write()
  loop caps at ~500 MB/s.
- **Networks** are 10-100 Gbps, but rsync's single-threaded pipeline cannot
  saturate even a 1 Gbps link for many small files.
- **MD5** is cryptographically broken (known collision attacks) and has no
  SIMD acceleration.

resync is a ground-up redesign built on five principles:

1. **Parallel everything.** Every pipeline stage runs on a work-stealing pool.
2. **Zero-copy everywhere.** mmap, copy_file_range, fallocate -- minimize
   data movement between kernel and userspace.
3. **Modern cryptography.** BLAKE3 is faster and stronger than MD5.
4. **Memory safety.** Rust prevents data races, buffer overflows, and
   use-after-free bugs at compile time.
5. **rsync compatibility.** Drop-in replacement -- same flags, same behavior.

---

## Standing On

- **[rsync](https://rsync.samba.org/)** by Andrew Tridgell and Paul Mackerras
- **[BLAKE3](https://github.com/BLAKE3-team/BLAKE3)** by Jack O'Connor et al.
- **[FastCDC](https://www.usenix.org/conference/atc16/technical-sessions/presentation/xia)** by Wen Xia et al.
- **[rayon](https://github.com/rayon-rs/rayon)** by Niko Matsakis et al.
- **[zstd](https://facebook.github.io/zstd/)** by Yann Collet

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, coding
standards, and pull request guidelines.

## Code of Conduct

See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## License

MIT -- see [LICENSE](LICENSE) for details.

---

<div align="center">

Built with Rust. Engineered for speed.

[Website](https://vedLinuxian.github.io/resync-rs) |
[GitHub](https://github.com/vedLinuxian/resync-rs) |
[Crates.io](https://crates.io/crates/resync-rs)

</div>