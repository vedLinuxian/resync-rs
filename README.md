<div align="center">

# resync

**rsync, rewritten in Rust. Parallel. Zero-copy. 4–56× faster.**

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](LICENSE)
[![Crates.io](https://img.shields.io/crates/v/resync-rs?style=flat-square)](https://crates.io/crates/resync-rs)
[![Rust](https://img.shields.io/badge/rust-2024_edition-orange?style=flat-square)](https://www.rust-lang.org)

[Homepage](https://vedLinuxian.github.io/resync-rs) · [Benchmarks](#benchmarks) · [Install](#install) · [Usage](#usage) · [Architecture](#architecture)

</div>

---

## Benchmarks

All numbers measured in isolated Docker containers with `tc netem` traffic
shaping. rsync 3.2.7 vs resync 1.0.0-beta.1. Reproducible via
`bash bench/docker_network_benchmark.sh`.

### Network — LAN (1 Gbps, 0.5 ms RTT)

```
Scenario                       rsync        resync       Speedup
─────────────────────────────────────────────────────────────────
Full copy (mixed tree)         3.18s        1.62s        2.0×
No-change re-sync              0.71s        0.06s       12.2×
Incremental (5% modified)      2.37s        0.20s       11.8×
Compressible data              8.37s        0.15s       55.7×
Large binary (256 MB)          3.00s        2.45s        1.2×
─────────────────────────────────────────────────────────────────
Geometric mean                                           4.6×
```

### Network — WAN (50 Mbps, 40 ms RTT, 0.5% loss)

```
Scenario                       rsync        resync       Speedup
─────────────────────────────────────────────────────────────────
Full copy (mixed tree)         4.88s        1.72s        2.8×
No-change re-sync              1.88s        0.12s       15.6×
Incremental (5% modified)      2.77s        0.22s       12.5×
Compressible data              9.71s        0.17s       56.0×
Large binary (256 MB)         46.37s       42.06s        1.1×
─────────────────────────────────────────────────────────────────
Geometric mean                                           4.2×
```

### Local — NVMe SSD (warm cache)

```
Scenario                       rsync        resync       Speedup
─────────────────────────────────────────────────────────────────
10K small files: full copy     0.405s       0.188s       2.1×
500 medium files: full copy    0.741s       0.282s       2.6×
5×100 MB: full copy            1.371s       0.647s       2.1×
10K files: no change           0.135s       0.055s       2.4×
10K files: 5% modified         0.194s       0.088s       2.1×
100K files: --trust-mtime      0.760s       0.006s     ~120×
```

Hardware: 12th Gen Intel i3-1215U (8 cores), NVMe ext4.

---

## Why it's faster

| # | Technique | What it does |
|---|---|---|
| 1 | **Parallel everything** | Rayon work-stealing across scan, hash, delta, apply. rsync is single-threaded. |
| 2 | **Zero-copy I/O** | `mmap` + `madvise(SEQUENTIAL\|HUGEPAGE)` + `copy_file_range(2)`. Bytes never enter userspace. |
| 3 | **BLAKE3 SIMD** | 10–15× faster than MD5. Auto-vectorizes with AVX-512 / AVX2 / NEON. |
| 4 | **FastCDC** | Gear-hash content-defined chunking. 1-byte insert doesn't invalidate every chunk. |
| 5 | **Zstd** | 10× faster than zlib at equivalent compression ratios. |
| 6 | **No fsync** | Skips `fsync()` by default. `--fsync` when durability matters. |
| 7 | **Manifest cache** | Binary manifest + TOC skips destination re-scan. `--trust-mtime` skips source stat() too. |
| 8 | **Inode-sorted I/O** | Files processed in inode order. Optimal readahead on SSD and rotational. |
| 9 | **fallocate(2)** | Pre-allocates output files. Zero fragmentation on ext4/xfs/btrfs. |

---

## At a Glance

| | rsync 3.2.7 | resync |
|---|---|---|
| Hash | MD5 (128-bit, broken) | BLAKE3 (256-bit, SIMD) |
| Parallelism | Single-threaded | Rayon work-stealing |
| I/O | read()/write() | mmap + copy_file_range |
| Chunking | Fixed-size | FastCDC (Gear hash) |
| Compression | zlib | zstd |
| TLS | via stunnel | Native TLS 1.3 (rustls) |
| Crash safety | Temp + rename | Temp + rename (atomic) |
| Incremental cache | None | Manifest + TOC |
| Timestamps | 1 s granularity | Nanosecond (utimensat) |
| Sparse files | Basic | FIEMAP + seek-over-zeros |
| Extended attrs | No | `--xattrs` / `-X` |
| Language | C | Rust 2024 |

---

## Install

### From source

```bash
git clone https://github.com/vedLinuxian/resync-rs.git
cd resync-rs
cargo install --path .
```

### From crates.io

```bash
cargo install resync-rs
```

### Prerequisites

- Rust 1.85+ (2024 edition)
- Linux (primary target), macOS, FreeBSD
- CMake + C compiler (for aws-lc-sys TLS backend)

---

## Usage

```bash
# Mirror a directory (like rsync -avz)
resync -avz /data/source/ /data/backup/

# Progress bar + final stats
resync -avzP --stats /data/source/ /data/backup/

# Dry run
resync -avzn /data/source/ /data/backup/

# Incremental backup with hardlinks
resync -aH --link-dest /backups/yesterday /src/ /backups/today/

# Network push with TLS
resync push -avz --tls /data/ server.example.com:2377:/backup/

# Server mode
resync serve --port 2377 --tls

# Pull from remote
resync pull -avz --tls server.example.com:2377:/data/ /local/

# Ultra-fast no-change (CI/CD deployments)
resync -avz --trust-mtime /build/output/ /deploy/

# Sparse-aware sync (VM images)
resync -avz --sparse /var/lib/vms/ /backup/vms/

# Exclude patterns
resync -avz --exclude '*.log' --exclude-from .rsyncignore /app/ /backup/

# Bandwidth limit
resync -avz --bwlimit 1000 /data/ /remote/

# Maximum safety
resync -avzc --fsync --checksum-verify /data/ /critical/

# CoW reflinks (btrfs/XFS)
resync -avz --reflink /snapshots/latest/ /snapshots/working/

# Energy/CO₂ savings report
resync -avz --energy-saved /data/ /backup/
```

<details>
<summary><strong>Full flag reference</strong></summary>

```
TRANSFER
  -a, --archive            Archive mode (-rlptgo)
  -r, --recursive          Recurse into directories
  -v, --verbose            Print each file
  -z, --compress           Zstd compression
  -n, --dry-run            Show what would change
  -u, --update             Skip newer destination files
  -W, --whole-file         Skip delta, copy whole files
  -c, --checksum           Force hash comparison
  -H, --hard-links         Preserve hard links
  -x, --one-file-system    Don't cross filesystem boundaries
      --append             Append to shorter files
      --sparse             Seek over zero blocks

PRESERVATION
  -p, --perms              Permissions
  -t, --times              Timestamps (nanosecond)
  -l, --links              Symbolic links
  -o, --owner              Owner (root)
  -g, --group              Group
  -X, --xattrs             Extended attributes
  -D, --devices            Device files
      --specials           Special files

FILTERING
      --exclude PATTERN    Glob exclude
      --include PATTERN    Glob include
      --exclude-from F     Exclude file
      --include-from F     Include file
      --max-size SIZE      Max file size
      --min-size SIZE      Min file size

DELETE
      --delete             Remove extraneous files
      --delete-excluded    Delete excluded files too
      --max-delete NUM     Limit deletions
      --force              Delete non-empty dirs
      --ignore-errors      Delete despite errors

BACKUP
      --backup             Backup replaced files
      --backup-dir DIR     Backup directory
      --suffix SUFFIX      Backup suffix (default: ~)
      --link-dest DIR      Hardlink unchanged files

MODES
      --existing           Only update existing
      --ignore-existing    Skip existing
      --inplace            In-place (not crash-safe)
      --partial            Keep partial files
      --size-only          Compare by size only
      --modify-window N    Timestamp window (seconds)

OUTPUT
  -P, --progress           Progress bar
      --stats              Transfer summary
  -i, --itemize-changes    Per-file change summary
      --log-file FILE      Log to file
      --human-readable     Human numbers

PERFORMANCE
  -j, --jobs N             Worker threads
      --chunk-size N       Chunk bytes (default: 8192)
      --bwlimit RATE       KB/s bandwidth limit
      --compress-level N   Zstd level (1-22)
      --fsync              Force fsync per file
      --timeout SECS       I/O timeout

ADVANCED
      --trust-mtime        Dir-mtime no-change detection
      --files-from FILE    Source file list
      --chmod PERMS        Override permissions
      --chown USER:GRP     Override ownership
      --checksum-verify    BLAKE3 post-transfer check
      --reflink            CoW reflinks (btrfs/XFS)
      --manifest-dir DIR   Custom manifest location
      --energy-saved       Print energy/CO₂ savings

NETWORK
  resync serve             Server daemon
    --bind ADDR            Bind address
    --port PORT            Port (default: 2377)
    --tls                  TLS 1.3
    --tls-cert FILE        Custom certificate
    --tls-key FILE         Custom key

  resync push SRC HOST     Push to remote
  resync pull HOST DST     Pull from remote
    --tls                  TLS 1.3
    --tls-verify           Verify server cert
```

</details>

---

## Architecture

```
Scanner ──▸ FilterEngine ──▸ Phase 1: Sequential Decision
                                  │
                    ┌──────────── ┼ ────────────┐
                    │             │              │
                  New file     Skip         Update
                    │                          │
              Applier::copy_new()         Hasher (BLAKE3)
              (copy_file_range)                │
                                          CdcChunker (FastCDC)
                                               │
                                          DeltaEngine
                                               │
                                          Applier::apply()

              Phase 2: Parallel I/O (rayon work-stealing)
              Files sorted by inode for optimal disk access
```

### Modules

| Module | Purpose |
|---|---|
| `sync_engine.rs` | Core orchestrator: two-phase pipeline |
| `cdc.rs` | FastCDC content-defined chunking (Gear hash) |
| `fiemap.rs` | FIEMAP ioctl for sparse file detection |
| `scanner.rs` | Adaptive directory walking |
| `hasher.rs` | BLAKE3 + mmap + madvise + hugepages |
| `delta.rs` | Delta computation (index + hash-map) |
| `applier.rs` | Atomic writes, copy_file_range, fallocate |
| `filter.rs` | Glob matching, rate limit, backup |
| `net/` | Binary protocol, async TCP, TLS 1.3 |
| `progress.rs` | Progress bars and transfer statistics |
| `cli.rs` | rsync-compatible CLI (clap, 60+ flags) |

---

## Security

- **BLAKE3** (256-bit) — no known collisions, immune to length-extension
- **TLS 1.3** via rustls — memory-safe, no OpenSSL, forward secrecy
- **Atomic writes** — temp + rename(2), crash never corrupts destination
- **Path traversal guards** — rejects `..` and absolute paths, blocks /etc /proc /sys
- **Decompression limits** — zstd capped at 256 MiB, rejects bombs
- **setuid stripping** — non-root users strip setuid/setgid automatically
- **Streaming writes** — pull mode streams to disk, no OOM on large files

See [SECURITY.md](SECURITY.md) for vulnerability reporting.

---

## Sustainability

```bash
$ resync -avz --energy-saved /data/ /backup/
# → Saved 12.4 CPU-seconds, 0.034 Wh, 0.016 kg CO₂
```

BLAKE3 SIMD processes 16–64 bytes/cycle vs MD5's ~1 byte/cycle.
`copy_file_range` keeps data in kernel space. CDC transfers only changed
chunks. The wall-clock speedup translates directly to less energy per sync.

---

## Standing on

[rsync](https://rsync.samba.org/) · [BLAKE3](https://github.com/BLAKE3-team/BLAKE3) · [FastCDC](https://www.usenix.org/conference/atc16/technical-sessions/presentation/xia) · [rayon](https://github.com/rayon-rs/rayon) · [zstd](https://facebook.github.io/zstd/)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Code of conduct: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## License

MIT — see [LICENSE](LICENSE).

---

<div align="center">

Built with Rust. Engineered for speed.

[Website](https://vedLinuxian.github.io/resync-rs) ·
[GitHub](https://github.com/vedLinuxian/resync-rs) ·
[Crates.io](https://crates.io/crates/resync-rs)

</div>
