<div align="center">

# ⚡ resync-rs

### Next-Generation Parallel Delta-Sync Engine — rsync Reimagined in Rust

[![CI](https://github.com/vedprakashpandey/resync-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/vedprakashpandey/resync-rs/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.75%2B-orange.svg)](https://www.rust-lang.org)
[![Crates.io](https://img.shields.io/badge/crates.io-v0.1.0-green.svg)](https://crates.io/crates/resync-rs)

**1.5x–6x faster than rsync** • **Parallel everything** • **Zero-copy I/O** • **96%+ delta savings**

[Homepage](https://vedprakashpandey.github.io/resync-rs) · [Installation](#installation) · [Usage](#usage) · [Benchmarks](#benchmarks) · [Carbon Impact](#-carbon-footprint-reduction)

</div>

---

## Why resync-rs?

`rsync` is a 30-year-old single-threaded C tool. Modern hardware has 8–128 cores, NVMe drives with 7 GB/s bandwidth, and AVX-512 SIMD units — rsync uses none of them.

**resync-rs** is a ground-up Rust rewrite that parallelizes every stage of the sync pipeline:

| Stage | rsync | resync-rs |
|---|---|---|
| Directory scan | Single-threaded `readdir` | Parallel `jwalk` across all cores |
| Checksumming | MD5 (single-threaded, no SIMD) | BLAKE3 (multi-threaded, SIMD/AVX-512) |
| Delta detection | Rolling checksum (sequential) | Content-Defined Chunking (parallel) |
| File I/O | `read()`/`write()` syscalls | Zero-copy `mmap` + 256 KB buffered writes |
| Network | Single TCP stream | TCP + optional TLS 1.3 |

---

## Features

- 🚀 **Parallel directory walking** — jwalk scans directory trees across all cores
- ⚡ **BLAKE3 hashing** — cryptographic, SIMD-accelerated, multi-threaded (10 GB/s+ per core)
- 🧩 **Content-Defined Chunking (CDC)** — Gear-hash based deduplication with configurable chunk sizes
- 📦 **Delta sync** — only transfers changed chunks (96%+ bandwidth savings)
- 🔒 **TLS 1.3 network mode** — TCP client-server with optional encryption
- 🗑️ **`--delete` support** — removes orphan files at destination
- 🎯 **`--dry-run`** — preview changes without writing
- 📊 **Rich progress** — real-time progress bars with ETA, throughput, and delta stats
- 🔗 **Symlink handling** — follows or preserves symlinks
- 🎛️ **Include/exclude filters** — rsync-compatible glob patterns with `**` support
- 🪶 **Zero-copy I/O** — memory-mapped files avoid redundant copies
- 📁 **Archive mode** (`-a`) — preserves timestamps, permissions, recursive by default
- 🌍 **Lower carbon footprint** — finishes faster = less CPU energy consumed

---

## Installation

### From source (recommended)

```bash
git clone https://github.com/vedprakashpandey/resync-rs.git
cd resync-rs
cargo build --release
# Binary at target/release/resync (4.7 MB, statically optimized)
```

### Requirements

- Rust 1.75+ (edition 2021)
- Linux/macOS (Windows untested)

---

## Usage

### Local sync (like rsync)

```bash
# Sync a directory (archive mode — recursive, preserves timestamps/perms)
resync -a /path/to/source/ /path/to/dest/

# Use all 8 cores for parallel hashing
resync -a -j 8 /src/ /dst/

# Delete files in dest that don't exist in source
resync -a --delete /src/ /dst/

# Dry-run — see what would change without writing
resync -a --dry-run /src/ /dst/

# Exclude patterns
resync -a --exclude '*.log' --exclude 'node_modules/' /src/ /dst/

# Verbose output with itemized changes
resync -avv /src/ /dst/
```

### Network sync (TCP client-server)

```bash
# Start server on remote machine
resync serve --listen 0.0.0.0:8730 --root /data/backup/

# Push files to remote server
resync push /local/data/ --server 192.168.1.100:8730

# With TLS encryption
resync serve --listen 0.0.0.0:8730 --root /data/ --cert server.pem --key server-key.pem
resync push /local/ --server 192.168.1.100:8730 --tls --ca-cert ca.pem
```

### CLI Reference

```
resync [OPTIONS] <SOURCE> <DEST>

Options:
  -a, --archive          Archive mode (recursive, preserve times/perms)
  -r, --recursive        Recurse into directories
  -j, --jobs <N>         Number of parallel threads (default: CPU count)
      --delete           Delete files in dest not in source
  -n, --dry-run          Show what would be done without making changes
  -v, --verbose          Increase verbosity (-vv for itemized)
      --exclude <PAT>    Exclude files matching glob pattern
      --include <PAT>    Include files matching glob pattern
  -L, --copy-links       Follow symlinks (default in archive mode)
  -B, --backup           Create backup of replaced files
      --chunk-size <N>   CDC target chunk size in bytes (default: 65536)
  -h, --help             Show help
  -V, --version          Show version
```

---

## Benchmarks

Tested on **Intel i3-1215U (8 cores)** • Linux 6.x • NVMe SSD • rsync 3.2.7 vs resync 0.1.0

### Scenario 1: 10,000 Small Files (40 MB)

```
┌────────────────────────┬─────────┬─────────┬─────────┐
│ Operation              │ rsync   │ resync  │ Speedup │
├────────────────────────┼─────────┼─────────┼─────────┤
│ Cold sync (all new)    │ 433 ms  │ 179 ms  │  2.4x   │
│ Warm sync (no change)  │ 120 ms  │  48 ms  │  2.5x   │
│ Delta (2% mutated)     │ 182 ms  │ 105 ms  │  1.7x   │
└────────────────────────┴─────────┴─────────┴─────────┘
```

### Scenario 2: 10 × 50 MB Files (500 MB)

```
┌────────────────────────┬─────────┬─────────┬─────────┐
│ Operation              │ rsync   │ resync  │ Speedup │
├────────────────────────┼─────────┼─────────┼─────────┤
│ Cold sync (all new)    │ 1253 ms │ 665 ms  │  1.9x   │
│ Warm sync (no change)  │  53 ms  │   9 ms  │  5.9x   │
│ Delta (3/10 changed)   │ 472 ms  │ 323 ms  │  1.5x   │
└────────────────────────┴─────────┴─────────┴─────────┘
```

### Scenario 3: 5,000 Mixed Files (290 MB)

```
┌────────────────────────┬─────────┬─────────┬─────────┐
│ Operation              │ rsync   │ resync  │ Speedup │
├────────────────────────┼─────────┼─────────┼─────────┤
│ Cold sync (all new)    │ 1393 ms │ 234 ms  │  6.0x   │
│ Warm sync (no change)  │  90 ms  │  27 ms  │  3.3x   │
└────────────────────────┴─────────┴─────────┴─────────┘
```

### Delta Efficiency

A 1-byte change in an 800 KB file transfers only **8–33 KB** (96%+ savings), verified with `diff -rq`.

---

## 🌍 Carbon Footprint Reduction

File synchronization is one of the most common operations in cloud infrastructure, CI/CD pipelines, backup systems, and developer workflows. **Every millisecond of CPU time consumes energy and produces CO₂.**

### The Math

Modern server CPUs consume approximately **150–250 W** under load. Using the global average grid carbon intensity of **0.475 kg CO₂/kWh** ([Ember Climate, 2024](https://ember-climate.org/data/)), we can calculate the carbon cost of sync operations:

| Metric | Formula |
|---|---|
| Energy per sync | $E = P \times t$ |
| CO₂ per sync | $CO_2 = E \times I_{grid}$ |
| Annual savings | $\Delta CO_2 = N_{syncs} \times (CO_{2_{rsync}} - CO_{2_{resync}})$ |

Where $P$ = CPU power (W), $t$ = sync duration (s), $I_{grid}$ = grid carbon intensity (kg CO₂/kWh).

### Per-Operation Savings

Using measured benchmark data (200 W TDP, 8 cores):

```
Energy per sync:
  rsync  cold  (500 MB): 200W × 1.253s = 250.6 J  = 0.0696 Wh
  resync cold  (500 MB): 200W × 0.665s = 133.0 J  = 0.0369 Wh
  ─────────────────────────────────────────────────────────
  Savings per sync:              117.6 J  = 0.0327 Wh  (47%)

CO₂ per sync:
  rsync:  0.0696 Wh × 0.475 kg/kWh = 0.0331 g CO₂
  resync: 0.0369 Wh × 0.475 kg/kWh = 0.0175 g CO₂
  ─────────────────────────────────────────────────────────
  Savings per sync:                    0.0156 g CO₂  (47%)
```

### At Scale: Annual Impact

```
┌──────────────────────────┬──────────────┬──────────────┬──────────────┐
│ Deployment Scale         │ Syncs/Year   │ Energy Saved │ CO₂ Saved    │
├──────────────────────────┼──────────────┼──────────────┼──────────────┤
│ Solo developer           │     10,000   │    0.33 kWh  │    0.16 kg   │
│ Small team (10 devs)     │    100,000   │    3.27 kWh  │    1.55 kg   │
│ CI/CD pipeline (mid)     │  1,000,000   │   32.70 kWh  │   15.53 kg   │
│ Enterprise (1000 nodes)  │ 10,000,000   │  327.00 kWh  │  155.33 kg   │
│ Cloud provider (global)  │  1 billion   │ 32,700  kWh  │ 15,533  kg   │
│                          │              │              │ (15.5 tons)  │
└──────────────────────────┴──────────────┴──────────────┴──────────────┘
```

### Visualization: Energy per Sync Operation

```
Energy Consumption per 500 MB Sync (Joules)
═══════════════════════════════════════════════════

rsync   ██████████████████████████████████████████████████  250.6 J
resync  ██████████████████████████                          133.0 J
                                                        ▲
                                                   47% less energy

CO₂ Emissions per Sync (milligrams)
═══════════════════════════════════════════════════

rsync   ██████████████████████████████████████████████████  33.1 mg
resync  ██████████████████████████                          17.5 mg
                                                        ▲
                                                   47% less CO₂
```

### Equivalence: What 15.5 Tons CO₂ Savings Means

At cloud-provider scale (1 billion syncs/year), switching from rsync to resync saves **15.5 metric tons of CO₂**, equivalent to:

```
🚗  Driving 62,000 km (38,500 miles) — 1.5× around Earth
🌳  Planting 775 trees and letting them grow for 1 year
💡  Powering 10 homes for an entire year
✈️  Canceling 5 transatlantic flights (NYC ↔ London)
⛽  Burning 6,500 liters of gasoline NOT consumed
```

### Why resync-rs Is Greener

1. **Parallel execution** — 8 cores finish in 1/3 the wall-clock time, so the CPU complex powers down sooner
2. **Zero-copy mmap** — avoids redundant memory copies that waste cache energy
3. **BLAKE3 SIMD** — processes 10 GB/s per core using hardware vector units (vs MD5's 400 MB/s scalar path)
4. **Smart delta** — 96%+ bandwidth savings means less network I/O energy
5. **Fast warm-sync** — 5.9× faster no-op detection means the CPU barely wakes up

> *"The greenest computation is the one that finishes fastest."*

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    resync-rs Pipeline                    │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐            │
│  │ Scanner  │──▶│ Hasher   │──▶│  Delta   │            │
│  │ (jwalk)  │   │ (BLAKE3) │   │ Engine   │            │
│  │ parallel │   │ parallel │   │          │            │
│  └──────────┘   └──────────┘   └────┬─────┘            │
│                                     │                   │
│                                     ▼                   │
│                               ┌──────────┐              │
│                               │ Applier  │              │
│                               │ (atomic) │              │
│                               │ mmap+buf │              │
│                               └──────────┘              │
│                                                         │
│  ┌──────────────────────────────────────────┐           │
│  │         Network Layer (Phase 2)          │           │
│  │  TCP Client ←──bincode──→ TCP Server     │           │
│  │  Optional TLS 1.3 (tokio-rustls)         │           │
│  └──────────────────────────────────────────┘           │
│                                                         │
│  ┌──────────────────────────────────────────┐           │
│  │         Supporting Modules               │           │
│  │  Filter (glob) · Progress (indicatif)    │           │
│  │  CDC (gear-hash) · Error (thiserror)     │           │
│  └──────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────┘
```

---

## Test Suite

```bash
# Unit tests (52 tests)
cargo test

# End-to-end local sync tests (13 tests)
bash tests/e2e_test.sh

# Network TCP client-server tests (11 tests)
bash tests/network_test.sh
```

**Total: 76 tests** — covering CDC chunking, delta computation, filter patterns, scanner behavior, symlinks, dry-run, delete, network push, delta transfer, idempotency, and large file handling.

---

## Project Structure

```
resync-rs/
├── Cargo.toml          # Dependencies & release profile
├── src/
│   ├── main.rs         # Entry point & CLI dispatch
│   ├── lib.rs          # Public API surface
│   ├── cli.rs          # Clap argument parsing
│   ├── scanner.rs      # Parallel directory walking (jwalk)
│   ├── hasher.rs       # BLAKE3 parallel hashing + mmap
│   ├── cdc.rs          # Content-Defined Chunking (Gear hash)
│   ├── delta.rs        # Delta computation engine
│   ├── applier.rs      # Atomic file application (mmap + BufWriter)
│   ├── sync_engine.rs  # Orchestrator (scan → hash → delta → apply)
│   ├── filter.rs       # Include/exclude glob patterns
│   ├── progress.rs     # Progress bars (indicatif)
│   ├── error.rs        # Error types (thiserror)
│   └── net/
│       ├── mod.rs      # Network module
│       ├── protocol.rs # Binary protocol (bincode)
│       ├── server.rs   # TCP server
│       ├── client.rs   # TCP client
│       └── tls.rs      # TLS 1.3 support
├── tests/
│   ├── e2e_test.sh     # End-to-end local tests + benchmarks
│   └── network_test.sh # TCP client-server tests
└── docs/               # GitHub Pages website
```

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT License — see [LICENSE](LICENSE) for details.

## Author

**Ved Prakash Pandey** — [GitHub](https://github.com/vedprakashpandey)

---

<div align="center">

*Built with 🦀 Rust — because life's too short for single-threaded sync.*

</div>
