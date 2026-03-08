//! CLI — drop-in replacement for rsync's interface + Phase 2 subcommands.
//!
//! Legacy mode (Phase 1, rsync-compatible):
//!   `resync [OPTIONS] SOURCE DEST`
//!
//! Subcommand mode (Phase 2, network):
//!   `resync serve [OPTIONS]`
//!   `resync push [OPTIONS] SOURCE HOST:DEST`

use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// resync-rs — next-generation parallel delta-sync engine written in Rust.
///
/// 100% compatible with rsync's common flags.  Drop-in replacement for local
/// and remote transfers.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "resync",
    version,
    about = "Next-generation parallel delta-sync engine (rsync reimagined in Rust)",
    long_about = None,
    after_help = "EXAMPLES:\n  resync -avz /data/source/ /data/backup/\n  resync serve --port 2377\n  resync push -av /src/ 192.168.1.10:/dst/\n  resync -avz --exclude '*.log' --exclude-from .rsyncignore /src/ /dst/\n  resync -avci /data/source/ /data/backup/\n  resync --backup --backup-dir /backups --suffix .bak /src/ /dst/"
)]
pub struct Cli {
    /// Subcommand (serve | push). Omit for local rsync-compatible sync.
    #[command(subcommand)]
    pub command: Option<Command>,

    /// Source directory or file (local sync mode)
    pub source: Option<PathBuf>,

    /// Destination directory or file (local sync mode)
    pub dest: Option<PathBuf>,

    // ───── rsync-compatible shorthand flags ──────────────────────────────────
    /// Archive mode: preserves permissions, symlinks, timestamps (-rlptgoD)
    #[arg(short = 'a', long = "archive", global = true)]
    pub archive: bool,

    /// Verbose output — print each file being processed
    #[arg(short = 'v', long = "verbose", global = true)]
    pub verbose: bool,

    /// Compress data during transfer (zstd)
    #[arg(short = 'z', long = "compress", global = true)]
    pub compress: bool,

    /// Show real-time progress bar and transfer metrics
    #[arg(long = "progress", short = 'P', global = true)]
    pub progress: bool,

    /// Perform a trial run — show what would be transferred without doing it
    #[arg(short = 'n', long = "dry-run", global = true)]
    pub dry_run: bool,

    /// Delete files in DEST that are not present in SOURCE
    #[arg(long = "delete", global = true)]
    pub delete: bool,

    /// Preserve symbolic links
    #[arg(short = 'l', long = "links", global = true)]
    pub preserve_links: bool,

    /// Preserve file permissions (chmod bits)
    #[arg(short = 'p', long = "perms", global = true)]
    pub preserve_perms: bool,

    /// Preserve file owner/group (chown — requires root)
    #[arg(short = 'o', long = "owner", global = true)]
    pub preserve_owner: bool,

    /// Preserve file modification timestamps
    #[arg(short = 't', long = "times", global = true)]
    pub preserve_times: bool,

    /// Recurse into directories
    #[arg(short = 'r', long = "recursive", global = true)]
    pub recursive: bool,

    // ───── exclude / include filters ─────────────────────────────────────────
    /// Exclude files matching PATTERN (glob). May be repeated.
    /// Examples: --exclude '*.log' --exclude 'tmp/' --exclude '.git/'
    #[arg(long = "exclude", value_name = "PATTERN", action = clap::ArgAction::Append, global = true)]
    pub exclude: Vec<String>,

    /// Override an exclude for files matching PATTERN. May be repeated.
    /// Rules are evaluated in order (first match wins, like rsync).
    #[arg(long = "include", value_name = "PATTERN", action = clap::ArgAction::Append, global = true)]
    pub include: Vec<String>,

    /// Read exclude patterns from FILE (one per line, # comments allowed)
    #[arg(long = "exclude-from", value_name = "FILE", action = clap::ArgAction::Append, global = true)]
    pub exclude_from: Vec<PathBuf>,

    // ───── bandwidth limiting ────────────────────────────────────────────────
    /// Limit I/O bandwidth to RATE kilobytes per second (0 = unlimited).
    /// Same semantics as rsync --bwlimit.
    #[arg(
        long = "bwlimit",
        value_name = "RATE",
        default_value_t = 0,
        global = true
    )]
    pub bwlimit: u64,

    // ───── checksum mode ─────────────────────────────────────────────────────
    /// Skip the mtime+size fast path — always hash files to detect changes.
    /// Equivalent to rsync -c.
    #[arg(short = 'c', long = "checksum", global = true)]
    pub checksum: bool,

    // ───── backup mode ───────────────────────────────────────────────────────
    /// Make backups: rename replaced files with a suffix before overwriting
    #[arg(long = "backup", global = true)]
    pub backup: bool,

    /// Move replaced files into DIR instead of renaming in-place.
    /// Implies --backup.
    #[arg(long = "backup-dir", value_name = "DIR", global = true)]
    pub backup_dir: Option<PathBuf>,

    /// Change the backup suffix (default: ~). Used with --backup.
    #[arg(
        long = "suffix",
        value_name = "SUFFIX",
        default_value = "~",
        global = true
    )]
    pub suffix: String,

    // ───── itemize changes ───────────────────────────────────────────────────
    /// Output a change-summary for all updates (like rsync -i).
    /// Format: >f.st...... file.txt
    #[arg(short = 'i', long = "itemize-changes", global = true)]
    pub itemize_changes: bool,

    // ───── log file ──────────────────────────────────────────────────────────
    /// Write a detailed transfer log to FILE
    #[arg(long = "log-file", value_name = "FILE", global = true)]
    pub log_file: Option<PathBuf>,

    // ───── resync-specific performance knobs ─────────────────────────────────
    /// Chunk size in bytes for BLAKE3 delta hashing (min: 512, default: 8192)
    #[arg(
        long = "chunk-size",
        default_value_t = 8192,
        value_name = "BYTES",
        value_parser = clap::value_parser!(u64).range(512..),
        global = true,
    )]
    pub chunk_size: u64,

    /// Number of parallel worker threads (default: all logical CPUs)
    #[arg(
        short = 'j',
        long = "jobs",
        default_value_t = num_cpus(),
        value_name = "N",
        global = true,
    )]
    pub threads: usize,

    /// Log level: error | warn | info | debug | trace
    #[arg(
        long = "log-level",
        default_value = "warn",
        value_name = "LEVEL",
        global = true
    )]
    pub log_level: String,

    /// Print final benchmark summary (throughput, CPU utilisation, time)
    #[arg(long = "stats", global = true)]
    pub stats: bool,
}

#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    /// Start the resync server daemon (listens for incoming syncs)
    Serve {
        /// Address to bind to
        #[arg(long = "bind", default_value = "0.0.0.0")]
        bind: String,

        /// Port to listen on
        #[arg(long = "port", default_value_t = 2377)]
        port: u16,

        /// Enable TLS for encrypted transport
        #[arg(long = "tls")]
        tls: bool,

        /// Path to TLS certificate file (PEM). Auto-generated self-signed if omitted.
        #[arg(long = "tls-cert", value_name = "FILE")]
        tls_cert: Option<PathBuf>,

        /// Path to TLS private key file (PEM). Auto-generated if omitted.
        #[arg(long = "tls-key", value_name = "FILE")]
        tls_key: Option<PathBuf>,
    },

    /// Push files to a remote resync server
    Push {
        /// Local source directory
        source: PathBuf,

        /// Remote destination in the form HOST:PATH or HOST:PORT:PATH
        remote: String,

        /// Enable TLS encryption for the connection
        #[arg(long = "tls")]
        tls: bool,

        /// Verify the server's TLS certificate against system CAs
        /// (by default, self-signed certificates are accepted)
        #[arg(long = "tls-verify")]
        tls_verify: bool,
    },
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

impl Cli {
    /// Resolve implied flags from `-a` (archive) shorthand, same as rsync.
    /// Note: `-a` implies -rlpt. Owner (-o) requires root so it is NOT
    /// auto-enabled (user must pass -o explicitly).
    pub fn resolve_archive(&mut self) {
        if self.archive {
            self.recursive = true;
            self.preserve_links = true;
            self.preserve_perms = true;
            self.preserve_times = true;
        }
    }

    /// If `--backup-dir` is set, force `--backup` on.
    pub fn resolve_backup(&mut self) {
        if self.backup_dir.is_some() {
            self.backup = true;
        }
    }

    /// Build a [`crate::filter::FilterEngine`] from the CLI exclude/include
    /// flags.  Returns an error only if an `--exclude-from` file cannot be
    /// read.
    pub fn build_filter_engine(&self) -> anyhow::Result<crate::filter::FilterEngine> {
        crate::filter::FilterEngine::from_cli_patterns(
            &self.exclude,
            &self.include,
            &self.exclude_from,
        )
    }

    /// Build a [`crate::filter::RateLimiter`] from `--bwlimit`.
    pub fn build_rate_limiter(&self) -> crate::filter::RateLimiter {
        crate::filter::RateLimiter::new(self.bwlimit)
    }
}

/// Parse a remote string like "192.168.1.10:/path" or "host:2377:/path".
/// Returns (addr, path).
pub fn parse_remote(remote: &str) -> anyhow::Result<(std::net::SocketAddr, PathBuf)> {
    // Try HOST:PORT:PATH first
    let parts: Vec<&str> = remote.splitn(3, ':').collect();
    match parts.len() {
        3 => {
            // HOST:PORT:PATH
            let host = parts[0];
            let port: u16 = parts[1]
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid port in remote spec: {}", parts[1]))?;
            let path = PathBuf::from(parts[2]);
            let addr: std::net::SocketAddr = format!("{host}:{port}").parse()?;
            Ok((addr, path))
        }
        2 => {
            // HOST:PATH — default port 2377
            let host = parts[0];
            let path = PathBuf::from(parts[1]);
            let addr: std::net::SocketAddr = format!("{host}:2377").parse()?;
            Ok((addr, path))
        }
        _ => {
            anyhow::bail!("invalid remote format: use HOST:PATH or HOST:PORT:PATH\n  got: {remote}")
        }
    }
}
