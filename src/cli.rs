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
    after_help = "EXAMPLES:\n  resync -avz /data/source/ /data/backup/\n  resync serve --port 2377\n  resync push -av /src/ 192.168.1.10:/dst/\n  resync -avz --exclude '*.log' --exclude-from .rsyncignore /src/ /dst/\n  resync -avci /data/source/ /data/backup/\n  resync --backup --backup-dir /backups --suffix .bak /src/ /dst/\n  resync -aH --link-dest /backups/yesterday /src/ /backups/today/"
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
    /// Archive mode: preserves permissions, symlinks, timestamps, group, owner (-rlptgo)
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

    /// Also delete excluded files from destination (use with --delete)
    #[arg(long = "delete-excluded", global = true)]
    pub delete_excluded: bool,

    /// Preserve symbolic links
    #[arg(short = 'l', long = "links", global = true)]
    pub preserve_links: bool,

    /// Preserve file permissions (chmod bits)
    #[arg(short = 'p', long = "perms", global = true)]
    pub preserve_perms: bool,

    /// Preserve file owner (chown — requires root or CAP_CHOWN)
    #[arg(short = 'o', long = "owner", global = true)]
    pub preserve_owner: bool,

    /// Preserve file group (chgrp — may require privileges)
    #[arg(short = 'g', long = "group", global = true)]
    pub preserve_group: bool,

    /// Preserve file modification timestamps
    #[arg(short = 't', long = "times", global = true)]
    pub preserve_times: bool,

    /// Recurse into directories
    #[arg(short = 'r', long = "recursive", global = true)]
    pub recursive: bool,

    /// Preserve hard links between files
    #[arg(short = 'H', long = "hard-links", global = true)]
    pub hard_links: bool,

    /// Preserve device files (requires root)
    #[arg(short = 'D', long = "devices", global = true)]
    pub devices: bool,

    /// Preserve special files (named pipes, sockets)
    #[arg(long = "specials", global = true)]
    pub specials: bool,

    // ───── transfer control flags ────────────────────────────────────────────
    /// Skip files that are newer on the destination
    #[arg(short = 'u', long = "update", global = true)]
    pub update: bool,

    /// Disable delta-transfer algorithm — always copy whole files.
    /// Faster on fast links where delta overhead exceeds savings.
    #[arg(short = 'W', long = "whole-file", global = true)]
    pub whole_file: bool,

    /// Don't cross filesystem boundaries during scan
    #[arg(short = 'x', long = "one-file-system", global = true)]
    pub one_file_system: bool,

    /// Only update files that already exist on the receiver
    #[arg(long = "existing", global = true)]
    pub existing: bool,

    /// Only transfer files that don't exist on the receiver
    #[arg(long = "ignore-existing", global = true)]
    pub ignore_existing: bool,

    /// Update destination files in-place (no temp file + rename)
    #[arg(long = "inplace", global = true)]
    pub inplace: bool,

    /// Append data onto shorter destination files
    #[arg(long = "append", global = true)]
    pub append: bool,

    /// Keep partially transferred files on interrupt
    #[arg(long = "partial", global = true)]
    pub partial: bool,

    /// Handle sparse files efficiently (seek over zero blocks)
    #[arg(long = "sparse", global = true)]
    pub sparse: bool,

    // ───── exclude / include filters ─────────────────────────────────────────
    /// Exclude files matching PATTERN (glob). May be repeated.
    #[arg(long = "exclude", value_name = "PATTERN", action = clap::ArgAction::Append, global = true)]
    pub exclude: Vec<String>,

    /// Override an exclude for files matching PATTERN. May be repeated.
    #[arg(long = "include", value_name = "PATTERN", action = clap::ArgAction::Append, global = true)]
    pub include: Vec<String>,

    /// Read exclude patterns from FILE (one per line, # comments allowed)
    #[arg(long = "exclude-from", value_name = "FILE", action = clap::ArgAction::Append, global = true)]
    pub exclude_from: Vec<PathBuf>,

    /// Read include patterns from FILE (one per line, # comments allowed)
    #[arg(long = "include-from", value_name = "FILE", action = clap::ArgAction::Append, global = true)]
    pub include_from: Vec<PathBuf>,

    // ───── size filters ──────────────────────────────────────────────────────
    /// Skip files larger than SIZE (e.g. "100M", "2G")
    #[arg(long = "max-size", value_name = "SIZE", global = true)]
    pub max_size: Option<String>,

    /// Skip files smaller than SIZE (e.g. "1K", "100")
    #[arg(long = "min-size", value_name = "SIZE", global = true)]
    pub min_size: Option<String>,

    // ───── bandwidth limiting ────────────────────────────────────────────────
    /// Limit I/O bandwidth to RATE kilobytes per second (0 = unlimited).
    #[arg(long = "bwlimit", value_name = "RATE", default_value_t = 0, global = true)]
    pub bwlimit: u64,

    // ───── checksum mode ─────────────────────────────────────────────────────
    /// Skip the mtime+size fast path — always hash files to detect changes.
    #[arg(short = 'c', long = "checksum", global = true)]
    pub checksum: bool,

    /// Skip based on size match only — ignore modification time
    #[arg(long = "size-only", global = true)]
    pub size_only: bool,

    /// Timestamp comparison window in seconds (for FAT32 etc.)
    #[arg(long = "modify-window", value_name = "SECONDS", default_value_t = 0, global = true)]
    pub modify_window: u32,

    // ───── backup mode ───────────────────────────────────────────────────────
    /// Make backups: rename replaced files with a suffix before overwriting
    #[arg(long = "backup", global = true)]
    pub backup: bool,

    /// Move replaced files into DIR instead of renaming in-place. Implies --backup.
    #[arg(long = "backup-dir", value_name = "DIR", global = true)]
    pub backup_dir: Option<PathBuf>,

    /// Change the backup suffix (default: ~). Used with --backup.
    #[arg(long = "suffix", value_name = "SUFFIX", default_value = "~", global = true)]
    pub suffix: String,

    // ───── incremental backup ────────────────────────────────────────────────
    /// Hardlink to files in DIR when unchanged (for incremental backup chains)
    #[arg(long = "link-dest", value_name = "DIR", global = true)]
    pub link_dest: Option<PathBuf>,

    // ───── itemize changes ───────────────────────────────────────────────────
    /// Output a change-summary for all updates (like rsync -i).
    #[arg(short = 'i', long = "itemize-changes", global = true)]
    pub itemize_changes: bool,

    // ───── log file ──────────────────────────────────────────────────────────
    /// Write a detailed transfer log to FILE
    #[arg(long = "log-file", value_name = "FILE", global = true)]
    pub log_file: Option<PathBuf>,

    // ───── delete safety ─────────────────────────────────────────────────────
    /// Don't delete more than NUM files (--delete safety limit)
    #[arg(long = "max-delete", value_name = "NUM", global = true)]
    pub max_delete: Option<u64>,

    /// Force deletion of non-empty directories when replaced by files
    #[arg(long = "force", global = true)]
    pub force: bool,

    /// Delete files even when there are I/O errors
    #[arg(long = "ignore-errors", global = true)]
    pub ignore_errors: bool,

    /// Prune empty directory chains from transfer
    #[arg(short = 'm', long = "prune-empty-dirs", global = true)]
    pub prune_empty_dirs: bool,

    // ───── output formatting ─────────────────────────────────────────────────
    /// Output numbers in human-readable format
    #[arg(long = "human-readable", global = true)]
    pub human_readable: bool,

    // ───── privilege control ─────────────────────────────────────────────────
    /// Don't map uid/gid to names — transfer numeric IDs
    #[arg(long = "numeric-ids", global = true)]
    pub numeric_ids: bool,

    /// Attempt super-user activities even when not root
    #[arg(long = "super", global = true)]
    pub super_mode: bool,

    // ───── I/O timeout ───────────────────────────────────────────────────────
    /// I/O timeout in seconds (0 = no timeout)
    #[arg(long = "timeout", value_name = "SECONDS", default_value_t = 0, global = true)]
    pub timeout: u64,

    // ───── resync-specific performance knobs ─────────────────────────────────
    /// Chunk size in bytes for BLAKE3 delta hashing (min: 512, default: 8192)
    #[arg(
        long = "chunk-size",
        visible_alias = "block-size",
        default_value_t = 8192,
        value_name = "BYTES",
        value_parser = clap::value_parser!(u64).range(512..),
        global = true,
    )]
    pub chunk_size: u64,

    /// Number of parallel worker threads (default: all logical CPUs)
    #[arg(short = 'j', long = "jobs", default_value_t = num_cpus(), value_name = "N", global = true)]
    pub threads: usize,

    /// Log level: error | warn | info | debug | trace
    #[arg(long = "log-level", default_value = "warn", value_name = "LEVEL", global = true)]
    pub log_level: String,

    /// Zstd compression level (1-22, default: 3)
    #[arg(long = "compress-level", default_value_t = 3, value_name = "LEVEL", global = true)]
    pub compress_level: i32,

    /// Force fsync after each file write (default: off for speed).
    #[arg(long = "fsync", global = true)]
    pub fsync: bool,

    /// Print final benchmark summary (throughput, CPU utilisation, time)
    #[arg(long = "stats", global = true)]
    pub stats: bool,

    // ───── manifest cache (resync performance) ───────────────────────────
    /// Disable manifest cache (always scan destination fresh).
    #[arg(long = "no-manifest", global = true)]
    pub no_manifest: bool,

    /// Directory to store the manifest cache file (default: destination dir).
    #[arg(long = "manifest-dir", value_name = "DIR", global = true)]
    pub manifest_dir: Option<PathBuf>,

    /// Ultra-fast no-change detection: if directory mtimes haven't changed
    /// since last sync, assume no files were modified. This is safe when NO
    /// files are edited in-place (e.g. build outputs, deployments). Not safe
    /// when users may edit files directly. Enables ~100-500x speedup.
    #[arg(long = "trust-mtime", global = true)]
    pub trust_mtime: bool,

    // ───── additional rsync-compat flags ─────────────────────────────────
    /// Read list of files to transfer from FILE (one per line).
    #[arg(long = "files-from", value_name = "FILE", global = true)]
    pub files_from: Option<PathBuf>,

    /// Override destination permissions using chmod-style string (e.g. "u+rw,go+r").
    #[arg(long = "chmod", value_name = "PERMS", global = true)]
    pub chmod: Option<String>,

    /// Override destination owner:group (e.g. "nobody:nogroup" or "1000:1000").
    #[arg(long = "chown", value_name = "USER:GROUP", global = true)]
    pub chown: Option<String>,

    /// Verify file checksums after transfer (post-transfer integrity check).
    #[arg(long = "checksum-verify", global = true)]
    pub checksum_verify: bool,

    /// Use reflinks (CoW copies) on supported filesystems (btrfs, XFS).
    #[arg(long = "reflink", global = true)]
    pub reflink: bool,

    /// Preserve extended attributes (xattrs). Equivalent to rsync's -X flag.
    #[arg(short = 'X', long = "xattrs", global = true)]
    pub xattrs: bool,
}

#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    /// Start the resync server daemon (listens for incoming syncs)
    Serve {
        #[arg(long = "bind", default_value = "0.0.0.0")]
        bind: String,
        #[arg(long = "port", default_value_t = 2377)]
        port: u16,
        #[arg(long = "tls")]
        tls: bool,
        #[arg(long = "tls-cert", value_name = "FILE")]
        tls_cert: Option<PathBuf>,
        #[arg(long = "tls-key", value_name = "FILE")]
        tls_key: Option<PathBuf>,
    },

    /// Push files to a remote resync server
    Push {
        source: PathBuf,
        remote: String,
        #[arg(long = "tls")]
        tls: bool,
        #[arg(long = "tls-verify")]
        tls_verify: bool,
    },

    /// Pull files from a remote resync server to local destination
    Pull {
        /// Remote source in HOST:PATH or HOST:PORT:PATH format
        remote: String,
        /// Local destination directory
        dest: PathBuf,
        #[arg(long = "tls")]
        tls: bool,
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
    /// Resolve implied flags from `-a` (archive) shorthand.
    /// `-a` implies -rlptgo (recursive, links, perms, times, group, owner).
    pub fn resolve_archive(&mut self) {
        if self.archive {
            self.recursive = true;
            self.preserve_links = true;
            self.preserve_perms = true;
            self.preserve_times = true;
            self.preserve_group = true;
            self.preserve_owner = true;
        }
    }

    /// If `--backup-dir` is set, force `--backup` on.
    pub fn resolve_backup(&mut self) {
        if self.backup_dir.is_some() {
            self.backup = true;
        }
    }

    /// Build a [`crate::filter::FilterEngine`] from the CLI flags.
    pub fn build_filter_engine(&self) -> anyhow::Result<crate::filter::FilterEngine> {
        crate::filter::FilterEngine::from_cli_patterns(
            &self.exclude,
            &self.include,
            &self.exclude_from,
            &self.include_from,
        )
    }

    /// Build a [`crate::filter::RateLimiter`] from `--bwlimit`.
    pub fn build_rate_limiter(&self) -> crate::filter::RateLimiter {
        crate::filter::RateLimiter::new(self.bwlimit)
    }

    /// Parse a human-readable size string like "100M", "2G", "1024" into bytes.
    pub fn parse_size(s: &str) -> Option<u64> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }
        let (num_str, multiplier) = if s.ends_with('G') || s.ends_with('g') {
            (&s[..s.len() - 1], 1024u64 * 1024 * 1024)
        } else if s.ends_with('M') || s.ends_with('m') {
            (&s[..s.len() - 1], 1024u64 * 1024)
        } else if s.ends_with('K') || s.ends_with('k') {
            (&s[..s.len() - 1], 1024u64)
        } else {
            (s, 1u64)
        };
        num_str.parse::<u64>().ok().map(|n| n * multiplier)
    }
}

/// Parse a remote string like "192.168.1.10:/path" or "host:2377:/path".
pub fn parse_remote(remote: &str) -> anyhow::Result<(std::net::SocketAddr, PathBuf)> {
    let parts: Vec<&str> = remote.splitn(3, ':').collect();
    match parts.len() {
        3 => {
            let host = parts[0];
            let port: u16 = parts[1]
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid port in remote spec: {}", parts[1]))?;
            let path = PathBuf::from(parts[2]);
            let addr: std::net::SocketAddr = format!("{host}:{port}").parse()?;
            Ok((addr, path))
        }
        2 => {
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
