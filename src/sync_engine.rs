//! Core sync engine — orchestrates the full pipeline:
//!
//!  ```text
//!  Scanner (jwalk, parallel)
//!      │
//!      ▼
//!  Per-file decision (new / update / skip / delete)
//!      │
//!      ├── New file  ──────────────────► copy_new()  [atomic: tmp+rename]
//!      │
//!      └── Existing  →  hash src+dst   → compute_delta()  →  apply()
//!                       (parallel BLAKE3, memmap2)        [atomic: tmp+rename]
//!  ```
//!
//!  All per-file work runs inside a scoped `rayon` thread pool so the OS
//!  scheduler saturates all available CPU cores.

use std::collections::HashSet;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use rayon::prelude::*;
use tracing::{debug, error, info, warn};

use crate::applier::Applier;
use crate::cli::Cli;
use crate::delta::DeltaEngine;
use crate::error::{ResyncError, Result};
use crate::filter::{FilterEngine, RateLimiter};
use crate::hasher::Hasher;
use crate::progress::ProgressReporter;
use crate::scanner::Scanner;

// ─── Options extracted from CLI ──────────────────────────────────────────────

pub struct SyncOptions {
    pub source: PathBuf,
    pub dest: PathBuf,
    pub chunk_size: usize,
    pub threads: usize,
    pub verbose: bool,
    pub dry_run: bool,
    pub delete: bool,
    pub preserve_perms: bool,
    pub preserve_times: bool,
    pub preserve_links: bool,
    pub recursive: bool,
    pub show_progress: bool,
    pub show_stats: bool,
    pub use_cdc: bool,
    pub checksum: bool,
    pub filter_engine: Option<FilterEngine>,
    pub rate_limiter: Option<RateLimiter>,
    pub backup: bool,
    pub backup_dir: Option<PathBuf>,
    pub backup_suffix: String,
    pub itemize_changes: bool,
    pub log_file: Option<PathBuf>,
}

impl From<&Cli> for SyncOptions {
    fn from(cli: &Cli) -> Self {
        let filter_engine = cli.build_filter_engine().ok();
        let rate_limiter = if cli.bwlimit > 0 {
            Some(cli.build_rate_limiter())
        } else {
            None
        };

        SyncOptions {
            source: cli.source.clone().unwrap_or_default(),
            dest: cli.dest.clone().unwrap_or_default(),
            chunk_size: cli.chunk_size as usize,
            threads: cli.threads,
            verbose: cli.verbose,
            dry_run: cli.dry_run,
            delete: cli.delete,
            preserve_perms: cli.preserve_perms,
            preserve_times: cli.preserve_times,
            preserve_links: cli.preserve_links,
            recursive: cli.recursive,
            show_progress: cli.progress,
            show_stats: cli.stats,
            use_cdc: true, // always enable CDC for local sync
            checksum: cli.checksum,
            filter_engine,
            rate_limiter,
            backup: cli.backup || cli.backup_dir.is_some(),
            backup_dir: cli.backup_dir.clone(),
            backup_suffix: cli.suffix.clone(),
            itemize_changes: cli.itemize_changes,
            log_file: cli.log_file.clone(),
        }
    }
}

// ─── Engine ──────────────────────────────────────────────────────────────────

pub struct SyncEngine {
    opts: SyncOptions,
}

impl SyncEngine {
    pub fn new(opts: SyncOptions) -> Self {
        Self { opts }
    }

    pub fn run(&self) -> Result<()> {
        let start = Instant::now();

        // ── 0. Set up log file if requested ───────────────────────────────
        let log_writer: Option<Mutex<BufWriter<fs::File>>> =
            if let Some(ref log_path) = self.opts.log_file {
                let file = fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(log_path)
                    .map_err(|e| ResyncError::Io {
                        path: log_path.display().to_string(),
                        source: e,
                    })?;
                info!("Logging to {}", log_path.display());
                Some(Mutex::new(BufWriter::new(file)))
            } else {
                None
            };

        // ── 1. Validate source ────────────────────────────────────────────────
        if !self.opts.source.exists() {
            return Err(ResyncError::SourceNotFound(
                self.opts.source.display().to_string(),
            ));
        }

        // ── 2. Build a scoped rayon thread pool ──────────────────────────────
        // BUG FIX #6: build_global().ok() silently fails on second call.
        // Use a scoped local pool instead — guarantees thread config is applied.
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.opts.threads)
            .thread_name(|idx| format!("resync-worker-{idx}"))
            .build()
            .map_err(|e| ResyncError::Other(anyhow::anyhow!("failed to build thread pool: {e}")))?;

        // ── 3. Scan source in parallel ────────────────────────────────────────
        info!("Scanning source: {}", self.opts.source.display());
        let scanner = Scanner::new(
            &self.opts.source,
            self.opts.recursive,
            self.opts.preserve_links,
        )?;
        let src_result = scanner.scan()?;

        if src_result.errors > 0 {
            warn!(
                "{} entries could not be read during source scan",
                src_result.errors
            );
        }

        info!(
            "Source: {} files, {}",
            src_result.files.len(),
            bytesize::ByteSize::b(src_result.total_bytes)
        );

        // ── 3b. Apply filter engine to exclude files before processing ────
        let mut src_result = src_result;
        if let Some(ref filter) = self.opts.filter_engine {
            let file_before = src_result.files.len();
            src_result
                .files
                .retain(|f| !filter.is_excluded(&f.rel_path, false));
            // BUG FIX: Also filter directories — previously excluded dirs
            // (e.g. `--exclude '.git/'`) were still created in the destination.
            let dir_before = src_result.dirs.len();
            src_result
                .dirs
                .retain(|d| !filter.is_excluded(&d.rel_path, true));
            let excluded = (file_before - src_result.files.len())
                + (dir_before - src_result.dirs.len());
            if excluded > 0 {
                info!("Filtered out {excluded} entries by exclude/include rules");
            }
        }

        // ── 4. Scan destination AFTER sync for --delete ───────────────────────
        // BUG FIX: We collect src_paths before sync and scan dst AFTER sync.
        // Previously, scanning dst before sync meant the delete list was stale
        // (it would try to delete files that were just freshly synced).
        let src_paths: HashSet<PathBuf> = src_result
            .files
            .iter()
            .map(|f| f.rel_path.clone())
            .collect();

        // ── 5. Create destination root and directory tree ─────────────────────
        if !self.opts.dry_run {
            fs::create_dir_all(&self.opts.dest).map_err(|e| ResyncError::Io {
                path: self.opts.dest.display().to_string(),
                source: e,
            })?;

            // BUG FIX #16: Report dir creation failures
            for dir in &src_result.dirs {
                let dst_dir = self.opts.dest.join(&dir.rel_path);
                if let Err(e) = fs::create_dir_all(&dst_dir) {
                    warn!("failed to create directory {}: {e}", dst_dir.display());
                }
            }
        }

        // ── 6. Build progress reporter ────────────────────────────────────────
        let reporter = ProgressReporter::new(
            src_result.files.len() as u64,
            src_result.total_bytes,
            self.opts.show_progress,
        );

        let hasher = if self.opts.use_cdc {
            Hasher::with_cdc(self.opts.chunk_size)
        } else {
            Hasher::new(self.opts.chunk_size)
        };
        let applier = Applier::new(
            self.opts.preserve_perms,
            self.opts.preserve_times,
            self.opts.dry_run,
        );

        // BUG FIX #7: Track error count so we can return Err on failures
        let error_count = AtomicU64::new(0);

        // ── 7. Process every source file in parallel ──────────────────────────
        pool.install(|| {
            src_result.files.par_iter().for_each(|src_entry| {
                let dst_path = self.opts.dest.join(&src_entry.rel_path);

                if self.opts.verbose {
                    println!(
                        "{} -> {}",
                        src_entry.rel_path.display(),
                        dst_path.display()
                    );
                }

                if !dst_path.exists() {
                    // ── New file: atomic copy ─────────────────────────────────
                    debug!("NEW  {}", src_entry.rel_path.display());
                    match applier.copy_new(&src_entry.abs_path, &dst_path, src_entry) {
                        Ok(bytes) => {
                            reporter.counters.files_new.fetch_add(1, Ordering::Relaxed);
                            reporter.on_file_done(
                                src_entry.size,
                                bytes,
                                &src_entry.rel_path.display().to_string(),
                            );
                        }
                        Err(e) => {
                            error!(
                                "copy_new failed for {}: {e}",
                                src_entry.rel_path.display()
                            );
                            error_count.fetch_add(1, Ordering::Relaxed);
                            reporter.on_file_error(src_entry.size);
                        }
                    }
                } else {
                    // ── Existing file: mtime+size fast-path ───────────────────
                    // Like rsync: if size AND mtime match, skip expensive hashing.
                    // This makes warm syncs (nothing changed) near-instant.
                    // When --checksum is set, skip this fast-path and always hash.
                    if !self.opts.checksum {
                        if let Ok(dst_meta) = fs::metadata(&dst_path) {
                            let size_match = dst_meta.len() == src_entry.size;
                            let mtime_match = dst_meta
                                .modified()
                                .ok()
                                .map(|dst_mtime| {
                                    // Compare with nanosecond granularity
                                    src_entry.modified == dst_mtime
                                })
                                .unwrap_or(false);

                            if size_match && mtime_match {
                                debug!("SKIP (mtime+size) {}", src_entry.rel_path.display());
                                reporter.on_file_skipped(src_entry.size);
                                return;
                            }
                        }
                    }

                    // ── Existing file: delta sync ─────────────────────────────
                    let src_manifest = match hasher.hash_file(&src_entry.abs_path) {
                        Ok(m) => m,
                        Err(e) => {
                            error!(
                                "hash failed for {}: {e}",
                                src_entry.abs_path.display()
                            );
                            error_count.fetch_add(1, Ordering::Relaxed);
                            reporter.on_file_error(src_entry.size);
                            return;
                        }
                    };
                    let dst_manifest = match hasher.hash_file(&dst_path) {
                        Ok(m) => m,
                        Err(e) => {
                            error!("hash failed for {}: {e}", dst_path.display());
                            error_count.fetch_add(1, Ordering::Relaxed);
                            reporter.on_file_error(src_entry.size);
                            return;
                        }
                    };

                    let delta = DeltaEngine::compute_full(&src_manifest, &dst_manifest);

                    if delta.is_no_op() {
                        debug!("SKIP {}", src_entry.rel_path.display());
                        reporter.on_file_skipped(src_entry.size);
                        return;
                    }

                    // Track delta-reused bytes for accurate stats
                    reporter.counters.bytes_delta_reused.fetch_add(
                        delta.reuse_bytes,
                        Ordering::Relaxed,
                    );

                    debug!(
                        "DELTA {} — {:.1}% transfer ({} / {})",
                        src_entry.rel_path.display(),
                        (1.0 - delta.savings_ratio()) * 100.0,
                        bytesize::ByteSize::b(delta.transfer_bytes),
                        bytesize::ByteSize::b(src_entry.size),
                    );

                    // ── Backup before overwriting ─────────────────────────────
                    // BUG FIX: Previously used `fs::rename()` which MOVED the
                    // destination file away.  The subsequent `applier.apply()`
                    // then tried to read the destination for Copy ops and
                    // failed because the file no longer existed at `dst_path`.
                    // Every delta Copy op silently degraded to a full-file
                    // transfer.
                    //
                    // Fix: use `hard_link` (instant, same inode) so the dst
                    // remains readable at its original path.  The backup name
                    // is just an additional directory entry for the same inode.
                    // When `apply()` atomically renames the temp file over
                    // `dst_path`, the old inode's link count drops from 2→1
                    // but the backup hard link keeps the data alive.
                    //
                    // Falls back to `fs::copy()` when hard links are not
                    // supported (cross-filesystem, FAT32, etc.).
                    if self.opts.backup && !self.opts.dry_run {
                        let backup_path = if let Some(ref bdir) = self.opts.backup_dir {
                            let bp = bdir.join(&src_entry.rel_path);
                            if let Some(parent) = bp.parent() {
                                fs::create_dir_all(parent).ok();
                            }
                            bp
                        } else {
                            let mut bp = dst_path.as_os_str().to_owned();
                            bp.push(&self.opts.backup_suffix);
                            PathBuf::from(bp)
                        };
                        // Remove stale backup if it exists (hard_link fails on existing targets)
                        let _ = fs::remove_file(&backup_path);
                        if fs::hard_link(&dst_path, &backup_path).is_err() {
                            // Cross-device or unsupported — copy fallback
                            if let Err(e) = fs::copy(&dst_path, &backup_path) {
                                warn!(
                                    "backup failed for {} -> {}: {e}",
                                    dst_path.display(),
                                    backup_path.display()
                                );
                            } else {
                                debug!(
                                    "BACKUP (copy) {} -> {}",
                                    dst_path.display(),
                                    backup_path.display()
                                );
                            }
                        } else {
                            debug!(
                                "BACKUP (hardlink) {} -> {}",
                                dst_path.display(),
                                backup_path.display()
                            );
                        }
                    }

                    match applier.apply(
                        &src_entry.abs_path,
                        &dst_path,
                        &delta,
                        src_entry,
                    ) {
                        Ok(bytes) => {
                            reporter
                                .counters
                                .files_updated
                                .fetch_add(1, Ordering::Relaxed);
                            reporter.on_file_done(
                                src_entry.size,
                                bytes,
                                &src_entry.rel_path.display().to_string(),
                            );

                            // ── Itemize changes output ───────────────────────
                            if self.opts.itemize_changes {
                                println!(
                                    ">f.st...... {}",
                                    src_entry.rel_path.display()
                                );
                            }

                            // ── Structured log entry ─────────────────────────
                            if let Some(ref lw) = log_writer {
                                if let Ok(mut w) = lw.lock() {
                                    let _ = writeln!(
                                        w,
                                        "UPDATED {} bytes={} transfer={}",
                                        src_entry.rel_path.display(),
                                        src_entry.size,
                                        delta.transfer_bytes
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                "apply failed for {}: {e}",
                                src_entry.rel_path.display()
                            );
                            error_count.fetch_add(1, Ordering::Relaxed);
                            reporter.on_file_error(src_entry.size);
                        }
                    }
                }
            });
        });

        // ── 8. Handle --delete ────────────────────────────────────────────────
        // BUG FIX: Scan destination AFTER sync to avoid stale delete list.
        if self.opts.delete && self.opts.dest.exists() {
            let dst_scanner = Scanner::new(
                &self.opts.dest,
                self.opts.recursive,
                self.opts.preserve_links,
            )?;
            let dst_result = dst_scanner.scan()?;

            let to_delete: Vec<_> = dst_result
                .files
                .iter()
                .filter(|f| !src_paths.contains(&f.rel_path))
                // BUG FIX: Don't delete files that match exclude rules.
                // rsync does NOT delete excluded files by default — only
                // `--delete-excluded` does that (which we don't support yet).
                // Previously, excluded files weren't in `src_paths`, so
                // `--delete` would silently wipe them from the destination.
                .filter(|f| {
                    self.opts
                        .filter_engine
                        .as_ref()
                        .is_none_or(|fe| !fe.is_excluded(&f.rel_path, false))
                })
                .collect();

            for entry in &to_delete {
                if self.opts.verbose {
                    println!("deleting {}", entry.rel_path.display());
                }
                if !self.opts.dry_run {
                    if let Err(e) = fs::remove_file(&entry.abs_path) {
                        warn!("failed to delete {}: {e}", entry.abs_path.display());
                        error_count.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                }
                // BUG FIX #15: Only count if actually deleted (or dry-run)
                reporter
                    .counters
                    .files_deleted
                    .fetch_add(1, Ordering::Relaxed);
            }

            // BUG FIX #14: Also remove orphan empty directories
            if !self.opts.dry_run {
                let src_dirs: HashSet<PathBuf> = src_result
                    .dirs
                    .iter()
                    .map(|d| d.rel_path.clone())
                    .collect();
                // Sort by depth descending so children are removed before parents
                let mut orphan_dirs: Vec<_> = dst_result
                    .dirs
                    .iter()
                    .filter(|d| !src_dirs.contains(&d.rel_path))
                    .collect();
                orphan_dirs.sort_by(|a, b| {
                    b.rel_path
                        .components()
                        .count()
                        .cmp(&a.rel_path.components().count())
                });
                for dir in orphan_dirs {
                    // remove_dir only succeeds if empty, which is fine
                    fs::remove_dir(&dir.abs_path).ok();
                }
            }
        }

        // ── 9. Report ─────────────────────────────────────────────────────────
        reporter.finish();
        if self.opts.show_stats || self.opts.verbose {
            reporter.print_summary();
        }

        let errors = error_count.load(Ordering::Relaxed);
        let elapsed = start.elapsed().as_secs_f64();

        if errors > 0 {
            // BUG FIX #7: Propagate error count so callers/scripts see failure
            error!("sync completed with {errors} error(s) in {elapsed:.3}s");
            Err(ResyncError::Other(anyhow::anyhow!(
                "sync completed with {errors} error(s)"
            )))
        } else {
            info!("sync complete in {elapsed:.3}s — 0 errors");
            Ok(())
        }
    }
}
