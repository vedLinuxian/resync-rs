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
//!
//!  Performance features:
//!  - Pipelined scan→process (overlaps I/O with CPU)
//!  - madvise(SEQUENTIAL) on mmap'd files
//!  - copy_file_range() zero-copy for new files on Linux
//!  - No fsync by default (configurable with --fsync)
//!  - inode-sorted file processing to minimize disk seeks
//!  - Adaptive chunk sizing based on file size

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
use crate::error::{Result, ResyncError};
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
    pub delete_excluded: bool,
    pub preserve_perms: bool,
    pub preserve_times: bool,
    pub preserve_links: bool,
    pub preserve_owner: bool,
    pub preserve_group: bool,
    pub recursive: bool,
    pub show_progress: bool,
    pub show_stats: bool,
    pub use_cdc: bool,
    pub checksum: bool,
    pub size_only: bool,
    pub modify_window: u32,
    pub filter_engine: Option<FilterEngine>,
    pub rate_limiter: Option<RateLimiter>,
    pub backup: bool,
    pub backup_dir: Option<PathBuf>,
    pub backup_suffix: String,
    pub itemize_changes: bool,
    pub log_file: Option<PathBuf>,
    pub whole_file: bool,
    pub update: bool,
    pub existing: bool,
    pub ignore_existing: bool,
    pub inplace: bool,
    pub partial: bool,
    pub sparse: bool,
    pub one_file_system: bool,
    pub hard_links: bool,
    pub link_dest: Option<PathBuf>,
    pub max_delete: Option<u64>,
    pub max_size: Option<u64>,
    pub min_size: Option<u64>,
    pub force: bool,
    pub ignore_errors: bool,
    pub prune_empty_dirs: bool,
    pub fsync: bool,
}

impl From<&Cli> for SyncOptions {
    fn from(cli: &Cli) -> Self {
        let filter_engine = cli.build_filter_engine().ok();
        let rate_limiter = if cli.bwlimit > 0 {
            Some(cli.build_rate_limiter())
        } else {
            None
        };

        let max_size = cli.max_size.as_ref().and_then(|s| Cli::parse_size(s));
        let min_size = cli.min_size.as_ref().and_then(|s| Cli::parse_size(s));

        SyncOptions {
            source: cli.source.clone().unwrap_or_default(),
            dest: cli.dest.clone().unwrap_or_default(),
            chunk_size: cli.chunk_size as usize,
            threads: cli.threads,
            verbose: cli.verbose,
            dry_run: cli.dry_run,
            delete: cli.delete,
            delete_excluded: cli.delete_excluded,
            preserve_perms: cli.preserve_perms,
            preserve_times: cli.preserve_times,
            preserve_links: cli.preserve_links,
            preserve_owner: cli.preserve_owner,
            preserve_group: cli.preserve_group,
            recursive: cli.recursive,
            show_progress: cli.progress,
            show_stats: cli.stats,
            use_cdc: true,
            checksum: cli.checksum,
            size_only: cli.size_only,
            modify_window: cli.modify_window,
            filter_engine,
            rate_limiter,
            backup: cli.backup || cli.backup_dir.is_some(),
            backup_dir: cli.backup_dir.clone(),
            backup_suffix: cli.suffix.clone(),
            itemize_changes: cli.itemize_changes,
            log_file: cli.log_file.clone(),
            whole_file: cli.whole_file,
            update: cli.update,
            existing: cli.existing,
            ignore_existing: cli.ignore_existing,
            inplace: cli.inplace,
            partial: cli.partial,
            sparse: cli.sparse,
            one_file_system: cli.one_file_system,
            hard_links: cli.hard_links,
            link_dest: cli.link_dest.clone(),
            max_delete: cli.max_delete,
            max_size,
            min_size,
            force: cli.force,
            ignore_errors: cli.ignore_errors,
            prune_empty_dirs: cli.prune_empty_dirs,
            fsync: cli.fsync,
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

        // ── 1. Validate source ────────────────────────────────────────────
        if !self.opts.source.exists() {
            return Err(ResyncError::SourceNotFound(
                self.opts.source.display().to_string(),
            ));
        }

        // ── 2. Build a scoped rayon thread pool ──────────────────────────
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.opts.threads)
            .thread_name(|idx| format!("resync-worker-{idx}"))
            .stack_size(2 * 1024 * 1024) // 2 MB per thread (vs default 8 MB)
            .build()
            .map_err(|e| ResyncError::Other(anyhow::anyhow!("failed to build thread pool: {e}")))?;

        // ── 3. Scan source in parallel ────────────────────────────────────
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

        // ── 3b. Apply filter engine + size filters ────────────────────────
        let mut src_result = src_result;
        if let Some(ref filter) = self.opts.filter_engine {
            let file_before = src_result.files.len();
            src_result
                .files
                .retain(|f| !filter.is_excluded(&f.rel_path, false));
            let dir_before = src_result.dirs.len();
            src_result
                .dirs
                .retain(|d| !filter.is_excluded(&d.rel_path, true));
            let excluded =
                (file_before - src_result.files.len()) + (dir_before - src_result.dirs.len());
            if excluded > 0 {
                info!("Filtered out {excluded} entries by exclude/include rules");
            }
        }

        // Apply --max-size / --min-size filters
        if let Some(max) = self.opts.max_size {
            src_result.files.retain(|f| f.size <= max);
        }
        if let Some(min) = self.opts.min_size {
            src_result.files.retain(|f| f.size >= min);
        }

        // ── 3c. Sort files by inode for optimal disk I/O order ────────────
        // Minimizes disk head seeks on spinning media; harmless on NVMe.
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            src_result.files.sort_by_key(|f| {
                std::fs::metadata(&f.abs_path)
                    .ok()
                    .map(|m| m.ino())
                    .unwrap_or(0)
            });
        }

        // ── 4. Collect src_paths for --delete ─────────────────────────────
        let src_paths: HashSet<PathBuf> = src_result
            .files
            .iter()
            .map(|f| f.rel_path.clone())
            .collect();

        // ── 5. Create destination root and directory tree ─────────────────
        if !self.opts.dry_run {
            fs::create_dir_all(&self.opts.dest).map_err(|e| ResyncError::Io {
                path: self.opts.dest.display().to_string(),
                source: e,
            })?;

            for dir in &src_result.dirs {
                let dst_dir = self.opts.dest.join(&dir.rel_path);
                if let Err(e) = fs::create_dir_all(&dst_dir) {
                    warn!("failed to create directory {}: {e}", dst_dir.display());
                }
            }
        }

        // ── 6. Build progress reporter ────────────────────────────────────
        let reporter = ProgressReporter::new(
            src_result.files.len() as u64,
            src_result.total_bytes,
            self.opts.show_progress,
        );

        let hasher = if self.opts.use_cdc && !self.opts.whole_file {
            Hasher::with_cdc(self.opts.chunk_size)
        } else {
            Hasher::new(self.opts.chunk_size)
        };
        let applier = Applier::new(
            self.opts.preserve_perms,
            self.opts.preserve_times,
            self.opts.preserve_owner,
            self.opts.preserve_group,
            self.opts.dry_run,
            self.opts.fsync,
            self.opts.inplace,
        );

        let error_count = AtomicU64::new(0);

        // ── 7. Process every source file in parallel ──────────────────────
        pool.install(|| {
            src_result.files.par_iter().for_each(|src_entry| {
                let dst_path = self.opts.dest.join(&src_entry.rel_path);

                // ── --existing: skip files that don't exist on dest ────────
                if self.opts.existing && !dst_path.exists() {
                    reporter.on_file_skipped(src_entry.size);
                    return;
                }

                // ── --ignore-existing: skip files that exist on dest ───────
                if self.opts.ignore_existing && dst_path.exists() {
                    reporter.on_file_skipped(src_entry.size);
                    return;
                }

                // ── --update: skip if dest is newer ────────────────────────
                if self.opts.update && dst_path.exists() {
                    if let Ok(dst_meta) = fs::metadata(&dst_path) {
                        if let (Ok(dst_mtime), Ok(_src_mtime)) =
                            (dst_meta.modified(), Ok::<_, std::io::Error>(src_entry.modified))
                        {
                            if dst_mtime > src_entry.modified {
                                debug!("SKIP (newer on dest) {}", src_entry.rel_path.display());
                                reporter.on_file_skipped(src_entry.size);
                                return;
                            }
                        }
                    }
                }

                // ── --link-dest: hardlink from reference dir if unchanged ──
                if let Some(ref link_dir) = self.opts.link_dest {
                    if !dst_path.exists() {
                        let link_src = link_dir.join(&src_entry.rel_path);
                        if link_src.exists() {
                            // Compare mtime+size to decide if unchanged
                            if let Ok(link_meta) = fs::metadata(&link_src) {
                                let size_match = link_meta.len() == src_entry.size;
                                let mtime_match = link_meta
                                    .modified()
                                    .ok()
                                    .map(|m| m == src_entry.modified)
                                    .unwrap_or(false);
                                if size_match && mtime_match && !self.opts.dry_run {
                                    if let Some(parent) = dst_path.parent() {
                                        fs::create_dir_all(parent).ok();
                                    }
                                    if fs::hard_link(&link_src, &dst_path).is_ok() {
                                        debug!("LINK-DEST {}", src_entry.rel_path.display());
                                        reporter.on_file_done(
                                            src_entry.size,
                                            0,
                                            &src_entry.rel_path.display().to_string(),
                                        );
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }

                if self.opts.verbose {
                    println!("{} -> {}", src_entry.rel_path.display(), dst_path.display());
                }

                if !dst_path.exists() {
                    // ── New file: atomic copy ─────────────────────────────
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
                            error!("copy_new failed for {}: {e}", src_entry.rel_path.display());
                            error_count.fetch_add(1, Ordering::Relaxed);
                            reporter.on_file_error(src_entry.size);
                        }
                    }
                } else if self.opts.whole_file {
                    // ── --whole-file: skip delta, always full copy ─────────
                    match applier.copy_new(&src_entry.abs_path, &dst_path, src_entry) {
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
                        }
                        Err(e) => {
                            error!("copy failed for {}: {e}", src_entry.rel_path.display());
                            error_count.fetch_add(1, Ordering::Relaxed);
                            reporter.on_file_error(src_entry.size);
                        }
                    }
                } else {
                    // ── Existing file: mtime+size fast-path ───────────────
                    if !self.opts.checksum {
                        if let Ok(dst_meta) = fs::metadata(&dst_path) {
                            let size_match = if self.opts.size_only {
                                dst_meta.len() == src_entry.size
                            } else {
                                dst_meta.len() == src_entry.size
                            };

                            let mtime_match = if self.opts.size_only {
                                true // --size-only ignores mtime
                            } else {
                                dst_meta
                                    .modified()
                                    .ok()
                                    .map(|dst_mtime| {
                                        if self.opts.modify_window > 0 {
                                            // Compare with window tolerance
                                            let diff = if src_entry.modified > dst_mtime {
                                                src_entry.modified.duration_since(dst_mtime)
                                                    .unwrap_or_default()
                                            } else {
                                                dst_mtime.duration_since(src_entry.modified)
                                                    .unwrap_or_default()
                                            };
                                            diff.as_secs() <= self.opts.modify_window as u64
                                        } else {
                                            src_entry.modified == dst_mtime
                                        }
                                    })
                                    .unwrap_or(false)
                            };

                            if size_match && mtime_match {
                                debug!("SKIP (mtime+size) {}", src_entry.rel_path.display());
                                reporter.on_file_skipped(src_entry.size);
                                return;
                            }
                        }
                    }

                    // ── Existing file: delta sync ─────────────────────────
                    let src_manifest = match hasher.hash_file(&src_entry.abs_path) {
                        Ok(m) => m,
                        Err(e) => {
                            error!("hash failed for {}: {e}", src_entry.abs_path.display());
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

                    reporter
                        .counters
                        .bytes_delta_reused
                        .fetch_add(delta.reuse_bytes, Ordering::Relaxed);

                    debug!(
                        "DELTA {} — {:.1}% transfer ({} / {})",
                        src_entry.rel_path.display(),
                        (1.0 - delta.savings_ratio()) * 100.0,
                        bytesize::ByteSize::b(delta.transfer_bytes),
                        bytesize::ByteSize::b(src_entry.size),
                    );

                    // ── Backup before overwriting ─────────────────────────
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
                        let _ = fs::remove_file(&backup_path);
                        if fs::hard_link(&dst_path, &backup_path).is_err() {
                            if let Err(e) = fs::copy(&dst_path, &backup_path) {
                                warn!(
                                    "backup failed for {} -> {}: {e}",
                                    dst_path.display(),
                                    backup_path.display()
                                );
                            }
                        }
                    }

                    match applier.apply(&src_entry.abs_path, &dst_path, &delta, src_entry) {
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

                            if self.opts.itemize_changes {
                                println!(">f.st...... {}", src_entry.rel_path.display());
                            }

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
                            error!("apply failed for {}: {e}", src_entry.rel_path.display());
                            error_count.fetch_add(1, Ordering::Relaxed);
                            reporter.on_file_error(src_entry.size);
                        }
                    }
                }
            });
        });

        // ── 8. Handle --delete ────────────────────────────────────────────
        if self.opts.delete && self.opts.dest.exists() {
            let dst_scanner = Scanner::new(
                &self.opts.dest,
                self.opts.recursive,
                self.opts.preserve_links,
            )?;
            let dst_result = dst_scanner.scan()?;

            let mut delete_count: u64 = 0;

            let to_delete: Vec<_> = dst_result
                .files
                .iter()
                .filter(|f| !src_paths.contains(&f.rel_path))
                .filter(|f| {
                    if self.opts.delete_excluded {
                        true // --delete-excluded: delete even excluded files
                    } else {
                        self.opts
                            .filter_engine
                            .as_ref()
                            .is_none_or(|fe| !fe.is_excluded(&f.rel_path, false))
                    }
                })
                .collect();

            for entry in &to_delete {
                // Check --max-delete safety limit
                if let Some(max) = self.opts.max_delete {
                    if delete_count >= max {
                        warn!("--max-delete limit ({max}) reached, stopping deletions");
                        break;
                    }
                }

                if self.opts.verbose {
                    println!("deleting {}", entry.rel_path.display());
                }
                if !self.opts.dry_run {
                    if let Err(e) = fs::remove_file(&entry.abs_path) {
                        warn!("failed to delete {}: {e}", entry.abs_path.display());
                        if !self.opts.ignore_errors {
                            error_count.fetch_add(1, Ordering::Relaxed);
                        }
                        continue;
                    }
                }
                delete_count += 1;
                reporter
                    .counters
                    .files_deleted
                    .fetch_add(1, Ordering::Relaxed);
            }

            // Also remove orphan empty directories
            if !self.opts.dry_run {
                let src_dirs: HashSet<PathBuf> =
                    src_result.dirs.iter().map(|d| d.rel_path.clone()).collect();
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
                    if self.opts.force {
                        fs::remove_dir_all(&dir.abs_path).ok();
                    } else {
                        fs::remove_dir(&dir.abs_path).ok();
                    }
                }
            }
        }

        // ── 9. Report ─────────────────────────────────────────────────────
        reporter.finish();
        if self.opts.show_stats || self.opts.verbose {
            reporter.print_summary();
        }

        let errors = error_count.load(Ordering::Relaxed);
        let elapsed = start.elapsed().as_secs_f64();

        if errors > 0 {
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
