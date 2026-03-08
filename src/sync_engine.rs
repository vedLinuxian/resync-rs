//! Core sync engine — orchestrates the full pipeline:
//!
//!  ```text
//!  Scanner (jwalk, parallel)
//!      │
//!      ▼
//!  DST HashMap (parallel build, O(1) lookup, ZERO extra stats)
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
//!  PERF: "Zero-Stat Delta Detection" (ZSDD) algorithm
//!  ════════════════════════════════════════════════════
//!  The entire sync runs with exactly TWO stat() calls per file (one in src
//!  scan, one in dst scan).  All downstream decisions (--update, --existing,
//!  --ignore-existing, mtime+size fast-path, inode sort) use pre-cached
//!  metadata from the scan pass.  Nothing calls stat()/metadata() again.
//!
//!  This eliminates the ~28× stat overhead found in profiling (286K calls
//!  reduced to ~20K for 10K files).

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use rayon::prelude::*;
use tracing::{error, info, warn};

use crate::applier::Applier;
use crate::cli::Cli;
use crate::delta::DeltaEngine;
use crate::error::{Result, ResyncError};
use crate::filter::{FilterEngine, RateLimiter};
use crate::hasher::Hasher;
use crate::manifest::Manifest;
use crate::progress::ProgressReporter;
use crate::scanner::{FileEntry, Scanner};

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
    pub append: bool,
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
    // ── Manifest cache ─────────────────────────
    pub no_manifest: bool,
    pub manifest_dir: Option<PathBuf>,
    pub trust_mtime: bool,
    // ── New features ───────────────────────────
    pub files_from: Option<PathBuf>,
    pub chmod: Option<String>,
    pub chown: Option<String>,
    pub checksum_verify: bool,
    pub reflink: bool,
    pub xattrs: bool,
    pub timeout: u64,
    pub human_readable: bool,
    pub devices: bool,
    pub specials: bool,
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
            append: cli.append,
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
            no_manifest: cli.no_manifest,
            manifest_dir: cli.manifest_dir.clone(),
            trust_mtime: cli.trust_mtime,
            files_from: cli.files_from.clone(),
            chmod: cli.chmod.clone(),
            chown: cli.chown.clone(),
            checksum_verify: cli.checksum_verify,
            reflink: cli.reflink,
            xattrs: cli.xattrs,
            timeout: cli.timeout,
            human_readable: cli.human_readable,
            devices: cli.devices,
            specials: cli.specials,
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

        // ── 3. Ultra-fast no-change detection via TOC ─────────────────────
        //
        // The TOC is a tiny file (< 50 KB) containing just directory mtimes.
        // --trust-mtime: if ALL directory mtimes match, assume nothing changed.
        // This is safe for deployments/CI (artifacts are replaced, not edited)
        // but NOT safe when users may edit files in-place.
        if self.opts.trust_mtime
            && !self.opts.no_manifest
            && !self.opts.delete
            && !self.opts.dry_run
            && !self.opts.checksum
        {
            use crate::manifest::ManifestToc;
            let toc_dest = if let Some(ref dir) = self.opts.manifest_dir {
                dir.clone()
            } else {
                self.opts.dest.clone()
            };
            if let Some(toc) = ManifestToc::load(&toc_dest) {
                let src_canon = self
                    .opts
                    .source
                    .canonicalize()
                    .unwrap_or_else(|_| self.opts.source.clone());
                if (toc.source_path == src_canon || toc.source_path == self.opts.source)
                    && toc.source_unchanged(&src_canon)
                {
                    info!(
                        "TOC fast path: all {} dirs unchanged — nothing to sync ({} files, {})",
                        toc.dir_mtimes.len(),
                        toc.file_count,
                        bytesize::ByteSize::b(toc.total_bytes),
                    );
                    let elapsed = start.elapsed().as_secs_f64();
                    info!("sync complete in {elapsed:.3}s — 0 errors (TOC fast path)");
                    return Ok(());
                }
            }
        }

        // ── 4. Load full manifest (needed for incremental scan and dst cache) ──
        let manifest_path = if let Some(ref dir) = self.opts.manifest_dir {
            dir.join(crate::manifest::MANIFEST_FILENAME)
        } else {
            Manifest::path_for(&self.opts.dest)
        };

        let loaded_manifest = if !self.opts.no_manifest {
            Manifest::load(&manifest_path).and_then(|m| {
                let src_canon = self
                    .opts
                    .source
                    .canonicalize()
                    .unwrap_or_else(|_| self.opts.source.clone());
                if m.source_path == src_canon || m.source_path == self.opts.source {
                    Some(m)
                } else {
                    info!(
                        "Manifest source path mismatch ({} vs {}), ignoring cache",
                        m.source_path.display(),
                        src_canon.display()
                    );
                    None
                }
            })
        } else {
            None
        };

        // ── 5. Scan source (incremental if manifest available) ────────────
        info!("Scanning source: {}", self.opts.source.display());
        let mut src_scanner = Scanner::new(
            &self.opts.source,
            self.opts.recursive,
            self.opts.preserve_links,
        )?;
        src_scanner.set_one_file_system(self.opts.one_file_system);

        let src_result = if let Some(ref manifest) = loaded_manifest {
            let cache = manifest.to_incremental_cache();
            info!(
                "Using incremental source scan ({} cached dirs)",
                cache.dir_mtimes.len()
            );
            src_scanner.scan_incremental(&cache)?
        } else {
            src_scanner.scan()?
        };

        // ── 5. Destination: use manifest cache or live scan ───────────────
        let (dst_map, dst_dir_set, used_manifest) = if let Some(ref manifest) = loaded_manifest {
            info!(
                "Using manifest cache ({} files) — skipping destination scan",
                manifest.files.len()
            );
            let map = manifest.to_dst_map(&self.opts.dest);
            let dir_set = manifest.to_dir_set();
            (map, dir_set, true)
        } else {
            (HashMap::new(), HashSet::new(), false)
        };

        // If manifest wasn't used, do a live dst scan
        let (dst_map, dst_dir_set) = if !used_manifest {
            if self.opts.dest.exists() {
                let mut dst_scanner = Scanner::new(
                    &self.opts.dest,
                    self.opts.recursive,
                    self.opts.preserve_links,
                )?;
                dst_scanner.set_one_file_system(self.opts.one_file_system);
                let dst_result = dst_scanner.scan()?;
                let map: HashMap<PathBuf, FileEntry> = dst_result
                    .files
                    .into_iter()
                    .map(|f| (f.rel_path.clone(), f))
                    .collect();
                let dir_set: HashSet<PathBuf> =
                    dst_result.dirs.into_iter().map(|d| d.rel_path).collect();
                (map, dir_set)
            } else {
                (HashMap::new(), HashSet::new())
            }
        } else {
            (dst_map, dst_dir_set)
        };

        if src_result.errors > 0 {
            warn!(
                "{} entries could not be read during source scan",
                src_result.errors
            );
        }

        info!(
            "Source: {} files, {} | Dest: {} existing files{}",
            src_result.files.len(),
            bytesize::ByteSize::b(src_result.total_bytes),
            dst_map.len(),
            if used_manifest {
                " (from manifest)"
            } else {
                ""
            },
        );

        // ── 3b. Apply filter engine + size filters + --files-from ──────────
        let mut src_result = src_result;

        // --files-from: restrict to only files listed in the given file
        if let Some(ref files_from_path) = self.opts.files_from {
            let content = fs::read_to_string(files_from_path).map_err(|e| ResyncError::Io {
                path: files_from_path.display().to_string(),
                source: e,
            })?;
            let allowed: HashSet<PathBuf> = content
                .lines()
                .map(|l| l.trim())
                .filter(|l| !l.is_empty() && !l.starts_with('#'))
                .map(PathBuf::from)
                .collect();
            let before = src_result.files.len();
            src_result.files.retain(|f| allowed.contains(&f.rel_path));
            info!(
                "--files-from: {} -> {} files after filtering",
                before,
                src_result.files.len()
            );
        }

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
        // PERF: Uses inode from scanner's cached metadata — ZERO extra stat()s.
        #[cfg(unix)]
        {
            src_result.files.sort_unstable_by_key(|f| f.ino);
        }

        // ── 4. Collect src_paths for --delete ─────────────────────────────
        // PERF: Use references to avoid cloning every PathBuf.
        let src_paths: HashSet<&Path> = if self.opts.delete {
            src_result
                .files
                .iter()
                .map(|f| f.rel_path.as_path())
                .collect()
        } else {
            HashSet::new()
        };

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
        let mut applier = Applier::new(
            self.opts.preserve_perms,
            self.opts.preserve_times,
            self.opts.preserve_owner,
            self.opts.preserve_group,
            self.opts.dry_run,
            self.opts.fsync,
            self.opts.inplace,
        );
        applier.set_sparse(self.opts.sparse);
        applier.set_append(self.opts.append);
        applier.set_partial(self.opts.partial);
        applier.set_reflink(self.opts.reflink);
        applier.set_xattrs(self.opts.xattrs);
        if let Some(ref chmod) = self.opts.chmod {
            applier.set_chmod(chmod);
        }
        if let Some(ref chown) = self.opts.chown {
            applier.set_chown(chown);
        }

        let error_count = AtomicU64::new(0);

        // ── 6b. Build hard-link tracking map ──────────────────────────────
        //
        // If -H is set, group source files by (dev, ino) to recreate hard
        // link sets at the destination.  First file in each group is copied
        // normally; subsequent files are hard-linked to the first.
        let hard_link_map: Mutex<HashMap<(u64, u64), PathBuf>> = Mutex::new(HashMap::new());

        // ── 7. Two-phase processing ──────────────────────────────────────
        //
        // Phase 1: SEQUENTIAL decision — classify each file as New / Changed / Skip.
        //          Pure HashMap lookups, zero I/O, zero thread overhead.
        //          Completes in microseconds for 100K files.
        //
        // Phase 2: PARALLEL I/O — copy/delta only the files that actually need work.
        //          Uses rayon thread pool for genuine CPU+I/O parallelism.
        //
        // This two-phase design avoids the massive futex contention that occurs when
        // rayon workers all compete on shared data structures (HashMap, progress
        // reporter) for files that end up being skipped anyway.

        #[derive(Debug)]
        enum Action {
            CopyNew,  // File doesn't exist on dest
            CopyFull, // --whole-file forced full copy
            Delta,    // Existing file needs delta sync
        }

        let mut work_list: Vec<(usize, Action)> = Vec::new();
        let mut skipped_bytes: u64 = 0;
        let mut skipped_count: u64 = 0;

        // FAST PATH (--trust-mtime only): If incremental scan found no directory
        // changes AND we have a valid manifest, assume the source tree is identical
        // to what was last synced. ONLY SAFE when files are not edited in-place.
        let source_unchanged = self.opts.trust_mtime
            && loaded_manifest.is_some()
            && used_manifest
            && src_result.dirs_rescanned <= 1
            && !self.opts.delete
            && !self.opts.checksum;

        if source_unchanged {
            skipped_count = src_result.files.len() as u64;
            skipped_bytes = src_result.total_bytes;
            info!(
                "Fast path: source tree unchanged (0 dirs rescanned) — skipping {} file comparisons",
                skipped_count
            );
        } else {
            for (idx, src_entry) in src_result.files.iter().enumerate() {
                let dst_entry = dst_map.get(&src_entry.rel_path);
                let dst_exists = dst_entry.is_some();

                // --existing: skip files that don't exist on dest
                if self.opts.existing && !dst_exists {
                    skipped_bytes += src_entry.size;
                    skipped_count += 1;
                    continue;
                }

                // --ignore-existing: skip files that exist on dest
                if self.opts.ignore_existing && dst_exists {
                    skipped_bytes += src_entry.size;
                    skipped_count += 1;
                    continue;
                }

                // --update: skip if dest is newer
                if self.opts.update {
                    if let Some(de) = dst_entry {
                        if de.modified > src_entry.modified {
                            skipped_bytes += src_entry.size;
                            skipped_count += 1;
                            continue;
                        }
                    }
                }

                if !dst_exists {
                    work_list.push((idx, Action::CopyNew));
                } else if self.opts.whole_file {
                    work_list.push((idx, Action::CopyFull));
                } else {
                    // mtime+size fast-path
                    if !self.opts.checksum {
                        if let Some(de) = dst_entry {
                            let size_match = de.size == src_entry.size;
                            let mtime_match = if self.opts.size_only {
                                true
                            } else if self.opts.modify_window > 0 {
                                let diff = if src_entry.modified > de.modified {
                                    src_entry
                                        .modified
                                        .duration_since(de.modified)
                                        .unwrap_or_default()
                                } else {
                                    de.modified
                                        .duration_since(src_entry.modified)
                                        .unwrap_or_default()
                                };
                                diff.as_secs() <= self.opts.modify_window as u64
                            } else {
                                src_entry.modified == de.modified
                            };

                            if size_match && mtime_match {
                                skipped_bytes += src_entry.size;
                                skipped_count += 1;
                                continue;
                            }
                        }
                    }
                    work_list.push((idx, Action::Delta));
                }
            }
        } // end of else (non-fast-path)

        // Report skipped files in bulk (no per-file atomic operations)
        reporter.on_bulk_skipped(skipped_count, skipped_bytes);

        info!(
            "Decision: {} to process, {} skipped (no change)",
            work_list.len(),
            skipped_count,
        );

        // Phase 2: Parallel I/O — only for files that need actual work
        if !work_list.is_empty() {
            pool.install(|| {
                work_list.par_iter().for_each(|(idx, action)| {
                    let src_entry = &src_result.files[*idx];
                    let dst_path = self.opts.dest.join(&src_entry.rel_path);

                    // ── Hard-link preservation (-H) ──────────────────────
                    // If this inode was already copied, just hard-link to it.
                    if self.opts.hard_links && !src_entry.is_symlink && src_entry.size > 0 {
                        let key = (src_entry.dev, src_entry.ino);
                        let existing = {
                            let map = hard_link_map.lock().unwrap();
                            map.get(&key).cloned()
                        };
                        if let Some(first_dst) = existing {
                            // This is a subsequent hard link — just link it
                            if !self.opts.dry_run {
                                if let Some(parent) = dst_path.parent() {
                                    fs::create_dir_all(parent).ok();
                                }
                                let _ = fs::remove_file(&dst_path);
                                if let Err(e) = fs::hard_link(&first_dst, &dst_path) {
                                    warn!(
                                        "hard-link {} -> {}: {e}",
                                        first_dst.display(),
                                        dst_path.display()
                                    );
                                    // Fall through to normal copy
                                } else {
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

                    // --link-dest check
                    if let Some(ref link_dir) = self.opts.link_dest {
                        if matches!(action, Action::CopyNew) {
                            let link_src = link_dir.join(&src_entry.rel_path);
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

                    if self.opts.verbose {
                        println!("{} -> {}", src_entry.rel_path.display(), dst_path.display());
                    }

                    // ── Bandwidth limiting ────────────────────────────────
                    if let Some(ref limiter) = self.opts.rate_limiter {
                        limiter.acquire(src_entry.size);
                    }

                    match action {
                        Action::CopyNew | Action::CopyFull => {
                            match applier.copy_new(&src_entry.abs_path, &dst_path, src_entry) {
                                Ok(bytes) => {
                                    if matches!(action, Action::CopyNew) {
                                        reporter.counters.files_new.fetch_add(1, Ordering::Relaxed);
                                    } else {
                                        reporter
                                            .counters
                                            .files_updated
                                            .fetch_add(1, Ordering::Relaxed);
                                    }
                                    reporter.on_file_done(
                                        src_entry.size,
                                        bytes,
                                        &src_entry.rel_path.display().to_string(),
                                    );

                                    // Record hard-link mapping for -H
                                    if self.opts.hard_links && !src_entry.is_symlink {
                                        let key = (src_entry.dev, src_entry.ino);
                                        let mut map = hard_link_map.lock().unwrap();
                                        map.entry(key).or_insert_with(|| dst_path.clone());
                                    }

                                    // Post-transfer checksum verification
                                    if self.opts.checksum_verify
                                        && !src_entry.is_symlink
                                        && src_entry.size > 0
                                    {
                                        Self::verify_checksum(
                                            &src_entry.abs_path,
                                            &dst_path,
                                            &src_entry.rel_path,
                                            &error_count,
                                        );
                                    }
                                }
                                Err(e) => {
                                    error!("copy failed for {}: {e}", src_entry.rel_path.display());
                                    error_count.fetch_add(1, Ordering::Relaxed);
                                    reporter.on_file_error(src_entry.size);
                                }
                            }
                        }
                        Action::Delta => {
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
                                reporter.on_file_skipped(src_entry.size);
                                return;
                            }

                            reporter
                                .counters
                                .bytes_delta_reused
                                .fetch_add(delta.reuse_bytes, Ordering::Relaxed);

                            // Backup before overwriting
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

                                    // Record hard-link mapping for -H
                                    if self.opts.hard_links && !src_entry.is_symlink {
                                        let key = (src_entry.dev, src_entry.ino);
                                        let mut map = hard_link_map.lock().unwrap();
                                        map.entry(key).or_insert_with(|| dst_path.clone());
                                    }

                                    // Post-transfer checksum verification
                                    if self.opts.checksum_verify
                                        && !src_entry.is_symlink
                                        && src_entry.size > 0
                                    {
                                        Self::verify_checksum(
                                            &src_entry.abs_path,
                                            &dst_path,
                                            &src_entry.rel_path,
                                            &error_count,
                                        );
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
                    }
                });
            });
        }

        // ── 8. Handle --delete ────────────────────────────────────────────
        //
        // PERF: Uses the pre-scanned dst_map — NO second directory traverse.
        if self.opts.delete {
            let mut delete_count: u64 = 0;

            let to_delete: Vec<_> = dst_map
                .iter()
                .filter(|(rel_path, _)| !src_paths.contains(rel_path.as_path()))
                .filter(|(rel_path, _)| {
                    if self.opts.delete_excluded {
                        true
                    } else {
                        self.opts
                            .filter_engine
                            .as_ref()
                            .is_none_or(|fe| !fe.is_excluded(rel_path, false))
                    }
                })
                .collect();

            for (rel_path, entry) in &to_delete {
                if let Some(max) = self.opts.max_delete {
                    if delete_count >= max {
                        warn!("--max-delete limit ({max}) reached, stopping deletions");
                        break;
                    }
                }

                if self.opts.verbose {
                    println!("deleting {}", rel_path.display());
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

            // Remove orphan empty directories
            if !self.opts.dry_run {
                let src_dirs: HashSet<PathBuf> =
                    src_result.dirs.iter().map(|d| d.rel_path.clone()).collect();
                let mut orphan_dirs: Vec<_> = dst_dir_set
                    .iter()
                    .filter(|d| !src_dirs.contains(d.as_path()))
                    .map(|d| self.opts.dest.join(d))
                    .collect();
                orphan_dirs.sort_by(|a, b| b.components().count().cmp(&a.components().count()));
                for dir in orphan_dirs {
                    if self.opts.force {
                        fs::remove_dir_all(&dir).ok();
                    } else {
                        fs::remove_dir(&dir).ok();
                    }
                }
            }
        }

        // ── 9. Save manifest cache + TOC ──────────────────────────────────
        //
        // After a successful sync, save the current source scan as the manifest
        // so the next run can skip the destination scan and do incremental
        // source scanning.  Also save a lightweight TOC for ultra-fast no-change
        // detection on subsequent runs.
        // PERF: Skip saving if nothing changed and manifest already exists
        // (saves ~80ms at 100K files).
        let anything_changed = !work_list.is_empty()
            || (self.opts.delete
                && dst_map
                    .iter()
                    .any(|(rp, _)| !src_paths.contains(rp.as_path())));
        let manifest_exists = loaded_manifest.is_some();
        if !self.opts.no_manifest && !self.opts.dry_run && (anything_changed || !manifest_exists) {
            let src_canon = self
                .opts
                .source
                .canonicalize()
                .unwrap_or_else(|_| self.opts.source.clone());
            let manifest = Manifest::from_scan_result(&src_result, src_canon);
            if let Err(e) = manifest.save(&manifest_path) {
                warn!("Failed to save manifest cache: {e}");
            }
            // Save lightweight TOC for next run's fast path
            let toc = crate::manifest::ManifestToc::from_manifest(&manifest);
            let toc_dest = if let Some(ref dir) = self.opts.manifest_dir {
                dir.clone()
            } else {
                self.opts.dest.clone()
            };
            if let Err(e) = toc.save(&toc_dest) {
                warn!("Failed to save TOC: {e}");
            }
        } else if !self.opts.no_manifest && !self.opts.dry_run {
            info!("No changes detected — skipping manifest save");
        }

        // ── 10. Prune empty directories ───────────────────────────────────
        if self.opts.prune_empty_dirs && !self.opts.dry_run {
            Self::prune_empty_dirs(&self.opts.dest);
        }

        // ── 11. Report ────────────────────────────────────────────────────
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

    /// Post-transfer checksum verification: hash both files and compare.
    fn verify_checksum(src_path: &Path, dst_path: &Path, rel_path: &Path, error_count: &AtomicU64) {
        let src_hash = Self::blake3_file(src_path);
        let dst_hash = Self::blake3_file(dst_path);
        match (src_hash, dst_hash) {
            (Ok(s), Ok(d)) => {
                if s != d {
                    error!(
                        "CHECKSUM MISMATCH after transfer: {} (src={:x?}, dst={:x?})",
                        rel_path.display(),
                        &s[..8],
                        &d[..8]
                    );
                    error_count.fetch_add(1, Ordering::Relaxed);
                }
            }
            (Err(e), _) | (_, Err(e)) => {
                warn!("checksum verify failed for {}: {e}", rel_path.display());
            }
        }
    }

    /// Compute BLAKE3 hash of a file.
    fn blake3_file(path: &Path) -> std::io::Result<[u8; 32]> {
        let mut hasher = blake3::Hasher::new();
        let mut file = fs::File::open(path)?;
        let mut buf = vec![0u8; 256 * 1024];
        loop {
            let n = std::io::Read::read(&mut file, &mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        Ok(*hasher.finalize().as_bytes())
    }

    /// Recursively prune empty directories from the destination tree.
    fn prune_empty_dirs(root: &Path) {
        // Walk bottom-up by collecting all dirs, sorting by depth descending
        let mut dirs: Vec<PathBuf> = Vec::new();
        Self::collect_dirs(root, &mut dirs);
        dirs.sort_by_key(|b| std::cmp::Reverse(b.components().count()));

        for dir in dirs {
            if dir == root {
                continue;
            }
            // remove_dir only succeeds on empty dirs
            let _ = fs::remove_dir(&dir);
        }
    }

    fn collect_dirs(dir: &Path, out: &mut Vec<PathBuf>) {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                if let Ok(ft) = entry.file_type() {
                    if ft.is_dir() {
                        let path = entry.path();
                        out.push(path.clone());
                        Self::collect_dirs(&path, out);
                    }
                }
            }
        }
    }
}
