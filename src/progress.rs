//! Real-time progress bars and final statistics reporter.
//!
//!  Uses `indicatif` multi-progress to render two bars simultaneously:
//!   • Files bar — N / M files processed
//!   • Bytes bar — bytes transferred with throughput rate

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytesize::ByteSize;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

// ─── Shared counters (updated from rayon threads) ────────────────────────────

#[derive(Default)]
pub struct SyncCounters {
    pub files_done: AtomicU64,
    pub files_skipped: AtomicU64,
    pub files_errored: AtomicU64,
    pub bytes_transferred: AtomicU64,
    pub bytes_skipped: AtomicU64,
    pub bytes_delta_reused: AtomicU64,
    pub files_new: AtomicU64,
    pub files_updated: AtomicU64,
    pub files_deleted: AtomicU64,
}

impl SyncCounters {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

// ─── Progress reporter ───────────────────────────────────────────────────────

pub struct ProgressReporter {
    _multi: MultiProgress,
    files_bar: ProgressBar,
    bytes_bar: ProgressBar,
    pub counters: Arc<SyncCounters>,
    start: Instant,
    enabled: bool,
    total_bytes: u64,
}

impl ProgressReporter {
    pub fn new(total_files: u64, total_bytes: u64, enabled: bool) -> Self {
        let counters = SyncCounters::new();
        let multi = MultiProgress::new();

        let files_bar = if enabled {
            let pb = multi.add(ProgressBar::new(total_files));
            pb.set_style(
                ProgressStyle::with_template(
                    " {spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} \
                     {pos:>7}/{len:7} files  {msg}",
                )
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
            );
            pb
        } else {
            ProgressBar::hidden()
        };

        let bytes_bar = if enabled {
            let pb = multi.add(ProgressBar::new(total_bytes));
            pb.set_style(
                ProgressStyle::with_template(
                    " {spinner:.yellow} [{elapsed_precise}] {bar:40.yellow/red} \
                     {bytes:>10}/{total_bytes:10}  {binary_bytes_per_sec}  eta {eta}",
                )
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
            );
            pb
        } else {
            ProgressBar::hidden()
        };

        Self {
            _multi: multi,
            files_bar,
            bytes_bar,
            counters,
            start: Instant::now(),
            enabled,
            total_bytes,
        }
    }

    /// Called after each file finishes processing (new or updated).
    /// `file_size` is the TOTAL file size (for progress bar advancement).
    /// `bytes_written` is only the bytes transferred from source (for stats).
    pub fn on_file_done(&self, file_size: u64, bytes_written: u64, file_name: &str) {
        let prev = self.counters.files_done.fetch_add(1, Ordering::Relaxed);
        self.counters
            .bytes_transferred
            .fetch_add(bytes_written, Ordering::Relaxed);
        // BUG FIX #12: Advance bytes bar by TOTAL file size so it reaches 100%
        self.files_bar.inc(1);
        self.bytes_bar.inc(file_size);
        // PERF 9: Only update the UI text every 100 files or for large files
        // to avoid expensive alloc/lock overhead during small-file storms
        if self.enabled && (prev % 100 == 0 || file_size >= 1024 * 1024 * 10) {
            self.files_bar.set_message(file_name.to_string());
        }
    }

    /// Called when a file is skipped (hashes match — no work needed).
    pub fn on_file_skipped(&self, size: u64) {
        self.counters.files_skipped.fetch_add(1, Ordering::Relaxed);
        self.counters
            .bytes_skipped
            .fetch_add(size, Ordering::Relaxed);
        self.files_bar.inc(1);
        // BUG FIX #12: Advance bytes bar for skipped files too
        self.bytes_bar.inc(size);
    }

    /// Bulk skip — avoids per-file atomic contention for large skip batches.
    pub fn on_bulk_skipped(&self, count: u64, bytes: u64) {
        self.counters
            .files_skipped
            .fetch_add(count, Ordering::Relaxed);
        self.counters
            .bytes_skipped
            .fetch_add(bytes, Ordering::Relaxed);
        self.files_bar.inc(count);
        self.bytes_bar.inc(bytes);
    }

    /// BUG FIX #19: Called when a file encounters an error — still counts
    /// toward progress so bars reach 100%.
    pub fn on_file_error(&self, size: u64) {
        self.counters.files_errored.fetch_add(1, Ordering::Relaxed);
        self.files_bar.inc(1);
        self.bytes_bar.inc(size);
    }

    pub fn finish(&self) {
        self.files_bar.finish_and_clear();
        self.bytes_bar.finish_and_clear();
    }

    /// Print the final summary table to stdout.
    pub fn print_summary(&self) {
        let elapsed = self.start.elapsed();
        let c = &self.counters;
        let transferred = c.bytes_transferred.load(Ordering::Relaxed);
        let _skipped_bytes = c.bytes_skipped.load(Ordering::Relaxed);
        let delta_reused = c.bytes_delta_reused.load(Ordering::Relaxed);
        let files_done = c.files_done.load(Ordering::Relaxed);
        let files_skipped = c.files_skipped.load(Ordering::Relaxed);
        let files_errored = c.files_errored.load(Ordering::Relaxed);
        let files_new = c.files_new.load(Ordering::Relaxed);
        let files_updated = c.files_updated.load(Ordering::Relaxed);
        let files_deleted = c.files_deleted.load(Ordering::Relaxed);

        let throughput_bps = if elapsed.as_secs_f64() > 0.0 {
            (transferred as f64 / elapsed.as_secs_f64()) as u64
        } else {
            0
        };

        // BUG FIX #12: "savings" is the ratio of bytes NOT transferred
        // (both whole-file skips and delta-reused chunks) vs total data.
        let total_src_bytes = self.total_bytes;
        let saved_pct = if total_src_bytes > 0 {
            (total_src_bytes - transferred) as f64 / total_src_bytes as f64 * 100.0
        } else {
            0.0
        };

        println!();
        println!("─────────────────────────────────────────────────────");
        println!("  resync-rs  —  sync complete");
        println!("─────────────────────────────────────────────────────");
        println!(
            "  Files total     : {} ({} new, {} updated, {} unchanged, {} errors, {} deleted)",
            files_done + files_skipped + files_errored,
            files_new,
            files_updated,
            files_skipped,
            files_errored,
            files_deleted
        );
        println!("  Data transferred: {}", ByteSize::b(transferred));
        println!(
            "  Data saved      : {}  ({:.1}% of total)",
            ByteSize::b(total_src_bytes.saturating_sub(transferred)),
            saved_pct
        );
        if delta_reused > 0 {
            println!(
                "  Delta reused    : {}  (chunks with matching BLAKE3 hashes)",
                ByteSize::b(delta_reused)
            );
        }
        println!("  Throughput      : {}/s", ByteSize::b(throughput_bps));
        println!("  Wall-clock time : {:.3}s", elapsed.as_secs_f64());
        println!("─────────────────────────────────────────────────────");
    }

    /// Accessor for total source bytes.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}
