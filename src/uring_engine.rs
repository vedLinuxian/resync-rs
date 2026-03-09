//! io_uring Batch Engine — amortized syscall overhead for many-file workloads.
//!
//! For workloads with thousands of small files, the per-file syscall overhead
//! dominates.  Each file needs ~7 syscalls (open×2 + read + write + close×2 +
//! metadata), each costing ~1-2μs in user→kernel transitions.
//!
//! This engine batches these operations through Linux io_uring (kernel 5.1+),
//! submitting up to 128 operations per `io_uring_enter()` call.  This reduces
//! per-file syscall overhead from ~7 to an amortized ~3 (metadata ops like
//! fchmod/futimens are not yet supported in io_uring, so they use regular
//! syscalls).
//!
//! ## Modes
//!
//! - **Normal**: Uses `io_uring_enter()` to submit and reap batches.  Works
//!   on all kernels ≥ 5.1 without special privileges.
//!
//! - **SQPOLL**: A dedicated kernel thread polls the submission queue,
//!   eliminating `io_uring_enter()` calls entirely.  Ideal for ultra-high
//!   IOPS workloads.  May require elevated privileges on some kernels.
//!
//! ## SSD SAFETY
//! ────────────────────────────────────────────────────────────────
//! io_uring submits the **exact same** kernel I/O operations as regular
//! syscalls (openat, read, write, close) — just through a shared-memory
//! ring buffer instead of individual system calls.
//!
//! **No raw device access.  No DMA bypass.  No TRIM/DISCARD.**
//! All operations go through the filesystem (ext4) which handles
//! wear leveling, journaling, and all data safety mechanisms.
//!
//! ## Fallback
//!
//! If io_uring is unavailable (old kernel, seccomp restrictions, etc.),
//! `try_new()` returns `None` and the caller falls back to regular
//! syscalls.  Individual operation failures within a batch are also
//! returned as `Err` so the caller can retry through the regular path.

use std::ffi::CString;
use std::fs::File;
use std::io;
use std::mem::ManuallyDrop;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::FromRawFd;
use std::path::PathBuf;
use std::time::SystemTime;

use io_uring::{IoUring, opcode, types};

// ─── Constants ───────────────────────────────────────────────────────────────

/// Maximum files per batch.  With a 256-SQE ring, each phase submits
/// ≤128 entries, leaving headroom for completion processing.
const BATCH_SIZE: usize = 128;

/// Files larger than this go through the regular copy_file_range path
/// (which is bandwidth-optimized rather than syscall-optimized).
pub const URING_SIZE_THRESHOLD: u64 = 32 * 1024; // 32 KB

// ─── Public types ────────────────────────────────────────────────────────────

/// A single file copy task for the io_uring batch engine.
pub struct BatchCopyTask {
    pub src_path: PathBuf,
    pub dst_path: PathBuf,
    pub size: u64,
    pub mode: u32,
    pub modified: SystemTime,
    pub uid: u32,
    pub gid: u32,
}

// ─── UringEngine ─────────────────────────────────────────────────────────────

/// io_uring batch file copy engine.
///
/// Reduces syscall overhead by batching openat/read/write/close operations
/// into shared-memory ring submissions.
pub struct UringEngine {
    ring: IoUring,
}

impl UringEngine {
    /// Create a regular io_uring engine (no special privileges needed).
    /// Returns `None` if io_uring is unavailable on this kernel.
    pub fn try_new() -> Option<Self> {
        IoUring::new(256).ok().map(|ring| Self { ring })
    }

    /// Create an engine with SQPOLL mode (kernel thread polls submissions).
    ///
    /// Eliminates `io_uring_enter()` syscalls for submissions — the kernel
    /// thread continuously polls the SQ.  Falls back to regular mode on
    /// permission errors.
    pub fn try_new_sqpoll() -> Option<Self> {
        // Try SQPOLL first (may need CAP_SYS_ADMIN on older kernels)
        if let Ok(ring) = IoUring::builder()
            .setup_sqpoll(500) // 500ms idle timeout
            .build(256)
        {
            return Some(Self { ring });
        }
        // Fall back to regular mode
        Self::try_new()
    }

    /// Create an engine with IOPOLL mode (polled I/O completion).
    ///
    /// # HACK 3: IORING_SETUP_IOPOLL
    ///
    /// Normal I/O completion uses hardware interrupts which cost ~2-3μs
    /// each (interrupt delivery + context switch + softirq processing).
    /// With IOPOLL, the kernel busy-polls the NVMe completion queue
    /// directly, eliminating interrupt overhead entirely.
    ///
    /// **Requirements**: Opened file descriptors MUST use O_DIRECT.
    /// IOPOLL with buffered I/O returns EINVAL.
    ///
    /// **Best for**: NVMe SSDs where individual I/O latency is <10μs
    /// and interrupt overhead is a significant fraction of total latency.
    ///
    /// Falls back to SQPOLL → regular mode if IOPOLL is unavailable.
    pub fn try_new_iopoll() -> Option<Self> {
        if let Ok(ring) = IoUring::builder()
            .setup_iopoll()
            .build(256)
        {
            return Some(Self { ring });
        }
        // IOPOLL unavailable — try SQPOLL, then regular
        Self::try_new_sqpoll()
    }

    /// Batch-copy multiple small files using io_uring.
    ///
    /// Processes files in chunks of up to 128, performing multi-phase
    /// batched I/O:
    ///
    /// 1. Batch `openat(src, O_RDONLY)` — one `io_uring_enter()` for 128 opens
    /// 2. Batch `openat(dst, O_CREAT|O_WRONLY|O_TRUNC)` — one enter for 128
    /// 3. Batch `read(src_fd, buffer)` — one enter for data reads
    /// 4. Batch `close(src_fd)` — release source descriptors
    /// 5. Batch `write(dst_fd, buffer)` — one enter for data writes
    /// 6. Regular `fchmod` + `futimens` per file (not in io_uring)
    /// 7. Batch `close(dst_fd)` — release dest descriptors
    ///
    /// Total: ~5 `io_uring_enter()` + 2-3 regular syscalls per file
    /// vs. ~7 regular syscalls per file without io_uring.
    pub fn batch_copy(
        &mut self,
        tasks: &[BatchCopyTask],
        preserve_perms: bool,
        preserve_times: bool,
        preserve_owner: bool,
    ) -> Vec<io::Result<u64>> {
        let mut all_results = Vec::with_capacity(tasks.len());

        for chunk in tasks.chunks(BATCH_SIZE) {
            let chunk_results =
                self.copy_chunk(chunk, preserve_perms, preserve_times, preserve_owner);
            all_results.extend(chunk_results);
        }

        all_results
    }

    /// Process a single batch of ≤128 files through the io_uring pipeline.
    fn copy_chunk(
        &mut self,
        tasks: &[BatchCopyTask],
        preserve_perms: bool,
        preserve_times: bool,
        preserve_owner: bool,
    ) -> Vec<io::Result<u64>> {
        let n = tasks.len();
        let mut results: Vec<io::Result<u64>> = (0..n).map(|_| Ok(0u64)).collect();
        let mut failed = vec![false; n];

        // ── Pre-allocate CStrings (must outlive io_uring submissions) ──
        let src_cstrs: Vec<Option<CString>> = tasks
            .iter()
            .map(|t| CString::new(t.src_path.as_os_str().as_bytes()).ok())
            .collect();
        let dst_cstrs: Vec<Option<CString>> = tasks
            .iter()
            .map(|t| CString::new(t.dst_path.as_os_str().as_bytes()).ok())
            .collect();

        for i in 0..n {
            if src_cstrs[i].is_none() || dst_cstrs[i].is_none() {
                failed[i] = true;
                results[i] = Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "path contains null byte",
                ));
            }
        }

        // ── Pre-allocate read buffers ──
        let mut buffers: Vec<Vec<u8>> = tasks
            .iter()
            .map(|t| {
                if t.size > 0 {
                    vec![0u8; t.size as usize]
                } else {
                    Vec::new()
                }
            })
            .collect();

        let mut src_fds = vec![-1i32; n];
        let mut dst_fds = vec![-1i32; n];

        // ═══════════════════════════════════════════════════════════════
        // Phase 1: MERGED openat(src) + openat(dst) — single submission
        //
        // BUG 7 FIX: Previously these were two sequential phases, each
        // with its own submit_and_wait().  If any single open was slow
        // (FUSE mount, network FS, cold inode cache), it blocked ALL
        // subsequent operations.  By merging into one submission, the
        // kernel can process all opens concurrently and independently.
        //
        // User data encoding: lower bits = file index,
        //   bit 31 = 0 for src open, 1 for dst open.
        // ═══════════════════════════════════════════════════════════════
        const DST_FLAG: u64 = 1 << 31;
        let mut phase_count = 0;

        // Submit all src opens
        for i in 0..n {
            if failed[i] {
                continue;
            }
            let c_path = src_cstrs[i].as_ref().unwrap();
            let entry = opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), c_path.as_ptr())
                .flags(libc::O_RDONLY | libc::O_CLOEXEC)
                .build()
                .user_data(i as u64); // bit 31 = 0 → src

            if unsafe { self.ring.submission().push(&entry) }.is_err() {
                failed[i] = true;
                results[i] = Err(io::Error::new(
                    io::ErrorKind::Other,
                    "io_uring: submission queue full",
                ));
                continue;
            }
            phase_count += 1;
        }

        // Submit all dst opens in the SAME batch
        for i in 0..n {
            if failed[i] {
                continue;
            }
            let c_path = dst_cstrs[i].as_ref().unwrap();
            let entry = opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), c_path.as_ptr())
                .flags(libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC | libc::O_CLOEXEC)
                .mode(tasks[i].mode & 0o7777)
                .build()
                .user_data(DST_FLAG | i as u64); // bit 31 = 1 → dst

            if unsafe { self.ring.submission().push(&entry) }.is_err() {
                failed[i] = true;
                results[i] = Err(io::Error::new(
                    io::ErrorKind::Other,
                    "io_uring: submission queue full",
                ));
                continue;
            }
            phase_count += 1;
        }

        if phase_count > 0 {
            if let Err(e) = self.ring.submit_and_wait(phase_count) {
                self.mark_all_failed(&mut failed, &mut results, n, "open", &e);
                return results;
            }

            // Reap ALL open results — demux by DST_FLAG bit
            for cqe in self.ring.completion() {
                let ud = cqe.user_data();
                let is_dst = ud & DST_FLAG != 0;
                let i = (ud & !DST_FLAG) as usize;

                if cqe.result() >= 0 {
                    if is_dst {
                        dst_fds[i] = cqe.result();
                    } else {
                        src_fds[i] = cqe.result();
                    }
                } else if is_dst && -cqe.result() == libc::ENOENT {
                    // Parent directory missing — create + retry with regular open
                    if let Some(parent) = tasks[i].dst_path.parent() {
                        if std::fs::create_dir_all(parent).is_ok() {
                            if let Ok(f) = std::fs::OpenOptions::new()
                                .write(true)
                                .create(true)
                                .truncate(true)
                                .open(&tasks[i].dst_path)
                            {
                                use std::os::unix::io::AsRawFd;
                                dst_fds[i] = f.as_raw_fd();
                                std::mem::forget(f);
                                continue;
                            }
                        }
                    }
                    failed[i] = true;
                    results[i] = Err(io::Error::from_raw_os_error(libc::ENOENT));
                    Self::close_fd(&mut src_fds[i]);
                } else {
                    failed[i] = true;
                    results[i] = Err(io::Error::from_raw_os_error(-cqe.result()));
                    // Clean up the paired fd if the other side succeeded
                    if is_dst {
                        Self::close_fd(&mut src_fds[i]);
                    }
                    // dst_fds[i] stays -1 (never opened)
                }
            }

            // Cross-check: if either src or dst failed, mark as failed and clean up
            for i in 0..n {
                if failed[i] {
                    continue;
                }
                if src_fds[i] < 0 || dst_fds[i] < 0 {
                    failed[i] = true;
                    results[i] = Err(io::Error::new(
                        io::ErrorKind::Other,
                        "io_uring: open phase incomplete",
                    ));
                    Self::close_fd(&mut src_fds[i]);
                    Self::close_fd(&mut dst_fds[i]);
                }
            }
        }

        // ═══════════════════════════════════════════════════════════════
        // Phase 3: Batch read(src_fd, buffer) — for files with data
        // ═══════════════════════════════════════════════════════════════
        phase_count = 0;
        for i in 0..n {
            if failed[i] || tasks[i].size == 0 {
                continue;
            }
            let entry = opcode::Read::new(
                types::Fd(src_fds[i]),
                buffers[i].as_mut_ptr(),
                tasks[i].size.min(u32::MAX as u64) as u32,
            )
            .offset(0)
            .build()
            .user_data(i as u64);

            if unsafe { self.ring.submission().push(&entry) }.is_err() {
                failed[i] = true;
                results[i] = Err(io::Error::new(
                    io::ErrorKind::Other,
                    "io_uring: submission queue full",
                ));
                continue;
            }
            phase_count += 1;
        }

        if phase_count > 0 {
            if let Err(e) = self.ring.submit_and_wait(phase_count) {
                self.cleanup_fds(&mut src_fds, &mut dst_fds, n);
                self.mark_all_failed(&mut failed, &mut results, n, "read", &e);
                return results;
            }
            for cqe in self.ring.completion() {
                let i = cqe.user_data() as usize;
                if cqe.result() >= 0 {
                    let actual = cqe.result() as usize;
                    let expected = tasks[i].size as usize;
                    // BUG 6 FIX: Linux does NOT guarantee a full read in one
                    // io_uring op.  A "short read" (actual < expected) means
                    // the remaining bytes were not read.  If we truncate the
                    // buffer and proceed, the destination gets a truncated
                    // file — silent data loss.
                    //
                    // Mark short reads as failed so the caller retries this
                    // file through the regular read() loop which handles
                    // partial reads correctly.
                    if actual < expected {
                        failed[i] = true;
                        results[i] = Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!(
                                "io_uring short read: got {actual} of {expected} bytes"
                            ),
                        ));
                    } else {
                        buffers[i].truncate(actual);
                    }
                } else {
                    failed[i] = true;
                    results[i] = Err(io::Error::from_raw_os_error(-cqe.result()));
                }
            }
        }

        // ═══════════════════════════════════════════════════════════════
        // Phase 4: Batch close(src_fd) — release source descriptors
        // ═══════════════════════════════════════════════════════════════
        self.batch_close(&mut src_fds, n);

        // ═══════════════════════════════════════════════════════════════
        // Phase 5: Batch write(dst_fd, buffer) — for files with data
        // ═══════════════════════════════════════════════════════════════
        phase_count = 0;
        for i in 0..n {
            if failed[i] || tasks[i].size == 0 {
                continue;
            }
            let len = buffers[i].len();
            if len == 0 {
                continue;
            }
            let entry = opcode::Write::new(
                types::Fd(dst_fds[i]),
                buffers[i].as_ptr(),
                len as u32,
            )
            .offset(0)
            .build()
            .user_data(i as u64);

            if unsafe { self.ring.submission().push(&entry) }.is_err() {
                failed[i] = true;
                results[i] = Err(io::Error::new(
                    io::ErrorKind::Other,
                    "io_uring: submission queue full",
                ));
                continue;
            }
            phase_count += 1;
        }

        if phase_count > 0 {
            if let Err(e) = self.ring.submit_and_wait(phase_count) {
                for i in 0..n {
                    Self::close_fd(&mut dst_fds[i]);
                }
                self.mark_all_failed(&mut failed, &mut results, n, "write", &e);
                return results;
            }
            for cqe in self.ring.completion() {
                let i = cqe.user_data() as usize;
                if cqe.result() >= 0 {
                    results[i] = Ok(cqe.result() as u64);
                } else {
                    failed[i] = true;
                    results[i] = Err(io::Error::from_raw_os_error(-cqe.result()));
                }
            }
        }

        // Set Ok(0) for empty files that didn't fail
        for i in 0..n {
            if !failed[i] && tasks[i].size == 0 {
                results[i] = Ok(0);
            }
        }

        // ═══════════════════════════════════════════════════════════════
        // Phase 6: Metadata — regular syscalls (not in io_uring)
        //
        // fchmod, futimens, fchown use the still-open dst_fd to avoid
        // path resolution overhead.
        // ═══════════════════════════════════════════════════════════════
        for i in 0..n {
            if failed[i] || dst_fds[i] < 0 {
                continue;
            }
            // Use ManuallyDrop to borrow the fd without closing it
            let file = ManuallyDrop::new(unsafe { File::from_raw_fd(dst_fds[i]) });
            crate::io_engine::apply_metadata_fast(
                &file,
                if preserve_perms {
                    Some(tasks[i].mode & 0o7777)
                } else {
                    None
                },
                if preserve_times {
                    Some(tasks[i].modified)
                } else {
                    None
                },
                if preserve_owner {
                    Some(tasks[i].uid)
                } else {
                    None
                },
                if preserve_owner {
                    Some(tasks[i].gid)
                } else {
                    None
                },
            );
        }

        // ═══════════════════════════════════════════════════════════════
        // Phase 7: Batch close(dst_fd)
        // ═══════════════════════════════════════════════════════════════
        self.batch_close(&mut dst_fds, n);

        results
    }

    // ── Helper methods ───────────────────────────────────────────────────

    /// Batch-close an array of fds using io_uring.
    fn batch_close(&mut self, fds: &mut [i32], n: usize) {
        let mut count = 0;
        for i in 0..n {
            if fds[i] < 0 {
                continue;
            }
            let entry = opcode::Close::new(types::Fd(fds[i]))
                .build()
                .user_data(i as u64);

            if unsafe { self.ring.submission().push(&entry) }.is_ok() {
                count += 1;
            } else {
                // SQ full — close synchronously
                unsafe { libc::close(fds[i]) };
            }
            fds[i] = -1;
        }
        if count > 0 {
            let _ = self.ring.submit_and_wait(count);
            // Drain completions
            for _cqe in self.ring.completion() {}
        }
    }

    /// Close a single fd synchronously.
    #[inline]
    fn close_fd(fd: &mut i32) {
        if *fd >= 0 {
            unsafe { libc::close(*fd) };
            *fd = -1;
        }
    }

    /// Close all open fds synchronously (error cleanup).
    fn cleanup_fds(&self, src_fds: &mut [i32], dst_fds: &mut [i32], n: usize) {
        for i in 0..n {
            if src_fds[i] >= 0 {
                unsafe { libc::close(src_fds[i]) };
                src_fds[i] = -1;
            }
            if dst_fds[i] >= 0 {
                unsafe { libc::close(dst_fds[i]) };
                dst_fds[i] = -1;
            }
        }
    }

    /// Mark all non-failed entries as failed (ring-level error).
    fn mark_all_failed(
        &self,
        failed: &mut [bool],
        results: &mut [io::Result<u64>],
        n: usize,
        phase: &str,
        err: &io::Error,
    ) {
        for i in 0..n {
            if !failed[i] {
                failed[i] = true;
                results[i] = Err(io::Error::new(
                    err.kind(),
                    format!("io_uring {phase}: {err}"),
                ));
            }
        }
    }
}

// ─── Convenience functions ───────────────────────────────────────────────────

/// Try to batch-copy small files using io_uring.
///
/// Returns `None` if io_uring is unavailable on this kernel.
/// Returns `Some(results)` with per-file `Ok(bytes)` or `Err(e)`.
///
/// Files that fail can be retried through the regular copy path.
pub fn try_batch_copy(
    tasks: &[BatchCopyTask],
    preserve_perms: bool,
    preserve_times: bool,
    preserve_owner: bool,
) -> Option<Vec<io::Result<u64>>> {
    let mut engine = UringEngine::try_new()?;
    Some(engine.batch_copy(tasks, preserve_perms, preserve_times, preserve_owner))
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn skip_if_no_uring() -> Option<UringEngine> {
        UringEngine::try_new()
    }

    #[test]
    fn test_uring_single_file_copy() {
        let Some(mut engine) = skip_if_no_uring() else {
            eprintln!("io_uring not available, skipping test");
            return;
        };

        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src.txt");
        let dst = dir.path().join("dst.txt");
        std::fs::write(&src, b"hello io_uring world!").unwrap();

        let meta = std::fs::metadata(&src).unwrap();
        let tasks = vec![BatchCopyTask {
            src_path: src,
            dst_path: dst.clone(),
            size: meta.len(),
            mode: 0o644,
            modified: meta.modified().unwrap(),
            uid: 0,
            gid: 0,
        }];

        let results = engine.batch_copy(&tasks, true, true, false);
        assert!(results[0].is_ok(), "copy failed: {:?}", results[0]);
        assert_eq!(*results[0].as_ref().unwrap(), meta.len());
        assert_eq!(
            std::fs::read_to_string(&dst).unwrap(),
            "hello io_uring world!"
        );
    }

    #[test]
    fn test_uring_empty_files() {
        let Some(mut engine) = skip_if_no_uring() else {
            return;
        };

        let dir = tempfile::tempdir().unwrap();
        let tasks: Vec<BatchCopyTask> = (0..10)
            .map(|i| {
                let src = dir.path().join(format!("src_{i}"));
                let dst = dir.path().join(format!("dst_{i}"));
                std::fs::write(&src, b"").unwrap();
                BatchCopyTask {
                    src_path: src,
                    dst_path: dst,
                    size: 0,
                    mode: 0o644,
                    modified: SystemTime::now(),
                    uid: 0,
                    gid: 0,
                }
            })
            .collect();

        let results = engine.batch_copy(&tasks, false, false, false);
        for (i, r) in results.iter().enumerate() {
            assert!(r.is_ok(), "file {i} failed: {r:?}");
            assert_eq!(*r.as_ref().unwrap(), 0);
        }

        // Verify all dst files exist
        for task in &tasks {
            assert!(task.dst_path.exists(), "{:?} missing", task.dst_path);
        }
    }

    #[test]
    fn test_uring_batch_many() {
        let Some(mut engine) = skip_if_no_uring() else {
            return;
        };

        let dir = tempfile::tempdir().unwrap();
        let tasks: Vec<BatchCopyTask> = (0..200)
            .map(|i| {
                let src = dir.path().join(format!("src_{i:04}"));
                let dst = dir.path().join(format!("dst_{i:04}"));
                let data = format!("file content #{i}");
                std::fs::write(&src, data.as_bytes()).unwrap();
                BatchCopyTask {
                    src_path: src,
                    dst_path: dst,
                    size: data.len() as u64,
                    mode: 0o644,
                    modified: SystemTime::now(),
                    uid: 0,
                    gid: 0,
                }
            })
            .collect();

        let results = engine.batch_copy(&tasks, false, false, false);
        let ok_count = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(ok_count, 200, "all 200 files should copy successfully");

        for (i, task) in tasks.iter().enumerate() {
            let expected = format!("file content #{i}");
            let actual = std::fs::read_to_string(&task.dst_path).unwrap();
            assert_eq!(actual, expected, "file {i} content mismatch");
        }
    }

    #[test]
    fn test_uring_nonexistent_src() {
        let Some(mut engine) = skip_if_no_uring() else {
            return;
        };

        let dir = tempfile::tempdir().unwrap();
        let tasks = vec![BatchCopyTask {
            src_path: dir.path().join("no_such_file"),
            dst_path: dir.path().join("dst"),
            size: 100,
            mode: 0o644,
            modified: SystemTime::now(),
            uid: 0,
            gid: 0,
        }];

        let results = engine.batch_copy(&tasks, false, false, false);
        assert!(results[0].is_err(), "should fail for nonexistent source");
    }

    #[test]
    fn test_uring_mixed_success_failure() {
        let Some(mut engine) = skip_if_no_uring() else {
            return;
        };

        let dir = tempfile::tempdir().unwrap();
        let good_src = dir.path().join("good_src");
        let good_dst = dir.path().join("good_dst");
        std::fs::write(&good_src, b"good data").unwrap();

        let tasks = vec![
            BatchCopyTask {
                src_path: good_src,
                dst_path: good_dst.clone(),
                size: 9,
                mode: 0o644,
                modified: SystemTime::now(),
                uid: 0,
                gid: 0,
            },
            BatchCopyTask {
                src_path: dir.path().join("nonexistent"),
                dst_path: dir.path().join("dst_bad"),
                size: 100,
                mode: 0o644,
                modified: SystemTime::now(),
                uid: 0,
                gid: 0,
            },
        ];

        let results = engine.batch_copy(&tasks, false, false, false);
        assert!(results[0].is_ok(), "good file should succeed");
        assert!(results[1].is_err(), "bad file should fail");
        assert_eq!(std::fs::read_to_string(&good_dst).unwrap(), "good data");
    }

    #[test]
    fn test_try_batch_copy_convenience() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        std::fs::write(&src, b"convenience test").unwrap();

        let tasks = vec![BatchCopyTask {
            src_path: src,
            dst_path: dst.clone(),
            size: 16,
            mode: 0o644,
            modified: SystemTime::now(),
            uid: 0,
            gid: 0,
        }];

        if let Some(results) = try_batch_copy(&tasks, false, false, false) {
            assert!(results[0].is_ok());
            assert_eq!(std::fs::read_to_string(&dst).unwrap(), "convenience test");
        } else {
            eprintln!("io_uring not available");
        }
    }
}
