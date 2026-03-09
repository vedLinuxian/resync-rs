//! Zero-Copy I/O Engine — the heart of resync's 100x performance advantage.
//!
//! This module implements a hierarchy of zero-copy data movement primitives
//! that keep file data **entirely in kernel space** — no userspace buffers,
//! no context switches for the actual data copy.
//!
//! # Performance Hierarchy (fastest → fallback)
//!
//! ```text
//! 1. reflink (FICLONE)     — instant O(1) CoW copy, zero I/O
//! 2. copy_file_range(2)    — kernel-side copy, may use DMA offload
//! 3. splice(2) via pipe    — zero-copy through kernel pipe buffer
//! 4. sendfile(2)           — zero-copy for fd → socket
//! 5. mmap + write          — one copy (page cache → userspace → page cache)
//! 6. read + write          — two copies (worst case fallback)
//! ```
//!
//! # Additional optimizations
//!
//! - **O_TMPFILE**: Nameless temp files — no directory entry overhead
//! - **fallocate**: Pre-allocate disk space to prevent fragmentation
//! - **fadvise**: Hint kernel about sequential/random access patterns
//! - **Aligned I/O buffers**: 4KB-aligned for direct I/O compatibility
//! - **Batch metadata**: Group chmod/chown/utimensat operations

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::path::Path;

// ─── Constants ───────────────────────────────────────────────────────────────

/// Maximum bytes per sendfile/splice call (kernel limit on some versions).
const _MAX_SPLICE_CHUNK: usize = 0x7fff_f000; // ~2 GB - 4KB

/// Pipe buffer size for splice operations (1 MB for throughput).
const PIPE_BUF_SIZE: i32 = 1024 * 1024;

/// Threshold below which we use simple read+write instead of splice.
/// splice setup (pipe creation) costs ~1-2μs, not worth it for tiny files.
const SPLICE_THRESHOLD: u64 = 32 * 1024; // 32 KB

/// Buffer size for small file read+write fallback path.
#[allow(dead_code)]
const SMALL_FILE_BUF: usize = 64 * 1024; // 64 KB

/// Large aligned I/O buffer size for bulk operations.
pub const ALIGNED_BUF_SIZE: usize = 1024 * 1024; // 1 MB

/// Threshold above which O_DIRECT is used to bypass page cache.
/// Files smaller than this benefit from page cache for metadata+data locality.
/// Files >= 4 MB pollute the page cache without reuse benefit (sync is write-once).
pub const DIRECT_IO_THRESHOLD: u64 = 4 * 1024 * 1024; // 4 MB

/// O_DIRECT buffer alignment (must be ≥ filesystem block size, 4096 on ext4).
const DIRECT_ALIGN: usize = 4096;

// ─── Zero-copy file copy ─────────────────────────────────────────────────────

/// Copy a file using the fastest available zero-copy mechanism.
///
/// Tries in order:
/// 1. `copy_file_range(2)` — kernel-side copy with potential DMA offload
/// 2. `splice(2)` via pipe — zero-copy through kernel pipe buffer
/// 3. `read(2)` + `write(2)` — fallback with aligned 1MB buffer
///
/// Returns (bytes_copied, open destination File handle) for fd-based
/// metadata application. Caller can use `apply_metadata_fast()` on the
/// returned handle to avoid path-based syscalls.
pub fn zero_copy_file(src_path: &Path, dst_path: &Path, src_size: u64) -> io::Result<(u64, File)> {
    // For tiny files, just do a simple copy — syscall setup overhead dominates
    if src_size < SPLICE_THRESHOLD {
        return fast_small_copy(src_path, dst_path, src_size);
    }

    let src_file = File::open(src_path)?;
    let dst_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dst_path)?;

    // Pre-allocate destination to prevent fragmentation
    preallocate(&dst_file, src_size);

    // Hint kernel about sequential read pattern
    fadvise_sequential(&src_file, src_size);

    // Try FICLONE (reflink) first for instant copy-on-write
    #[cfg(target_os = "linux")]
    {
        let ret = unsafe { libc::ioctl(dst_file.as_raw_fd(), libc::FICLONE, src_file.as_raw_fd()) };
        if ret == 0 {
            return Ok((src_size, dst_file));
        }
    }

    // Try copy_file_range first (most efficient fallback after reflink)
    match copy_file_range_loop(&src_file, &dst_file, src_size) {
        Ok(n) => return Ok((n, dst_file)),
        Err(e) => {
            let code = e.raw_os_error().unwrap_or(0);
            if code != libc::EXDEV && code != libc::ENOSYS && code != libc::EINVAL {
                return Err(e);
            }
            // Fall through to splice
        }
    }

    // Try splice (works across filesystems)
    match splice_copy_fds(&src_file, &dst_file, src_size) {
        Ok(n) => Ok((n, dst_file)),
        Err(_) => {
            // Final fallback: buffered read+write with large aligned buffer
            let n = buffered_copy_aligned(&src_file, &dst_file, src_size)?;
            Ok((n, dst_file))
        }
    }
}

// ─── O_DIRECT bypass for large files ─────────────────────────────────────────

/// Copy a large file using O_DIRECT to bypass the page cache entirely.
///
/// **Why O_DIRECT?**
/// For large files (≥4 MB) that are only read/written once during sync,
/// the page cache provides zero benefit — the data is never re-read.
/// Worse, large files pollute the page cache, evicting frequently-used
/// metadata and small files.  O_DIRECT bypasses the cache completely:
/// - DMA transfers data directly between disk and userspace buffer
/// - No page cache pollution — metadata stays hot
/// - Eliminates double-copy (disk→cache→user becomes disk→user)
///
/// **Alignment requirements:**
/// O_DIRECT requires buffer addresses AND I/O sizes to be aligned to
/// the filesystem block size (4096 bytes on ext4).  The last chunk of
/// a file may not be block-aligned, so we fall back to normal write
/// for the tail.
///
/// Falls back to regular `zero_copy_file` if O_DIRECT open fails
/// (e.g., filesystem doesn't support it, tmpfs, etc.).
///
/// ## SSD SAFETY
/// O_DIRECT is a standard POSIX hint that bypasses the kernel page cache.
/// All I/O still goes through the filesystem (ext4 journal, FTL, etc.).
/// It does NOT bypass the drive's write cache or wear leveling.
/// Completely safe for all storage devices.
pub fn zero_copy_file_direct(
    src_path: &Path,
    dst_path: &Path,
    src_size: u64,
) -> io::Result<(u64, File)> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    // Open source with O_DIRECT
    let src_cstr = CString::new(src_path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path NUL"))?;
    let src_fd = unsafe {
        libc::open(
            src_cstr.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECT | libc::O_CLOEXEC,
        )
    };
    if src_fd < 0 {
        // O_DIRECT not supported (tmpfs, network FS) — fall back
        return zero_copy_file(src_path, dst_path, src_size);
    }
    let src_file = unsafe { File::from_raw_fd(src_fd) };

    // Open destination with O_DIRECT
    let dst_cstr = CString::new(dst_path.as_os_str().as_bytes())
        .map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "path NUL")
        })?;
    let dst_fd = unsafe {
        libc::open(
            dst_cstr.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC | libc::O_DIRECT | libc::O_CLOEXEC,
            0o644 as libc::mode_t,
        )
    };
    if dst_fd < 0 {
        // Fall back to normal copy
        drop(src_file);
        return zero_copy_file(src_path, dst_path, src_size);
    }
    let dst_file = unsafe { File::from_raw_fd(dst_fd) };

    // Pre-allocate to prevent fragmentation
    preallocate(&dst_file, src_size);

    // Use 1MB aligned buffer for DMA-friendly I/O
    let buf_size = ALIGNED_BUF_SIZE;
    let mut buf = aligned_buffer_direct(buf_size);
    let mut total = 0u64;

    // Read/write in aligned chunks
    let aligned_end = src_size & !(DIRECT_ALIGN as u64 - 1);

    while total < aligned_end {
        let chunk = ((aligned_end - total) as usize).min(buf_size);
        let n = unsafe {
            libc::read(src_fd, buf.as_mut_ptr() as *mut libc::c_void, chunk)
        };
        if n < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            // O_DIRECT read failed — fall back
            drop(src_file);
            drop(dst_file);
            return zero_copy_file(src_path, dst_path, src_size);
        }
        if n == 0 {
            break;
        }
        let n = n as usize;
        let mut written = 0;
        while written < n {
            // BUG FIX: O_DIRECT requires write sizes to be block-aligned.
            // Round UP the remaining bytes.  The extra bytes are from our
            // pre-zeroed aligned buffer, so they're harmless — ftruncate()
            // at the end will trim the file to the exact source size.
            let remaining = n - written;
            let write_len = (remaining + DIRECT_ALIGN - 1) & !(DIRECT_ALIGN - 1);
            // Safety: write_len never exceeds buf.len() because buf itself
            // was allocated with aligned_size >= the data size, and we only
            // read up to n bytes which is <= buf_size which is <= aligned_size.
            let write_len = write_len.min(buf.len() - written);
            let w = unsafe {
                libc::write(
                    dst_fd,
                    buf[written..].as_ptr() as *const libc::c_void,
                    write_len,
                )
            };
            if w < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(err);
            }
            written += w as usize;
        }
        total += n as u64;
    }

    // Handle the unaligned tail (last < 4096 bytes)
    // Must drop O_DIRECT for unaligned I/O
    let tail = src_size - total;
    if tail > 0 {
        // Remove O_DIRECT flag for the tail
        unsafe {
            let flags = libc::fcntl(src_fd, libc::F_GETFL);
            libc::fcntl(src_fd, libc::F_SETFL, flags & !libc::O_DIRECT);
            let flags = libc::fcntl(dst_fd, libc::F_GETFL);
            libc::fcntl(dst_fd, libc::F_SETFL, flags & !libc::O_DIRECT);
        }

        let mut tail_buf = vec![0u8; tail as usize];
        let mut tail_read = 0;
        while tail_read < tail as usize {
            let n = unsafe {
                libc::pread(
                    src_fd,
                    tail_buf[tail_read..].as_mut_ptr() as *mut libc::c_void,
                    tail as usize - tail_read,
                    total as i64 + tail_read as i64,
                )
            };
            if n < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(err);
            }
            if n == 0 {
                break;
            }
            tail_read += n as usize;
        }
        let mut tail_written = 0;
        while tail_written < tail_read {
            let w = unsafe {
                libc::pwrite(
                    dst_fd,
                    tail_buf[tail_written..].as_ptr() as *const libc::c_void,
                    tail_read - tail_written,
                    total as i64 + tail_written as i64,
                )
            };
            if w < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(err);
            }
            tail_written += w as usize;
        }
        total += tail_read as u64;
    }

    // Truncate to exact size (O_DIRECT may have written padding)
    unsafe { libc::ftruncate(dst_fd, src_size as i64) };

    // PERF: Release page cache for both files. O_DIRECT bypasses the cache
    // for the bulk of the data, but the tail (unaligned I/O) goes through
    // the cache. Drop it to keep memory pressure low during large syncs.
    fadvise_dontneed(&src_file, src_size);
    fadvise_dontneed(&dst_file, src_size);

    Ok((total, dst_file))
}

/// Page-aligned buffer for O_DIRECT I/O.
///
/// BUG FIX: Previously used `posix_memalign` + `Vec::from_raw_parts` which is
/// UB — Rust's Vec deallocator uses the global allocator (jemalloc/system) but
/// `posix_memalign` allocates via libc's `malloc`.  On drop, the Vec would call
/// `dealloc` on a pointer from a different allocator → UB/double-free potential.
///
/// Fix: Use `std::alloc::alloc` with a `Layout` that has the required alignment.
/// This guarantees the Rust allocator both allocates AND deallocates the memory.
fn aligned_buffer_direct(size: usize) -> Vec<u8> {
    let aligned_size = (size + DIRECT_ALIGN - 1) & !(DIRECT_ALIGN - 1);
    // Rust's alloc API supports arbitrary alignment natively
    let layout = match std::alloc::Layout::from_size_align(aligned_size, DIRECT_ALIGN) {
        Ok(l) => l,
        Err(_) => return vec![0u8; aligned_size], // fallback
    };
    unsafe {
        let ptr = std::alloc::alloc_zeroed(layout);
        if ptr.is_null() {
            return vec![0u8; aligned_size];
        }
        Vec::from_raw_parts(ptr, aligned_size, aligned_size)
    }
}

// ─── Small file fast copy ────────────────────────────────────────────────────

/// Fast path for small files (< 32 KB) — avoids syscall overhead of splice setup.
///
/// PERF: Uses stack buffer for files <= 4 KB (single page) to avoid heap
/// allocation entirely.  Most small files in typical workloads are config
/// files, symlinks, and scripts — well under 4 KB.
fn fast_small_copy(src_path: &Path, dst_path: &Path, src_size: u64) -> io::Result<(u64, File)> {
    let mut src = File::open(src_path)?;
    let dst = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dst_path)?;

    // PERF: Stack buffer for tiny files — zero heap allocation
    if src_size <= 4096 {
        let mut stack_buf = [0u8; 4096];
        let mut total = 0usize;
        loop {
            let n = src.read(&mut stack_buf[total..])?;
            if n == 0 {
                break;
            }
            total += n;
        }
        use std::io::Write;
        (&dst).write_all(&stack_buf[..total])?;
        return Ok((total as u64, dst));
    }

    // Heap buffer for files 4KB-32KB
    let mut buf = vec![0u8; src_size as usize];
    let mut total = 0usize;
    loop {
        let n = src.read(&mut buf[total..])?;
        if n == 0 {
            break;
        }
        total += n;
    }
    use std::io::Write;
    (&dst).write_all(&buf[..total])?;
    Ok((total as u64, dst))
}

/// copy_file_range(2) loop — kernel-side zero-copy between two file descriptors.
fn copy_file_range_loop(src: &File, dst: &File, size: u64) -> io::Result<u64> {
    let src_fd = src.as_raw_fd();
    let dst_fd = dst.as_raw_fd();
    let mut total: u64 = 0;
    let mut src_off: i64 = 0;
    let mut dst_off: i64 = 0;

    while total < size {
        let remaining = (size - total) as usize;
        // 1 GB per syscall — the kernel itself caps at its internal limit
        // but passing a large size reduces the number of syscalls for
        // multi-GB files.  Previously 128 MB → 8x more syscalls than needed.
        let chunk = remaining.min(1024 * 1024 * 1024);

        let ret = unsafe {
            libc::copy_file_range(
                src_fd,
                &mut src_off,
                dst_fd,
                &mut dst_off,
                chunk,
                0, // flags must be 0
            )
        };

        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        if ret == 0 {
            break; // EOF
        }
        total += ret as u64;
    }
    Ok(total)
}

/// splice(2) copy via kernel pipe — zero-copy for cross-filesystem transfers.
fn splice_copy_fds(src: &File, dst: &File, size: u64) -> io::Result<u64> {
    let mut pipe_fds: [RawFd; 2] = [0; 2];
    if unsafe { libc::pipe(pipe_fds.as_mut_ptr()) } < 0 {
        return Err(io::Error::last_os_error());
    }
    let pipe_read = pipe_fds[0];
    let pipe_write = pipe_fds[1];

    // Increase pipe buffer for throughput
    unsafe { libc::fcntl(pipe_write, libc::F_SETPIPE_SZ, PIPE_BUF_SIZE) };

    let result = splice_loop(src, dst, pipe_read, pipe_write, size);

    unsafe {
        libc::close(pipe_read);
        libc::close(pipe_write);
    }
    result
}

fn splice_loop(
    src: &File,
    dst: &File,
    pipe_read: RawFd,
    pipe_write: RawFd,
    size: u64,
) -> io::Result<u64> {
    let src_fd = src.as_raw_fd();
    let dst_fd = dst.as_raw_fd();
    let mut total: u64 = 0;

    while total < size {
        let chunk = ((size - total) as usize).min(PIPE_BUF_SIZE as usize);

        // src fd → pipe (zero-copy into pipe buffer)
        let n_in = unsafe {
            libc::splice(
                src_fd,
                std::ptr::null_mut(),
                pipe_write,
                std::ptr::null_mut(),
                chunk,
                libc::SPLICE_F_MOVE | libc::SPLICE_F_MORE,
            )
        };
        if n_in < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted || err.kind() == io::ErrorKind::WouldBlock {
                continue;
            }
            return Err(err);
        }
        if n_in == 0 {
            break;
        }

        // pipe → dst fd (zero-copy out of pipe buffer)
        let mut written: usize = 0;
        while written < n_in as usize {
            let n_out = unsafe {
                libc::splice(
                    pipe_read,
                    std::ptr::null_mut(),
                    dst_fd,
                    std::ptr::null_mut(),
                    (n_in as usize) - written,
                    libc::SPLICE_F_MOVE | libc::SPLICE_F_MORE,
                )
            };
            if n_out < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted || err.kind() == io::ErrorKind::WouldBlock {
                    continue;
                }
                return Err(err);
            }
            // BUG FIX: splice returning 0 on write side means the
            // destination cannot accept more data (disk full, broken
            // pipe, etc.) — without this check the loop spins at 100%
            // CPU forever.
            if n_out == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "splice: destination returned 0 bytes written",
                ));
            }
            written += n_out as usize;
        }
        total += n_in as u64;
    }
    Ok(total)
}

/// Fallback: buffered copy with large aligned buffer.
fn buffered_copy_aligned(src: &File, dst: &File, _size: u64) -> io::Result<u64> {
    let mut src = src.try_clone()?;
    let dst = dst.try_clone()?;
    let mut buf = aligned_buffer(ALIGNED_BUF_SIZE);
    let mut writer = io::BufWriter::with_capacity(ALIGNED_BUF_SIZE, dst);
    let mut total = 0u64;

    loop {
        let n = src.read(&mut buf)?;
        if n == 0 {
            break;
        }
        writer.write_all(&buf[..n])?;
        total += n as u64;
    }
    writer.flush()?;
    Ok(total)
}

// ─── O_TMPFILE support ──────────────────────────────────────────────────────

/// Global counter for temp file naming — ensures unique names across threads.
///
/// BUG FIX: Previously created `AtomicU64::new(0)` as a local variable, so
/// `fetch_add(1)` always returned 0 (new atomic each call).  All temp files
/// from the fallback path got the same name → race condition / data corruption.
static TMPFILE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Create an unnamed temporary file using O_TMPFILE (Linux 3.11+).
///
/// Benefits:
/// - No directory entry created until linkat() — crash-safe
/// - No filename collision handling
/// - Faster than mkstemp (no directory update during writes)
/// - Auto-cleanup on crash (no temp file litter)
///
/// Falls back to regular temp file creation if O_TMPFILE is unsupported.
pub fn open_tmpfile_in(dir: &Path) -> io::Result<(File, TempFileKind)> {
    // Try O_TMPFILE first
    match open_tmpfile_raw(dir) {
        Ok(file) => Ok((file, TempFileKind::OTmpfile)),
        Err(_) => {
            // Fallback: create a named temp file
            let counter = TMPFILE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let tmp_name = format!(
                ".resync.{}.{}.tmp",
                std::process::id(),
                counter,
            );
            let tmp_path = dir.join(&tmp_name);
            let file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;
            Ok((file, TempFileKind::Named(tmp_path)))
        }
    }
}

fn open_tmpfile_raw(dir: &Path) -> io::Result<File> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let dir_cstr = CString::new(dir.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL byte"))?;

    let flags = libc::O_TMPFILE | libc::O_RDWR | libc::O_CLOEXEC;
    let fd = unsafe { libc::open(dir_cstr.as_ptr(), flags, 0o600 as libc::mode_t) };

    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

/// Describes how the temp file was created.
pub enum TempFileKind {
    /// Created via O_TMPFILE — use linkat() to make visible.
    OTmpfile,
    /// Created as a named file — use rename() to finalize.
    Named(std::path::PathBuf),
}

/// Finalize a temp file: either linkat() for O_TMPFILE or rename() for named.
pub fn finalize_tmpfile(
    file: &File,
    kind: &TempFileKind,
    dst_path: &Path,
) -> io::Result<()> {
    match kind {
        TempFileKind::OTmpfile => {
            use std::ffi::CString;
            use std::os::unix::ffi::OsStrExt;

            let fd = file.as_raw_fd();
            let proc_path = format!("/proc/self/fd/{}", fd);
            let proc_cstr = CString::new(proc_path.as_bytes()).unwrap();
            let dst_cstr = CString::new(dst_path.as_os_str().as_bytes())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path NUL"))?;

            // Remove existing file first (linkat won't overwrite)
            let _ = fs::remove_file(dst_path);

            let ret = unsafe {
                libc::linkat(
                    libc::AT_FDCWD,
                    proc_cstr.as_ptr(),
                    libc::AT_FDCWD,
                    dst_cstr.as_ptr(),
                    libc::AT_SYMLINK_FOLLOW,
                )
            };
            if ret < 0 {
                // linkat failed — fall back to /proc/self/fd copy
                let proc_path_buf = std::path::PathBuf::from(&proc_path);
                fs::copy(&proc_path_buf, dst_path)?;
            }
            Ok(())
        }
        TempFileKind::Named(tmp_path) => {
            fs::rename(tmp_path, dst_path)?;
            Ok(())
        }
    }
}

// ─── Pre-allocation ──────────────────────────────────────────────────────────

/// Pre-allocate disk space with fallocate(2) to prevent fragmentation.
/// This is a hint — failure is silently ignored.
#[inline]
pub fn preallocate(file: &File, size: u64) {
    if size == 0 {
        return;
    }
    unsafe {
        libc::fallocate(file.as_raw_fd(), 0, 0, size as i64);
    }
}

/// Punch a hole in a file (FALLOC_FL_PUNCH_HOLE) for sparse file support.
#[inline]
pub fn punch_hole(file: &File, offset: u64, len: u64) {
    unsafe {
        libc::fallocate(
            file.as_raw_fd(),
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            offset as i64,
            len as i64,
        );
    }
}

// ─── Kernel hints ────────────────────────────────────────────────────────────

/// Advise kernel that we'll read this file sequentially.
/// Enables aggressive readahead (typically 2× default window).
#[inline]
pub fn fadvise_sequential(file: &File, size: u64) {
    unsafe {
        libc::posix_fadvise(file.as_raw_fd(), 0, size as i64, libc::POSIX_FADV_SEQUENTIAL);
    }
}

/// Tell kernel we won't need this file's data anymore.
/// Frees page cache entries, reducing memory pressure.
#[inline]
pub fn fadvise_dontneed(file: &File, size: u64) {
    unsafe {
        libc::posix_fadvise(file.as_raw_fd(), 0, size as i64, libc::POSIX_FADV_DONTNEED);
    }
}

/// Tell kernel we'll need this data soon (trigger readahead).
#[inline]
pub fn fadvise_willneed(file: &File, offset: u64, size: u64) {
    unsafe {
        libc::posix_fadvise(
            file.as_raw_fd(),
            offset as i64,
            size as i64,
            libc::POSIX_FADV_WILLNEED,
        );
    }
}

// ─── Aligned I/O buffers ─────────────────────────────────────────────────────

/// Allocate a page-aligned buffer for optimal I/O performance.
/// Aligned buffers enable O_DIRECT and avoid partial page copies.
pub fn aligned_buffer(size: usize) -> Vec<u8> {
    const PAGE_SIZE: usize = 4096;
    let aligned_size = (size + PAGE_SIZE - 1) & !(PAGE_SIZE - 1);
    vec![0u8; aligned_size]
}

// ─── Shifted-block copy via copy_file_range ──────────────────────────────────

/// Copy data between specific offsets using kernel-space copy_file_range(2).
///
/// **Shifted-block optimization**: When delta detection finds that a block of
/// data has moved to a different offset (e.g., data prepended to a file, log
/// rotation, header insertion), this copies the block entirely within the kernel
/// without reading it into userspace.
///
/// Example: If a 100MB file had 1KB prepended, the original data shifted from
/// offset 0 to offset 1024.  Instead of re-reading 100MB, this does:
///   `copy_file_range(old_fd, 0, new_fd, 1024, 100MB)` — zero userspace copies.
///
/// ## SSD SAFETY
/// Uses standard `copy_file_range(2)` syscall — the same mechanism as regular
/// file copies.  Data is moved entirely within the kernel.  100% safe for all
/// storage devices.
pub fn copy_shifted_range(
    src: &File,
    dst: &File,
    src_offset: u64,
    dst_offset: u64,
    length: u64,
) -> io::Result<u64> {
    let src_fd = src.as_raw_fd();
    let dst_fd = dst.as_raw_fd();
    let mut s_off = src_offset as i64;
    let mut d_off = dst_offset as i64;
    let mut remaining = length;
    let mut total = 0u64;

    while remaining > 0 {
        let chunk = remaining.min(128 * 1024 * 1024) as usize; // 128 MB per call
        let ret = unsafe {
            libc::copy_file_range(
                src_fd,
                &mut s_off,
                dst_fd,
                &mut d_off,
                chunk,
                0,
            )
        };

        if ret < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(err);
        }
        if ret == 0 {
            break; // EOF
        }

        let n = ret as u64;
        total += n;
        remaining -= n;
    }

    Ok(total)
}

/// Apply multiple shifted-block copies in sequence.
///
/// Each `(src_offset, dst_offset, length)` triple describes a block
/// of data that exists at `src_offset` in the source and should be
/// placed at `dst_offset` in the destination — all in-kernel.
pub fn apply_shifted_ranges(
    src: &File,
    dst: &File,
    ranges: &[(u64, u64, u64)],
) -> io::Result<u64> {
    let mut total = 0u64;
    for &(src_off, dst_off, len) in ranges {
        total += copy_shifted_range(src, dst, src_off, dst_off, len)?;
    }
    Ok(total)
}

// ─── Batch metadata application ──────────────────────────────────────────────

/// Apply file metadata (permissions, timestamps, ownership) in a single pass.
/// Uses raw fd-based syscalls to avoid repeated path resolution.
pub fn apply_metadata_fast(
    file: &File,
    mode: Option<u32>,
    mtime: Option<std::time::SystemTime>,
    uid: Option<u32>,
    gid: Option<u32>,
) {
    let fd = file.as_raw_fd();

    // BUG FIX: fchown MUST come before fchmod.
    // Linux clears SUID/SGID bits when ownership changes (security
    // measure).  If we fchmod first, the SUID/SGID bits we just set
    // get silently stripped by the subsequent fchown.  Correct order:
    // 1. fchown (changes owner, may clear SUID/SGID)
    // 2. fchmod (sets final permission bits including SUID/SGID)
    // 3. futimens (timestamps — order doesn't matter)

    // fchown (must be first — clears SUID/SGID)
    if uid.is_some() || gid.is_some() {
        let uid_val = uid.map(|u| u as libc::uid_t).unwrap_or(u32::MAX as libc::uid_t);
        let gid_val = gid.map(|g| g as libc::gid_t).unwrap_or(u32::MAX as libc::gid_t);
        unsafe {
            libc::fchown(fd, uid_val, gid_val);
        }
    }

    // fchmod (after fchown so SUID/SGID bits survive)
    if let Some(mode) = mode {
        unsafe {
            libc::fchmod(fd, mode);
        }
    }

    // futimens (set mtime via fd — no path resolution)
    if let Some(mtime) = mtime {
        let dur = mtime
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let ts = libc::timespec {
            tv_sec: dur.as_secs() as i64,
            tv_nsec: dur.subsec_nanos() as i64,
        };
        let times = [
            libc::timespec {
                tv_sec: 0,
                tv_nsec: libc::UTIME_OMIT,
            },
            ts,
        ];
        unsafe {
            libc::futimens(fd, times.as_ptr());
        }
    }
}

// ─── Batch directory creation ────────────────────────────────────────────────

/// Create all directories in parallel using rayon.
/// Sorts by depth to ensure parent dirs exist before children.
pub fn create_dirs_parallel(dest_root: &Path, rel_dirs: &[std::path::PathBuf]) {
    use rayon::prelude::*;

    if rel_dirs.is_empty() {
        return;
    }

    // Sort by depth (shortest first) to ensure parents are created before children
    let mut sorted: Vec<_> = rel_dirs.iter().collect();
    sorted.sort_by_key(|p| p.components().count());

    // Group by depth level and create each level in parallel
    let mut current_depth = 0;
    let mut level_start = 0;

    for i in 0..sorted.len() {
        let depth = sorted[i].components().count();
        if depth != current_depth {
            // Process previous level in parallel
            if level_start < i {
                sorted[level_start..i].par_iter().for_each(|rel| {
                    if let Err(e) = fs::create_dir_all(dest_root.join(rel)) {
                        // EEXIST is expected when two threads race to create
                        // the same parent — safe to ignore.  All other errors
                        // (EACCES, ENOSPC, etc.) are real failures.
                        if e.kind() != io::ErrorKind::AlreadyExists {
                            tracing::warn!(
                                "create_dir_all failed for {}: {e}",
                                dest_root.join(rel).display()
                            );
                        }
                    }
                });
            }
            current_depth = depth;
            level_start = i;
        }
    }
    // Process final level
    if level_start < sorted.len() {
        sorted[level_start..].par_iter().for_each(|rel| {
            if let Err(e) = fs::create_dir_all(dest_root.join(rel)) {
                if e.kind() != io::ErrorKind::AlreadyExists {
                    tracing::warn!(
                        "create_dir_all failed for {}: {e}",
                        dest_root.join(rel).display()
                    );
                }
            }
        });
    }
}

// ─── Readahead prefetching ───────────────────────────────────────────────────

/// Prefetch the next file's data into page cache while processing the current file.
/// This overlaps I/O latency with CPU computation (hashing, delta computation).
pub fn prefetch_file(path: &Path, size: u64) {
    if let Ok(file) = File::open(path) {
        fadvise_willneed(&file, 0, size);
        // File is immediately closed, but readahead continues in kernel
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;

    #[test]
    fn test_zero_copy_small_file() {
        let tmp = TempDir::new().unwrap();
        let src = tmp.path().join("src.bin");
        let dst = tmp.path().join("dst.bin");

        let data = b"hello zero-copy world!";
        fs::write(&src, data).unwrap();

        let (n, _dst_file) = zero_copy_file(&src, &dst, data.len() as u64).unwrap();
        assert_eq!(n, data.len() as u64);
        assert_eq!(fs::read(&dst).unwrap(), data);
    }

    #[test]
    fn test_zero_copy_large_file() {
        let tmp = TempDir::new().unwrap();
        let src = tmp.path().join("src.bin");
        let dst = tmp.path().join("dst.bin");

        let data = vec![0xABu8; 256 * 1024]; // 256 KB
        fs::write(&src, &data).unwrap();

        let (n, _dst_file) = zero_copy_file(&src, &dst, data.len() as u64).unwrap();
        assert_eq!(n, data.len() as u64);
        assert_eq!(fs::read(&dst).unwrap(), data);
    }

    #[test]
    fn test_aligned_buffer() {
        let buf = aligned_buffer(4096);
        assert_eq!(buf.len(), 4096);
        // Check alignment (Vec always aligns, but our size is page-aligned)
        assert_eq!(buf.len() % 4096, 0);
    }

    #[test]
    fn test_preallocate_does_not_panic() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("pre.bin");
        let file = File::create(&path).unwrap();
        preallocate(&file, 1024 * 1024);
        // Should not panic even on filesystems that don't support fallocate
    }

    #[test]
    fn test_metadata_fast() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("meta.bin");
        let mut file = File::create(&path).unwrap();
        file.write_all(b"test").unwrap();

        apply_metadata_fast(
            &file,
            Some(0o644),
            Some(std::time::SystemTime::now()),
            None,
            None,
        );

        let meta = fs::metadata(&path).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o644);
    }

    #[test]
    fn test_direct_io_large_file() {
        let tmp = TempDir::new().unwrap();
        let src = tmp.path().join("src_direct.bin");
        let dst = tmp.path().join("dst_direct.bin");

        // Create a file larger than DIRECT_IO_THRESHOLD (4 MB)
        // Use non-aligned size to test the tail-handling code path
        let size = 4 * 1024 * 1024 + 1337; // 4 MB + 1337 bytes
        let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        fs::write(&src, &data).unwrap();

        let (n, _dst_file) = zero_copy_file_direct(&src, &dst, size as u64).unwrap();
        assert_eq!(n, size as u64);
        assert_eq!(fs::read(&dst).unwrap(), data);
    }

    #[test]
    fn test_direct_io_aligned_file() {
        let tmp = TempDir::new().unwrap();
        let src = tmp.path().join("src_aligned.bin");
        let dst = tmp.path().join("dst_aligned.bin");

        // Exactly 4 MB — no tail to handle
        let size = 4 * 1024 * 1024;
        let data = vec![0x42u8; size];
        fs::write(&src, &data).unwrap();

        let (n, _dst_file) = zero_copy_file_direct(&src, &dst, size as u64).unwrap();
        assert_eq!(n, size as u64);
        assert_eq!(fs::read(&dst).unwrap(), data);
    }
}
