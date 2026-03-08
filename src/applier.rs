//! Delta applier — writes the computed patch to the destination file.
//!
//!  Uses a temporary file + atomic rename so that a crash mid-write never
//!  leaves a corrupt destination at the target path.
//!
//!  Strategy:
//!  1. Create a temp file with a unique name (PID + thread ID + counter).
//!  2. For each [`DeltaOp::Copy`]: copy the chunk from the existing `dst`.
//!  3. For each [`DeltaOp::Write`]: copy the chunk from `src`.
//!  4. Optionally `fsync` (only when `--fsync` is set).
//!  5. Rename into place.
//!  6. On error: clean up the temp file.
//!
//!  Performance features:
//!  - copy_file_range() zero-copy for new files on Linux
//!  - madvise(MADV_SEQUENTIAL) on mmap'd files
//!  - 256 KB BufWriter to reduce write(2) syscalls by 32x
//!  - No fsync by default (configurable with --fsync)
//!  - Owner/group preservation via libc::fchown

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use memmap2::Mmap;
use tracing::warn;

use crate::delta::{DeltaOp, FileDelta};
use crate::error::{Result, ResyncError};
use crate::scanner::FileEntry;

// ─── Zero-copy file data wrapper ─────────────────────────────────────────────

/// Zero-copy file data wrapper — avoids heap-copying mmap'd files.
///
/// PERF FIX: Previously `read_file_data()` called `mmap.to_vec()` which copied
/// the entire file into heap memory, defeating the purpose of mmap and doubling
/// RAM usage per file.  Now large files stay as mmap references (zero-copy)
/// while small files are read into a Vec (cheaper than mmap setup for < 64KB).
enum FileData {
    Mmap(Mmap),
    Buf(Vec<u8>),
}

impl std::ops::Deref for FileData {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            FileData::Mmap(m) => m,
            FileData::Buf(v) => v,
        }
    }
}

// ─── Unique temp file naming ─────────────────────────────────────────────────

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a unique temp file path in the same directory as `dst_path`.
///
/// BUG FIX #1: Previously used `with_extension("resync.tmp")` which
/// REPLACES the last extension. Two files like `report.txt` and `report.log`
/// in the same directory both produced `report.resync.tmp`, causing parallel
/// threads to corrupt each other's temp files.
///
/// New approach: append `.resync.<pid>.<counter>.tmp` to the FULL filename.
fn temp_path_for(dst_path: &Path) -> PathBuf {
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let file_name = dst_path.file_name().unwrap_or_default().to_string_lossy();
    let tmp_name = format!(".{file_name}.resync.{pid}.{counter}.tmp");
    dst_path.with_file_name(tmp_name)
}

// ─── Platform-specific: copy_file_range ──────────────────────────────────────

/// Zero-copy file copy using Linux's copy_file_range(2) syscall.
/// Falls back to std::fs::copy on non-Linux or on failure.
#[cfg(target_os = "linux")]
fn copy_file_range_all(src_path: &Path, dst_path: &Path) -> std::io::Result<u64> {
    use std::os::unix::io::AsRawFd;

    let src_file = File::open(src_path)?;
    let src_size = src_file.metadata()?.len();

    let dst_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dst_path)?;

    let src_fd = src_file.as_raw_fd();
    let dst_fd = dst_file.as_raw_fd();

    let mut total_copied: u64 = 0;
    let mut src_off: i64 = 0;
    let mut dst_off: i64 = 0;

    while total_copied < src_size {
        let remaining = (src_size - total_copied) as usize;
        // Copy in 128 MB chunks to avoid holding kernel resources too long
        let chunk = remaining.min(128 * 1024 * 1024);

        let ret = unsafe {
            libc::copy_file_range(
                src_fd,
                &mut src_off,
                dst_fd,
                &mut dst_off,
                chunk,
                0, // flags (must be 0)
            )
        };

        if ret < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EXDEV)
                || err.raw_os_error() == Some(libc::ENOSYS)
                || err.raw_os_error() == Some(libc::EINVAL)
            {
                // Fallback: kernel doesn't support it for this fs combination
                // Re-do from scratch with std::fs::copy
                drop(src_file);
                drop(dst_file);
                return fs::copy(src_path, dst_path);
            }
            return Err(err);
        }
        if ret == 0 {
            break; // EOF
        }
        total_copied += ret as u64;
    }

    Ok(total_copied)
}

#[cfg(not(target_os = "linux"))]
fn copy_file_range_all(src_path: &Path, dst_path: &Path) -> std::io::Result<u64> {
    fs::copy(src_path, dst_path)
}

// ─── Platform-specific: madvise ──────────────────────────────────────────────

/// Advise kernel on mmap usage pattern for better readahead.
#[cfg(target_os = "linux")]
fn madvise_sequential(mmap: &Mmap) {
    unsafe {
        libc::madvise(
            mmap.as_ptr() as *mut libc::c_void,
            mmap.len(),
            libc::MADV_SEQUENTIAL,
        );
    }
}

#[cfg(not(target_os = "linux"))]
fn madvise_sequential(_mmap: &Mmap) {
    // No-op on non-Linux
}

// ─── Applier ─────────────────────────────────────────────────────────────────

pub struct Applier {
    pub preserve_perms: bool,
    pub preserve_times: bool,
    pub preserve_owner: bool,
    pub preserve_group: bool,
    pub dry_run: bool,
    pub fsync: bool,
    pub inplace: bool,
}

impl Applier {
    pub fn new(
        preserve_perms: bool,
        preserve_times: bool,
        preserve_owner: bool,
        preserve_group: bool,
        dry_run: bool,
        fsync: bool,
        inplace: bool,
    ) -> Self {
        Self {
            preserve_perms,
            preserve_times,
            preserve_owner,
            preserve_group,
            dry_run,
            fsync,
            inplace,
        }
    }

    /// Apply `delta` to produce `dst_path`, reading new data from `src_path`.
    ///
    /// Returns the number of bytes actually written to storage.
    pub fn apply(
        &self,
        src_path: &Path,
        dst_path: &Path,
        delta: &FileDelta,
        src_entry: &FileEntry,
    ) -> Result<u64> {
        if self.dry_run {
            return Ok(delta.transfer_bytes);
        }

        // Handle directory creation
        if let Some(parent) = dst_path.parent() {
            fs::create_dir_all(parent).map_err(|e| ResyncError::Io {
                path: parent.display().to_string(),
                source: e,
            })?;
        }

        // Handle symlinks separately
        if src_entry.is_symlink {
            return self.apply_symlink(src_entry, dst_path);
        }

        // BUG FIX #11: Handle empty source files without mmap
        if src_entry.size == 0 || delta.final_size == 0 {
            return self.write_empty_file(dst_path, src_entry);
        }

        // Read source data (zero-copy via mmap for large files)
        let src_data = self.read_file_data(src_path)?;

        // Read destination data if it exists (for Copy ops)
        let dst_data: Option<FileData> = if dst_path.exists() {
            Some(self.read_file_data(dst_path)?)
        } else {
            None
        };

        // Validate: if we have Copy ops, we MUST have dst data
        let has_copy_ops = delta
            .ops
            .iter()
            .any(|op| matches!(op, DeltaOp::Copy { .. }));
        if has_copy_ops && dst_data.is_none() {
            return Err(ResyncError::DeltaApplyFailed(format!(
                "delta contains Copy ops but destination {} does not exist",
                dst_path.display()
            )));
        }

        // ── --inplace mode: write directly to destination ────────────────
        if self.inplace {
            let dst_slice: Option<&[u8]> = dst_data.as_deref();
            return self.write_delta_inplace(dst_path, &src_data, dst_slice, delta, src_entry);
        }

        // ── Standard mode: temp file + atomic rename ─────────────────────
        let tmp_path = temp_path_for(dst_path);
        let dst_slice: Option<&[u8]> = dst_data.as_deref();
        match self.write_delta(&tmp_path, &src_data, dst_slice, delta) {
            Ok(bytes_written) => {
                // Atomic rename into place
                fs::rename(&tmp_path, dst_path).map_err(|e| ResyncError::Io {
                    path: dst_path.display().to_string(),
                    source: e,
                })?;
                self.apply_metadata(dst_path, src_entry)?;
                Ok(bytes_written)
            }
            Err(e) => {
                // Clean up temp file on failure
                let _ = fs::remove_file(&tmp_path);
                Err(e)
            }
        }
    }

    /// Write the delta ops to a temp file, return bytes written from source.
    fn write_delta(
        &self,
        tmp_path: &Path,
        src_data: &[u8],
        dst_data: Option<&[u8]>,
        delta: &FileDelta,
    ) -> Result<u64> {
        let tmp_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(tmp_path)
            .map_err(|e| ResyncError::Io {
                path: tmp_path.display().to_string(),
                source: e,
            })?;

        // PERF FIX: 256 KB buffer instead of default 8 KB — reduces
        // write(2) syscall count by 32x for large files.
        let mut writer = BufWriter::with_capacity(256 * 1024, &tmp_file);
        let mut bytes_written: u64 = 0;

        for op in &delta.ops {
            match op {
                DeltaOp::Copy { src_offset, len } => {
                    // Copy from existing destination
                    let dst = dst_data.expect("dst_data validated above");
                    let start = *src_offset as usize;
                    let end = start + len;

                    // BUG FIX #9: Bounds check — panic-safe with clear error
                    if end > dst.len() {
                        return Err(ResyncError::DeltaApplyFailed(format!(
                            "Copy op references offset {}..{} but dst is only {} bytes \
                             (file may have changed during sync)",
                            start,
                            end,
                            dst.len()
                        )));
                    }

                    writer
                        .write_all(&dst[start..end])
                        .map_err(|e| ResyncError::Io {
                            path: tmp_path.display().to_string(),
                            source: e,
                        })?;
                }
                DeltaOp::Write { src_offset, len } => {
                    let start = *src_offset as usize;
                    let end = start + len;

                    // BUG FIX #9: Bounds check
                    if end > src_data.len() {
                        return Err(ResyncError::DeltaApplyFailed(format!(
                            "Write op references offset {}..{} but src is only {} bytes \
                             (file may have changed during sync)",
                            start,
                            end,
                            src_data.len()
                        )));
                    }

                    writer
                        .write_all(&src_data[start..end])
                        .map_err(|e| ResyncError::Io {
                            path: tmp_path.display().to_string(),
                            source: e,
                        })?;
                    bytes_written += *len as u64;
                }
            }
        }

        // Flush BufWriter before calling sync_data on the underlying file
        writer.flush().map_err(|e| ResyncError::Io {
            path: tmp_path.display().to_string(),
            source: e,
        })?;
        drop(writer);

        // Only fsync when explicitly requested (massive perf win for many small files)
        if self.fsync {
            tmp_file.sync_data().map_err(|e| ResyncError::Io {
                path: tmp_path.display().to_string(),
                source: e,
            })?;
        }

        Ok(bytes_written)
    }

    /// Write delta directly to destination file (--inplace mode).
    /// Faster but not crash-safe: a power failure mid-write leaves a corrupt file.
    fn write_delta_inplace(
        &self,
        dst_path: &Path,
        src_data: &[u8],
        _dst_data: Option<&[u8]>,
        delta: &FileDelta,
        src_entry: &FileEntry,
    ) -> Result<u64> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(dst_path)
            .map_err(|e| ResyncError::Io {
                path: dst_path.display().to_string(),
                source: e,
            })?;

        // Set the final file size first
        file.set_len(delta.final_size).map_err(|e| ResyncError::Io {
            path: dst_path.display().to_string(),
            source: e,
        })?;

        let mut writer = BufWriter::with_capacity(256 * 1024, &file);
        let mut bytes_written: u64 = 0;
        let mut current_offset: u64 = 0;

        for op in &delta.ops {
            match op {
                DeltaOp::Copy { src_offset: _, len } => {
                    // For inplace, Copy ops reference data already in dst at the right offset
                    // We skip over them (they're already in place)
                    current_offset += *len as u64;
                    writer.seek(SeekFrom::Start(current_offset)).ok();
                }
                DeltaOp::Write { src_offset, len } => {
                    let start = *src_offset as usize;
                    let end = start + len;
                    if end > src_data.len() {
                        return Err(ResyncError::DeltaApplyFailed(format!(
                            "Write op references offset {}..{} but src is only {} bytes",
                            start, end, src_data.len()
                        )));
                    }
                    writer.seek(SeekFrom::Start(current_offset)).map_err(|e| ResyncError::Io {
                        path: dst_path.display().to_string(),
                        source: e,
                    })?;
                    writer.write_all(&src_data[start..end]).map_err(|e| ResyncError::Io {
                        path: dst_path.display().to_string(),
                        source: e,
                    })?;
                    bytes_written += *len as u64;
                    current_offset += *len as u64;
                }
            }
        }

        writer.flush().map_err(|e| ResyncError::Io {
            path: dst_path.display().to_string(),
            source: e,
        })?;
        drop(writer);

        if self.fsync {
            file.sync_data().map_err(|e| ResyncError::Io {
                path: dst_path.display().to_string(),
                source: e,
            })?;
        }

        self.apply_metadata(dst_path, src_entry)?;
        Ok(bytes_written)
    }

    /// Read file data, using zero-copy mmap for large files and read() for
    /// small ones.  Applies madvise(MADV_SEQUENTIAL) on Linux for readahead.
    fn read_file_data(&self, path: &Path) -> Result<FileData> {
        let file = File::open(path).map_err(|e| ResyncError::Io {
            path: path.display().to_string(),
            source: e,
        })?;
        let meta = file.metadata().map_err(|e| ResyncError::Io {
            path: path.display().to_string(),
            source: e,
        })?;
        let size = meta.len() as usize;

        if size == 0 {
            return Ok(FileData::Buf(vec![]));
        }

        // Use mmap for files >= 64KB (zero-copy), buffered read for smaller
        if size >= 65536 {
            let mmap = unsafe {
                Mmap::map(&file).map_err(|e| ResyncError::Mmap {
                    path: path.display().to_string(),
                    source: e,
                })?
            };
            // PERF FIX: Hint to kernel that we'll read sequentially
            madvise_sequential(&mmap);
            Ok(FileData::Mmap(mmap))
        } else {
            let mut buf = vec![0u8; size];
            let mut f = file;
            f.read_exact(&mut buf).map_err(|e| ResyncError::Io {
                path: path.display().to_string(),
                source: e,
            })?;
            Ok(FileData::Buf(buf))
        }
    }

    /// Create an empty file at dst (or truncate existing).
    fn write_empty_file(&self, dst_path: &Path, src_entry: &FileEntry) -> Result<u64> {
        if let Some(parent) = dst_path.parent() {
            fs::create_dir_all(parent).map_err(|e| ResyncError::Io {
                path: parent.display().to_string(),
                source: e,
            })?;
        }
        File::create(dst_path).map_err(|e| ResyncError::Io {
            path: dst_path.display().to_string(),
            source: e,
        })?;
        self.apply_metadata(dst_path, src_entry)?;
        Ok(0)
    }

    /// Fully copy a file atomically (shortcut when no destination exists yet).
    ///
    /// PERF FIX: Uses copy_file_range(2) on Linux for zero-copy kernel-space
    /// file duplication. Data never enters userspace. Falls back to std::fs::copy
    /// on other platforms or unsupported filesystem combinations.
    pub fn copy_new(&self, src_path: &Path, dst_path: &Path, src_entry: &FileEntry) -> Result<u64> {
        if self.dry_run {
            return Ok(src_entry.size);
        }

        if src_entry.is_symlink {
            return self.apply_symlink(src_entry, dst_path);
        }

        if let Some(parent) = dst_path.parent() {
            fs::create_dir_all(parent).map_err(|e| ResyncError::Io {
                path: parent.display().to_string(),
                source: e,
            })?;
        }

        if src_entry.size == 0 {
            return self.write_empty_file(dst_path, src_entry);
        }

        // Atomic copy via temp file + zero-copy on Linux
        let tmp_path = temp_path_for(dst_path);
        match copy_file_range_all(src_path, &tmp_path) {
            Ok(_) => {
                if self.fsync {
                    if let Ok(f) = File::open(&tmp_path) {
                        f.sync_data().ok();
                    }
                }
                fs::rename(&tmp_path, dst_path).map_err(|e| ResyncError::Io {
                    path: dst_path.display().to_string(),
                    source: e,
                })?;
                self.apply_metadata(dst_path, src_entry)?;
                Ok(src_entry.size)
            }
            Err(e) => {
                let _ = fs::remove_file(&tmp_path);
                Err(ResyncError::Io {
                    path: dst_path.display().to_string(),
                    source: e,
                })
            }
        }
    }

    // ─── Private helpers ─────────────────────────────────────────────────────

    fn apply_symlink(&self, src_entry: &FileEntry, dst_path: &Path) -> Result<u64> {
        if let Some(parent) = dst_path.parent() {
            fs::create_dir_all(parent).map_err(|e| ResyncError::Io {
                path: parent.display().to_string(),
                source: e,
            })?;
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            if let Some(ref target) = src_entry.symlink_target {
                if dst_path.exists() || dst_path.symlink_metadata().is_ok() {
                    fs::remove_file(dst_path).ok();
                }
                symlink(target, dst_path).map_err(|e| ResyncError::Io {
                    path: dst_path.display().to_string(),
                    source: e,
                })?;
            }
        }
        Ok(0)
    }

    fn apply_metadata(&self, dst_path: &Path, src: &FileEntry) -> Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;

            if self.preserve_perms {
                let perms = fs::Permissions::from_mode(src.mode & 0o7777);
                fs::set_permissions(dst_path, perms).map_err(|e| ResyncError::Io {
                    path: dst_path.display().to_string(),
                    source: e,
                })?;
            }

            if self.preserve_times {
                if let Ok(file) = fs::OpenOptions::new().write(true).open(dst_path) {
                    if let Err(e) = file.set_modified(src.modified) {
                        warn!("failed to set mtime on {}: {e}", dst_path.display());
                    }
                }
            }

            // Owner/group preservation via libc::chown
            if self.preserve_owner || self.preserve_group {
                if let Ok(dst_meta) = fs::metadata(dst_path) {
                    let src_uid = if self.preserve_owner {
                        // We need the original uid from the source file
                        fs::metadata(&src.abs_path)
                            .ok()
                            .map(|m| m.uid())
                            .unwrap_or(u32::MAX) // -1 means "don't change"
                    } else {
                        u32::MAX
                    };

                    let src_gid = if self.preserve_group {
                        fs::metadata(&src.abs_path)
                            .ok()
                            .map(|m| m.gid())
                            .unwrap_or(u32::MAX)
                    } else {
                        u32::MAX
                    };

                    let dst_uid = dst_meta.uid();
                    let dst_gid = dst_meta.gid();

                    let new_uid = if self.preserve_owner && src_uid != u32::MAX {
                        src_uid
                    } else {
                        dst_uid
                    };
                    let new_gid = if self.preserve_group && src_gid != u32::MAX {
                        src_gid
                    } else {
                        dst_gid
                    };

                    if new_uid != dst_uid || new_gid != dst_gid {
                        use std::ffi::CString;
                        if let Ok(c_path) = CString::new(dst_path.to_string_lossy().as_bytes()) {
                            let ret = unsafe {
                                libc::chown(c_path.as_ptr(), new_uid, new_gid)
                            };
                            if ret != 0 {
                                warn!(
                                    "chown failed for {} (uid={}, gid={}): {}",
                                    dst_path.display(),
                                    new_uid,
                                    new_gid,
                                    std::io::Error::last_os_error()
                                );
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
