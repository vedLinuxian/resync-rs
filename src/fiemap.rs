//! FIEMAP — kernel extent map query for O(1) sparse detection.
//!
//! Instead of reading file data to find zero regions, this module queries
//! the kernel directly for the physical extent map via FS_IOC_FIEMAP ioctl.
//! This returns the exact on-disk layout — which byte ranges have data and
//! which are holes — in O(1) time regardless of file size.
//!
//! ## SSD SAFETY
//! ────────────────────────────────────────────────────────────────
//! **FIEMAP is a read-only ioctl.**  It queries filesystem metadata
//! without modifying any data, inodes, or filesystem structures.
//! No writes, no TRIM, no DISCARD.  100% safe for SSDs and all
//! storage devices.
//!
//! ## Supported filesystems
//! ext4, xfs, btrfs, f2fs, ocfs2, nilfs2, ceph, lustre

use std::fs::File;
use std::io;
use std::os::unix::io::AsRawFd;

// ─── Kernel ABI definitions ─────────────────────────────────────────────────
//
// From linux/fiemap.h — must match kernel ABI exactly.
// FS_IOC_FIEMAP = _IOWR('f', 11, struct fiemap)
// = (3 << 30) | (32 << 16) | (0x66 << 8) | 0x0B
// = 0xC020660B  (x86_64, aarch64, and all modern arches)

const FS_IOC_FIEMAP: libc::c_ulong = 0xC020660B;

// FIEMAP request flags
#[allow(dead_code)]
const FIEMAP_FLAG_SYNC: u32 = 0x0000_0001;

// Extent flags (returned by kernel)
const FIEMAP_EXTENT_LAST: u32 = 0x0000_0001;
const FIEMAP_EXTENT_UNWRITTEN: u32 = 0x0000_0800;
const FIEMAP_EXTENT_SHARED: u32 = 0x0000_2000;

/// On-disk fiemap header (32 bytes).
#[repr(C)]
struct FiemapHeader {
    fm_start: u64,           // logical start offset (input)
    fm_length: u64,          // logical length to map (input)
    fm_flags: u32,           // FIEMAP_FLAG_* (input)
    fm_mapped_extents: u32,  // number of extents returned (output)
    fm_extent_count: u32,    // max extents to return (input, 0 = count only)
    fm_reserved: u32,
}

/// On-disk fiemap extent entry (56 bytes).
#[repr(C)]
#[derive(Clone)]
struct FiemapExtentRaw {
    fe_logical: u64,         // logical offset in file
    fe_physical: u64,        // physical offset on device
    fe_length: u64,          // length in bytes
    fe_reserved64: [u64; 2],
    fe_flags: u32,           // FIEMAP_EXTENT_* flags
    fe_reserved: [u32; 3],
}

// ─── Public types ────────────────────────────────────────────────────────────

/// A contiguous data region (extent) within a file.
#[derive(Debug, Clone, PartialEq)]
pub struct Extent {
    /// Logical byte offset within the file (0-based).
    pub logical: u64,
    /// Physical byte offset on the block device.
    pub physical: u64,
    /// Length of this extent in bytes.
    pub length: u64,
    /// Kernel extent flags (FIEMAP_EXTENT_*).
    pub flags: u32,
}

impl Extent {
    /// Is this the last extent in the file?
    #[inline]
    pub fn is_last(&self) -> bool {
        self.flags & FIEMAP_EXTENT_LAST != 0
    }

    /// Is this an unwritten (pre-allocated but zeroed) extent?
    #[inline]
    pub fn is_unwritten(&self) -> bool {
        self.flags & FIEMAP_EXTENT_UNWRITTEN != 0
    }

    /// Is this extent shared (deduplicated / reflinked)?
    #[inline]
    pub fn is_shared(&self) -> bool {
        self.flags & FIEMAP_EXTENT_SHARED != 0
    }

    /// Logical end offset (exclusive).
    #[inline]
    pub fn end(&self) -> u64 {
        self.logical + self.length
    }
}

/// Summary of a file's extent layout.
#[derive(Debug, Clone)]
pub struct SparseInfo {
    /// True if the file has holes (sparse regions).
    pub is_sparse: bool,
    /// Total bytes covered by data extents on disk.
    pub data_bytes: u64,
    /// Number of data extents.
    pub extent_count: u32,
    /// Number of hole regions between/after extents.
    pub hole_count: u32,
}

/// Result of comparing two files' extent maps.
#[derive(Debug, Clone, PartialEq)]
pub enum ExtentDiff {
    /// Files definitely have different extent layouts.
    Different,
    /// Files have identical logical extent layouts (data may still differ).
    SameLayout,
}

// ─── Constants ───────────────────────────────────────────────────────────────

/// Maximum extents per ioctl call.  1024 × 56 = 56 KB buffer.
const MAX_EXTENTS_PER_CALL: u32 = 1024;

// ─── Core FIEMAP query ──────────────────────────────────────────────────────

/// Query the kernel for all extents of an open file.
///
/// Returns the file's physical extent map — the exact on-disk layout
/// showing where data lives and where holes exist.
///
/// This is a **read-only** operation.  It never modifies data.
pub fn get_extents(file: &File) -> io::Result<Vec<Extent>> {
    get_extents_range(file, 0, u64::MAX)
}

/// Query extents within a specific byte range [start, start+length).
pub fn get_extents_range(file: &File, start: u64, length: u64) -> io::Result<Vec<Extent>> {
    let fd = file.as_raw_fd();

    let header_size = std::mem::size_of::<FiemapHeader>();
    let extent_size = std::mem::size_of::<FiemapExtentRaw>();
    let buf_size = header_size + (MAX_EXTENTS_PER_CALL as usize) * extent_size;
    let mut buf = vec![0u8; buf_size];

    let mut all_extents = Vec::new();
    let mut query_start = start;

    loop {
        // Zero the header
        buf[..header_size].fill(0);

        let header = unsafe { &mut *(buf.as_mut_ptr() as *mut FiemapHeader) };
        header.fm_start = query_start;
        header.fm_length = if length == u64::MAX {
            u64::MAX - query_start
        } else {
            length.saturating_sub(query_start.saturating_sub(start))
        };
        // Don't use FIEMAP_FLAG_SYNC — it forces a full fsync before
        // mapping, which can block for seconds on large files on live
        // servers.  We read the current stable extent map instead.
        header.fm_flags = 0;
        header.fm_extent_count = MAX_EXTENTS_PER_CALL;

        let ret = unsafe { libc::ioctl(fd, FS_IOC_FIEMAP, buf.as_mut_ptr()) };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }

        let header = unsafe { &*(buf.as_ptr() as *const FiemapHeader) };
        let mapped = header.fm_mapped_extents as usize;

        if mapped == 0 {
            break;
        }

        let extent_base = unsafe {
            buf.as_ptr().add(header_size) as *const FiemapExtentRaw
        };

        let mut saw_last = false;
        for i in 0..mapped {
            let raw = unsafe { &*extent_base.add(i) };
            let ext = Extent {
                logical: raw.fe_logical,
                physical: raw.fe_physical,
                length: raw.fe_length,
                flags: raw.fe_flags,
            };

            // Advance query past this extent for next iteration
            query_start = raw.fe_logical + raw.fe_length;

            if ext.is_last() {
                saw_last = true;
            }
            all_extents.push(ext);
        }

        if saw_last || mapped < MAX_EXTENTS_PER_CALL as usize {
            break;
        }
    }

    Ok(all_extents)
}

/// Analyze a file's extent layout for sparse detection.
///
/// Replaces the naive "read and scan for zeros" approach with a single
/// ioctl sequence that returns the exact layout in O(extents) time —
/// typically microseconds even for multi-terabyte sparse files.
pub fn sparse_info(file: &File, file_size: u64) -> io::Result<SparseInfo> {
    if file_size == 0 {
        return Ok(SparseInfo {
            is_sparse: false,
            data_bytes: 0,
            extent_count: 0,
            hole_count: 0,
        });
    }

    let extents = get_extents(file)?;
    let data_bytes: u64 = extents.iter().map(|e| e.length).sum();

    let mut hole_count = 0u32;
    let mut prev_end = 0u64;
    for ext in &extents {
        if ext.logical > prev_end {
            hole_count += 1;
        }
        prev_end = ext.logical + ext.length;
    }
    // Trailing hole
    if prev_end < file_size {
        hole_count += 1;
    }

    Ok(SparseInfo {
        is_sparse: hole_count > 0 || data_bytes < file_size,
        data_bytes,
        extent_count: extents.len() as u32,
        hole_count,
    })
}

/// Compare two files' extent maps for quick inequality detection.
///
/// Returns `ExtentDiff::Different` if the files DEFINITELY have different
/// logical layouts (different extent count or logical ranges).
///
/// Returns `ExtentDiff::SameLayout` if the logical layout matches —
/// but the actual data could still differ (physical blocks may contain
/// different bytes).  Always follow up with a hash comparison.
///
/// This is a fast pre-check: if extents differ, we KNOW a copy is needed.
/// If extents match, we might still need to hash.
pub fn compare_extents(file1: &File, file2: &File) -> io::Result<ExtentDiff> {
    let ext1 = get_extents(file1)?;
    let ext2 = get_extents(file2)?;

    if ext1.len() != ext2.len() {
        return Ok(ExtentDiff::Different);
    }

    for (a, b) in ext1.iter().zip(ext2.iter()) {
        if a.logical != b.logical || a.length != b.length {
            return Ok(ExtentDiff::Different);
        }
    }

    Ok(ExtentDiff::SameLayout)
}

/// Compute the data density of a file (data_bytes / file_size).
///
/// - 1.0 = fully dense (no holes)
/// - 0.0 = empty or all holes
/// - < 0.5 = very sparse, benefits hugely from sparse-aware copy
pub fn data_density(file: &File, file_size: u64) -> io::Result<f64> {
    if file_size == 0 {
        return Ok(1.0);
    }
    let info = sparse_info(file, file_size)?;
    Ok(info.data_bytes as f64 / file_size as f64)
}

/// Copy only data extents from src to dst, punching holes for sparse regions.
///
/// For a 50 GB sparse file with 1 GB of actual data, this copies only
/// the 1 GB of data and creates holes for the remaining 49 GB — versus
/// naively writing 50 GB of zeros.
///
/// Returns the number of data bytes copied.
pub fn sparse_copy(src: &File, dst: &File, file_size: u64) -> io::Result<u64> {
    use std::os::unix::io::AsRawFd;

    let extents = get_extents(src)?;

    if extents.is_empty() {
        // No data extents — file is all holes or empty
        // Just set the file size
        unsafe { libc::ftruncate(dst.as_raw_fd(), file_size as libc::off_t) };
        return Ok(0);
    }

    // Set destination to full file size first (creates holes everywhere)
    unsafe { libc::ftruncate(dst.as_raw_fd(), file_size as libc::off_t) };

    let mut total_copied = 0u64;

    for ext in &extents {
        if ext.is_unwritten() {
            // Pre-allocated but no data — skip, already a hole in dst
            continue;
        }

        // Copy this extent using kernel-side copy_file_range
        let mut src_off = ext.logical as i64;
        let mut dst_off = ext.logical as i64;
        let mut remaining = ext.length;

        while remaining > 0 {
            let chunk = remaining.min(128 * 1024 * 1024); // 128 MB
            let ret = unsafe {
                libc::copy_file_range(
                    src.as_raw_fd(),
                    &mut src_off,
                    dst.as_raw_fd(),
                    &mut dst_off,
                    chunk as usize,
                    0,
                )
            };

            if ret < 0 {
                let err = io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                if err.raw_os_error() == Some(libc::EXDEV) {
                    // copy_file_range doesn't work across filesystems —
                    // fall back to userspace read+write for this extent.
                    return sparse_copy_fallback_extent(
                        src, dst, ext.logical, ext.length,
                    ).map(|n| total_copied + n);
                }
                return Err(err);
            }
            if ret == 0 {
                break;
            }

            let n = ret as u64;
            total_copied += n;
            remaining -= n;
        }
    }

    Ok(total_copied)
}

/// Fallback for sparse_copy when copy_file_range fails with EXDEV
/// (cross-device).  Uses standard pread/pwrite at the specific offset.
fn sparse_copy_fallback_extent(
    src: &File,
    dst: &File,
    offset: u64,
    length: u64,
) -> io::Result<u64> {
    use std::os::unix::io::AsRawFd;

    let mut buf = vec![0u8; 128 * 1024]; // 128 KB buffer
    let mut remaining = length;
    let mut pos = offset;
    let mut total = 0u64;

    while remaining > 0 {
        let chunk = remaining.min(buf.len() as u64) as usize;
        let n_read = unsafe {
            libc::pread(
                src.as_raw_fd(),
                buf.as_mut_ptr() as *mut libc::c_void,
                chunk,
                pos as libc::off_t,
            )
        };
        if n_read < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(err);
        }
        if n_read == 0 {
            break;
        }

        let mut written = 0usize;
        while written < n_read as usize {
            let n_written = unsafe {
                libc::pwrite(
                    dst.as_raw_fd(),
                    buf[written..].as_ptr() as *const libc::c_void,
                    (n_read as usize) - written,
                    (pos + written as u64) as libc::off_t,
                )
            };
            if n_written < 0 {
                let err = io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                return Err(err);
            }
            written += n_written as usize;
        }

        let n = n_read as u64;
        pos += n;
        total += n;
        remaining -= n;
    }

    Ok(total)
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_dense_file_extents() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dense.bin");
        let mut f = File::create(&path).unwrap();
        f.write_all(&[42u8; 4096]).unwrap();
        f.sync_all().unwrap();

        let f = File::open(&path).unwrap();
        let extents = get_extents(&f).unwrap();
        assert!(!extents.is_empty(), "dense file must have at least one extent");

        let info = sparse_info(&f, 4096).unwrap();
        assert!(!info.is_sparse, "dense file should not be sparse");
        assert!(info.data_bytes >= 4096, "data bytes should cover file");
    }

    #[test]
    fn test_sparse_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sparse.bin");

        // Create a 2MB file with data only at offset 1MB
        let file = File::create(&path).unwrap();
        let fd = file.as_raw_fd();
        unsafe {
            libc::ftruncate(fd, 2 * 1024 * 1024);
        }

        // Write 4KB at offset 1MB
        use std::io::Seek;
        let mut file = file;
        file.seek(io::SeekFrom::Start(1024 * 1024)).unwrap();
        file.write_all(&[0xAB; 4096]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let f = File::open(&path).unwrap();
        let info = sparse_info(&f, 2 * 1024 * 1024).unwrap();
        assert!(info.is_sparse, "file with leading hole should be sparse");
        assert!(
            info.data_bytes < 2 * 1024 * 1024,
            "data bytes ({}) should be less than file size (2MB)",
            info.data_bytes
        );
    }

    #[test]
    fn test_extent_comparison_identical() {
        let dir = tempfile::tempdir().unwrap();
        let data = vec![42u8; 8192];

        let p1 = dir.path().join("a.bin");
        let p2 = dir.path().join("b.bin");
        std::fs::write(&p1, &data).unwrap();
        std::fs::write(&p2, &data).unwrap();

        let f1 = File::open(&p1).unwrap();
        let f2 = File::open(&p2).unwrap();
        let result = compare_extents(&f1, &f2).unwrap();
        assert_eq!(result, ExtentDiff::SameLayout);
    }

    #[test]
    fn test_extent_comparison_different_size() {
        let dir = tempfile::tempdir().unwrap();

        let p1 = dir.path().join("a.bin");
        let p2 = dir.path().join("b.bin");
        std::fs::write(&p1, &[1u8; 4096]).unwrap();
        std::fs::write(&p2, &[2u8; 65536]).unwrap();

        let f1 = File::open(&p1).unwrap();
        let f2 = File::open(&p2).unwrap();
        let result = compare_extents(&f1, &f2).unwrap();
        assert_eq!(result, ExtentDiff::Different);
    }

    #[test]
    fn test_data_density_dense() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dense.bin");
        std::fs::write(&path, &[1u8; 4096]).unwrap();

        let f = File::open(&path).unwrap();
        let d = data_density(&f, 4096).unwrap();
        assert!(d >= 0.99, "dense file density should be ~1.0, got {d}");
    }

    #[test]
    fn test_empty_file_sparse_info() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty");
        File::create(&path).unwrap();

        let f = File::open(&path).unwrap();
        let info = sparse_info(&f, 0).unwrap();
        assert!(!info.is_sparse);
        assert_eq!(info.data_bytes, 0);
        assert_eq!(info.extent_count, 0);
    }

    #[test]
    fn test_sparse_copy() {
        let dir = tempfile::tempdir().unwrap();
        let src_path = dir.path().join("src_sparse");
        let dst_path = dir.path().join("dst_sparse");

        // Create sparse source: 1MB file with 4KB data at offset 512KB
        {
            let f = File::create(&src_path).unwrap();
            unsafe { libc::ftruncate(f.as_raw_fd(), 1024 * 1024) };
            let mut f = f;
            f.seek(io::SeekFrom::Start(512 * 1024)).unwrap();
            f.write_all(&[0xCD; 4096]).unwrap();
            f.sync_all().unwrap();
        }

        let src = File::open(&src_path).unwrap();
        let dst = File::create(&dst_path).unwrap();
        let copied = sparse_copy(&src, &dst, 1024 * 1024).unwrap();

        // Should have copied only the data extent (~4KB), not the full 1MB
        assert!(copied <= 8192, "sparse copy should transfer minimal data, got {copied}");

        // Verify destination has correct size
        let meta = std::fs::metadata(&dst_path).unwrap();
        assert_eq!(meta.len(), 1024 * 1024);
    }

    use std::io::Seek;
}
