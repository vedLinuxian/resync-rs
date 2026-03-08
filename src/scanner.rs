//! Parallel directory scanner powered by `jwalk` + `rayon`.
//!
//! Produces a flat list of [`FileEntry`] records in wall-clock time that
//! scales linearly with available CPU cores.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use jwalk::WalkDir;
use rayon::prelude::*;
use tracing::warn;

use crate::error::{Result, ResyncError};

// ─── Data model ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct FileEntry {
    /// Absolute path on the local filesystem
    pub abs_path: PathBuf,
    /// Path relative to the scan root (used to locate the peer in dest)
    pub rel_path: PathBuf,
    /// File size in bytes
    pub size: u64,
    /// Last-modification time
    pub modified: SystemTime,
    /// Unix mode bits (permissions + file-type flags)
    pub mode: u32,
    /// Whether this entry is a symbolic link
    pub is_symlink: bool,
    /// Symlink target (only set when `is_symlink == true`)
    pub symlink_target: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct DirEntry {
    /// Absolute path of the directory
    pub abs_path: PathBuf,
    /// Relative path from the scan root
    pub rel_path: PathBuf,
    /// Unix mode bits
    pub mode: u32,
}

#[derive(Debug, Default)]
pub struct ScanResult {
    pub files: Vec<FileEntry>,
    pub dirs: Vec<DirEntry>,
    /// Total number of bytes across all regular files
    pub total_bytes: u64,
    /// Number of entries that could not be read (permission denied, I/O error)
    pub errors: u64,
}

// ─── Scanner ─────────────────────────────────────────────────────────────────

pub struct Scanner {
    root: PathBuf,
    recursive: bool,
    preserve_links: bool,
}

impl Scanner {
    /// Create a new scanner. The root path is canonicalized immediately so
    /// that `strip_prefix` never fails downstream.
    ///
    /// BUG FIX #2: Previously non-canonical roots caused `strip_prefix` to
    /// fail, producing absolute `rel_path` values. `Path::join(absolute)`
    /// replaces the base entirely, causing the applier to overwrite the
    /// **source** instead of writing to the destination.
    pub fn new(root: &Path, recursive: bool, preserve_links: bool) -> Result<Self> {
        let root = root.canonicalize().map_err(|e| ResyncError::Io {
            path: root.display().to_string(),
            source: e,
        })?;
        Ok(Self {
            root,
            recursive,
            preserve_links,
        })
    }

    /// Walk the directory tree in parallel and return a [`ScanResult`].
    pub fn scan(&self) -> Result<ScanResult> {
        use std::os::unix::fs::MetadataExt;
        use std::sync::atomic::{AtomicU64, Ordering};

        // BUG FIX #4: min_depth=1 excludes the root directory itself.
        // Previously min_depth=0 included root with empty rel_path.
        let min_depth = 1usize;
        let max_depth = if self.recursive { usize::MAX } else { 1 };

        let error_count = AtomicU64::new(0);

        // BUG FIX #5: Don't silently swallow walk errors — log them and count.
        let raw: Vec<_> = WalkDir::new(&self.root)
            .min_depth(min_depth)
            .max_depth(max_depth)
            .skip_hidden(false)
            .follow_links(false) // we handle symlinks manually
            .into_iter()
            .filter_map(|e| match e {
                Ok(entry) => Some(entry),
                Err(err) => {
                    warn!("scan error: {err}");
                    error_count.fetch_add(1, Ordering::Relaxed);
                    None
                }
            })
            .collect();

        let root = &self.root;
        let preserve_links = self.preserve_links;

        let (files, dirs): (Vec<_>, Vec<_>) = raw
            .into_par_iter()
            .filter_map(|entry| {
                let abs = entry.path();

                // BUG FIX #2b: With canonicalized root, strip_prefix should
                // always succeed. If it doesn't, skip with warning.
                let rel = match abs.strip_prefix(root) {
                    Ok(p) => p.to_path_buf(),
                    Err(_) => {
                        warn!(
                            "could not compute relative path for {}, skipping",
                            abs.display()
                        );
                        error_count.fetch_add(1, Ordering::Relaxed);
                        return None;
                    }
                };

                if rel.as_os_str().is_empty() {
                    return None;
                }

                let meta = match entry.metadata() {
                    Ok(m) => m,
                    Err(e) => {
                        warn!("stat failed for {}: {e}", abs.display());
                        error_count.fetch_add(1, Ordering::Relaxed);
                        return None;
                    }
                };

                let mode = meta.mode();

                // BUG FIX #3: Check symlinks FIRST, before is_dir/is_file.
                // Previously symlinks to directories were classified as dirs.
                // Use symlink_metadata for reliable detection on all filesystems.
                let is_symlink = entry.file_type().is_symlink()
                    || std::fs::symlink_metadata(&abs)
                        .map(|m| m.file_type().is_symlink())
                        .unwrap_or(false);

                if is_symlink {
                    if preserve_links {
                        let target = std::fs::read_link(&abs).ok();
                        Some(Either::File(FileEntry {
                            abs_path: abs.clone(),
                            rel_path: rel,
                            size: 0,
                            modified: meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                            mode,
                            is_symlink: true,
                            symlink_target: target,
                        }))
                    } else {
                        // Follow the symlink and sync target content
                        match std::fs::metadata(&abs) {
                            Ok(target_meta) if target_meta.is_file() => {
                                Some(Either::File(FileEntry {
                                    abs_path: abs.clone(),
                                    rel_path: rel,
                                    size: target_meta.len(),
                                    modified: target_meta
                                        .modified()
                                        .unwrap_or(SystemTime::UNIX_EPOCH),
                                    mode: target_meta.mode(),
                                    is_symlink: false,
                                    symlink_target: None,
                                }))
                            }
                            Ok(target_meta) if target_meta.is_dir() => {
                                Some(Either::Dir(DirEntry {
                                    abs_path: abs.clone(),
                                    rel_path: rel,
                                    mode: target_meta.mode(),
                                }))
                            }
                            _ => {
                                warn!("skipping dangling symlink: {}", abs.display());
                                None
                            }
                        }
                    }
                } else if meta.is_dir() {
                    Some(Either::Dir(DirEntry {
                        abs_path: abs.clone(),
                        rel_path: rel,
                        mode,
                    }))
                } else if meta.is_file() {
                    Some(Either::File(FileEntry {
                        abs_path: abs.clone(),
                        rel_path: rel,
                        size: meta.len(),
                        modified: meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                        mode,
                        is_symlink: false,
                        symlink_target: None,
                    }))
                } else {
                    None // skip devices, sockets, etc.
                }
            })
            .partition_map(|e| e.into_rayon_partition());

        let total_bytes: u64 = files.iter().filter(|f| !f.is_symlink).map(|f| f.size).sum();
        let errors = error_count.load(Ordering::Relaxed);

        Ok(ScanResult {
            files,
            dirs,
            total_bytes,
            errors,
        })
    }
}

// ─── Internal helpers ────────────────────────────────────────────────────────

enum Either {
    File(FileEntry),
    Dir(DirEntry),
}

impl Either {
    fn into_rayon_partition(self) -> rayon::iter::Either<FileEntry, DirEntry> {
        match self {
            Either::File(f) => rayon::iter::Either::Left(f),
            Either::Dir(d) => rayon::iter::Either::Right(d),
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn make_tree() -> TempDir {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir(tmp.path().join("sub")).unwrap();
        fs::write(tmp.path().join("a.txt"), b"hello").unwrap();
        fs::write(tmp.path().join("b.txt"), b"world").unwrap();
        fs::write(tmp.path().join("sub/c.txt"), b"deep").unwrap();
        tmp
    }

    #[test]
    fn scan_finds_all_files() {
        let tmp = make_tree();
        let scanner = Scanner::new(tmp.path(), true, false).unwrap();
        let result = scanner.scan().unwrap();
        assert_eq!(result.files.len(), 3, "should find 3 regular files");
        assert_eq!(result.total_bytes, 14);
        assert_eq!(result.errors, 0);
    }

    #[test]
    fn non_recursive_scan_skips_subdirs() {
        let tmp = make_tree();
        let scanner = Scanner::new(tmp.path(), false, false).unwrap();
        let result = scanner.scan().unwrap();
        assert_eq!(result.files.len(), 2);
    }

    #[test]
    fn rel_paths_are_never_absolute() {
        let tmp = make_tree();
        let scanner = Scanner::new(tmp.path(), true, false).unwrap();
        let result = scanner.scan().unwrap();
        for f in &result.files {
            assert!(
                f.rel_path.is_relative(),
                "rel_path should be relative, got: {}",
                f.rel_path.display()
            );
        }
        for d in &result.dirs {
            assert!(
                d.rel_path.is_relative(),
                "rel_path should be relative, got: {}",
                d.rel_path.display()
            );
        }
    }

    #[test]
    fn root_dir_not_included_in_scan() {
        let tmp = make_tree();
        let scanner = Scanner::new(tmp.path(), true, false).unwrap();
        let result = scanner.scan().unwrap();
        for d in &result.dirs {
            assert!(
                !d.rel_path.as_os_str().is_empty(),
                "root dir should not be in scan results"
            );
        }
    }

    #[test]
    fn symlink_to_file_followed_when_preserve_links_false() {
        let tmp = make_tree();
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(tmp.path().join("a.txt"), tmp.path().join("link_to_a"))
                .unwrap();
            let scanner = Scanner::new(tmp.path(), true, false).unwrap();
            let result = scanner.scan().unwrap();
            // a.txt, b.txt, sub/c.txt, link_to_a (dereferenced)
            assert_eq!(result.files.len(), 4);
            let link_entry = result
                .files
                .iter()
                .find(|f| f.rel_path.to_str().unwrap().contains("link_to_a"))
                .expect("followed symlink should appear as regular file");
            assert!(!link_entry.is_symlink);
            assert_eq!(link_entry.size, 5); // "hello"
        }
    }
}
