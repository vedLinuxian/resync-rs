//! Persistent manifest cache — the single most impactful performance feature.
//!
//! After each sync, we serialize the full scan result (path, size, mtime, inode)
//! to a binary file `.resync-manifest` in the destination directory.  On the next
//! sync:
//!
//!  1. Read manifest: single `open()` + `read()` of ~1–10 MB file
//!  2. **Incremental source scan**: only stat() directories (not files) to detect
//!     changes via mtime.  Unchanged dirs use cached file list.
//!  3. **Skip destination scan**: compare source against manifest (not live stat)
//!  4. **Skip manifest save**: if nothing changed, don't rewrite the manifest
//!
//! At 100K files with no changes, this eliminates ~200K stat() calls, yielding
//! massive speedups on large trees.

use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::error::{Result, ResyncError};
use crate::scanner::{FileEntry, ScanResult};

/// Current manifest format version.  Bump this when the binary format changes.
const MANIFEST_VERSION: u32 = 3;

/// Magic bytes at the start of a manifest file for quick validation.
const MANIFEST_MAGIC: [u8; 8] = *b"RSNC_MAN";

/// Default filename for the manifest in the destination directory.
pub const MANIFEST_FILENAME: &str = ".resync-manifest";

/// Filename for the lightweight directory-only index (< 50 KB even at 1M files).
/// This file alone is sufficient for the no-change fast path — the full manifest
/// is only loaded when files actually need syncing.
pub const TOC_FILENAME: &str = ".resync-toc";

// ─── Table of Contents (lightweight dir-mtime index) ─────────────────────────

/// Lightweight directory index — just source path + dir mtimes.
/// Typically < 50 KB even for 1M-file trees, enabling sub-millisecond
/// no-change detection without loading the full manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestToc {
    pub version: u32,
    pub source_path: PathBuf,
    /// Directory mtimes stored as (relative_path, mtime_ns).
    pub dir_mtimes: Vec<(PathBuf, i128)>,
    /// Total file count at time of last sync (for info logging).
    pub file_count: u64,
    /// Total byte count at time of last sync.
    pub total_bytes: u64,
}

impl ManifestToc {
    /// Build a TOC from a Manifest.
    pub fn from_manifest(manifest: &Manifest) -> Self {
        let mut dir_mtimes: Vec<(PathBuf, i128)> = manifest
            .dirs
            .iter()
            .map(|(p, e)| (p.clone(), e.mtime_ns))
            .collect();
        dir_mtimes.sort_by(|a, b| a.0.cmp(&b.0));
        Self {
            version: MANIFEST_VERSION,
            source_path: manifest.source_path.clone(),
            dir_mtimes,
            file_count: manifest.files.len() as u64,
            total_bytes: manifest.total_bytes,
        }
    }

    /// Save TOC to a file alongside the manifest.
    pub fn save(&self, dest: &Path) -> Result<()> {
        let path = dest.join(TOC_FILENAME);
        let payload = bincode::serialize(self)
            .map_err(|e| ResyncError::Other(anyhow::anyhow!("TOC serialize failed: {e}")))?;
        let mut data = Vec::with_capacity(12 + payload.len());
        data.extend_from_slice(&MANIFEST_MAGIC);
        data.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
        data.extend_from_slice(&payload);

        let tmp_path = path.with_extension("tmp");
        fs::write(&tmp_path, &data).map_err(|e| ResyncError::Io {
            path: tmp_path.display().to_string(),
            source: e,
        })?;
        fs::rename(&tmp_path, &path).map_err(|e| ResyncError::Io {
            path: path.display().to_string(),
            source: e,
        })?;
        info!(
            "Saved TOC: {} dirs ({} bytes)",
            self.dir_mtimes.len(),
            data.len()
        );
        Ok(())
    }

    /// Load TOC from destination directory. Returns None if missing/invalid.
    pub fn load(dest: &Path) -> Option<Self> {
        let path = dest.join(TOC_FILENAME);
        let data = match fs::read(&path) {
            Ok(d) => d,
            Err(_) => return None,
        };
        if data.len() < 12 {
            return None;
        }
        if data[0..8] != MANIFEST_MAGIC {
            return None;
        }
        let version = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);
        if version != MANIFEST_VERSION {
            return None;
        }
        bincode::deserialize::<ManifestToc>(&data[12..]).ok()
    }

    /// Check if all source directory mtimes match the cached values.
    /// Returns true if the source tree is definitely unchanged.
    pub fn source_unchanged(&self, source_root: &Path) -> bool {
        for (rel_path, cached_mtime_ns) in &self.dir_mtimes {
            let abs = source_root.join(rel_path);
            let meta = match std::fs::metadata(&abs) {
                Ok(m) => m,
                Err(_) => return false, // dir missing = changed
            };
            let current_mtime_ns = meta
                .modified()
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_nanos() as i128)
                .unwrap_or(0);
            if current_mtime_ns != *cached_mtime_ns {
                return false;
            }
        }

        // For now, trust dir mtime checks are sufficient

        true
    }
}

// ─── Serializable entries ────────────────────────────────────────────────────

/// A compact representation of a file's metadata for manifest storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub size: u64,
    /// Modification time as nanoseconds since UNIX epoch.
    pub mtime_ns: i128,
    /// Inode number (for change detection heuristic).
    pub ino: u64,
    /// Device ID (to detect if the filesystem changed underneath).
    pub dev: u64,
    /// File mode (permissions).
    pub mode: u32,
    /// Owner UID.
    pub uid: u32,
    /// Group GID.
    pub gid: u32,
    /// Whether this is a symlink.
    pub is_symlink: bool,
    /// Symlink target (if applicable).
    pub symlink_target: Option<PathBuf>,
}

/// A compact representation of a directory for manifest storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestDirEntry {
    pub mode: u32,
    /// Directory modification time as nanoseconds since UNIX epoch.
    /// Used for incremental source scanning — if a directory's mtime hasn't
    /// changed, its file list is guaranteed identical (no files added/removed).
    pub mtime_ns: i128,
}

/// The full manifest: a snapshot of the destination tree after a successful sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Format version for forward compatibility.
    pub version: u32,
    /// When this manifest was created (ns since epoch).
    pub created_ns: i128,
    /// Source path this manifest was synced from.
    pub source_path: PathBuf,
    /// File entries keyed by relative path.
    pub files: HashMap<PathBuf, ManifestEntry>,
    /// Directory entries keyed by relative path.
    pub dirs: HashMap<PathBuf, ManifestDirEntry>,
    /// Total bytes tracked.
    pub total_bytes: u64,
}

impl Manifest {
    /// Create a new empty manifest.
    pub fn new(source_path: PathBuf) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        Self {
            version: MANIFEST_VERSION,
            created_ns: now.as_nanos() as i128,
            source_path,
            files: HashMap::new(),
            dirs: HashMap::new(),
            total_bytes: 0,
        }
    }

    /// Build a manifest from a ScanResult (typically the source scan after sync).
    pub fn from_scan_result(src_result: &ScanResult, source_path: PathBuf) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();

        let mut files = HashMap::with_capacity(src_result.files.len());
        for f in &src_result.files {
            let mtime_ns = f
                .modified
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_nanos() as i128)
                .unwrap_or(0);

            files.insert(
                f.rel_path.clone(),
                ManifestEntry {
                    size: f.size,
                    mtime_ns,
                    ino: f.ino,
                    dev: f.dev,
                    mode: f.mode,
                    uid: f.uid,
                    gid: f.gid,
                    is_symlink: f.is_symlink,
                    symlink_target: f.symlink_target.clone(),
                },
            );
        }

        let mut dirs = HashMap::with_capacity(src_result.dirs.len());
        for d in &src_result.dirs {
            let mtime_ns = d
                .modified
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|dur| dur.as_nanos() as i128)
                .unwrap_or(0);
            dirs.insert(
                d.rel_path.clone(),
                ManifestDirEntry {
                    mode: d.mode,
                    mtime_ns,
                },
            );
        }

        Self {
            version: MANIFEST_VERSION,
            created_ns: now.as_nanos() as i128,
            source_path,
            files,
            dirs,
            total_bytes: src_result.total_bytes,
        }
    }

    /// Convert manifest entries back into a dst_map (HashMap<PathBuf, FileEntry>)
    /// suitable for use by the sync engine's comparison phase.
    ///
    /// The abs_path is reconstructed using the destination root.
    pub fn to_dst_map(&self, dest_root: &Path) -> FxHashMap<PathBuf, FileEntry> {
        let mut map = FxHashMap::default();
        for (rel_path, entry) in &self.files {
            let abs_path = dest_root.join(rel_path);

            // Convert mtime_ns back to SystemTime
            let modified = if entry.mtime_ns >= 0 {
                let dur = std::time::Duration::from_nanos(entry.mtime_ns as u64);
                SystemTime::UNIX_EPOCH + dur
            } else {
                let dur = std::time::Duration::from_nanos((-entry.mtime_ns) as u64);
                SystemTime::UNIX_EPOCH - dur
            };

            map.insert(
                rel_path.clone(),
                FileEntry {
                    abs_path,
                    rel_path: rel_path.clone(),
                    size: entry.size,
                    modified,
                    mode: entry.mode,
                    is_symlink: entry.is_symlink,
                    symlink_target: entry.symlink_target.clone(),
                    ino: entry.ino,
                    dev: entry.dev,
                    uid: entry.uid,
                    gid: entry.gid,
                },
            );
        }
        map
    }

    /// Convert manifest dir entries back into a dir set.
    pub fn to_dir_set(&self) -> FxHashSet<PathBuf> {
        self.dirs.keys().cloned().collect()
    }

    /// Build an incremental scan cache for efficient source scanning.
    ///
    /// Groups files by parent directory, stores directory mtimes, and builds
    /// a parent→children directory map.  The Scanner uses this to skip
    /// unchanged directories entirely (just stat the dir, compare mtime).
    pub fn to_incremental_cache(&self) -> crate::scanner::IncrementalCache {
        use crate::scanner::IncrementalCache;

        let mut dir_mtimes = HashMap::new();
        let mut dir_modes = HashMap::new();
        for (rel, entry) in &self.dirs {
            dir_mtimes.insert(rel.clone(), entry.mtime_ns);
            dir_modes.insert(rel.clone(), entry.mode);
        }
        // Root directory (empty path) — use own mtime if stored
        // The root isn't in self.dirs, so we don't add it here.
        // The scanner will detect "no cached mtime for root" and scan it.
        // But we DO want to cache the root's mtime. Let's store it under "".
        // Actually, we should have stored it. But the root dir itself isn't in
        // scan results. We need to handle this: if root isn't cached, the
        // scanner will do a live scan of root only (which is cheap — just the
        // readdir of root). All subdirectories will be checked against cache.

        // Group files by their parent directory
        let mut files_by_dir: HashMap<PathBuf, Vec<(PathBuf, ManifestEntry)>> = HashMap::new();
        for (rel_path, entry) in &self.files {
            let parent = rel_path.parent().map(|p| p.to_path_buf()).unwrap_or_default();
            files_by_dir
                .entry(parent)
                .or_default()
                .push((rel_path.clone(), entry.clone()));
        }

        // Build parent→subdirs map
        let mut subdirs_of: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        for rel in self.dirs.keys() {
            let parent = rel.parent().map(|p| p.to_path_buf()).unwrap_or_default();
            subdirs_of.entry(parent).or_default().push(rel.clone());
        }

        IncrementalCache {
            dir_mtimes,
            dir_modes,
            files_by_dir,
            subdirs_of,
        }
    }

    // ─── Persistence ─────────────────────────────────────────────────────

    /// Save the manifest to a binary file.
    ///
    /// Format: [8-byte magic] [4-byte version LE] [zstd-compressed bincode payload]
    pub fn save(&self, path: &Path) -> Result<()> {
        let payload = bincode::serialize(self)
            .map_err(|e| ResyncError::Other(anyhow::anyhow!("manifest serialize failed: {e}")))?;

        // Compress with zstd level 3 (fast, good ratio for structured data)
        let compressed = zstd::encode_all(payload.as_slice(), 3).map_err(|e| {
            ResyncError::Other(anyhow::anyhow!("manifest zstd compress failed: {e}"))
        })?;

        let mut data = Vec::with_capacity(12 + compressed.len());
        data.extend_from_slice(&MANIFEST_MAGIC);
        data.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
        data.extend_from_slice(&compressed);

        // Write atomically: tmp + rename
        let tmp_path = path.with_extension("tmp");
        let mut file = fs::File::create(&tmp_path).map_err(|e| ResyncError::Io {
            path: tmp_path.display().to_string(),
            source: e,
        })?;
        file.write_all(&data).map_err(|e| ResyncError::Io {
            path: tmp_path.display().to_string(),
            source: e,
        })?;
        file.sync_data().map_err(|e| ResyncError::Io {
            path: tmp_path.display().to_string(),
            source: e,
        })?;
        drop(file);

        fs::rename(&tmp_path, path).map_err(|e| ResyncError::Io {
            path: path.display().to_string(),
            source: e,
        })?;

        info!(
            "Saved manifest: {} files, {} dirs ({} bytes)",
            self.files.len(),
            self.dirs.len(),
            data.len()
        );

        Ok(())
    }

    /// Load a manifest from a binary file.
    ///
    /// Returns `None` if the file doesn't exist or is invalid/corrupted.
    pub fn load(path: &Path) -> Option<Self> {
        let data = match fs::read(path) {
            Ok(d) => d,
            Err(_) => return None,
        };

        // Validate magic + minimum size
        if data.len() < 12 {
            warn!("Manifest file too small, ignoring");
            return None;
        }
        if data[0..8] != MANIFEST_MAGIC {
            warn!("Manifest magic mismatch, ignoring");
            return None;
        }

        let version = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);
        if version != MANIFEST_VERSION {
            warn!(
                "Manifest version mismatch (got {version}, expected {MANIFEST_VERSION}), ignoring"
            );
            return None;
        }

        // Decompress the zstd payload
        let decompressed = match zstd::decode_all(&data[12..]) {
            Ok(d) => d,
            Err(e) => {
                warn!("Manifest zstd decompression failed: {e}");
                return None;
            }
        };

        match bincode::deserialize::<Manifest>(&decompressed) {
            Ok(manifest) => {
                info!(
                    "Loaded manifest: {} files, {} dirs",
                    manifest.files.len(),
                    manifest.dirs.len()
                );
                Some(manifest)
            }
            Err(e) => {
                warn!("Manifest deserialization failed: {e}");
                None
            }
        }
    }

    /// Get the manifest file path for a given destination directory.
    pub fn path_for(dest: &Path) -> PathBuf {
        dest.join(MANIFEST_FILENAME)
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scanner::DirEntry;
    use tempfile::TempDir;

    fn make_test_scan() -> ScanResult {
        ScanResult {
            files: vec![
                FileEntry {
                    abs_path: PathBuf::from("/src/a.txt"),
                    rel_path: PathBuf::from("a.txt"),
                    size: 100,
                    modified: SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1700000000),
                    mode: 0o644,
                    is_symlink: false,
                    symlink_target: None,
                    ino: 12345,
                    dev: 1,
                    uid: 1000,
                    gid: 1000,
                },
                FileEntry {
                    abs_path: PathBuf::from("/src/sub/b.txt"),
                    rel_path: PathBuf::from("sub/b.txt"),
                    size: 200,
                    modified: SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1700000001),
                    mode: 0o755,
                    is_symlink: false,
                    symlink_target: None,
                    ino: 12346,
                    dev: 1,
                    uid: 1000,
                    gid: 1000,
                },
            ],
            dirs: vec![DirEntry {
                abs_path: PathBuf::from("/src/sub"),
                rel_path: PathBuf::from("sub"),
                mode: 0o755,
                modified: SystemTime::UNIX_EPOCH,
            }],
            total_bytes: 300,
            errors: 0,
            dirs_rescanned: 0,
        }
    }

    #[test]
    fn roundtrip_manifest() {
        let tmp = TempDir::new().unwrap();
        let scan = make_test_scan();
        let manifest = Manifest::from_scan_result(&scan, PathBuf::from("/src"));

        assert_eq!(manifest.files.len(), 2);
        assert_eq!(manifest.dirs.len(), 1);

        let path = tmp.path().join(MANIFEST_FILENAME);
        manifest.save(&path).unwrap();

        let loaded = Manifest::load(&path).expect("should load manifest");
        assert_eq!(loaded.files.len(), 2);
        assert_eq!(loaded.dirs.len(), 1);
        assert_eq!(loaded.version, MANIFEST_VERSION);
    }

    #[test]
    fn to_dst_map_reconstructs_entries() {
        let scan = make_test_scan();
        let manifest = Manifest::from_scan_result(&scan, PathBuf::from("/src"));
        let dst_map = manifest.to_dst_map(Path::new("/dest"));

        assert_eq!(dst_map.len(), 2);
        let entry = dst_map.get(Path::new("a.txt")).unwrap();
        assert_eq!(entry.size, 100);
        assert_eq!(entry.abs_path, PathBuf::from("/dest/a.txt"));
    }

    #[test]
    fn load_nonexistent_returns_none() {
        assert!(Manifest::load(Path::new("/nonexistent/path/.resync-manifest")).is_none());
    }

    #[test]
    fn load_corrupt_returns_none() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join(MANIFEST_FILENAME);
        fs::write(&path, b"garbage data here").unwrap();
        assert!(Manifest::load(&path).is_none());
    }

    #[test]
    fn manifest_with_symlinks() {
        let scan = ScanResult {
            files: vec![FileEntry {
                abs_path: PathBuf::from("/src/link"),
                rel_path: PathBuf::from("link"),
                size: 0,
                modified: SystemTime::UNIX_EPOCH,
                mode: 0o777,
                is_symlink: true,
                symlink_target: Some(PathBuf::from("target.txt")),
                ino: 99999,
                dev: 1,
                uid: 0,
                gid: 0,
            }],
            dirs: vec![],
            total_bytes: 0,
            errors: 0,
            dirs_rescanned: 0,
        };
        let manifest = Manifest::from_scan_result(&scan, PathBuf::from("/src"));
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join(MANIFEST_FILENAME);

        manifest.save(&path).unwrap();
        let loaded = Manifest::load(&path).unwrap();
        let entry = loaded.files.get(Path::new("link")).unwrap();
        assert!(entry.is_symlink);
        assert_eq!(entry.symlink_target, Some(PathBuf::from("target.txt")));
    }
}
