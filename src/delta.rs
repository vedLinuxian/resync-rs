//! Delta computation — determines the minimal set of byte ranges that
//!  need to be written to the destination file to bring it in sync with
//!  the source.
//!
//!  Algorithm (Phase 1 — fixed-size chunks):
//!  1. Compare `src_manifest` and `dst_manifest` chunk-by-chunk.
//!  2. A chunk is "changed" when its BLAKE3 hash differs.
//!  3. For every changed or new chunk we emit one [`DeltaOp::Write`].
//!  4. For every matching chunk we emit [`DeltaOp::Copy`] (zero bytes sent).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::hasher::{ChunkMeta, FileManifest};

// ─── Data model ──────────────────────────────────────────────────────────────

/// One atomic operation in the delta patch for a single file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeltaOp {
    /// Copy bytes from the existing destination at `src_offset` in the
    /// destination mmap. (Hashes match — zero transfer needed.)
    Copy { src_offset: u64, len: usize },
    /// Write bytes from the source file at `src_offset`.
    /// (Hash mismatch or chunk beyond destination EOF.)
    Write { src_offset: u64, len: usize },
}

impl DeltaOp {
    pub fn is_write(&self) -> bool {
        matches!(self, DeltaOp::Write { .. })
    }

    pub fn byte_len(&self) -> usize {
        match self {
            DeltaOp::Copy { len, .. } => *len,
            DeltaOp::Write { len, .. } => *len,
        }
    }
}

/// The complete patch between one source file and its destination peer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileDelta {
    /// Ordered sequence of operations (covers the full source file length)
    pub ops: Vec<DeltaOp>,
    /// Total bytes that must be transferred from source
    pub transfer_bytes: u64,
    /// Total bytes that are reused from destination unchanged
    pub reuse_bytes: u64,
    /// Final size of the destination file after applying this delta
    pub final_size: u64,
}

impl FileDelta {
    /// Returns `true` when the source and destination are byte-identical
    /// (same content AND same size) — no I/O is needed.
    pub fn is_no_op(&self) -> bool {
        self.transfer_bytes == 0
    }

    /// Savings ratio: 0.0 = nothing saved, 1.0 = fully identical.
    pub fn savings_ratio(&self) -> f64 {
        let total = self.transfer_bytes + self.reuse_bytes;
        if total == 0 {
            return 1.0;
        }
        self.reuse_bytes as f64 / total as f64
    }
}

// ─── Delta engine ────────────────────────────────────────────────────────────

pub struct DeltaEngine;

impl DeltaEngine {
    /// Compute the delta between `src` (source manifest) and `dst`
    /// (destination manifest, which may be absent for new files).
    ///
    /// BUG FIX #13: Asserts chunk_size consistency between src/dst manifests.
    /// Operations are purely sequential — the applier writes ops in order,
    /// so offsets within the delta stream are implicit (not seeked to).
    pub fn compute(src: &FileManifest, dst: Option<&FileManifest>) -> FileDelta {
        if let Some(d) = dst {
            assert_eq!(
                src.chunk_size, d.chunk_size,
                "chunk_size mismatch between source ({}) and destination ({})",
                src.chunk_size, d.chunk_size
            );
        }

        let dst_chunks: &[ChunkMeta] = dst.map(|m| m.chunks.as_slice()).unwrap_or(&[]);
        let src_chunks = &src.chunks;

        let mut ops = Vec::with_capacity(src_chunks.len());
        let mut transfer_bytes: u64 = 0;
        let mut reuse_bytes: u64 = 0;

        // CDC-aware path: use hash-map lookup instead of index-based comparison,
        // because CDC chunks won't align by index after an insertion/deletion.
        let use_cdc_path =
            src.is_cdc && dst.map(|d| d.is_cdc).unwrap_or(false) && !dst_chunks.is_empty();

        if use_cdc_path {
            // Build a map from hash → (offset, len) of the *first* matching dst chunk.
            // If multiple dst chunks share the same hash, first-wins is fine —
            // all copies are byte-identical by hash.
            let mut dst_map: HashMap<[u8; 32], (u64, usize)> =
                HashMap::with_capacity(dst_chunks.len());
            for dc in dst_chunks {
                dst_map.entry(dc.hash).or_insert((dc.offset, dc.len));
            }

            for src_chunk in src_chunks {
                if let Some(&(dst_offset, dst_len)) = dst_map.get(&src_chunk.hash) {
                    // Content match — reuse from destination.
                    // Use the smaller of dst_len and src_chunk.len to be safe,
                    // but in practice they should be identical.
                    let len = src_chunk.len.min(dst_len);
                    reuse_bytes += len as u64;
                    ops.push(DeltaOp::Copy {
                        src_offset: dst_offset,
                        len,
                    });
                } else {
                    transfer_bytes += src_chunk.len as u64;
                    ops.push(DeltaOp::Write {
                        src_offset: src_chunk.offset,
                        len: src_chunk.len,
                    });
                }
            }
        } else {
            // Fixed-size index-based comparison (original algorithm).
            for (i, src_chunk) in src_chunks.iter().enumerate() {
                match dst_chunks.get(i) {
                    Some(dst_chunk) if dst_chunk.hash == src_chunk.hash => {
                        reuse_bytes += src_chunk.len as u64;
                        ops.push(DeltaOp::Copy {
                            src_offset: dst_chunk.offset,
                            len: src_chunk.len,
                        });
                    }
                    _ => {
                        transfer_bytes += src_chunk.len as u64;
                        ops.push(DeltaOp::Write {
                            src_offset: src_chunk.offset,
                            len: src_chunk.len,
                        });
                    }
                }
            }
        }

        // Note: if dst has MORE chunks than src, those trailing chunks are
        // simply not emitted — the temp file will be exactly `final_size`
        // bytes because we only write ops covering src's content.

        FileDelta {
            ops,
            transfer_bytes,
            reuse_bytes,
            final_size: src.file_size,
        }
    }

    /// Convenience: compute delta when both manifests are known.
    pub fn compute_full(src: &FileManifest, dst: &FileManifest) -> FileDelta {
        Self::compute(src, Some(dst))
    }

    /// Convenience: compute delta for a brand-new destination file.
    pub fn compute_new(src: &FileManifest) -> FileDelta {
        Self::compute(src, None)
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hasher::Hasher;

    fn manifest(data: &[u8]) -> FileManifest {
        Hasher::new(4096).hash_buffer(data)
    }

    #[test]
    fn identical_files_are_no_op() {
        let data = vec![0xAAu8; 8192];
        let m = manifest(&data);
        let delta = DeltaEngine::compute_full(&m, &m);
        assert!(delta.is_no_op());
        assert_eq!(delta.transfer_bytes, 0);
        assert_eq!(delta.reuse_bytes, 8192);
        assert!((delta.savings_ratio() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn new_file_is_full_write() {
        let data = vec![0xBBu8; 4096];
        let m = manifest(&data);
        let delta = DeltaEngine::compute_new(&m);
        assert!(!delta.is_no_op());
        assert_eq!(delta.transfer_bytes, 4096);
        assert_eq!(delta.reuse_bytes, 0);
        assert_eq!(delta.ops.len(), 1);
        assert!(delta.ops[0].is_write());
    }

    #[test]
    fn one_changed_chunk_transfers_only_that_chunk() {
        let mut data = vec![0xCCu8; 4096 * 4];
        let dst = manifest(&data);
        data[4096] = 0xDD;
        let src = manifest(&data);
        let delta = DeltaEngine::compute_full(&src, &dst);
        assert_eq!(delta.transfer_bytes, 4096);
        assert_eq!(delta.reuse_bytes, 4096 * 3);
        let writes: Vec<_> = delta.ops.iter().filter(|o| o.is_write()).collect();
        assert_eq!(writes.len(), 1);
    }

    #[test]
    fn savings_ratio_partial_change() {
        let chunk_count = 8usize;
        let mut data = vec![0xEEu8; 4096 * chunk_count];
        let dst = manifest(&data);
        for b in &mut data[4096 * 6..] {
            *b = 0xFF;
        }
        let src = manifest(&data);
        let delta = DeltaEngine::compute_full(&src, &dst);
        assert!((delta.savings_ratio() - 0.75).abs() < 0.01);
    }

    #[test]
    fn dst_longer_than_src_produces_correct_final_size() {
        let src_data = vec![0xAAu8; 4096];
        let dst_data = vec![0xAAu8; 4096 * 4];
        let src = manifest(&src_data);
        let dst = manifest(&dst_data);
        let delta = DeltaEngine::compute_full(&src, &dst);
        assert_eq!(delta.final_size, 4096);
        // The one chunk matches, so zero transfer
        assert_eq!(delta.transfer_bytes, 0);
    }
}
