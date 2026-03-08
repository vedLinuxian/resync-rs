//! Content-Defined Chunking (CDC) via FastCDC with Gear rolling hash.
//!
//! Fixed-size chunking suffers from the "shift-byte problem": inserting or
//! deleting a single byte at the start of a file shifts every chunk boundary,
//! making **all** chunk hashes change and forcing full retransmission.
//!
//! FastCDC solves this by deriving chunk boundaries from the **content itself**
//! using a Gear rolling hash.  Boundaries naturally align to data patterns, so
//! insertions and deletions only affect nearby chunks — the rest of the file
//! keeps its existing chunk hashes.
//!
//! This implementation follows the FastCDC paper's *normalized chunking*
//! approach with two discriminating masks (`mask_s` and `mask_l`) to produce a
//! tighter chunk-size distribution around the target size.
//!
//! # References
//!
//! - Wen Xia et al., "FastCDC: a Fast and Efficient Content-Defined Chunking
//!   Approach for Data Deduplication", USENIX ATC 2016.

use crate::hasher::{ChunkMeta, Hash256};

// ─── Gear hash lookup table ─────────────────────────────────────────────────

/// Pre-computed 64-bit random values for the Gear rolling hash, one per byte
/// value (0..=255).  Generated with a deterministic splitmix64 PRNG so that
/// every build produces identical tables and chunk boundaries are reproducible
/// across machines.
///
/// We use splitmix64 (Vigna, 2015) rather than plain xorshift64 because the
/// latter has weak lower bits, which directly degrades Gear-hash mask checks.
///
/// Seed: `0x123456789ABCDEF0`
const GEAR_TABLE: [u64; 256] = {
    let mut table = [0u64; 256];
    let mut state: u64 = 0x123456789ABCDEF0;
    let mut i = 0usize;
    while i < 256 {
        // splitmix64 step
        state = state.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^= z >> 31;
        table[i] = z;
        i += 1;
    }
    table
};

// ─── Mask helpers ────────────────────────────────────────────────────────────

/// Build a bitmask with `bits` low bits set.
///
/// E.g. `mask_bits(13)` → `0x0000_0000_0000_1FFF`.
const fn mask_bits(bits: u32) -> u64 {
    if bits >= 64 {
        u64::MAX
    } else {
        (1u64 << bits) - 1
    }
}

/// Number of bits corresponding to `2^n ≈ target`.
///
/// This is just `floor(log2(target))`.
const fn log2_floor(target: usize) -> u32 {
    if target == 0 {
        return 0;
    }
    (usize::BITS - 1) - (target.leading_zeros())
}

// ─── CdcChunker ──────────────────────────────────────────────────────────────

/// A Content-Defined Chunker using Gear-hash rolling hash with two-threshold
/// normalized chunking (FastCDC).
///
/// # Chunk-size distribution
///
/// The two-mask approach biases chunk sizes toward `target_size`:
///
/// | Position in window              | Mask used | Effect                      |
/// |---------------------------------|-----------|-----------------------------|
/// | `min_size .. target_size`       | `mask_s`  | Harder to match → grow      |
/// | `target_size .. max_size`       | `mask_l`  | Easier to match → cut soon  |
/// | `>= max_size`                   | —         | Forced cut                  |
///
/// `mask_s` has **more** bits set (higher bar), `mask_l` has **fewer** bits set
/// (lower bar).  The paper recommends `bits_s = bits + 2` and
/// `bits_l = bits - 2` where `bits = log2(target_size)`.
#[derive(Debug, Clone)]
pub struct CdcChunker {
    /// Minimum chunk size — no cut point is considered before this offset.
    min_size: usize,
    /// Maximum chunk size — a cut is forced here if no boundary was found.
    max_size: usize,
    /// Target (average) chunk size.
    target_size: usize,
    /// "Small" mask (more bits → harder to match) used for offsets in
    /// `[min_size, target_size)`.
    mask_s: u64,
    /// "Large" mask (fewer bits → easier to match) used for offsets in
    /// `[target_size, max_size)`.
    mask_l: u64,
}

impl CdcChunker {
    /// Create a new `CdcChunker`.
    ///
    /// # Arguments
    ///
    /// * `min_size`    — minimum chunk size in bytes (typically `target / 4`).
    /// * `max_size`    — maximum chunk size in bytes (typically `target * 4`).
    /// * `target_size` — desired average chunk size in bytes.
    ///
    /// # Panics
    ///
    /// Panics if `min_size >= target_size` or `target_size >= max_size` or
    /// `min_size < 64`.
    pub fn new(min_size: usize, max_size: usize, target_size: usize) -> Self {
        assert!(min_size >= 64, "min_size must be >= 64");
        assert!(
            min_size < target_size,
            "min_size ({min_size}) must be < target_size ({target_size})"
        );
        assert!(
            target_size < max_size,
            "target_size ({target_size}) must be < max_size ({max_size})"
        );

        let bits = log2_floor(target_size);
        // FastCDC paper: mask_s uses (bits + 2) bits, mask_l uses (bits - 2) bits.
        // Clamp to sensible range.
        let bits_s = (bits + 2).min(63);
        let bits_l = bits.saturating_sub(2).max(1);

        Self {
            min_size,
            max_size,
            target_size,
            mask_s: mask_bits(bits_s),
            mask_l: mask_bits(bits_l),
        }
    }

    /// Convenience constructor deriving min/max from a target chunk size.
    ///
    /// * `min  = target / 4`
    /// * `max  = target * 4`
    ///
    /// # Panics
    ///
    /// Panics if `target < 256` (which would make min < 64).
    pub fn from_target(target_size: usize) -> Self {
        assert!(
            target_size >= 256,
            "target_size must be >= 256 (got {target_size})"
        );
        Self::new(target_size / 4, target_size * 4, target_size)
    }

    /// Chunk `data` into content-defined pieces using the Gear rolling hash.
    ///
    /// Returns a `Vec` of `(offset, length)` pairs describing each chunk's
    /// position within `data`.  The chunks partition `data` exactly — they are
    /// contiguous and non-overlapping, and their lengths sum to `data.len()`.
    ///
    /// # Algorithm outline
    ///
    /// 0. **Pre-roll** the Gear hash through `[0, min_size)` *without*
    ///    checking for boundaries.  This builds up enough rolling-hash
    ///    state that the hash at `min_size` is fully determined by the
    ///    chunk's content — any carry error from a byte insertion earlier
    ///    in the file has been right-shifted out after `min_size` steps
    ///    (min_size ≥ 64).
    /// 1. For each byte in `[min_size, target_size)`, update the Gear hash:
    ///    `hash = (hash >> 1) + GEAR_TABLE[byte]`
    ///    and test `hash & mask_s == 0` (strict mask, biases toward target).
    /// 2. For each byte in `[target_size, max_size)`, test
    ///    `hash & mask_l == 0` (relaxed mask, cuts sooner).
    /// 3. At `max_size`, force a cut.
    /// 4. Record the chunk and advance.
    pub fn chunk_data(&self, data: &[u8]) -> Vec<(usize, usize)> {
        if data.is_empty() {
            return Vec::new();
        }

        let data_len = data.len();
        let mut chunks = Vec::with_capacity(data_len / self.target_size + 1);
        let mut offset = 0usize;

        while offset < data_len {
            let remaining = data_len - offset;

            // If the remaining data is <= min_size, emit it as the final chunk.
            if remaining <= self.min_size {
                chunks.push((offset, remaining));
                break;
            }

            let chunk_end = self.find_boundary(&data[offset..]);
            chunks.push((offset, chunk_end));
            offset += chunk_end;
        }

        chunks
    }

    /// Find the next chunk boundary within `buf`, returning the chunk length.
    ///
    /// `buf` starts at the beginning of the prospective chunk.  The returned
    /// length is in `[min_size, max_size]` (clamped to `buf.len()` if the
    /// buffer is shorter than `max_size`).
    #[inline]
    fn find_boundary(&self, buf: &[u8]) -> usize {
        let buf_len = buf.len();
        let max = self.max_size.min(buf_len);

        // If the buffer is too short for even a min-size chunk, take it all.
        if buf_len <= self.min_size {
            return buf_len;
        }

        let mut hash: u64 = 0;
        let target = self.target_size.min(max);

        // Phase 0: pre-roll the hash through [0, min_size).
        //
        // No boundary can be placed here, but feeding these bytes into the
        // Gear hash builds up enough state that the hash at position
        // `min_size` is fully content-determined.  After `min_size` steps
        // of right-shifting (min_size ≥ 64), any carry error from a byte
        // insert or delete earlier in the file has decayed to exactly zero
        // — giving perfect shift-byte resilience for subsequent chunks.
        for pos in 0..self.min_size {
            hash = (hash >> 1).wrapping_add(GEAR_TABLE[buf[pos] as usize]);
        }

        // Phase 1: scan [min_size .. target_size) with the stricter mask_s.
        let mut pos = self.min_size;
        while pos < target {
            hash = (hash >> 1).wrapping_add(GEAR_TABLE[buf[pos] as usize]);
            if (hash & self.mask_s) == 0 {
                return pos;
            }
            pos += 1;
        }

        // Phase 2: scan [target_size .. max_size) with the relaxed mask_l.
        while pos < max {
            hash = (hash >> 1).wrapping_add(GEAR_TABLE[buf[pos] as usize]);
            if (hash & self.mask_l) == 0 {
                return pos;
            }
            pos += 1;
        }

        // Phase 3: forced cut at max_size (or end of buffer).
        max
    }

    /// Chunk `data` and compute a BLAKE3 hash for each chunk.
    ///
    /// Returns a `Vec<ChunkMeta>` ready to be stored in a [`FileManifest`].
    /// Each chunk receives a sequential `index` starting from 0.
    ///
    /// [`FileManifest`]: crate::hasher::FileManifest
    pub fn chunk_and_hash(&self, data: &[u8]) -> Vec<ChunkMeta> {
        let boundaries = self.chunk_data(data);

        boundaries
            .into_iter()
            .enumerate()
            .map(|(idx, (offset, len))| {
                let hash: Hash256 = *blake3::hash(&data[offset..offset + len]).as_bytes();
                ChunkMeta {
                    index: idx as u64,
                    offset: offset as u64,
                    len,
                    hash,
                }
            })
            .collect()
    }

    /// Chunk `data` and compute BLAKE3 hashes **in parallel** using rayon.
    ///
    /// Chunking itself is sequential (the rolling hash is inherently serial),
    /// but the BLAKE3 hashing of each chunk is embarrassingly parallel and
    /// dominates wall-clock time for large files.
    ///
    /// PERF FIX: Previously each chunk was a separate rayon work item.  With
    /// ~8 KB target chunks, rayon scheduling overhead (1-5 μs) dwarfed the
    /// hashing time (1-2 μs) — overhead was up to 250% of useful work.
    /// Now batches 128 chunks per rayon task (~1 MB), reducing overhead ~128×.
    /// Falls back to sequential for ≤ 128 chunks (not worth rayon setup).
    pub fn chunk_and_hash_parallel(&self, data: &[u8]) -> Vec<ChunkMeta> {
        use rayon::prelude::*;

        let boundaries = self.chunk_data(data);

        const BATCH_SIZE: usize = 128;

        if boundaries.len() <= BATCH_SIZE {
            // Sequential — rayon setup cost exceeds benefit for few chunks
            boundaries
                .into_iter()
                .enumerate()
                .map(|(idx, (offset, len))| {
                    let hash: Hash256 = *blake3::hash(&data[offset..offset + len]).as_bytes();
                    ChunkMeta {
                        index: idx as u64,
                        offset: offset as u64,
                        len,
                        hash,
                    }
                })
                .collect()
        } else {
            // Parallel with batching — 128 chunks per rayon work item
            let indexed: Vec<(usize, (usize, usize))> =
                boundaries.into_iter().enumerate().collect();
            indexed
                .par_chunks(BATCH_SIZE)
                .flat_map_iter(|batch| {
                    batch.iter().map(|&(idx, (offset, len))| {
                        let hash: Hash256 = *blake3::hash(&data[offset..offset + len]).as_bytes();
                        ChunkMeta {
                            index: idx as u64,
                            offset: offset as u64,
                            len,
                            hash,
                        }
                    })
                })
                .collect()
        }
    }

    // ── Accessors ────────────────────────────────────────────────────────

    /// Minimum chunk size.
    #[inline]
    pub fn min_size(&self) -> usize {
        self.min_size
    }

    /// Maximum chunk size.
    #[inline]
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Target (average) chunk size.
    #[inline]
    pub fn target_size(&self) -> usize {
        self.target_size
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Helpers ──────────────────────────────────────────────────────────

    fn default_chunker() -> CdcChunker {
        CdcChunker::from_target(4096)
    }

    // ── GEAR_TABLE sanity ───────────────────────────────────────────────

    #[test]
    fn gear_table_is_deterministic() {
        // Recompute the first and last entries by hand using splitmix64.
        let mut state: u64 = 0x123456789ABCDEF0;

        // Index 0
        state = state.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^= z >> 31;
        assert_eq!(GEAR_TABLE[0], z, "GEAR_TABLE[0] mismatch");

        // Advance to index 255.
        for _ in 1..256 {
            state = state.wrapping_add(0x9E3779B97F4A7C15);
            z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
            z ^= z >> 31;
        }
        assert_eq!(GEAR_TABLE[255], z, "GEAR_TABLE[255] mismatch");
    }

    #[test]
    fn gear_table_has_no_zero_entries() {
        // A zero in the Gear table would be degenerate — the rolling hash
        // would ignore that byte value entirely.
        for (i, &val) in GEAR_TABLE.iter().enumerate() {
            assert_ne!(val, 0, "GEAR_TABLE[{i}] is zero");
        }
    }

    // ── Basic chunking properties ───────────────────────────────────────

    #[test]
    fn empty_input_yields_no_chunks() {
        let chunks = default_chunker().chunk_data(&[]);
        assert!(chunks.is_empty());
    }

    #[test]
    fn small_input_yields_single_chunk() {
        let data = vec![0xAA; 100];
        let chunks = default_chunker().chunk_data(&data);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], (0, 100));
    }

    #[test]
    fn chunks_partition_data_exactly() {
        let data: Vec<u8> = (0..100_000).map(|i| (i % 251) as u8).collect();
        let chunker = default_chunker();
        let chunks = chunker.chunk_data(&data);

        // Contiguous and non-overlapping.
        let mut expected_offset = 0usize;
        for &(off, len) in &chunks {
            assert_eq!(off, expected_offset, "gap or overlap at offset {off}");
            assert!(len > 0, "zero-length chunk");
            expected_offset += len;
        }
        assert_eq!(expected_offset, data.len(), "chunks don't cover all data");
    }

    #[test]
    fn all_chunks_respect_min_size_except_last() {
        let data: Vec<u8> = (0..200_000).map(|i| (i * 7 % 256) as u8).collect();
        let chunker = default_chunker();
        let chunks = chunker.chunk_data(&data);

        for (i, &(_off, len)) in chunks.iter().enumerate() {
            if i < chunks.len() - 1 {
                assert!(
                    len >= chunker.min_size(),
                    "chunk {i} length {len} < min_size {}",
                    chunker.min_size()
                );
            }
        }
    }

    #[test]
    fn all_chunks_respect_max_size() {
        let data: Vec<u8> = (0..500_000).map(|i| (i * 13 % 256) as u8).collect();
        let chunker = default_chunker();
        let chunks = chunker.chunk_data(&data);

        for (i, &(_off, len)) in chunks.iter().enumerate() {
            assert!(
                len <= chunker.max_size(),
                "chunk {i} length {len} > max_size {}",
                chunker.max_size()
            );
        }
    }

    // ── Determinism ─────────────────────────────────────────────────────

    #[test]
    fn chunking_is_deterministic() {
        let data: Vec<u8> = (0..80_000).map(|i| (i % 199) as u8).collect();
        let chunker = default_chunker();
        let a = chunker.chunk_data(&data);
        let b = chunker.chunk_data(&data);
        assert_eq!(a, b, "two runs produced different boundaries");
    }

    // ── Shift-byte resilience ───────────────────────────────────────────

    #[test]
    fn single_byte_insert_preserves_most_chunks() {
        // This is the core value proposition of CDC: inserting a byte near the
        // start should only affect 1-2 chunks, not the entire file.
        //
        // Use splitmix64-generated data so the content is non-periodic; with
        // periodic data every middle chunk has identical bytes, collapsing the
        // hash-set and giving a misleading preservation count.
        let mut prng_state: u64 = 0xDEADBEEFCAFEBABE;
        let original: Vec<u8> = (0..200_000)
            .map(|_| {
                prng_state = prng_state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (prng_state >> 56) as u8
            })
            .collect();
        let chunker = CdcChunker::from_target(8192);

        let chunks_orig = chunker.chunk_and_hash(&original);

        // Insert one byte at position 100.
        let mut modified = original.clone();
        modified.insert(100, 0xFF);

        let chunks_mod = chunker.chunk_and_hash(&modified);

        // Positional (index-by-index) comparison: after the first boundary
        // shifts by 1 byte, every subsequent chunk should have identical
        // content and thus identical BLAKE3 hash.  This avoids the pitfall
        // of set-based comparison with periodic data.
        let positional_match = chunks_orig
            .iter()
            .zip(chunks_mod.iter())
            .filter(|(o, m)| o.hash == m.hash)
            .count();
        let total = chunks_orig.len();

        // We expect the vast majority (> 80%) of chunks to survive unchanged.
        // Typically only chunk 0 (containing the insertion) will differ.
        let ratio = positional_match as f64 / total as f64;
        assert!(
            ratio > 0.80,
            "only {positional_match}/{total} chunks preserved ({:.1}%) — CDC isn't working",
            ratio * 100.0
        );
    }

    // ── chunk_and_hash correctness ──────────────────────────────────────

    #[test]
    fn chunk_and_hash_matches_manual_blake3() {
        let data: Vec<u8> = (0..50_000).map(|i| (i % 173) as u8).collect();
        let chunker = default_chunker();
        let meta = chunker.chunk_and_hash(&data);

        for cm in &meta {
            let expected =
                *blake3::hash(&data[cm.offset as usize..cm.offset as usize + cm.len]).as_bytes();
            assert_eq!(cm.hash, expected, "hash mismatch at chunk {}", cm.index);
        }
    }

    #[test]
    fn chunk_and_hash_parallel_matches_sequential() {
        let data: Vec<u8> = (0..120_000).map(|i| (i * 11 % 256) as u8).collect();
        let chunker = default_chunker();
        let seq = chunker.chunk_and_hash(&data);
        let par = chunker.chunk_and_hash_parallel(&data);
        assert_eq!(seq, par, "parallel and sequential results differ");
    }

    // ── Edge cases ──────────────────────────────────────────────────────

    #[test]
    fn data_exactly_min_size() {
        let chunker = CdcChunker::from_target(1024);
        let data = vec![0xBB; chunker.min_size()];
        let chunks = chunker.chunk_data(&data);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].1, data.len());
    }

    #[test]
    fn data_exactly_max_size() {
        let chunker = CdcChunker::from_target(1024);
        let data = vec![0xCC; chunker.max_size()];
        let chunks = chunker.chunk_data(&data);
        // Should be 1 chunk (forced cut at max_size == data.len()).
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].1, data.len());
    }

    #[test]
    fn all_same_bytes() {
        // Pathological input: all bytes identical → Gear hash is periodic.
        // Chunks should still respect min/max bounds.
        let data = vec![0x00; 100_000];
        let chunker = default_chunker();
        let chunks = chunker.chunk_data(&data);
        for &(_off, len) in &chunks {
            assert!(len <= chunker.max_size());
        }
    }

    // ── Constructor validation ──────────────────────────────────────────

    #[test]
    #[should_panic(expected = "min_size")]
    fn rejects_tiny_min_size() {
        CdcChunker::new(32, 8192, 2048);
    }

    #[test]
    #[should_panic(expected = "min_size")]
    fn rejects_min_gte_target() {
        CdcChunker::new(4096, 16384, 4096);
    }

    #[test]
    #[should_panic(expected = "target_size")]
    fn rejects_target_gte_max() {
        CdcChunker::new(1024, 4096, 4096);
    }

    // ── Mask construction ───────────────────────────────────────────────

    #[test]
    fn masks_have_expected_relationship() {
        let chunker = default_chunker();
        // mask_s should have more bits set (stricter) than mask_l.
        assert!(
            chunker.mask_s.count_ones() > chunker.mask_l.count_ones(),
            "mask_s ({:#x}) should have more bits than mask_l ({:#x})",
            chunker.mask_s,
            chunker.mask_l,
        );
    }
}
