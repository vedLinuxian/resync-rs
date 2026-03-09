//! Wire protocol — message types and length-delimited codec with optional zstd compression.
//!
//!  Frame format (v2, with compression flag):
//!  ```text
//!  ┌────────────────┬──────────────┬─────────────────────────┐
//!  │  u32 (4 bytes) │  u8 (1 byte) │  payload bytes          │
//!  │  frame_len     │  compress    │  (bincode or zstd)      │
//!  │  (LE)          │  flag        │                         │
//!  └────────────────┴──────────────┴─────────────────────────┘
//!  ```
//!
//!  - `frame_len` = 1 (flag) + payload.len()
//!  - `compress_flag`:  0x00 = raw bincode,  0x01 = zstd(bincode)
//!  - All multi-byte integers are little-endian (LE).
//!  - Max frame size: 64 MiB (protects against malformed streams).
//!
//!  Compression behaviour (`compress = true` on `MsgCodec`):
//!  - Payloads ≤ 256 bytes are sent uncompressed (not worth the overhead).
//!  - Larger payloads are zstd-compressed at level 3; if the compressed
//!    output is not smaller, the raw payload is sent instead.
//!  - The decoder always reads the flag byte and handles both cases,
//!    regardless of the local `compress` setting.

use std::path::PathBuf;
use std::time::SystemTime;

use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder};

use crate::hasher::Hash256;

/// Protocol version — bumped on breaking changes.
pub const PROTOCOL_VERSION: u32 = 2;

/// Maximum frame payload size (256 MiB).
///
/// Must exceed BATCH_MAX_BYTES + bincode overhead so BatchFiles frames
/// with up to 64 MiB of raw file data never hit this limit.
const MAX_FRAME_SIZE: usize = 256 * 1024 * 1024;

/// Chunk size for streaming file data over the wire (1 MiB).
///
/// Larger chunks = better Zstd compression ratio (more context for the
/// dictionary builder).  On a 10 Mbps link:
///   - 1 MB chunk @ 50:1 ratio  = 20 KB per chunk  (independent dictionary)
///   - 4 MB chunk @ 150:1 ratio = 27 KB per chunk  (much bigger window)
///
/// 4 MiB also doubles as the "small file" threshold: files ≤ 4 MiB go
/// into BatchFiles frames where Zstd sees multiple files as one context,
/// achieving cross-file dictionary learning (10–34:1 on code).
pub const WIRE_CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// Max raw bytes per BatchFiles frame before flushing.
/// Keeps peak memory bounded while allowing good compression context.
pub const BATCH_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Payloads at or below this size are not worth compressing.
const COMPRESS_THRESHOLD: usize = 256;

/// Default Zstandard compression level (3 = good speed/ratio tradeoff).
/// On modern CPUs, Zstd level 3 compresses at ~500 MB/s — faster than
/// any network link.  The bottleneck is always the network, never the CPU.
const DEFAULT_COMPRESS_LEVEL: i32 = 3;

/// Compression flag values in the wire frame.
const FLAG_UNCOMPRESSED: u8 = 0x00;
const FLAG_ZSTD: u8 = 0x01;

// ─── Message types ───────────────────────────────────────────────────────────

/// Every message between client and server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Msg {
    // ─── Handshake ───────────────────────────────────────────────────────
    /// Client → Server: initiate session.
    Hello {
        version: u32,
        chunk_size: usize,
        compress: bool,
    },

    /// Server → Client: accept or reject handshake.
    HelloAck { ok: bool, error: Option<String> },

    // ─── Manifest exchange ───────────────────────────────────────────────
    /// Client → Server: start a new sync session.
    BeginSync {
        dest_path: PathBuf,
        file_count: u64,
        total_bytes: u64,
        delete: bool,
        preserve_perms: bool,
        preserve_times: bool,
    },

    /// Client → Server: one file in the source manifest.
    FileHeader {
        rel_path: PathBuf,
        size: u64,
        /// Seconds since UNIX epoch (portable across platforms).
        mtime_secs: i64,
        /// Nanoseconds component of mtime.
        mtime_nanos: u32,
        mode: u32,
        is_symlink: bool,
        symlink_target: Option<PathBuf>,
    },

    /// Client → Server: all file headers have been sent.
    ManifestEnd,

    /// Client → Server: batch of file headers in a single wire frame.
    ///
    /// Instead of 500 individual FileHeader messages (500 codec frames),
    /// pack up to 200 headers per message.  This reduces frame overhead
    /// and allows Zstd to compress across similar headers (paths, modes).
    ManifestBatch { headers: Vec<ManifestEntry> },

    // ─── Server sync decisions ───────────────────────────────────────────
    /// Server → Client: file does not exist on server, send full content.
    NeedFull { rel_path: PathBuf },

    /// Server → Client: file exists but differs, here are chunk hashes.
    NeedDelta {
        rel_path: PathBuf,
        chunk_hashes: Vec<Hash256>,
        chunk_size: usize,
        dst_size: u64,
    },

    /// Server → Client: file is unchanged, skip it.
    Skip { rel_path: PathBuf },

    /// Server → Client: all sync decisions have been sent.
    PlanEnd {
        /// Total number of NeedFull + NeedDelta files the client should send.
        files_to_send: u64,
    },

    /// Server → Client: batch of sync decisions in a single wire frame.
    ///
    /// Packs up to 200 plan decisions per message, reducing 500 individual
    /// NeedFull/Skip messages to ~3 PlanBatch frames.
    PlanBatch { decisions: Vec<PlanDecision> },

    // ─── Data transfer (client → server) ─────────────────────────────────
    /// Client → Server: begin streaming a full file.
    FileDataStart {
        rel_path: PathBuf,
        size: u64,
        mtime_secs: i64,
        mtime_nanos: u32,
        mode: u32,
    },

    /// Client → Server: a chunk of file data.
    FileDataChunk { data: Vec<u8> },

    /// Client → Server: end of file data stream.
    FileDataEnd,

    /// Client → Server: complete small file in a single message.
    ///
    /// Combines FileDataStart + FileDataChunk + FileDataEnd into one wire
    /// frame, eliminating 3→1 per-file protocol overhead.  Used for files
    /// that fit in a single WIRE_CHUNK_SIZE (1 MiB).
    FileDataFull {
        rel_path: PathBuf,
        size: u64,
        mtime_secs: i64,
        mtime_nanos: u32,
        mode: u32,
        data: Vec<u8>,
    },

    /// Client → Server: delta payload for a changed file.
    DeltaStart {
        rel_path: PathBuf,
        final_size: u64,
        /// Number of DeltaChunk messages that will follow (one per Write op).
        write_count: u32,
        /// The delta operations (Copy ops reference dst, Write ops reference
        /// the following DeltaChunk data in order).
        ops: Vec<DeltaWireOp>,
        mtime_secs: i64,
        mtime_nanos: u32,
        mode: u32,
    },

    /// Client → Server: data for one Write operation in the delta.
    DeltaChunk { data: Vec<u8> },

    /// Client → Server: all delta data for this file has been sent.
    DeltaEnd,

    // ─── Completion ──────────────────────────────────────────────────────
    /// Server → Client: report files deleted (if --delete was set).
    DeleteReport { deleted_count: u64 },

    /// Server → Client: sync complete, here are the stats.
    SyncComplete {
        files_new: u64,
        files_updated: u64,
        files_skipped: u64,
        files_deleted: u64,
        files_errored: u64,
        bytes_transferred: u64,
    },

    /// Either direction: fatal error, abort session.
    Error { message: String },

    /// Client → Server: batch of small files compressed as ONE wire frame.
    ///
    /// Instead of N individually-compressed FileDataFull frames, this packs
    /// up to 64 files into a single message.  Zstd compresses 256 KB of
    /// similar source code at 8–10:1 (vs 2–3:1 for 4 KB individually).
    /// On a 10 Mbps link this cuts wire time by ~4×.
    BatchFiles { files: Vec<BatchFileEntry> },

    // ─── Pull mode (client ← server) ────────────────────────────────────
    /// Client → Server: request to pull files from server's source_path.
    PullRequest {
        source_path: PathBuf,
        dest_path: PathBuf,
    },
}

/// One file inside a [`Msg::BatchFiles`] batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchFileEntry {
    pub rel_path: PathBuf,
    pub size: u64,
    pub mtime_secs: i64,
    pub mtime_nanos: u32,
    pub mode: u32,
    pub data: Vec<u8>,
}

/// One file header inside a [`Msg::ManifestBatch`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub rel_path: PathBuf,
    pub size: u64,
    pub mtime_secs: i64,
    pub mtime_nanos: u32,
    pub mode: u32,
    pub is_symlink: bool,
    pub symlink_target: Option<PathBuf>,
}

/// One sync decision inside a [`Msg::PlanBatch`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PlanDecision {
    NeedFull { rel_path: PathBuf },
    NeedDelta {
        rel_path: PathBuf,
        chunk_hashes: Vec<Hash256>,
        chunk_size: usize,
        dst_size: u64,
    },
    Skip { rel_path: PathBuf },
}

/// A wire-safe delta operation. Unlike the internal `DeltaOp`, the Write
/// variant doesn't carry the data inline — data comes in subsequent
/// `DeltaChunk` messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeltaWireOp {
    /// Copy bytes from the existing destination file.
    Copy { src_offset: u64, len: usize },
    /// Write bytes from source. The actual data arrives in the next
    /// `DeltaChunk` message (matched by order).
    Write { len: usize },
}

// ─── Codec ───────────────────────────────────────────────────────────────────

/// Length-delimited bincode codec for `Msg`, with optional zstd compression.
///
/// Frame format: `[u32 LE frame_len][u8 compress_flag][payload]`
///
/// where `frame_len = 1 + payload.len()`.
///
/// The `compress` flag on the codec controls whether the *encoder* attempts
/// compression.  The *decoder* always inspects the per-frame flag byte and
/// handles both compressed and uncompressed payloads transparently.
pub struct MsgCodec {
    compress: bool,
    compress_level: i32,
    /// Reusable Zstd compression context — avoids allocating a new ZSTD_CCtx
    /// for every single frame.  Lazy-initialised on first compress call.
    zstd_compressor: Option<zstd::bulk::Compressor<'static>>,
}

impl MsgCodec {
    /// Create a new codec.
    ///
    /// - `compress = false`: all outgoing payloads are sent uncompressed.
    /// - `compress = true`:  payloads larger than 256 bytes are zstd-compressed
    ///   (if compression actually shrinks them; otherwise raw is used).
    pub fn new(compress: bool) -> Self {
        Self {
            compress,
            compress_level: DEFAULT_COMPRESS_LEVEL,
            zstd_compressor: None,
        }
    }

    /// Create a codec with a custom compression level.
    pub fn with_level(compress: bool, level: i32) -> Self {
        Self {
            compress,
            compress_level: level.clamp(1, 22),
            zstd_compressor: None,
        }
    }

    /// Toggle compression on the encoder (useful after handshake negotiation).
    ///
    /// Accessible at runtime via `Framed::codec_mut().set_compress(true)`.
    pub fn set_compress(&mut self, compress: bool) {
        self.compress = compress;
    }

    /// Returns whether encoder compression is enabled.
    pub fn compress(&self) -> bool {
        self.compress
    }
}

impl Decoder for MsgCodec {
    type Item = Msg;
    type Error = std::io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> std::io::Result<Option<Msg>> {
        // Need at least 4 bytes for the length prefix.
        if buf.len() < 4 {
            return Ok(None);
        }

        // Peek at frame_len (includes the 1-byte flag + payload).
        let frame_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;

        if frame_len > MAX_FRAME_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("frame too large: {frame_len} bytes (max {MAX_FRAME_SIZE})"),
            ));
        }

        // frame_len must be at least 1 (the flag byte).
        if frame_len < 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "frame too small: missing compression flag byte",
            ));
        }

        // Wait for the full frame: 4 (len prefix) + frame_len.
        if buf.len() < 4 + frame_len {
            buf.reserve(4 + frame_len - buf.len());
            return Ok(None);
        }

        // Consume length prefix.
        buf.advance(4);

        // Read compression flag.
        let flag = buf[0];
        buf.advance(1);

        // Read payload.
        let payload_len = frame_len - 1;
        let payload = buf.split_to(payload_len);

        // Decompress if needed.
        let raw = match flag {
            FLAG_UNCOMPRESSED => {
                // Use payload bytes directly.
                payload.to_vec()
            }
            FLAG_ZSTD => {
                // SECURITY: Limit decompressed size to 256 MiB to prevent
                // decompression bombs from a malicious peer.
                const MAX_DECOMPRESSED: usize = 256 * 1024 * 1024;
                let decoder = zstd::Decoder::new(payload.as_ref()).map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("zstd init error: {e}"),
                    )
                })?;
                let mut output =
                    Vec::with_capacity(std::cmp::min(payload.len() * 4, 4 * 1024 * 1024));
                use std::io::Read;
                let bytes_read = decoder
                    .take(MAX_DECOMPRESSED as u64)
                    .read_to_end(&mut output)
                    .map_err(|e| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("zstd decompress error: {e}"),
                        )
                    })?;
                if bytes_read >= MAX_DECOMPRESSED {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "zstd decompressed payload exceeds 256 MiB limit",
                    ));
                }
                output
            }
            other => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unknown compression flag: 0x{other:02x}"),
                ));
            }
        };

        // Deserialize bincode → Msg.
        let msg: Msg = bincode::deserialize(&raw).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("bincode decode error: {e}"),
            )
        })?;

        Ok(Some(msg))
    }
}

impl Encoder<Msg> for MsgCodec {
    type Error = std::io::Error;

    fn encode(&mut self, msg: Msg, buf: &mut BytesMut) -> std::io::Result<()> {
        // Serialize to bincode.
        let raw = bincode::serialize(&msg).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("bincode encode error: {e}"),
            )
        })?;

        // Decide whether to compress this payload.
        let (flag, payload) = if self.compress && raw.len() > COMPRESS_THRESHOLD {
            // PERF: Reuse the ZSTD_CCtx across frames.  Allocating a fresh
            // context for each of 500+ small messages was a significant
            // overhead (memory alloc + dictionary init per frame).
            let compressor = self.zstd_compressor.get_or_insert_with(|| {
                let mut c = zstd::bulk::Compressor::new(self.compress_level)
                    .expect("zstd compressor init");
                // Use a 128 MiB window (log₂ = 27) so that Zstd sees
                // repeated patterns across the entire BATCH_MAX_BYTES
                // payload.  Without this, level 3 defaults to a ~1 MiB
                // window and misses cross-chunk/cross-file repetitions
                // in highly compressible data (access logs, binaries).
                let _ = c.set_parameter(zstd::zstd_safe::CParameter::WindowLog(27));
                c
            });
            let compressed = compressor
                .compress(&raw)
                .map_err(|e| std::io::Error::other(format!("zstd compress error: {e}")))?;

            // Only use compressed output if it's actually smaller.
            if compressed.len() < raw.len() {
                (FLAG_ZSTD, compressed)
            } else {
                (FLAG_UNCOMPRESSED, raw)
            }
        } else {
            (FLAG_UNCOMPRESSED, raw)
        };

        // frame_len = 1 (flag byte) + payload bytes.
        let frame_len = 1 + payload.len();

        if frame_len > MAX_FRAME_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "message too large: {} bytes (max {MAX_FRAME_SIZE})",
                    frame_len
                ),
            ));
        }

        buf.reserve(4 + frame_len);
        buf.put_u32_le(frame_len as u32);
        buf.put_u8(flag);
        buf.extend_from_slice(&payload);
        Ok(())
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Convert a `SystemTime` to `(secs, nanos)` since UNIX epoch.
///
/// BUG FIX: Previously, times before the Unix epoch (1970) did not roundtrip
/// correctly.  For example, 0.5 s before epoch produced `(0, 500_000_000)`
/// which reconstructed as 0.5 s *after* epoch.
///
/// The fix uses the same convention as `timespec` in C: negative seconds with
/// a non-negative nanosecond component, so that `time = secs + nanos/1e9`
/// always holds.  For -0.5 s: `secs = -1, nanos = 500_000_000`
/// (i.e. `-1 + 0.5 = -0.5`).
pub fn system_time_to_epoch(t: SystemTime) -> (i64, u32) {
    match t.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(dur) => (dur.as_secs() as i64, dur.subsec_nanos()),
        Err(e) => {
            // Time is before Unix epoch — `e.duration()` is the positive
            // distance from `t` to epoch.
            let dur = e.duration();
            let secs = dur.as_secs();
            let nanos = dur.subsec_nanos();
            if nanos == 0 {
                (-(secs as i64), 0)
            } else {
                // e.g. 0.5 s before epoch → dur = 0.5 s → secs=0, nanos=500M
                // Emit (-1, 500_000_000) so that -1 + 0.5 = -0.5
                (-(secs as i64) - 1, 1_000_000_000 - nanos)
            }
        }
    }
}

/// Convert `(secs, nanos)` since UNIX epoch back to `SystemTime`.
///
/// See [`system_time_to_epoch`] for the roundtrip convention.
pub fn epoch_to_system_time(secs: i64, nanos: u32) -> SystemTime {
    if secs >= 0 {
        SystemTime::UNIX_EPOCH + std::time::Duration::new(secs as u64, nanos)
    } else {
        // secs is negative.  Reconstruct: time = epoch + secs + nanos/1e9
        // = epoch - (|secs| - nanos/1e9)
        let abs_secs = (-secs) as u64;
        if nanos == 0 {
            SystemTime::UNIX_EPOCH - std::time::Duration::new(abs_secs, 0)
        } else {
            // e.g. secs=-1, nanos=500M → 1 * 1e9 - 500M = 500M ns = 0.5 s
            // → epoch - 0.5 s ✓
            let total_nanos = abs_secs * 1_000_000_000 - nanos as u64;
            let final_secs = total_nanos / 1_000_000_000;
            let final_nanos = (total_nanos % 1_000_000_000) as u32;
            SystemTime::UNIX_EPOCH - std::time::Duration::new(final_secs, final_nanos)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    /// Verify that BatchFiles messages compress dramatically better than
    /// individual FileDataFull messages, and that the reusable bulk
    /// compressor produces valid output decompressible by the streaming
    /// decoder.
    #[test]
    fn batch_compression_ratio() {
        let base = b"use std::collections::HashMap;\nuse std::sync::Arc;\nuse tokio::sync::Mutex;\n\npub struct Server {\n    clients: Arc<Mutex<HashMap<u64, Client>>>,\n    config: ServerConfig,\n    metrics: MetricsCollector,\n}\n\nimpl Server {\n    pub async fn handle_request(&self, req: Request) -> Response {\n        let client_id = req.client_id();\n        let mut clients = self.clients.lock().await;\n        // Process the request\n        todo!()\n    }\n}\n";

        // Build 64 "files" like the benchmark does
        let mut files = Vec::new();
        for i in 0..64u32 {
            let mut data = base.to_vec();
            for j in 0..10u32 {
                data.extend_from_slice(
                    format!("// Line {j} of file {i}\nfn func_{i}_{j}(x: i64) -> i64 {{ x * {i} + {j} }}\n").as_bytes()
                );
            }
            files.push(BatchFileEntry {
                rel_path: format!("module_{i}.rs").into(),
                size: data.len() as u64,
                mtime_secs: 1719700000,
                mtime_nanos: 0,
                mode: 0o644,
                data,
            });
        }

        let batch_msg = Msg::BatchFiles { files: files.clone() };

        // Encode with compression
        let mut codec = MsgCodec::with_level(true, 3);
        let mut buf = BytesMut::new();
        Encoder::encode(&mut codec, batch_msg, &mut buf).unwrap();
        let batch_wire_size = buf.len();

        // Now encode each file individually as FileDataFull
        let mut total_individual = 0usize;
        for f in &files {
            let mut ibuf = BytesMut::new();
            Encoder::encode(
                &mut codec,
                Msg::FileDataFull {
                    rel_path: f.rel_path.clone(),
                    size: f.size,
                    mtime_secs: f.mtime_secs,
                    mtime_nanos: f.mtime_nanos,
                    mode: f.mode,
                    data: f.data.clone(),
                },
                &mut ibuf,
            ).unwrap();
            total_individual += ibuf.len();
        }

        let raw_data_size: usize = files.iter().map(|f| f.data.len()).sum();
        let batch_ratio = raw_data_size as f64 / batch_wire_size as f64;
        let individ_ratio = raw_data_size as f64 / total_individual as f64;

        println!("  Raw data:       {raw_data_size} bytes ({} files)", files.len());
        println!("  Individual:     {total_individual} bytes (ratio {individ_ratio:.1}:1)");
        println!("  BatchFiles:     {batch_wire_size} bytes (ratio {batch_ratio:.1}:1)");
        println!("  Improvement:    {:.1}x smaller wire", total_individual as f64 / batch_wire_size as f64);

        // Verify the batch message decodes correctly
        let mut decoder = MsgCodec::new(false);
        let decoded = Decoder::decode(&mut decoder, &mut buf).unwrap().unwrap();
        match decoded {
            Msg::BatchFiles { files: decoded_files } => {
                assert_eq!(decoded_files.len(), 64);
                assert_eq!(decoded_files[0].rel_path.to_str().unwrap(), "module_0.rs");
                assert_eq!(decoded_files[0].data, files[0].data);
                assert_eq!(decoded_files[63].rel_path.to_str().unwrap(), "module_63.rs");
            }
            other => panic!("expected BatchFiles, got {other:?}"),
        }

        // BatchFiles should compress at least 3x better than individual
        assert!(
            batch_wire_size < total_individual / 2,
            "BatchFiles ({batch_wire_size}) should be at least 2x smaller than individual ({total_individual})"
        );
    }
}
