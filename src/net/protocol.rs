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

/// Maximum frame payload size (64 MiB).
const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;

/// Chunk size for streaming file data over the wire (256 KiB).
/// Larger than the BLAKE3 chunk size — this is for network I/O efficiency.
pub const WIRE_CHUNK_SIZE: usize = 256 * 1024;

/// Payloads at or below this size are not worth compressing.
const COMPRESS_THRESHOLD: usize = 256;

/// Zstandard compression level (3 = good speed/ratio tradeoff for network).
const COMPRESS_LEVEL: i32 = 3;

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

    // ─── Pull mode (client ← server) ────────────────────────────────────
    /// Client → Server: request to pull files from server's source_path.
    PullRequest {
        source_path: PathBuf,
        dest_path: PathBuf,
    },
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
}

impl MsgCodec {
    /// Create a new codec.
    ///
    /// - `compress = false`: all outgoing payloads are sent uncompressed.
    /// - `compress = true`:  payloads larger than 256 bytes are zstd-compressed
    ///   (if compression actually shrinks them; otherwise raw is used).
    pub fn new(compress: bool) -> Self {
        Self { compress }
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
            FLAG_ZSTD => zstd::stream::decode_all(payload.as_ref()).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("zstd decompress error: {e}"),
                )
            })?,
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
            let compressed = zstd::stream::encode_all(raw.as_slice(), COMPRESS_LEVEL)
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
