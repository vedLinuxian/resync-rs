//! TCP client — connects to a remote resync server and pushes a sync.
//!
//!  ```text
//!  resync push /local/source/ user@host:/remote/dest/
//!  ```
//!
//!  The client:
//!  1. Scans the local source directory.
//!  2. Sends the file manifest to the server.
//!  3. Receives the sync plan (NeedFull / NeedDelta / Skip).
//!  4. Streams file data / deltas for the requested files.
//!  5. Receives and reports completion stats.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Instant;

use futures::stream::StreamExt;
use futures::SinkExt;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::{debug, info};

use crate::delta::DeltaOp;
use crate::hasher::{FileManifest, Hash256, Hasher};
use crate::net::protocol::*;
use crate::net::tls::{connect_stream, MaybeTlsStream, TlsConfig};
use crate::scanner::{FileEntry, Scanner};

// ─── Client options ──────────────────────────────────────────────────────────

pub struct ClientOptions {
    pub source: PathBuf,
    pub remote_addr: SocketAddr,
    pub remote_dest: PathBuf,
    pub chunk_size: usize,
    pub compress: bool,
    pub delete: bool,
    pub preserve_perms: bool,
    pub preserve_times: bool,
    pub preserve_links: bool,
    pub recursive: bool,
    pub verbose: bool,
    pub show_stats: bool,
    pub tls_config: TlsConfig,
}

// ─── Client ──────────────────────────────────────────────────────────────────

pub struct Client {
    opts: ClientOptions,
}

impl Client {
    pub fn new(opts: ClientOptions) -> Self {
        Self { opts }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let start = Instant::now();

        // ── 1. Scan local source ──────────────────────────────────────────
        info!("scanning source: {}", self.opts.source.display());
        let scanner = Scanner::new(
            &self.opts.source,
            self.opts.recursive,
            self.opts.preserve_links,
        )?;
        let src_result = scanner.scan()?;

        info!(
            "source: {} files, {} bytes",
            src_result.files.len(),
            src_result.total_bytes
        );

        // ── 2. Connect to server ──────────────────────────────────────────
        info!("connecting to {}", self.opts.remote_addr);
        let stream = TcpStream::connect(self.opts.remote_addr).await?;
        stream.set_nodelay(true)?;
        let host = self.opts.remote_addr.ip().to_string();
        let stream = connect_stream(stream, &host, &self.opts.tls_config).await?;
        let mut framed = Framed::new(stream, MsgCodec::new(self.opts.compress));

        // ── 3. Handshake ──────────────────────────────────────────────────
        send(
            &mut framed,
            Msg::Hello {
                version: PROTOCOL_VERSION,
                chunk_size: self.opts.chunk_size,
                compress: self.opts.compress,
            },
        )
        .await?;

        match recv(&mut framed).await? {
            Msg::HelloAck { ok: true, .. } => {
                info!("handshake OK");
            }
            Msg::HelloAck {
                ok: false, error, ..
            } => {
                let msg = error.unwrap_or_else(|| "unknown".to_string());
                anyhow::bail!("server rejected handshake: {msg}");
            }
            other => anyhow::bail!("expected HelloAck, got {other:?}"),
        }

        // ── 4. Send sync session info ─────────────────────────────────────
        send(
            &mut framed,
            Msg::BeginSync {
                dest_path: self.opts.remote_dest.clone(),
                file_count: src_result.files.len() as u64,
                total_bytes: src_result.total_bytes,
                delete: self.opts.delete,
                preserve_perms: self.opts.preserve_perms,
                preserve_times: self.opts.preserve_times,
            },
        )
        .await?;

        // ── 5. Send file manifest ─────────────────────────────────────────
        for entry in &src_result.files {
            let (secs, nanos) = system_time_to_epoch(entry.modified);
            send(
                &mut framed,
                Msg::FileHeader {
                    rel_path: entry.rel_path.clone(),
                    size: entry.size,
                    mtime_secs: secs,
                    mtime_nanos: nanos,
                    mode: entry.mode,
                    is_symlink: entry.is_symlink,
                    symlink_target: entry.symlink_target.clone(),
                },
            )
            .await?;
        }
        send(&mut framed, Msg::ManifestEnd).await?;

        info!("manifest sent ({} files)", src_result.files.len());

        // Build lookup: rel_path → FileEntry
        let file_map: std::collections::HashMap<PathBuf, &FileEntry> = src_result
            .files
            .iter()
            .map(|f| (f.rel_path.clone(), f))
            .collect();

        // ── 6. Receive sync plan and send data ───────────────────────────
        let mut plan: Vec<SyncAction> = Vec::new();

        loop {
            match recv(&mut framed).await? {
                Msg::NeedFull { rel_path } => {
                    debug!("server needs full: {}", rel_path.display());
                    plan.push(SyncAction::Full(rel_path));
                }
                Msg::NeedDelta {
                    rel_path,
                    chunk_hashes,
                    chunk_size,
                    dst_size,
                } => {
                    debug!("server needs delta: {}", rel_path.display());
                    plan.push(SyncAction::Delta {
                        rel_path,
                        chunk_hashes,
                        chunk_size,
                        dst_size,
                    });
                }
                Msg::Skip { rel_path } => {
                    debug!("server skipping: {}", rel_path.display());
                }
                Msg::PlanEnd { files_to_send } => {
                    info!("plan received: {files_to_send} files to send");
                    break;
                }
                other => anyhow::bail!("expected plan message, got {other:?}"),
            }
        }

        // ── 7. Execute the plan ───────────────────────────────────────────
        let hasher = Hasher::new(self.opts.chunk_size);

        for action in &plan {
            match action {
                SyncAction::Full(rel_path) => {
                    let entry = file_map.get(rel_path).ok_or_else(|| {
                        anyhow::anyhow!("file not in manifest: {}", rel_path.display())
                    })?;

                    self.send_full_file(&mut framed, entry).await?;
                }
                SyncAction::Delta {
                    rel_path,
                    chunk_hashes,
                    chunk_size: _,
                    dst_size: _,
                } => {
                    let entry = file_map.get(rel_path).ok_or_else(|| {
                        anyhow::anyhow!("file not in manifest: {}", rel_path.display())
                    })?;

                    self.send_delta(&mut framed, entry, chunk_hashes, &hasher)
                        .await?;
                }
            }
        }

        // ── 8. Receive delete report if applicable ────────────────────────
        if self.opts.delete {
            match recv(&mut framed).await? {
                Msg::DeleteReport { deleted_count } => {
                    info!("deleted {deleted_count} orphan files on server");
                }
                other => anyhow::bail!("expected DeleteReport, got {other:?}"),
            }
        }

        // ── 9. Receive completion stats ───────────────────────────────────
        match recv(&mut framed).await? {
            Msg::SyncComplete {
                files_new,
                files_updated,
                files_skipped,
                files_deleted,
                files_errored,
                bytes_transferred,
            } => {
                let elapsed = start.elapsed();
                let throughput = if elapsed.as_secs_f64() > 0.0 {
                    bytesize::ByteSize::b((bytes_transferred as f64 / elapsed.as_secs_f64()) as u64)
                } else {
                    bytesize::ByteSize::b(0)
                };

                if self.opts.show_stats || self.opts.verbose {
                    println!();
                    println!("─────────────────────────────────────────────────────");
                    println!("  resync-rs  —  network sync complete");
                    println!("─────────────────────────────────────────────────────");
                    println!(
                        "  Files       : {} new, {} updated, {} unchanged, {} deleted, {} errors",
                        files_new, files_updated, files_skipped, files_deleted, files_errored,
                    );
                    println!(
                        "  Transferred : {}",
                        bytesize::ByteSize::b(bytes_transferred)
                    );
                    println!("  Throughput  : {throughput}/s");
                    println!("  Time        : {:.3}s", elapsed.as_secs_f64());
                    println!("─────────────────────────────────────────────────────");
                }
            }
            other => anyhow::bail!("expected SyncComplete, got {other:?}"),
        }

        Ok(())
    }

    // ─── Private helpers ─────────────────────────────────────────────────────

    /// Pull mode: request files FROM the remote server to local destination.
    ///
    /// This connects to the server and requests it to send files from
    /// `self.opts.source` (the remote path) to `self.opts.remote_dest`
    /// (the local destination).
    pub async fn pull(&self) -> anyhow::Result<()> {
        let start = Instant::now();

        info!("connecting to {} for pull", self.opts.remote_addr);
        let stream = TcpStream::connect(self.opts.remote_addr).await?;
        stream.set_nodelay(true)?;
        let host = self.opts.remote_addr.ip().to_string();
        let stream = connect_stream(stream, &host, &self.opts.tls_config).await?;
        let mut framed = Framed::new(stream, MsgCodec::new(self.opts.compress));

        // Handshake
        send(
            &mut framed,
            Msg::Hello {
                version: PROTOCOL_VERSION,
                chunk_size: self.opts.chunk_size,
                compress: self.opts.compress,
            },
        )
        .await?;

        match recv(&mut framed).await? {
            Msg::HelloAck { ok: true, .. } => {
                info!("handshake OK");
            }
            Msg::HelloAck {
                ok: false, error, ..
            } => {
                let msg = error.unwrap_or_else(|| "unknown".to_string());
                anyhow::bail!("server rejected handshake: {msg}");
            }
            other => anyhow::bail!("expected HelloAck, got {other:?}"),
        }

        // Request pull: tell server we want files FROM source path
        send(
            &mut framed,
            Msg::PullRequest {
                source_path: self.opts.source.clone(),
                dest_path: self.opts.remote_dest.clone(),
            },
        )
        .await?;

        // Receive files from server
        let local_dest = &self.opts.remote_dest;
        std::fs::create_dir_all(local_dest)?;

        let mut files_received: u64 = 0;
        let mut bytes_received: u64 = 0;

        loop {
            match recv(&mut framed).await? {
                Msg::FileDataStart {
                    rel_path,
                    size,
                    mode,
                    mtime_secs,
                    mtime_nanos,
                    ..
                } => {
                    let dst_path = local_dest.join(&rel_path);
                    if let Some(parent) = dst_path.parent() {
                        std::fs::create_dir_all(parent)?;
                    }

                    // Receive file data chunks
                    let mut file_data = Vec::with_capacity(size as usize);
                    loop {
                        match recv(&mut framed).await? {
                            Msg::FileDataChunk { data } => {
                                file_data.extend_from_slice(&data);
                            }
                            Msg::FileDataEnd => break,
                            other => anyhow::bail!("expected FileDataChunk/End, got {other:?}"),
                        }
                    }

                    // Write to local file
                    std::fs::write(&dst_path, &file_data)?;

                    // Apply permissions
                    #[cfg(unix)]
                    if self.opts.preserve_perms {
                        use std::os::unix::fs::PermissionsExt;
                        let perms = std::fs::Permissions::from_mode(mode & 0o7777);
                        std::fs::set_permissions(&dst_path, perms)?;
                    }

                    // Apply mtime
                    if self.opts.preserve_times {
                        use std::ffi::CString;
                        if let Ok(c_path) = CString::new(dst_path.to_string_lossy().as_bytes()) {
                            let ts = libc::timespec {
                                tv_sec: mtime_secs,
                                tv_nsec: mtime_nanos as i64,
                            };
                            let times = [
                                libc::timespec { tv_sec: 0, tv_nsec: libc::UTIME_OMIT },
                                ts,
                            ];
                            unsafe {
                                libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), 0);
                            }
                        }
                    }

                    files_received += 1;
                    bytes_received += file_data.len() as u64;

                    if self.opts.verbose {
                        println!("  RECV  {}", rel_path.display());
                    }
                }
                Msg::SyncComplete {
                    files_new,
                    files_updated,
                    files_skipped,
                    files_deleted,
                    files_errored,
                    bytes_transferred: _,
                } => {
                    let elapsed = start.elapsed();
                    if self.opts.show_stats || self.opts.verbose {
                        println!();
                        println!("─────────────────────────────────────────────────────");
                        println!("  resync-rs  —  pull complete");
                        println!("─────────────────────────────────────────────────────");
                        println!(
                            "  Files       : {} new, {} updated, {} unchanged, {} deleted, {} errors",
                            files_new, files_updated, files_skipped, files_deleted, files_errored,
                        );
                        println!(
                            "  Received    : {} ({} files)",
                            bytesize::ByteSize::b(bytes_received),
                            files_received,
                        );
                        println!("  Time        : {:.3}s", elapsed.as_secs_f64());
                        println!("─────────────────────────────────────────────────────");
                    }
                    break;
                }
                other => anyhow::bail!("unexpected message during pull: {other:?}"),
            }
        }

        Ok(())
    }

    /// Stream a full file to the server.
    async fn send_full_file(
        &self,
        framed: &mut Framed<MaybeTlsStream, MsgCodec>,
        entry: &FileEntry,
    ) -> anyhow::Result<()> {
        let (secs, nanos) = system_time_to_epoch(entry.modified);

        send(
            framed,
            Msg::FileDataStart {
                rel_path: entry.rel_path.clone(),
                size: entry.size,
                mtime_secs: secs,
                mtime_nanos: nanos,
                mode: entry.mode,
            },
        )
        .await?;

        if entry.size > 0 {
            // PERF FIX: Use mmap instead of std::fs::read() which loaded the
            // entire file into heap.  For a 10 GB file, that's 10 GB of RAM.
            // With mmap, the kernel pages in data on demand — max resident
            // memory is bounded by WIRE_CHUNK_SIZE (256 KB).
            let file = std::fs::File::open(&entry.abs_path)?;
            let mmap = unsafe { memmap2::Mmap::map(&file)? };
            for chunk in mmap.chunks(WIRE_CHUNK_SIZE) {
                send(
                    framed,
                    Msg::FileDataChunk {
                        data: chunk.to_vec(),
                    },
                )
                .await?;
            }
        }

        send(framed, Msg::FileDataEnd).await?;

        if self.opts.verbose {
            println!("  SENT  {}", entry.rel_path.display());
        }

        Ok(())
    }

    /// Compute delta using server's chunk hashes and send the result.
    async fn send_delta(
        &self,
        framed: &mut Framed<MaybeTlsStream, MsgCodec>,
        entry: &FileEntry,
        dst_hashes: &[Hash256],
        hasher: &Hasher,
    ) -> anyhow::Result<()> {
        // Hash the local source file
        let src_manifest = hasher.hash_file(&entry.abs_path)?;

        // PERF FIX: Use mmap instead of std::fs::read() to avoid loading
        // entire large files into heap memory.
        let file = std::fs::File::open(&entry.abs_path)?;
        let src_data = if entry.size > 0 {
            unsafe { memmap2::Mmap::map(&file)? }
        } else {
            // Empty files can't be mmap'd on Linux (EINVAL)
            return self.send_full_file(framed, entry).await;
        };

        // Compute delta against the server's chunk hashes
        let (_ops, wire_ops, write_data) =
            compute_network_delta(&src_manifest, dst_hashes, &src_data);

        let (secs, nanos) = system_time_to_epoch(entry.modified);

        send(
            framed,
            Msg::DeltaStart {
                rel_path: entry.rel_path.clone(),
                final_size: src_manifest.file_size,
                write_count: write_data.len() as u32,
                ops: wire_ops,
                mtime_secs: secs,
                mtime_nanos: nanos,
                mode: entry.mode,
            },
        )
        .await?;

        // Send Write data chunks
        for chunk_data in &write_data {
            send(
                framed,
                Msg::DeltaChunk {
                    data: chunk_data.clone(),
                },
            )
            .await?;
        }

        send(framed, Msg::DeltaEnd).await?;

        if self.opts.verbose {
            let transfer: u64 = write_data.iter().map(|d| d.len() as u64).sum();
            let pct = if entry.size > 0 {
                (1.0 - transfer as f64 / entry.size as f64) * 100.0
            } else {
                100.0
            };
            println!(
                "  DELTA {} — {:.1}% saved ({} / {})",
                entry.rel_path.display(),
                pct,
                bytesize::ByteSize::b(transfer),
                bytesize::ByteSize::b(entry.size),
            );
        }

        Ok(())
    }
}

// ─── Delta computation against remote hashes ─────────────────────────────────

/// Compute a delta between a local source file's manifest and the remote
/// destination's chunk hashes. Returns:
/// - The internal DeltaOps (for logging)
/// - Wire ops (for protocol)
/// - Vec<Vec<u8>> of data for each Write op
fn compute_network_delta(
    src_manifest: &FileManifest,
    dst_hashes: &[Hash256],
    src_data: &[u8],
) -> (Vec<DeltaOp>, Vec<DeltaWireOp>, Vec<Vec<u8>>) {
    let chunk_size = src_manifest.chunk_size;
    let mut ops = Vec::new();
    let mut wire_ops = Vec::new();
    let mut write_data = Vec::new();

    for (i, src_chunk) in src_manifest.chunks.iter().enumerate() {
        let matches = dst_hashes
            .get(i)
            .map(|dh| *dh == src_chunk.hash)
            .unwrap_or(false);

        if matches {
            // Chunk matches — Copy from existing destination
            let dst_offset = (i as u64) * (chunk_size as u64);
            ops.push(DeltaOp::Copy {
                src_offset: dst_offset,
                len: src_chunk.len,
            });
            wire_ops.push(DeltaWireOp::Copy {
                src_offset: dst_offset,
                len: src_chunk.len,
            });
        } else {
            // Chunk differs — Write from source
            let start = src_chunk.offset as usize;
            let end = start + src_chunk.len;
            let data = src_data[start..end].to_vec();

            ops.push(DeltaOp::Write {
                src_offset: src_chunk.offset,
                len: src_chunk.len,
            });
            wire_ops.push(DeltaWireOp::Write { len: src_chunk.len });
            write_data.push(data);
        }
    }

    (ops, wire_ops, write_data)
}

// ─── Framed helpers ──────────────────────────────────────────────────────────

async fn recv(framed: &mut Framed<MaybeTlsStream, MsgCodec>) -> anyhow::Result<Msg> {
    framed
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("connection closed unexpectedly"))?
        .map_err(|e| anyhow::anyhow!("receive error: {e}"))
}

async fn send(framed: &mut Framed<MaybeTlsStream, MsgCodec>, msg: Msg) -> anyhow::Result<()> {
    framed
        .send(msg)
        .await
        .map_err(|e| anyhow::anyhow!("send error: {e}"))
}

// ─── Plan action ─────────────────────────────────────────────────────────────

#[allow(dead_code)]
enum SyncAction {
    Full(PathBuf),
    Delta {
        rel_path: PathBuf,
        chunk_hashes: Vec<Hash256>,
        chunk_size: usize,
        dst_size: u64,
    },
}
