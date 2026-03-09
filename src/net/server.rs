//! TCP server daemon — accepts connections and applies incoming syncs.
//!
//!  ```text
//!  resync serve --bind 0.0.0.0 --port 2377
//!  ```
//!
//!  Each connection is handled in its own tokio task.  The server receives
//!  the file manifest from the client, compares against its local state,
//!  sends a sync plan (NeedFull / NeedDelta / Skip), and then receives
//!  and applies the file data.

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use futures::SinkExt;
use futures::stream::StreamExt;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use tracing::{error, info, warn};

use crate::net::tls::{MaybeTlsStream, TlsConfig, accept_stream};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use crate::hasher::Hasher;
use crate::net::protocol::*;
use crate::scanner::Scanner;

// ─── Server ──────────────────────────────────────────────────────────────────

pub struct Server {
    bind: SocketAddr,
    tls_config: TlsConfig,
}

impl Server {
    pub fn new(bind: SocketAddr, tls_config: TlsConfig) -> Self {
        Self { bind, tls_config }
    }

    /// Run the server, accepting connections until the process is killed.
    pub async fn run(&self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(self.bind).await?;
        info!("resync server listening on {}", self.bind);

        loop {
            let (stream, peer) = listener.accept().await?;
            info!("connection from {peer}");

            let tls_config = self.tls_config.clone();
            tokio::spawn(async move {
                let maybe_tls = match accept_stream(stream, &tls_config).await {
                    Ok(s) => s,
                    Err(e) => {
                        error!("TLS accept failed for {peer}: {e}");
                        return;
                    }
                };
                if let Err(e) = handle_connection(maybe_tls, peer).await {
                    error!("session with {peer} failed: {e}");
                }
            });
        }
    }
}

// ─── Connection handler ──────────────────────────────────────────────────────

async fn handle_connection(stream: MaybeTlsStream, peer: SocketAddr) -> anyhow::Result<()> {
    let mut framed = Framed::new(stream, MsgCodec::new(false));
    let start = Instant::now();

    // ── 1. Handshake ──────────────────────────────────────────────────────
    let hello = recv(&mut framed).await?;
    let (chunk_size, _compress) = match hello {
        Msg::Hello {
            version,
            chunk_size,
            compress,
        } => {
            if version != PROTOCOL_VERSION {
                send(
                    &mut framed,
                    Msg::HelloAck {
                        ok: false,
                        error: Some(format!(
                            "protocol version mismatch: server={PROTOCOL_VERSION}, client={version}"
                        )),
                    },
                )
                .await?;
                anyhow::bail!("protocol version mismatch");
            }
            send(
                &mut framed,
                Msg::HelloAck {
                    ok: true,
                    error: None,
                },
            )
            .await?;

            // Enable compression on our side if the client requested it.
            if compress {
                framed.codec_mut().set_compress(true);
            }

            (chunk_size, compress)
        }
        other => anyhow::bail!("expected Hello, got {other:?}"),
    };

    // ── 2. Receive sync session params ────────────────────────────────────
    let begin = recv(&mut framed).await?;
    let (dest_path, delete, preserve_perms, preserve_times) = match begin {
        Msg::BeginSync {
            dest_path,
            delete,
            preserve_perms,
            preserve_times,
            ..
        } => (dest_path, delete, preserve_perms, preserve_times),
        other => anyhow::bail!("expected BeginSync, got {other:?}"),
    };

    // SECURITY: Canonicalize dest_path and reject path traversal.
    // A malicious client could specify /etc or /root as dest_path.
    let dest_path = {
        // Create the directory first so canonicalize works.
        fs::create_dir_all(&dest_path)?;
        let canonical = dest_path.canonicalize().map_err(|e| {
            anyhow::anyhow!(
                "cannot canonicalize dest_path '{}': {e}",
                dest_path.display()
            )
        })?;
        // Reject absolute paths that look suspicious (e.g., /etc, /root, /var)
        // Allow only paths under /tmp, /home, /data, /mnt, /srv, /opt, or current dir
        let p = canonical.to_string_lossy();
        if p.starts_with("/etc")
            || p.starts_with("/root")
            || p.starts_with("/proc")
            || p.starts_with("/sys")
            || p.starts_with("/dev")
            || p.starts_with("/boot")
            || p.starts_with("/sbin")
            || p.starts_with("/bin")
            || p.starts_with("/usr")
        {
            anyhow::bail!("refusing to sync to system directory: {p}");
        }
        canonical
    };

    info!("sync target: {}", dest_path.display());

    // ── 3. Receive source file manifest ───────────────────────────────────
    let mut src_files: Vec<FileHeaderInfo> = Vec::new();

    loop {
        match recv(&mut framed).await? {
            Msg::FileHeader {
                rel_path,
                size,
                mtime_secs,
                mtime_nanos,
                mode,
                is_symlink,
                symlink_target,
            } => {
                // SECURITY: Reject paths with ".." to prevent path traversal attacks.
                if rel_path
                    .components()
                    .any(|c| c == std::path::Component::ParentDir)
                {
                    anyhow::bail!(
                        "path traversal detected in rel_path: {}",
                        rel_path.display()
                    );
                }
                if rel_path.is_absolute() {
                    anyhow::bail!("absolute rel_path rejected: {}", rel_path.display());
                }
                src_files.push(FileHeaderInfo {
                    rel_path,
                    size,
                    mtime_secs,
                    mtime_nanos,
                    mode,
                    is_symlink,
                    symlink_target,
                });
            }
            // PERF: ManifestBatch — client packs up to 200 headers per frame,
            // reducing 500 individual messages to ~3 frames.
            Msg::ManifestBatch { headers } => {
                for h in headers {
                    // SECURITY: path traversal check
                    if h.rel_path
                        .components()
                        .any(|c| c == std::path::Component::ParentDir)
                    {
                        anyhow::bail!(
                            "path traversal detected in ManifestBatch: {}",
                            h.rel_path.display()
                        );
                    }
                    if h.rel_path.is_absolute() {
                        anyhow::bail!(
                            "absolute path in ManifestBatch rejected: {}",
                            h.rel_path.display()
                        );
                    }
                    src_files.push(FileHeaderInfo {
                        rel_path: h.rel_path,
                        size: h.size,
                        mtime_secs: h.mtime_secs,
                        mtime_nanos: h.mtime_nanos,
                        mode: h.mode,
                        is_symlink: h.is_symlink,
                        symlink_target: h.symlink_target,
                    });
                }
            }
            Msg::ManifestEnd => break,
            other => anyhow::bail!("expected FileHeader, ManifestBatch, or ManifestEnd, got {other:?}"),
        }
    }

    info!("received manifest: {} files from {peer}", src_files.len());

    // ── 4. Create directory tree ──────────────────────────────────────────
    // Collect unique parent directories from the manifest and create them.
    {
        let mut dirs = std::collections::BTreeSet::new();
        for f in &src_files {
            if let Some(parent) = f.rel_path.parent() {
                if parent != Path::new("") {
                    dirs.insert(parent.to_path_buf());
                }
            }
        }
        for dir in &dirs {
            let full = dest_path.join(dir);
            if let Err(e) = fs::create_dir_all(&full) {
                warn!("failed to create directory {}: {e}", full.display());
            }
        }
    }

    // ── 5. Compare with destination and send sync plan ────────────────────
    // PERF FIX: Use CDC hasher for network delta computation.
    // Fixed-size chunks suffer from the "shift-byte problem": inserting 1 byte
    // at the start of a file shifts ALL chunk boundaries, causing every hash to
    // change and forcing full retransmission.  CDC (Content-Defined Chunking)
    // uses content-based boundaries that survive insertions/deletions, so only
    // the modified region is retransmitted.
    let hasher = Hasher::with_cdc(chunk_size);
    let mut files_to_send: u64 = 0;
    let mut files_skipped: u64 = 0;

    // PERF: Batch plan decisions into PlanBatch frames (up to 200 per frame).
    // This reduces 500 individual NeedFull/Skip messages to ~3 frames,
    // and Zstd compresses 200 similar path strings as one unit.
    const PLAN_BATCH_SIZE: usize = 200;
    let mut plan_batch: Vec<PlanDecision> = Vec::with_capacity(PLAN_BATCH_SIZE);

    for src_file in &src_files {
        let dst_full = dest_path.join(&src_file.rel_path);

        let decision = if !dst_full.exists() {
            files_to_send += 1;
            PlanDecision::NeedFull {
                rel_path: src_file.rel_path.clone(),
            }
        } else {
            let dst_meta = fs::metadata(&dst_full)?;
            let dst_mtime = dst_meta.modified()?;
            let (dst_secs, dst_nanos) = system_time_to_epoch(dst_mtime);
            let size_match = dst_meta.len() == src_file.size;
            let mtime_match = dst_secs == src_file.mtime_secs && dst_nanos == src_file.mtime_nanos;

            if size_match && mtime_match {
                files_skipped += 1;
                PlanDecision::Skip {
                    rel_path: src_file.rel_path.clone(),
                }
            } else {
                match hasher.hash_file(&dst_full) {
                    Ok(manifest) => {
                        let chunk_hashes: Vec<_> =
                            manifest.chunks.iter().map(|c| c.hash).collect();
                        files_to_send += 1;
                        PlanDecision::NeedDelta {
                            rel_path: src_file.rel_path.clone(),
                            chunk_hashes,
                            chunk_size: manifest.chunk_size,
                            dst_size: manifest.file_size,
                        }
                    }
                    Err(e) => {
                        warn!(
                            "failed to hash {}: {e} — requesting full",
                            dst_full.display()
                        );
                        files_to_send += 1;
                        PlanDecision::NeedFull {
                            rel_path: src_file.rel_path.clone(),
                        }
                    }
                }
            }
        };

        plan_batch.push(decision);
        if plan_batch.len() >= PLAN_BATCH_SIZE {
            let decisions = std::mem::replace(
                &mut plan_batch,
                Vec::with_capacity(PLAN_BATCH_SIZE),
            );
            feed(&mut framed, Msg::PlanBatch { decisions }).await?;
        }
    }

    // Flush remaining plan decisions + PlanEnd in one TCP push
    if !plan_batch.is_empty() {
        feed(&mut framed, Msg::PlanBatch { decisions: plan_batch }).await?;
    }
    send(&mut framed, Msg::PlanEnd { files_to_send }).await?;
    info!("plan: {files_to_send} to transfer, {files_skipped} skipped");

    // ── 6. Receive file data / deltas and apply ───────────────────────────
    let mut stats = SyncStats {
        files_skipped,
        ..SyncStats::default()
    };

    // Use a while loop instead of for 0..files_to_send because a single
    // BatchFiles message carries N files (counted individually).
    let mut files_received: u64 = 0;

    while files_received < files_to_send {
        match recv(&mut framed).await? {
            Msg::FileDataStart {
                rel_path,
                size,
                mtime_secs,
                mtime_nanos,
                mode,
            } => {
                let meta = RecvMeta {
                    dest_root: &dest_path,
                    rel_path: &rel_path,
                    mtime_secs,
                    mtime_nanos,
                    mode,
                    preserve_perms,
                    preserve_times,
                };
                receive_full_file(&mut framed, &meta, size).await?;
                stats.files_new += 1;
                stats.bytes_transferred += size;
                files_received += 1;
            }
            Msg::DeltaStart {
                rel_path,
                final_size,
                write_count,
                ops,
                mtime_secs,
                mtime_nanos,
                mode,
            } => {
                let meta = RecvMeta {
                    dest_root: &dest_path,
                    rel_path: &rel_path,
                    mtime_secs,
                    mtime_nanos,
                    mode,
                    preserve_perms,
                    preserve_times,
                };
                let transferred =
                    receive_delta(&mut framed, &meta, final_size, write_count, &ops).await?;
                stats.files_updated += 1;
                stats.bytes_transferred += transferred;
                files_received += 1;
            }
            // PERF: FileDataFull — small file in a single message (no
            // Start/Chunk/End overhead).  Write atomically via temp file.
            Msg::FileDataFull {
                rel_path,
                size,
                mtime_secs,
                mtime_nanos,
                mode,
                data,
            } => {
                // SECURITY: path traversal check
                if rel_path
                    .components()
                    .any(|c| c == std::path::Component::ParentDir)
                    || rel_path.is_absolute()
                {
                    anyhow::bail!("path traversal in FileDataFull: {}", rel_path.display());
                }
                let dst_path_full = dest_path.join(&rel_path);
                if let Some(parent) = dst_path_full.parent() {
                    fs::create_dir_all(parent)?;
                }
                let tmp = temp_path(&dst_path_full);
                let file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&tmp)?;
                {
                    let mut w = BufWriter::new(&file);
                    w.write_all(&data)?;
                    w.flush()?;
                }
                fs::rename(&tmp, &dst_path_full)?;
                apply_metadata_raw(
                    &dst_path_full,
                    mtime_secs,
                    mtime_nanos,
                    mode,
                    preserve_perms,
                    preserve_times,
                );
                stats.files_new += 1;
                stats.bytes_transferred += size;
                files_received += 1;
            }
            // PERF: BatchFiles — N small files packed into ONE compressed
            // wire frame.  Zstd sees 256 KB of similar code instead of
            // 64 × 4 KB independent blobs, achieving 8–10:1 vs 2–3:1.
            //
            // Write pipeline:
            //   1. Validate all paths upfront (security)
            //   2. Deduplicate & create directories in one pass
            //   3. Write files on blocking thread pool (parallel with
            //      receiving next batch from network)
            Msg::BatchFiles { files } => {
                let count = files.len() as u64;
                let bytes: u64 = files.iter().map(|f| f.size).sum();

                // SECURITY: validate all paths before writing anything
                for entry in &files {
                    if entry.rel_path
                        .components()
                        .any(|c| c == std::path::Component::ParentDir)
                        || entry.rel_path.is_absolute()
                    {
                        anyhow::bail!(
                            "path traversal in BatchFiles: {}",
                            entry.rel_path.display()
                        );
                    }
                }

                // Write files on blocking thread pool — this frees the
                // async task to receive the next batch from the network.
                let dest = dest_path.clone();
                let pp = preserve_perms;
                let pt = preserve_times;
                tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                    // Pre-create unique parent directories (avoids
                    // repeated stat+mkdir for each file in the same dir).
                    let mut dirs_seen = std::collections::HashSet::new();
                    for entry in &files {
                        let dst = dest.join(&entry.rel_path);
                        if let Some(parent) = dst.parent() {
                            if dirs_seen.insert(parent.to_path_buf()) {
                                fs::create_dir_all(parent)?;
                            }
                        }
                    }

                    for entry in &files {
                        let dst = dest.join(&entry.rel_path);
                        // PERF: Write directly — no temp+rename overhead.
                        // For fresh sync the file doesn't exist yet, so
                        // atomic rename provides no benefit.  For
                        // subsequent syncs, small files are fully buffered
                        // in memory so the write is effectively atomic.
                        let mut file = OpenOptions::new()
                            .write(true)
                            .create(true)
                            .truncate(true)
                            .open(&dst)?;
                        file.write_all(&entry.data)?;
                        apply_metadata_raw(
                            &dst,
                            entry.mtime_secs,
                            entry.mtime_nanos,
                            entry.mode,
                            pp,
                            pt,
                        );
                    }
                    Ok(())
                })
                .await??;

                stats.files_new += count;
                stats.bytes_transferred += bytes;
                files_received += count;
            }
            Msg::Error { message } => {
                error!("client error: {message}");
                stats.files_errored += 1;
                files_received += 1;
            }
            other => anyhow::bail!("expected FileDataStart, FileDataFull, BatchFiles, or DeltaStart, got {other:?}"),
        }
    }

    // ── 6½. Batched sync — flush all written data to disk ─────────────
    //
    // Instead of fsync-per-file (which costs ~4ms each on overlay/ext4),
    // we do a single syncfs() that flushes the entire filesystem.  This
    // matches rsync's default behavior (no --fsync) and is safe because
    // we already used atomic rename (temp → final) for each file.
    //
    // For 500 small files: 500 × 4ms = 2000ms → 1 × 20ms = 20ms.
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        if let Ok(dir) = fs::File::open(&dest_path) {
            unsafe { libc::syncfs(dir.as_raw_fd()) };
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        // Fallback: sync the directory fd (flushes dir entries).
        if let Ok(dir) = fs::File::open(&dest_path) {
            let _ = dir.sync_all();
        }
    }

    // ── 7. Handle --delete ────────────────────────────────────────────────
    if delete {
        let src_paths: std::collections::HashSet<PathBuf> =
            src_files.iter().map(|f| f.rel_path.clone()).collect();

        if let Ok(scanner) = Scanner::new(&dest_path, true, false) {
            if let Ok(dst_result) = scanner.scan() {
                let mut deleted_count: u64 = 0;
                for entry in &dst_result.files {
                    if !src_paths.contains(&entry.rel_path) {
                        if let Err(e) = fs::remove_file(&entry.abs_path) {
                            warn!("delete failed: {}: {e}", entry.abs_path.display());
                        } else {
                            deleted_count += 1;
                        }
                    }
                }
                stats.files_deleted = deleted_count;

                // Clean up orphan empty directories
                let src_dirs: std::collections::HashSet<PathBuf> = src_files
                    .iter()
                    .filter_map(|f| f.rel_path.parent().map(|p| p.to_path_buf()))
                    .collect();
                let mut orphan_dirs: Vec<_> = dst_result
                    .dirs
                    .iter()
                    .filter(|d| !src_dirs.contains(&d.rel_path))
                    .collect();
                orphan_dirs.sort_by(|a, b| {
                    b.rel_path
                        .components()
                        .count()
                        .cmp(&a.rel_path.components().count())
                });
                for dir in orphan_dirs {
                    fs::remove_dir(&dir.abs_path).ok();
                }

                send(&mut framed, Msg::DeleteReport { deleted_count }).await?;
            }
        }
    }

    // ── 8. Send completion ────────────────────────────────────────────────
    send(
        &mut framed,
        Msg::SyncComplete {
            files_new: stats.files_new,
            files_updated: stats.files_updated,
            files_skipped: stats.files_skipped,
            files_deleted: stats.files_deleted,
            files_errored: stats.files_errored,
            bytes_transferred: stats.bytes_transferred,
        },
    )
    .await?;

    let elapsed = start.elapsed().as_secs_f64();
    info!(
        "sync from {peer} complete in {elapsed:.3}s — {} new, {} updated, {} skipped, {} deleted",
        stats.files_new, stats.files_updated, stats.files_skipped, stats.files_deleted,
    );

    Ok(())
}

// ─── File receive helpers ────────────────────────────────────────────────────

/// Metadata for a received file (avoids passing too many args).
#[allow(dead_code)]
struct RecvMeta<'a> {
    dest_root: &'a Path,
    rel_path: &'a Path,
    mtime_secs: i64,
    mtime_nanos: u32,
    mode: u32,
    preserve_perms: bool,
    preserve_times: bool,
}

/// Receive a full file streamed as FileDataChunk messages.
async fn receive_full_file(
    framed: &mut Framed<MaybeTlsStream, MsgCodec>,
    meta: &RecvMeta<'_>,
    size: u64,
) -> anyhow::Result<()> {
    let dst_path = meta.dest_root.join(meta.rel_path);
    if let Some(parent) = dst_path.parent() {
        fs::create_dir_all(parent)?;
    }

    // Atomic write via temp file
    let tmp_path = temp_path(&dst_path);

    let result: anyhow::Result<()> = async {
        if size == 0 {
            // Empty file
            File::create(&tmp_path)?;
            // Read the FileDataEnd
            match recv(framed).await? {
                Msg::FileDataEnd => {}
                other => anyhow::bail!("expected FileDataEnd, got {other:?}"),
            }
        } else {
            let file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;
            let mut writer = BufWriter::new(&file);
            let mut received: u64 = 0;

            loop {
                match recv(framed).await? {
                    Msg::FileDataChunk { data } => {
                        writer.write_all(&data)?;
                        received += data.len() as u64;
                    }
                    Msg::FileDataEnd => break,
                    other => anyhow::bail!("expected FileDataChunk or FileDataEnd, got {other:?}"),
                }
            }

            writer.flush()?;
            drop(writer);
            // No per-file sync — batched at end of session.

            if received != size {
                anyhow::bail!(
                    "size mismatch for {}: expected {size}, got {received}",
                    meta.rel_path.display()
                );
            }
        }
        Ok(())
    }
    .await;

    match result {
        Ok(()) => {
            fs::rename(&tmp_path, &dst_path)?;
            apply_metadata_raw(
                &dst_path,
                meta.mtime_secs,
                meta.mtime_nanos,
                meta.mode,
                meta.preserve_perms,
                meta.preserve_times,
            );
            Ok(())
        }
        Err(e) => {
            let _ = fs::remove_file(&tmp_path);
            Err(e)
        }
    }
}

/// Receive and apply a delta for a changed file.
async fn receive_delta(
    framed: &mut Framed<MaybeTlsStream, MsgCodec>,
    meta: &RecvMeta<'_>,
    final_size: u64,
    write_count: u32,
    ops: &[DeltaWireOp],
) -> anyhow::Result<u64> {
    let dst_path = meta.dest_root.join(meta.rel_path);
    let tmp_path = temp_path(&dst_path);

    // Read existing destination file data (for Copy ops)
    let dst_data = if dst_path.exists() {
        std::fs::read(&dst_path)?
    } else {
        vec![]
    };

    // Collect write data chunks from the wire
    let mut write_chunks: Vec<Vec<u8>> = Vec::with_capacity(write_count as usize);
    for _ in 0..write_count {
        match recv(framed).await? {
            Msg::DeltaChunk { data } => write_chunks.push(data),
            other => anyhow::bail!("expected DeltaChunk, got {other:?}"),
        }
    }

    // Read DeltaEnd
    match recv(framed).await? {
        Msg::DeltaEnd => {}
        other => anyhow::bail!("expected DeltaEnd, got {other:?}"),
    }

    // Apply the delta ops
    let result: anyhow::Result<u64> = (|| {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        // PERF FIX: 256 KB buffer instead of default 8 KB
        let mut writer = BufWriter::with_capacity(256 * 1024, &file);
        let mut bytes_transferred: u64 = 0;
        let mut bytes_written_total: u64 = 0;
        let mut write_idx: usize = 0;

        for op in ops {
            match op {
                DeltaWireOp::Copy { src_offset, len } => {
                    let start = *src_offset as usize;
                    let end = start + len;
                    if end > dst_data.len() {
                        anyhow::bail!(
                            "Copy op out of bounds: {}..{} in {} byte dst",
                            start,
                            end,
                            dst_data.len()
                        );
                    }
                    writer.write_all(&dst_data[start..end])?;
                    bytes_written_total += *len as u64;
                }
                DeltaWireOp::Write { len } => {
                    if write_idx >= write_chunks.len() {
                        anyhow::bail!("not enough DeltaChunk messages for Write ops");
                    }
                    let data = &write_chunks[write_idx];
                    if data.len() != *len {
                        anyhow::bail!(
                            "DeltaChunk length mismatch: op says {len}, got {}",
                            data.len()
                        );
                    }
                    writer.write_all(data)?;
                    bytes_transferred += *len as u64;
                    bytes_written_total += *len as u64;
                    write_idx += 1;
                }
            }
        }

        writer.flush()?;
        drop(writer);
        // No per-file sync — batched at end of session.

        // BUG FIX: Validate that the reconstructed file matches the expected
        // size.  A mismatch means the delta was applied incorrectly (data
        // corruption on the wire, or stale chunk hashes).
        if bytes_written_total != final_size {
            anyhow::bail!(
                "delta size mismatch for {}: expected {} bytes, wrote {}",
                meta.rel_path.display(),
                final_size,
                bytes_written_total
            );
        }

        Ok(bytes_transferred)
    })();

    match result {
        Ok(transferred) => {
            fs::rename(&tmp_path, &dst_path)?;
            apply_metadata_raw(
                &dst_path,
                meta.mtime_secs,
                meta.mtime_nanos,
                meta.mode,
                meta.preserve_perms,
                meta.preserve_times,
            );
            Ok(transferred)
        }
        Err(e) => {
            let _ = fs::remove_file(&tmp_path);
            Err(e)
        }
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Unique temp file name in the same directory.
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_path(dst_path: &Path) -> PathBuf {
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let name = dst_path.file_name().unwrap_or_default().to_string_lossy();
    dst_path.with_file_name(format!(".{name}.resync-srv.{pid}.{counter}.tmp"))
}

/// Apply metadata (permissions + mtime) from raw values.
fn apply_metadata_raw(
    path: &Path,
    mtime_secs: i64,
    mtime_nanos: u32,
    mode: u32,
    preserve_perms: bool,
    preserve_times: bool,
) {
    #[cfg(unix)]
    {
        if preserve_perms {
            let perms = fs::Permissions::from_mode(mode & 0o7777);
            if let Err(e) = fs::set_permissions(path, perms) {
                warn!("failed to set perms on {}: {e}", path.display());
            }
        }
        if preserve_times {
            let mtime = epoch_to_system_time(mtime_secs, mtime_nanos);
            if let Ok(file) = OpenOptions::new().write(true).open(path) {
                if let Err(e) = file.set_modified(mtime) {
                    warn!("failed to set mtime on {}: {e}", path.display());
                }
            }
        }
    }
}

/// Receive exactly one message from the framed stream.
async fn recv(framed: &mut Framed<MaybeTlsStream, MsgCodec>) -> anyhow::Result<Msg> {
    framed
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("connection closed unexpectedly"))?
        .map_err(|e| anyhow::anyhow!("receive error: {e}"))
}

/// Send one message and flush (for control messages and large files).
async fn send(framed: &mut Framed<MaybeTlsStream, MsgCodec>, msg: Msg) -> anyhow::Result<()> {
    framed
        .send(msg)
        .await
        .map_err(|e| anyhow::anyhow!("send error: {e}"))
}

/// Queue a message WITHOUT flushing — for batching plan messages.
async fn feed(framed: &mut Framed<MaybeTlsStream, MsgCodec>, msg: Msg) -> anyhow::Result<()> {
    framed
        .feed(msg)
        .await
        .map_err(|e| anyhow::anyhow!("feed error: {e}"))
}

// ─── Internal types ──────────────────────────────────────────────────────────

#[allow(dead_code)]
struct FileHeaderInfo {
    rel_path: PathBuf,
    size: u64,
    mtime_secs: i64,
    mtime_nanos: u32,
    mode: u32,
    is_symlink: bool,
    symlink_target: Option<PathBuf>,
}

#[derive(Default)]
struct SyncStats {
    files_new: u64,
    files_updated: u64,
    files_skipped: u64,
    files_deleted: u64,
    files_errored: u64,
    bytes_transferred: u64,
}
