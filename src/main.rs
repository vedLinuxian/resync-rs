//! resync-rs — entry point.
//!
//!  Supports three modes:
//!  1. Local sync: `resync [OPTIONS] SOURCE DEST`  (Phase 1 — rsync compatible)
//!  2. Server:     `resync serve [--bind ADDR] [--port PORT]`
//!  3. Push:       `resync push [OPTIONS] SOURCE HOST:DEST`

use anyhow::Context;
use clap::Parser;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use resync_rs::cli::{Cli, Command, parse_remote};
use resync_rs::net::client::{Client, ClientOptions};
use resync_rs::net::server::Server;
use resync_rs::net::tls::TlsConfig;
use resync_rs::sync_engine::{SyncEngine, SyncOptions};

fn main() -> anyhow::Result<()> {
    let mut args = Cli::parse();

    // Expand `-a` (archive) into its constituent flags, same as rsync
    args.resolve_archive();

    // Expand --backup-dir into --backup
    args.resolve_backup();

    // Initialise structured logging — level controlled by --log-level or
    // the RUST_LOG env var (RUST_LOG takes precedence).
    tracing_subscriber::registry()
        .with(fmt::layer().with_target(false).compact())
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level)))
        .init();

    tracing::info!("resync-rs starting");

    match args.command {
        // ── Phase 2: Server daemon ────────────────────────────────────────
        Some(Command::Serve {
            bind,
            port,
            tls,
            tls_cert,
            tls_key,
        }) => {
            let addr = format!("{bind}:{port}")
                .parse()
                .context("invalid bind address")?;
            let tls_config = TlsConfig {
                enabled: tls,
                cert_path: tls_cert,
                key_path: tls_key,
                verify: false,
            };
            let server = Server::new(addr, tls_config);
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(server.run())?;
        }

        // ── Phase 2: Network push ─────────────────────────────────────────
        Some(Command::Push {
            source,
            remote,
            tls,
            tls_verify,
        }) => {
            let (addr, remote_dest) = parse_remote(&remote)?;
            let tls_config = TlsConfig {
                enabled: tls,
                cert_path: None,
                key_path: None,
                verify: tls_verify,
            };
            let opts = ClientOptions {
                source,
                remote_addr: addr,
                remote_dest,
                chunk_size: args.chunk_size as usize,
                compress: args.compress,
                delete: args.delete,
                preserve_perms: args.preserve_perms,
                preserve_times: args.preserve_times,
                preserve_links: args.preserve_links,
                recursive: args.recursive,
                verbose: args.verbose,
                show_stats: args.stats,
                tls_config,
            };
            let client = Client::new(opts);
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(client.run())?;
        }

        // ── Phase 2: Network pull (remote → local) ───────────────────────
        Some(Command::Pull {
            remote,
            dest,
            tls,
            tls_verify,
        }) => {
            let (addr, remote_source) = parse_remote(&remote)?;
            let tls_config = TlsConfig {
                enabled: tls,
                cert_path: None,
                key_path: None,
                verify: tls_verify,
            };
            let opts = ClientOptions {
                source: remote_source,
                remote_addr: addr,
                remote_dest: dest,
                chunk_size: args.chunk_size as usize,
                compress: args.compress,
                delete: args.delete,
                preserve_perms: args.preserve_perms,
                preserve_times: args.preserve_times,
                preserve_links: args.preserve_links,
                recursive: args.recursive,
                verbose: args.verbose,
                show_stats: args.stats,
                tls_config,
            };
            let client = Client::new(opts);
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(client.pull())?;
        }

        // ── Phase 1: Local sync (rsync-compatible) ────────────────────────
        None => {
            let source = args
                .source
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("SOURCE is required for local sync"))?;
            let dest = args
                .dest
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("DEST is required for local sync"))?;

            // Validate they exist as positional args
            if source.as_os_str().is_empty() || dest.as_os_str().is_empty() {
                anyhow::bail!("usage: resync [OPTIONS] SOURCE DEST");
            }

            let opts = SyncOptions::from(&args);
            let engine = SyncEngine::new(opts);
            engine.run().context("sync failed")?;
        }
    }

    Ok(())
}
