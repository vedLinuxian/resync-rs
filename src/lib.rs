//! resync-rs — Next-generation parallel delta-sync engine.
//!
//!  Library root.  All modules are public so benchmarks and integration tests
//!  can import them directly without going through the binary entry-point.

pub mod applier;
pub mod cdc;
pub mod cli;
pub mod delta;
pub mod error;
pub mod fiemap;
pub mod filter;
pub mod hasher;
pub mod io_engine;
pub mod manifest;
pub mod net;
pub mod progress;
pub mod scanner;
pub mod sync_engine;
pub mod uring_engine;
