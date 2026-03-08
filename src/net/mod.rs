//! Network module — Phase 2 TCP client-server architecture.
//!
//!  Provides the wire protocol, async TCP server daemon, and network client
//!  that together enable remote delta-sync over a custom binary protocol.

pub mod client;
pub mod protocol;
pub mod server;
pub mod tls;
