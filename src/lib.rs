//! Ironclad: an embeddable, replicated key-value store.
//!
//! * [`storage`] — single-node LSM-tree engine (WAL, memtable, SSTables,
//!   bloom filters, compaction, crash recovery). Usable on its own as an
//!   embedded database.
//! * [`raft`] — Raft consensus as a deterministic state machine with no
//!   I/O of its own except its durable log.
//! * [`protocol`] / [`client`] — the binary client protocol and an async
//!   client.
//! * [`server`] — glues everything into a networked, replicated node.

pub mod client;
pub mod codec;
pub mod error;
pub mod protocol;
pub mod raft;
pub mod server;
pub mod storage;

pub use error::{Error, Result};
