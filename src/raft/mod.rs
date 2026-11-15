//! Raft consensus.
//!
//! [`RaftCore`] is a deterministic state machine: it never touches the
//! network or the clock. The driver feeds it ticks, peer messages and client
//! requests, and after each batch of inputs runs the *ready loop*:
//!
//! ```text
//! core.step(..) / core.tick() / core.propose(..)   // any number of inputs
//! send(core.take_early_messages()?) // leader replication, overlaps the fsync
//! core.persist()?          // fsync the log; only now may we count ourselves
//! send(core.take_messages())  // everything else, after persistence
//! core.apply()?            // apply committed entries, release reads
//! ```
//!
//! The same core runs under the real TCP driver (`server::node`) and under
//! the deterministic network simulator in `tests/raft_sim.rs`, which injects
//! message loss, reordering, partitions and crashes.

mod core;
mod log;

use std::io;

use serde::{Deserialize, Serialize};

pub use self::core::{ApplyOutput, Config, RaftCore, RaftError, Role, Status};
pub use self::log::RaftLog;

pub type NodeId = u64;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub index: u64,
    pub term: u64,
    /// Empty data is a no-op entry (appended by every new leader).
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    RequestVote {
        term: u64,
        last_log_index: u64,
        last_log_term: u64,
    },
    VoteResponse {
        term: u64,
        granted: bool,
    },
    AppendEntries {
        term: u64,
        prev_index: u64,
        prev_term: u64,
        entries: Vec<Entry>,
        commit: u64,
        /// Read-index round; echoed back so the leader can confirm it is
        /// still leader before serving linearizable reads.
        seq: u64,
    },
    AppendResponse {
        term: u64,
        success: bool,
        match_index: u64,
        /// On rejection: where the leader should retry from.
        conflict_index: u64,
        seq: u64,
    },
    InstallSnapshot {
        term: u64,
        last_index: u64,
        last_term: u64,
        data: Vec<u8>,
    },
    SnapshotResponse {
        term: u64,
        last_index: u64,
    },
}

impl Message {
    pub fn term(&self) -> u64 {
        match self {
            Message::RequestVote { term, .. }
            | Message::VoteResponse { term, .. }
            | Message::AppendEntries { term, .. }
            | Message::AppendResponse { term, .. }
            | Message::InstallSnapshot { term, .. }
            | Message::SnapshotResponse { term, .. } => *term,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    pub from: NodeId,
    pub to: NodeId,
    pub msg: Message,
}

/// The replicated application. Implementations must make the effects of
/// `apply`/`restore` and the reported `applied()` position durable
/// *atomically*, so a restart never re-applies or skips an entry.
pub trait StateMachine {
    /// `(index, term)` of the last applied entry, `(0, 0)` if none.
    fn applied(&self) -> (u64, u64);
    /// Applies committed entries in order (including no-ops, which only
    /// advance `applied`).
    fn apply(&mut self, entries: &[Entry]) -> io::Result<()>;
    /// Serializes the full state as of `applied()`.
    fn snapshot(&self) -> io::Result<Vec<u8>>;
    /// Replaces the full state with a snapshot taken at `(index, term)`.
    /// Must be durable on return.
    fn restore(&mut self, index: u64, term: u64, data: &[u8]) -> io::Result<()>;
    /// Makes everything applied so far durable. Called before the log is
    /// compacted, so a state machine that applies without fsync never
    /// loses entries the log no longer holds.
    fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}
