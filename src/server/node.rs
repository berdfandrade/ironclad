//! The Raft driver: one dedicated OS thread owns the `RaftCore` and runs
//! the ready loop. Everything else talks to it through a bounded channel.
//!
//! Why a thread and not a Tokio task: the loop does synchronous fsyncs
//! (Raft log, storage WAL). On a dedicated thread they never stall the
//! async runtime that serves network I/O, and the core needs no locks.
//!
//! Group commit falls out of the loop structure: all events that arrived
//! while the previous iteration was fsyncing are handled together, then
//! persisted with one log fsync and applied with one storage write.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use super::Metrics;
use super::kv::KvStateMachine;
use crate::raft::{Envelope, NodeId, RaftCore, RaftError};
use crate::storage::Db;

pub enum Event {
    Peer(Envelope),
    Propose {
        data: Vec<u8>,
        reply: oneshot::Sender<WriteOutcome>,
    },
    Read {
        key: Bytes,
        reply: oneshot::Sender<ReadOutcome>,
    },
    Status {
        reply: oneshot::Sender<String>,
    },
}

#[derive(Debug)]
pub enum WriteOutcome {
    Committed,
    NotLeader(Option<NodeId>),
    /// The entry was overwritten by a new leader: it did not take effect.
    Aborted(String),
}

#[derive(Debug)]
pub enum ReadOutcome {
    Value(Option<Bytes>),
    NotLeader(Option<NodeId>),
    Aborted(String),
}

const MAX_EVENTS_PER_ITERATION: usize = 4096;

pub struct RaftLoop {
    pub core: RaftCore<KvStateMachine>,
    pub db: Arc<Db>,
    pub events: Receiver<Event>,
    pub peers: HashMap<NodeId, mpsc::Sender<Envelope>>,
    pub tick: Duration,
    pub metrics: Arc<Metrics>,
    pending_writes: HashMap<u64, (u64, oneshot::Sender<WriteOutcome>)>,
    pending_reads: HashMap<u64, (Bytes, oneshot::Sender<ReadOutcome>)>,
    next_read_id: u64,
}

impl RaftLoop {
    pub fn new(
        core: RaftCore<KvStateMachine>,
        db: Arc<Db>,
        events: Receiver<Event>,
        peers: HashMap<NodeId, mpsc::Sender<Envelope>>,
        tick: Duration,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            core,
            db,
            events,
            peers,
            tick,
            metrics,
            pending_writes: HashMap::new(),
            pending_reads: HashMap::new(),
            next_read_id: 0,
        }
    }

    /// Runs until a fatal error, which is returned. Callers should treat
    /// it as fail-stop: the node's durable state is intact, but this
    /// process must not continue.
    pub fn run(mut self) -> io::Error {
        match self.run_inner() {
            Ok(()) => io::Error::other("event channel closed"),
            Err(e) => e,
        }
    }

    fn run_inner(&mut self) -> io::Result<()> {
        let mut next_tick = Instant::now() + self.tick;
        loop {
            let timeout = next_tick.saturating_duration_since(Instant::now());
            match self.events.recv_timeout(timeout) {
                Ok(ev) => self.handle(ev)?,
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return Ok(()),
            }
            for _ in 0..MAX_EVENTS_PER_ITERATION {
                match self.events.try_recv() {
                    Ok(ev) => self.handle(ev)?,
                    Err(_) => break,
                }
            }
            let now = Instant::now();
            if now >= next_tick {
                self.core.tick()?;
                next_tick += self.tick;
                if next_tick < now {
                    next_tick = now + self.tick; // fell behind; don't burst
                }
            }
            self.ready()?;
        }
    }

    fn handle(&mut self, ev: Event) -> io::Result<()> {
        match ev {
            Event::Peer(env) => self.core.step(env)?,
            Event::Propose { data, reply } => match self.core.propose(data) {
                Ok((index, term)) => {
                    if let Some((_, old)) = self.pending_writes.insert(index, (term, reply)) {
                        let _ = old.send(WriteOutcome::Aborted(
                            "entry overwritten by a new leader".into(),
                        ));
                    }
                }
                Err(RaftError::NotLeader(l)) => {
                    let _ = reply.send(WriteOutcome::NotLeader(l));
                }
                Err(RaftError::Io(e)) => return Err(e),
            },
            Event::Read { key, reply } => {
                let id = self.next_read_id;
                self.next_read_id += 1;
                match self.core.read(id) {
                    Ok(()) => {
                        self.pending_reads.insert(id, (key, reply));
                    }
                    Err(RaftError::NotLeader(l)) => {
                        let _ = reply.send(ReadOutcome::NotLeader(l));
                    }
                    Err(RaftError::Io(e)) => return Err(e),
                }
            }
            Event::Status { reply } => {
                let _ = reply.send(self.status_text());
            }
        }
        Ok(())
    }

    fn ready(&mut self) -> io::Result<()> {
        let early = self.core.take_early_messages()?;
        self.send(early);
        self.core.persist()?;
        let messages = self.core.take_messages();
        self.send(messages);
        let out = self.core.apply()?;
        self.deliver(out)
    }

    fn send(&self, messages: Vec<Envelope>) {
        for env in messages {
            if let Some(tx) = self.peers.get(&env.to) {
                // Never block consensus on a slow peer: Raft tolerates loss.
                if tx.try_send(env).is_err() {
                    self.metrics
                        .peer_messages_dropped
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    fn deliver(&mut self, out: crate::raft::ApplyOutput) -> io::Result<()> {
        for (index, term) in out.applied {
            if let Some((t, reply)) = self.pending_writes.remove(&index) {
                let _ = reply.send(if t == term {
                    WriteOutcome::Committed
                } else {
                    WriteOutcome::Aborted("entry overwritten by a new leader".into())
                });
            }
        }
        for id in out.reads_ready {
            if let Some((key, reply)) = self.pending_reads.remove(&id) {
                let _ = reply.send(match self.db.get(&key) {
                    Ok(v) => ReadOutcome::Value(v),
                    Err(e) => ReadOutcome::Aborted(e.to_string()),
                });
            }
        }
        for id in out.reads_failed {
            if let Some((_, reply)) = self.pending_reads.remove(&id) {
                let _ = reply.send(ReadOutcome::Aborted("leadership lost".into()));
            }
        }
        Ok(())
    }

    fn status_text(&self) -> String {
        let s = self.core.status();
        let db = self.db.stats();
        let m = &self.metrics;
        let mut out = String::new();
        let mut line = |k: &str, v: String| {
            let _ = writeln!(out, "{k}={v}");
        };
        line("id", s.id.to_string());
        line("role", format!("{:?}", s.role).to_lowercase());
        line("term", s.term.to_string());
        line("leader", s.leader.map_or("none".into(), |l| l.to_string()));
        line("commit_index", s.commit_index.to_string());
        line("applied_index", s.applied_index.to_string());
        line("last_index", s.last_index.to_string());
        line("snapshot_index", s.snapshot_index.to_string());
        line("pending_writes", self.pending_writes.len().to_string());
        line("pending_reads", self.pending_reads.len().to_string());
        line("db_tables", db.tables.to_string());
        line("db_table_bytes", db.table_bytes.to_string());
        line("db_table_entries", db.table_entries.to_string());
        line("db_memtable_bytes", db.memtable_bytes.to_string());
        line("db_flushes", db.flushes.to_string());
        line("db_compactions", db.compactions.to_string());
        line("db_write_stalls", db.write_stalls.to_string());
        for (k, v) in m.snapshot() {
            line(k, v.to_string());
        }
        out
    }
}
