use std::collections::{HashMap, HashSet};
use std::io;

use super::{Entry, Envelope, Message, NodeId, RaftLog, StateMachine};

#[derive(Clone, Debug)]
pub struct Config {
    /// Base election timeout in ticks; the actual timeout is randomized in
    /// `[election_ticks, 2 * election_ticks)`. Also the check-quorum period.
    pub election_ticks: u32,
    pub heartbeat_ticks: u32,
    pub max_entries_per_message: usize,
    /// Payload budget per AppendEntries (at least one entry is always sent).
    pub max_bytes_per_message: usize,
    /// Compact the log once this many applied entries sit in it.
    pub compact_threshold: u64,
    /// Ticks before an unanswered snapshot is sent again.
    pub snapshot_retry_ticks: u32,
    /// Seed for election-timeout randomization (determinism in tests).
    pub seed: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            election_ticks: 10,
            heartbeat_ticks: 2,
            max_entries_per_message: 512,
            max_bytes_per_message: 1024 * 1024,
            compact_threshold: 10_000,
            snapshot_retry_ticks: 40,
            seed: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
pub enum RaftError {
    /// Not the leader; carries the current leader if known.
    NotLeader(Option<NodeId>),
    Io(io::Error),
}

impl From<io::Error> for RaftError {
    fn from(e: io::Error) -> Self {
        RaftError::Io(e)
    }
}

#[derive(Clone, Debug)]
pub struct Status {
    pub id: NodeId,
    pub role: Role,
    pub term: u64,
    pub leader: Option<NodeId>,
    pub commit_index: u64,
    pub applied_index: u64,
    pub last_index: u64,
    pub snapshot_index: u64,
}

/// What the driver must act on after [`RaftCore::apply`].
#[derive(Debug, Default)]
pub struct ApplyOutput {
    /// `(index, term)` of every entry applied in this call. A proposal
    /// succeeded iff its `(index, term)` appears here.
    pub applied: Vec<(u64, u64)>,
    /// Read ids that may now be served from the state machine.
    pub reads_ready: Vec<u64>,
    /// Read ids that can no longer be confirmed (leadership lost).
    pub reads_failed: Vec<u64>,
}

struct Progress {
    next: u64,
    matched: u64,
    /// Highest read-index round this peer has acknowledged.
    ack_seq: u64,
    /// Ticks since a snapshot was sent, while one is outstanding.
    snapshot_sent: Option<u32>,
    /// Heard from in the current check-quorum period.
    active: bool,
}

struct PendingRead {
    id: u64,
    index: u64,
    seq: u64,
    confirmed: bool,
}

pub struct RaftCore<S: StateMachine> {
    id: NodeId,
    peers: Vec<NodeId>,
    cfg: Config,
    log: RaftLog,
    sm: S,

    role: Role,
    leader: Option<NodeId>,
    commit: u64,
    applied: u64,

    election_elapsed: u32,
    election_timeout: u32,
    heartbeat_elapsed: u32,
    rng: u64,

    votes: HashSet<NodeId>,
    progress: HashMap<NodeId, Progress>,
    /// Last log index known to be on our own disk.
    durable_index: u64,
    /// Index of this leader's no-op entry.
    term_start: u64,
    pending_broadcast: bool,

    reads: Vec<PendingRead>,
    failed_reads: Vec<u64>,
    read_seq: u64,

    outbox: Vec<Envelope>,
}

impl<S: StateMachine> RaftCore<S> {
    /// `peers` excludes `id`. Node ids must be non-zero.
    pub fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        mut log: RaftLog,
        sm: S,
        cfg: Config,
    ) -> io::Result<Self> {
        assert!(id != 0 && !peers.contains(&id) && !peers.contains(&0));
        let (applied, applied_term) = sm.applied();
        if applied < log.snapshot_index() {
            return Err(io::Error::other(format!(
                "state machine at {applied} is behind the log snapshot at {}",
                log.snapshot_index()
            )));
        }
        // Crash between `restore` and `reset` while installing a snapshot:
        // the state machine is ahead of the log, so restart the log there.
        if log.term(applied) != Some(applied_term) && applied > 0 {
            log.reset(applied, applied_term)?;
        }
        let mut core = Self {
            id,
            peers,
            log,
            sm,
            role: Role::Follower,
            leader: None,
            commit: applied,
            applied,
            election_elapsed: 0,
            election_timeout: 0,
            heartbeat_elapsed: 0,
            rng: (cfg.seed ^ id.wrapping_mul(0x9e37_79b9_7f4a_7c15)) | 1,
            cfg,
            votes: HashSet::new(),
            progress: HashMap::new(),
            durable_index: 0,
            term_start: 0,
            pending_broadcast: false,
            reads: Vec::new(),
            failed_reads: Vec::new(),
            read_seq: 0,
            outbox: Vec::new(),
        };
        core.durable_index = core.log.last_index();
        core.reset_election_timer();
        Ok(core)
    }

    // ----- accessors -------------------------------------------------------

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn term(&self) -> u64 {
        self.log.hard_state().0
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    pub fn state_machine(&self) -> &S {
        &self.sm
    }

    pub fn log(&self) -> &RaftLog {
        &self.log
    }

    pub fn status(&self) -> Status {
        Status {
            id: self.id,
            role: self.role,
            term: self.term(),
            leader: self.leader,
            commit_index: self.commit,
            applied_index: self.applied,
            last_index: self.log.last_index(),
            snapshot_index: self.log.snapshot_index(),
        }
    }

    /// Splits the node into its durable parts (what survives a crash).
    pub fn into_parts(self) -> (RaftLog, S) {
        (self.log, self.sm)
    }

    fn quorum(&self) -> usize {
        let cluster_size = self.peers.len() + 1;
        cluster_size / 2 + 1
    }

    fn next_random(&mut self) -> u64 {
        // xorshift64*
        self.rng ^= self.rng >> 12;
        self.rng ^= self.rng << 25;
        self.rng ^= self.rng >> 27;
        self.rng.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        let base = self.cfg.election_ticks.max(1);
        self.election_timeout = base + (self.next_random() % base as u64) as u32;
    }

    fn send(&mut self, to: NodeId, msg: Message) {
        self.outbox.push(Envelope {
            from: self.id,
            to,
            msg,
        });
    }

    // ----- inputs ----------------------------------------------------------

    pub fn tick(&mut self) -> io::Result<()> {
        match self.role {
            Role::Leader => {
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= self.cfg.heartbeat_ticks {
                    self.heartbeat_elapsed = 0;
                    self.broadcast_append()?;
                }
                for p in self.progress.values_mut() {
                    if let Some(t) = &mut p.snapshot_sent {
                        *t += 1;
                        if *t >= self.cfg.snapshot_retry_ticks {
                            p.snapshot_sent = None;
                        }
                    }
                }
                // Check quorum: a leader that cannot reach a majority for a
                // whole election period steps down instead of lingering as a
                // stale leader on the minority side of a partition.
                self.election_elapsed += 1;
                if self.election_elapsed >= self.cfg.election_ticks {
                    self.election_elapsed = 0;
                    let active = 1 + self.progress.values().filter(|p| p.active).count();
                    for p in self.progress.values_mut() {
                        p.active = false;
                    }
                    if active < self.quorum() {
                        tracing::info!(
                            id = self.id,
                            term = self.term(),
                            "lost quorum, stepping down"
                        );
                        self.become_follower(self.term(), None)?;
                    }
                }
            }
            Role::Follower | Role::Candidate => {
                self.election_elapsed += 1;
                if self.election_elapsed >= self.election_timeout {
                    self.campaign()?;
                }
            }
        }
        Ok(())
    }

    /// Appends a command to the leader's log. Returns `(index, term)`: the
    /// command succeeded iff that pair is later reported as applied.
    pub fn propose(&mut self, data: Vec<u8>) -> Result<(u64, u64), RaftError> {
        if self.role != Role::Leader {
            return Err(RaftError::NotLeader(self.leader));
        }
        assert!(!data.is_empty(), "empty data is reserved for no-op entries");
        let entry = Entry {
            index: self.log.last_index() + 1,
            term: self.term(),
            data,
        };
        let pos = (entry.index, entry.term);
        self.log.append(&[entry])?;
        self.pending_broadcast = true;
        Ok(pos)
    }

    /// Starts a linearizable read (Raft §6.4 "ReadIndex"). The read may be
    /// served once `id` shows up in [`ApplyOutput::reads_ready`].
    pub fn read(&mut self, id: u64) -> Result<(), RaftError> {
        if self.role != Role::Leader {
            return Err(RaftError::NotLeader(self.leader));
        }
        self.read_seq += 1;
        // `term_start` (our no-op) is at least every index committed by
        // earlier leaders, so this is a safe lower bound for the commit
        // index even before our own no-op commits.
        self.reads.push(PendingRead {
            id,
            index: self.commit.max(self.term_start),
            seq: self.read_seq,
            confirmed: self.peers.is_empty(),
        });
        self.pending_broadcast = true;
        Ok(())
    }

    pub fn step(&mut self, env: Envelope) -> io::Result<()> {
        let from = env.from;
        if env.to != self.id || !self.peers.contains(&from) {
            return Ok(());
        }
        let term = self.term();
        let msg_term = env.msg.term();
        if msg_term > term {
            let leader = matches!(
                env.msg,
                Message::AppendEntries { .. } | Message::InstallSnapshot { .. }
            )
            .then_some(from);
            self.become_follower(msg_term, leader)?;
        } else if msg_term < term {
            // Stale sender: tell it about the newer term.
            let term = self.term();
            match env.msg {
                Message::RequestVote { .. } => self.send(
                    from,
                    Message::VoteResponse {
                        term,
                        granted: false,
                    },
                ),
                Message::AppendEntries { seq, .. } => self.send(
                    from,
                    Message::AppendResponse {
                        term,
                        success: false,
                        match_index: 0,
                        conflict_index: 0,
                        seq,
                    },
                ),
                Message::InstallSnapshot { .. } => self.send(
                    from,
                    Message::SnapshotResponse {
                        term,
                        last_index: 0,
                    },
                ),
                _ => {}
            }
            return Ok(());
        }

        match env.msg {
            Message::RequestVote {
                last_log_index,
                last_log_term,
                ..
            } => self.handle_vote_request(from, last_log_index, last_log_term),
            Message::VoteResponse { granted, .. } => {
                if self.role == Role::Candidate && granted {
                    self.votes.insert(from);
                    if self.votes.len() >= self.quorum() {
                        self.become_leader()?;
                    }
                }
                Ok(())
            }
            Message::AppendEntries {
                prev_index,
                prev_term,
                entries,
                commit,
                seq,
                ..
            } => self.handle_append(from, prev_index, prev_term, entries, commit, seq),
            Message::AppendResponse {
                success,
                match_index,
                conflict_index,
                seq,
                ..
            } => self.handle_append_response(from, success, match_index, conflict_index, seq),
            Message::InstallSnapshot {
                last_index,
                last_term,
                data,
                ..
            } => self.handle_snapshot(from, last_index, last_term, &data),
            Message::SnapshotResponse { last_index, .. } => {
                if self.role == Role::Leader {
                    if let Some(p) = self.progress.get_mut(&from) {
                        p.active = true;
                        p.snapshot_sent = None;
                        p.matched = p.matched.max(last_index);
                        p.next = p.next.max(last_index + 1);
                    }
                    self.send_append(from)?;
                }
                Ok(())
            }
        }
    }

    // ----- ready loop ------------------------------------------------------

    /// Makes appended entries durable. Must run after inputs and before the
    /// outbox is sent or anything is applied.
    pub fn persist(&mut self) -> io::Result<()> {
        self.log.sync()?;
        self.durable_index = self.log.last_index();
        if self.role == Role::Leader {
            self.maybe_commit();
            if self.pending_broadcast {
                self.broadcast_append()?;
            }
        }
        self.pending_broadcast = false;
        Ok(())
    }

    /// Replication messages from a leader do not depend on the leader's own
    /// log being durable — it only counts itself toward commit after
    /// `persist` — so they may be sent *before* the fsync, overlapping the
    /// leader's disk write with the followers' (Raft thesis §10.2.1).
    /// Everything else (votes, acks) must wait for `persist`.
    pub fn take_early_messages(&mut self) -> io::Result<Vec<Envelope>> {
        if self.role == Role::Leader && self.pending_broadcast {
            self.broadcast_append()?;
            self.pending_broadcast = false;
        }
        let (early, rest) = std::mem::take(&mut self.outbox).into_iter().partition(|e| {
            matches!(
                e.msg,
                Message::AppendEntries { .. } | Message::InstallSnapshot { .. }
            )
        });
        self.outbox = rest;
        Ok(early)
    }

    pub fn take_messages(&mut self) -> Vec<Envelope> {
        std::mem::take(&mut self.outbox)
    }

    /// Applies committed entries to the state machine, releases ready
    /// reads, and compacts the log when it grows past the threshold.
    pub fn apply(&mut self) -> io::Result<ApplyOutput> {
        let mut out = ApplyOutput {
            reads_failed: std::mem::take(&mut self.failed_reads),
            ..Default::default()
        };
        while self.applied < self.commit {
            let n = (self.commit - self.applied).min(1024) as usize;
            let entries = self.log.entries(self.applied + 1, n, usize::MAX);
            if entries.is_empty() {
                return Err(io::Error::other(format!(
                    "committed entry {} missing from log",
                    self.applied + 1
                )));
            }
            self.sm.apply(&entries)?;
            self.applied = entries.last().unwrap().index;
            out.applied
                .extend(entries.iter().map(|e| (e.index, e.term)));
        }

        let applied = self.applied;
        self.reads.retain(|r| {
            let ready = r.confirmed && r.index <= applied;
            if ready {
                out.reads_ready.push(r.id);
            }
            !ready
        });

        let threshold = self.cfg.compact_threshold.max(1);
        if self.applied - self.log.snapshot_index() >= threshold {
            // Never discard entries the state machine might still lose.
            self.sm.sync()?;
            // Keep a tail so slightly lagging followers do not need a full
            // snapshot.
            self.log.compact(self.applied - threshold / 2)?;
        }
        Ok(out)
    }

    // ----- role transitions ------------------------------------------------

    fn become_follower(&mut self, term: u64, leader: Option<NodeId>) -> io::Result<()> {
        if term > self.term() {
            self.log.set_hard_state(term, None)?;
        }
        if self.role == Role::Leader {
            self.failed_reads.extend(self.reads.drain(..).map(|r| r.id));
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.reset_election_timer();
        Ok(())
    }

    fn campaign(&mut self) -> io::Result<()> {
        let term = self.term() + 1;
        self.log.set_hard_state(term, Some(self.id))?; // durable before RequestVote
        self.role = Role::Candidate;
        self.leader = None;
        self.votes = HashSet::from([self.id]);
        self.reset_election_timer();
        tracing::debug!(id = self.id, term, "starting election");
        if self.votes.len() >= self.quorum() {
            return self.become_leader();
        }
        let (last_log_index, last_log_term) = (self.log.last_index(), self.log.last_term());
        for peer in self.peers.clone() {
            self.send(
                peer,
                Message::RequestVote {
                    term,
                    last_log_index,
                    last_log_term,
                },
            );
        }
        Ok(())
    }

    fn become_leader(&mut self) -> io::Result<()> {
        tracing::info!(id = self.id, term = self.term(), "became leader");
        self.role = Role::Leader;
        self.leader = Some(self.id);
        self.heartbeat_elapsed = 0;
        self.election_elapsed = 0;
        let next = self.log.last_index() + 1;
        self.progress = self
            .peers
            .iter()
            .map(|&p| {
                (
                    p,
                    Progress {
                        next,
                        matched: 0,
                        ack_seq: 0,
                        snapshot_sent: None,
                        active: false,
                    },
                )
            })
            .collect();
        // A no-op from the new term: entries from earlier terms can only be
        // committed indirectly, by committing an entry of the current term.
        let noop = Entry {
            index: next,
            term: self.term(),
            data: Vec::new(),
        };
        self.term_start = noop.index;
        self.log.append(&[noop])?;
        self.pending_broadcast = true;
        Ok(())
    }

    // ----- handlers --------------------------------------------------------

    fn handle_vote_request(
        &mut self,
        from: NodeId,
        last_index: u64,
        last_term: u64,
    ) -> io::Result<()> {
        let (term, vote) = self.log.hard_state();
        let up_to_date = (last_term, last_index) >= (self.log.last_term(), self.log.last_index());
        let granted = up_to_date && vote.is_none_or(|v| v == from);
        if granted {
            self.log.set_hard_state(term, Some(from))?; // durable before the reply
            self.reset_election_timer();
        }
        self.send(from, Message::VoteResponse { term, granted });
        Ok(())
    }

    fn handle_append(
        &mut self,
        from: NodeId,
        prev_index: u64,
        prev_term: u64,
        mut entries: Vec<Entry>,
        leader_commit: u64,
        seq: u64,
    ) -> io::Result<()> {
        if self.role == Role::Leader {
            // Two leaders in one term would violate election safety.
            return Err(io::Error::other(format!(
                "node {} received AppendEntries from {from} while leader of term {}",
                self.id,
                self.term()
            )));
        }
        self.role = Role::Follower;
        self.leader = Some(from);
        self.reset_election_timer();
        let term = self.term();

        // Entries at or below our snapshot are committed, hence identical.
        let snap = self.log.snapshot_index();
        let (prev_index, prev_term) = if prev_index < snap {
            entries.retain(|e| e.index > snap);
            (snap, self.log.snapshot_term())
        } else {
            (prev_index, prev_term)
        };

        if self.log.term(prev_index) != Some(prev_term) {
            let conflict_index = match self.log.term(prev_index) {
                None => self.log.last_index() + 1,
                Some(t) => {
                    // Skip back over the whole conflicting term.
                    let mut i = prev_index;
                    while i > snap + 1 && self.log.term(i - 1) == Some(t) {
                        i -= 1;
                    }
                    i
                }
            };
            self.send(
                from,
                Message::AppendResponse {
                    term,
                    success: false,
                    match_index: 0,
                    conflict_index,
                    seq,
                },
            );
            return Ok(());
        }

        let last_new = prev_index + entries.len() as u64;
        for (i, e) in entries.iter().enumerate() {
            match self.log.term(e.index) {
                Some(t) if t == e.term => continue,
                Some(_) => {
                    if e.index <= self.commit {
                        return Err(io::Error::other(format!(
                            "leader {from} tried to overwrite committed entry {}",
                            e.index
                        )));
                    }
                    self.log.truncate_from(e.index)?;
                    self.log.append(&entries[i..])?;
                }
                None => self.log.append(&entries[i..])?,
            }
            break;
        }
        self.commit = self.commit.max(leader_commit.min(last_new));
        self.send(
            from,
            Message::AppendResponse {
                term,
                success: true,
                match_index: last_new,
                conflict_index: 0,
                seq,
            },
        );
        Ok(())
    }

    fn handle_append_response(
        &mut self,
        from: NodeId,
        success: bool,
        match_index: u64,
        conflict_index: u64,
        seq: u64,
    ) -> io::Result<()> {
        if self.role != Role::Leader {
            return Ok(());
        }
        let last = self.log.last_index();
        let Some(p) = self.progress.get_mut(&from) else {
            return Ok(());
        };
        p.active = true;
        p.ack_seq = p.ack_seq.max(seq);
        let more = if success {
            p.matched = p.matched.max(match_index);
            p.next = p.next.max(p.matched + 1);
            p.next <= last
        } else {
            p.next = conflict_index.max(p.matched + 1).min(last + 1);
            true
        };
        self.confirm_reads();
        if success {
            self.maybe_commit();
        }
        if more {
            self.send_append(from)?;
        }
        Ok(())
    }

    fn handle_snapshot(
        &mut self,
        from: NodeId,
        last_index: u64,
        last_term: u64,
        data: &[u8],
    ) -> io::Result<()> {
        self.role = Role::Follower;
        self.leader = Some(from);
        self.reset_election_timer();
        let term = self.term();
        if last_index > self.commit {
            tracing::info!(id = self.id, last_index, "installing snapshot");
            // Order matters for crash safety: the state machine first; if we
            // crash before `reset`, `new` notices the state machine is ahead
            // and resets the log itself.
            self.sm.restore(last_index, last_term, data)?;
            self.log.reset(last_index, last_term)?;
            self.durable_index = last_index;
            self.commit = last_index;
            self.applied = last_index;
        }
        let last_index = self.commit;
        self.send(from, Message::SnapshotResponse { term, last_index });
        Ok(())
    }

    // ----- leader internals ------------------------------------------------

    fn confirm_reads(&mut self) {
        let quorum = self.quorum();
        for r in self.reads.iter_mut().filter(|r| !r.confirmed) {
            let acks = 1 + self
                .progress
                .values()
                .filter(|p| p.ack_seq >= r.seq)
                .count();
            r.confirmed = acks >= quorum;
        }
    }

    fn maybe_commit(&mut self) {
        let mut matched: Vec<u64> = self.progress.values().map(|p| p.matched).collect();
        matched.push(self.durable_index);
        matched.sort_unstable_by(|a, b| b.cmp(a));
        let n = matched[self.quorum() - 1];
        // Only entries from the current term are committed by counting
        // replicas (Raft §5.4.2).
        if n > self.commit && self.log.term(n) == Some(self.term()) {
            self.commit = n;
            self.pending_broadcast = true;
        }
    }

    fn broadcast_append(&mut self) -> io::Result<()> {
        for peer in self.peers.clone() {
            self.send_append(peer)?;
        }
        Ok(())
    }

    fn send_append(&mut self, to: NodeId) -> io::Result<()> {
        let term = self.term();
        let (commit, seq) = (self.commit, self.read_seq);
        let (max, max_bytes) = (
            self.cfg.max_entries_per_message,
            self.cfg.max_bytes_per_message,
        );
        let Some(p) = self.progress.get(&to) else {
            return Ok(());
        };
        let prev_index = p.next - 1;
        match self.log.term(prev_index) {
            Some(prev_term) => {
                let entries = self.log.entries(p.next, max, max_bytes);
                if let Some(last) = entries.last() {
                    // Optimistic pipelining; a rejection rewinds `next`.
                    self.progress.get_mut(&to).unwrap().next = last.index + 1;
                }
                self.send(
                    to,
                    Message::AppendEntries {
                        term,
                        prev_index,
                        prev_term,
                        entries,
                        commit,
                        seq,
                    },
                );
            }
            None => {
                // The entries this follower needs were compacted away.
                if p.snapshot_sent.is_some() {
                    return Ok(());
                }
                let (last_index, last_term) = self.sm.applied();
                let data = self.sm.snapshot()?;
                tracing::info!(
                    id = self.id,
                    to,
                    last_index,
                    bytes = data.len(),
                    "sending snapshot"
                );
                self.progress.get_mut(&to).unwrap().snapshot_sent = Some(0);
                self.send(
                    to,
                    Message::InstallSnapshot {
                        term,
                        last_index,
                        last_term,
                        data,
                    },
                );
            }
        }
        Ok(())
    }
}
