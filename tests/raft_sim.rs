//! Deterministic simulation of a Raft cluster under faults.
//!
//! The real `RaftCore` runs against an in-memory network that delays,
//! reorders, drops and partitions messages, while nodes crash and restart.
//! Every run is reproducible from its seed. After each step the simulator
//! checks Raft's safety properties; at the end it heals everything and
//! checks that all nodes converge.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;

use ironclad::raft::{Config, Entry, Envelope, RaftCore, RaftError, RaftLog, Role, StateMachine};

/// Records every applied command in order.
#[derive(Default)]
struct RecordingSm {
    applied: (u64, u64),
    data: Vec<Vec<u8>>,
}

impl StateMachine for RecordingSm {
    fn applied(&self) -> (u64, u64) {
        self.applied
    }

    fn apply(&mut self, entries: &[Entry]) -> io::Result<()> {
        for e in entries {
            assert_eq!(e.index, self.applied.0 + 1, "entries applied out of order");
            if !e.data.is_empty() {
                self.data.push(e.data.clone());
            }
            self.applied = (e.index, e.term);
        }
        Ok(())
    }

    fn snapshot(&self) -> io::Result<Vec<u8>> {
        Ok(bincode::serialize(&self.data).unwrap())
    }

    fn restore(&mut self, index: u64, term: u64, data: &[u8]) -> io::Result<()> {
        self.data = bincode::deserialize(data).unwrap();
        self.applied = (index, term);
        Ok(())
    }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn chance(&mut self, p: f64) -> bool {
        (self.next() % 1_000_000) as f64 / 1_000_000.0 < p
    }
}

#[allow(clippy::large_enum_variant)]
enum NodeState {
    Up(RaftCore<RecordingSm>),
    Down(RaftLog, RecordingSm),
}

struct Faults {
    drop_rate: f64,
    max_delay: u64,
    crash_rate: f64,
    restart_rate: f64,
    partition_rate: f64,
}

struct Sim {
    ids: Vec<u64>,
    nodes: BTreeMap<u64, Option<NodeState>>,
    rng: Rng,
    now: u64,
    network: Vec<(u64, Envelope)>,
    /// Nodes on one side of the current partition (None = fully connected).
    partition: Option<HashSet<u64>>,
    faults: Faults,
    cfg: Config,

    // Invariant tracking.
    leaders: HashMap<u64, u64>,
    applied_terms: HashMap<u64, u64>,
    proposals: HashMap<(u64, u64), Vec<u8>>,
    acked: Vec<Vec<u8>>,
    reads: HashMap<(u64, u64), usize>,
    next_payload: u64,
    next_read: u64,
    stats: HashMap<&'static str, u64>,
}

impl Sim {
    fn new(n: u64, seed: u64, faults: Faults) -> Self {
        let ids: Vec<u64> = (1..=n).collect();
        let cfg = Config {
            election_ticks: 10,
            heartbeat_ticks: 2,
            compact_threshold: 40,
            snapshot_retry_ticks: 20,
            seed,
            ..Config::default()
        };
        let mut sim = Self {
            nodes: BTreeMap::new(),
            rng: Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1),
            now: 0,
            network: Vec::new(),
            partition: None,
            faults,
            cfg,
            leaders: HashMap::new(),
            applied_terms: HashMap::new(),
            proposals: HashMap::new(),
            acked: Vec::new(),
            reads: HashMap::new(),
            next_payload: 0,
            next_read: 0,
            stats: HashMap::new(),
            ids: ids.clone(),
        };
        for &id in &ids {
            sim.nodes.insert(
                id,
                Some(NodeState::Down(RaftLog::memory(), RecordingSm::default())),
            );
            sim.restart(id);
        }
        sim
    }

    fn peers(&self, id: u64) -> Vec<u64> {
        self.ids.iter().copied().filter(|&p| p != id).collect()
    }

    fn core(&mut self, id: u64) -> Option<&mut RaftCore<RecordingSm>> {
        match self.nodes.get_mut(&id).unwrap() {
            Some(NodeState::Up(c)) => Some(c),
            _ => None,
        }
    }

    fn restart(&mut self, id: u64) {
        let peers = self.peers(id);
        let cfg = self.cfg.clone();
        let slot = self.nodes.get_mut(&id).unwrap();
        if matches!(slot, Some(NodeState::Down(..))) {
            let Some(NodeState::Down(log, sm)) = slot.take() else {
                unreachable!()
            };
            *slot = Some(NodeState::Up(
                RaftCore::new(id, peers, log, sm, cfg).unwrap(),
            ));
        }
    }

    fn crash(&mut self, id: u64) {
        let slot = self.nodes.get_mut(&id).unwrap();
        if matches!(slot, Some(NodeState::Up(_))) {
            let Some(NodeState::Up(core)) = slot.take() else {
                unreachable!()
            };
            let (log, sm) = core.into_parts();
            *slot = Some(NodeState::Down(log, sm));
            // Pending reads die with the node. Pending proposals stay
            // tracked: they may still commit through another leader, and
            // if they do they must never be lost.
            self.reads.retain(|(node, _), _| *node != id);
        }
    }

    fn connected(&self, a: u64, b: u64) -> bool {
        match &self.partition {
            None => true,
            Some(side) => side.contains(&a) == side.contains(&b),
        }
    }

    /// The ready loop, exactly as the real driver runs it.
    fn ready(&mut self, id: u64) {
        let Some(core) = self.core(id) else { return };
        let mut messages = core.take_early_messages().unwrap();
        core.persist().unwrap();
        messages.extend(core.take_messages());
        let out = core.apply().unwrap();
        let role = core.role();
        let term = core.term();
        let snapshot_data: Vec<Vec<u8>> = if out.reads_ready.is_empty() {
            Vec::new()
        } else {
            core.state_machine().data.clone()
        };

        if role == Role::Leader {
            if let Some(&other) = self.leaders.get(&term) {
                assert_eq!(other, id, "two leaders in term {term}");
            }
            self.leaders.insert(term, id);
        }
        for (index, term) in &out.applied {
            if let Some(&t) = self.applied_terms.get(index) {
                assert_eq!(t, *term, "nodes applied different entries at index {index}");
            }
            self.applied_terms.insert(*index, *term);
            if let Some(data) = self.proposals.remove(&(*index, *term)) {
                self.acked.push(data);
            }
        }
        for read in out.reads_ready {
            if let Some(required) = self.reads.remove(&(id, read)) {
                // Linearizability: a read sees every write acknowledged
                // before the read started.
                let have: HashSet<&Vec<u8>> = snapshot_data.iter().collect();
                for w in &self.acked[..required] {
                    assert!(have.contains(w), "stale read on node {id}");
                }
                *self.stats.entry("reads").or_default() += 1;
            }
        }
        for read in out.reads_failed {
            self.reads.remove(&(id, read));
        }

        for env in messages {
            // Round-trip through the wire encoding.
            let env: Envelope = bincode::deserialize(&bincode::serialize(&env).unwrap()).unwrap();
            if self.rng.chance(self.faults.drop_rate) || !self.connected(env.from, env.to) {
                *self.stats.entry("dropped").or_default() += 1;
                continue;
            }
            let delay = 1 + self.rng.below(self.faults.max_delay);
            self.network.push((self.now + delay, env));
        }
    }

    fn step(&mut self) {
        self.now += 1;

        for id in self.ids.clone() {
            if let Some(core) = self.core(id) {
                core.tick().unwrap();
                self.ready(id);
            }
        }

        let now = self.now;
        let (due, later): (Vec<_>, Vec<_>) = self.network.drain(..).partition(|(t, _)| *t <= now);
        self.network = later;
        for (_, env) in due {
            let to = env.to;
            if !self.connected(env.from, to) {
                continue;
            }
            if let Some(core) = self.core(to) {
                core.step(env).unwrap();
                self.ready(to);
            }
        }

        // Client traffic: send to a random node; only leaders accept.
        for _ in 0..2 {
            let id = self.ids[self.rng.below(self.ids.len() as u64) as usize];
            let payload = format!("cmd-{}", self.next_payload).into_bytes();
            let read_id = self.next_read;
            let acked_so_far = self.acked.len();
            let is_read = self.rng.chance(0.3);
            let Some(core) = self.core(id) else { continue };
            if is_read {
                if core.read(read_id).is_ok() {
                    self.next_read += 1;
                    self.reads.insert((id, read_id), acked_so_far);
                }
            } else {
                match core.propose(payload.clone()) {
                    Ok(pos) => {
                        self.next_payload += 1;
                        self.proposals.insert(pos, payload);
                    }
                    Err(RaftError::NotLeader(_)) => {}
                    Err(RaftError::Io(e)) => panic!("{e}"),
                }
            }
            self.ready(id);
        }

        // Faults.
        if self.rng.chance(self.faults.crash_rate) {
            let id = self.ids[self.rng.below(self.ids.len() as u64) as usize];
            let up = self
                .ids
                .iter()
                .filter(|&&i| matches!(self.nodes[&i], Some(NodeState::Up(_))))
                .count();
            if up > self.ids.len() / 2 + 1 || self.rng.chance(0.2) {
                self.crash(id);
                *self.stats.entry("crashes").or_default() += 1;
            }
        }
        for id in self.ids.clone() {
            if matches!(self.nodes[&id], Some(NodeState::Down(..)))
                && self.rng.chance(self.faults.restart_rate)
            {
                self.restart(id);
            }
        }
        if self.rng.chance(self.faults.partition_rate) {
            if self.partition.is_some() {
                self.partition = None;
            } else {
                let side: HashSet<u64> = self
                    .ids
                    .iter()
                    .copied()
                    .filter(|_| self.rng.chance(0.5))
                    .collect();
                self.partition = Some(side);
                *self.stats.entry("partitions").or_default() += 1;
            }
        }
    }

    fn heal_and_converge(&mut self) {
        self.partition = None;
        self.faults.drop_rate = 0.0;
        for id in self.ids.clone() {
            self.restart(id);
        }
        // Stop client writes; just let the cluster settle.
        for _ in 0..400 {
            self.now += 1;
            for id in self.ids.clone() {
                if let Some(core) = self.core(id) {
                    core.tick().unwrap();
                }
                self.ready(id);
            }
            let now = self.now;
            let (due, later): (Vec<_>, Vec<_>) =
                self.network.drain(..).partition(|(t, _)| *t <= now);
            self.network = later;
            for (_, env) in due {
                let to = env.to;
                if let Some(core) = self.core(to) {
                    core.step(env).unwrap();
                }
                self.ready(to);
            }
        }

        let states: Vec<(u64, Vec<Vec<u8>>)> = self
            .ids
            .clone()
            .into_iter()
            .map(|id| {
                let core = self.core(id).unwrap();
                (
                    core.status().applied_index,
                    core.state_machine().data.clone(),
                )
            })
            .collect();
        let (index0, data0) = &states[0];
        for (i, (index, data)) in states.iter().enumerate() {
            assert_eq!(index, index0, "node {} applied index differs", i + 1);
            assert_eq!(data, data0, "node {} state differs", i + 1);
        }
        let have: HashSet<&Vec<u8>> = data0.iter().collect();
        assert_eq!(have.len(), data0.len(), "a command was applied twice");
        for w in &self.acked {
            assert!(
                have.contains(w),
                "acknowledged write lost: {}",
                String::from_utf8_lossy(w)
            );
        }
        assert!(
            self.ids
                .iter()
                .filter(|&&id| {
                    matches!(&self.nodes[&id], Some(NodeState::Up(c)) if c.role() == Role::Leader)
                })
                .count()
                == 1,
            "no single leader after healing"
        );
    }
}

fn run(seed: u64, nodes: u64, ticks: u64, faults: Faults) -> HashMap<&'static str, u64> {
    let mut sim = Sim::new(nodes, seed, faults);
    for _ in 0..ticks {
        sim.step();
    }
    sim.heal_and_converge();
    let mut stats = sim.stats;
    stats.insert("acked", sim.acked.len() as u64);
    stats.insert("terms", sim.leaders.len() as u64);
    stats
}

#[test]
fn reliable_network_makes_progress() {
    let stats = run(
        1,
        3,
        500,
        Faults {
            drop_rate: 0.0,
            max_delay: 2,
            crash_rate: 0.0,
            restart_rate: 0.0,
            partition_rate: 0.0,
        },
    );
    assert!(stats["acked"] > 200, "{stats:?}");
    assert_eq!(stats["terms"], 1, "unexpected re-election: {stats:?}");
}

#[test]
fn single_node_cluster() {
    let stats = run(
        7,
        1,
        200,
        Faults {
            drop_rate: 0.0,
            max_delay: 1,
            crash_rate: 0.01,
            restart_rate: 0.2,
            partition_rate: 0.0,
        },
    );
    assert!(stats["acked"] > 50, "{stats:?}");
}

#[test]
fn safety_under_faults_many_seeds() {
    let mut total: HashMap<&'static str, u64> = HashMap::new();
    for seed in 1..=60 {
        let stats = run(
            seed,
            if seed % 2 == 0 { 5 } else { 3 },
            1500,
            Faults {
                drop_rate: 0.05,
                max_delay: 6,
                crash_rate: 0.01,
                restart_rate: 0.05,
                partition_rate: 0.01,
            },
        );
        for (k, v) in stats {
            *total.entry(k).or_default() += v;
        }
    }
    println!("{total:?}");
    // Make sure the faults actually happened and work still got done.
    for key in ["acked", "crashes", "partitions", "dropped", "reads"] {
        assert!(
            total.get(key).copied().unwrap_or(0) > 0,
            "no {key}: {total:?}"
        );
    }
}
