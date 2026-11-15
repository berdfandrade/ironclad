# ADR 0002 — Raft as a deterministic core, driven by a ready loop

**Status:** accepted

## Context

Consensus bugs appear only under rare interleavings of crashes and message
delays. Tests against real sockets and timers can't reproduce those
interleavings on demand, and a failure there usually can't be replayed.

## Decision

`RaftCore` is a plain state machine with no network, no clock and no
threads. Its inputs are `tick()`, `step(message)`, `propose(data)` and
`read(id)`. Its outputs are pulled by the driver in a fixed sequence, the
*ready loop*:

```
inputs… → take_early_messages() → persist() → take_messages() → apply()
```

The only I/O the core performs is through its durable `RaftLog`, which has
an in-memory mode. Randomized election timeouts come from a seeded PRNG.

Two drivers run the same core:

* `server::node::RaftLoop` — a dedicated OS thread fed by a channel, with
  TCP peers;
* `tests/raft_sim.rs` — a simulator with a virtual clock and a lossy,
  reordering, partitionable network, where crashes keep only the durable
  parts (`into_parts()` → log + state machine).

The implementation covers leader election with randomized timeouts,
log replication with fast conflict backtracking (skip the whole
conflicting term), the current-term commit rule (with a no-op on election),
check-quorum, ReadIndex reads, log compaction and InstallSnapshot.

## Consequences

* Every simulated run is reproducible from its seed; the suite checks
  election safety, state-machine safety, durability of acknowledged writes
  and read linearizability on every step, across 60 seeds.
* The driver has a small, explicit contract to honour ("persist before
  acknowledging"), documented in `raft/mod.rs`, instead of the core doing
  I/O behind its back.
* The core is synchronous: fsyncs block its thread, which is why it runs on
  a dedicated thread rather than a tokio task (see ARCHITECTURE).
