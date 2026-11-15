# ADR 0004 — The Raft log is the write-ahead log of the cluster

**Status:** accepted

## Context

A replicated write was fsynced twice on every node: once into the Raft log
(required for safety) and once into the storage engine's WAL when applied.
On a disk with ~10 ms fsync latency, the two fsyncs ran back to back in
every Raft loop iteration, and the leader fsynced its own log before
replicating it, adding a third serialized fsync to every commit.

## Decision

1. **Apply without fsync.** Under Raft the storage engine runs with
   `sync_writes = false`. Writes still reach the kernel immediately, so a
   process crash loses nothing. After power loss the engine may roll back to
   an earlier batch, but the applied `(index, term)` is in the same atomic
   batch, so it rolls back too and the lost entries are re-applied from the
   Raft log.
2. **Never compact the log past what the engine has made durable.**
   `StateMachine::sync()` (an engine WAL fsync) runs before every Raft log
   compaction. Snapshot restore fsyncs before the log is reset.
3. **Send AppendEntries before the leader's own fsync.** The leader counts
   itself toward a majority only after `persist()` has fsynced its log, so
   replication messages may go out first (Raft thesis §10.2.1). The core
   exposes these as `take_early_messages()`; votes and acknowledgements
   still wait for `persist()`.

`--storage-fsync` restores per-batch engine fsync for operators who want
it.

## Measurements

Three local nodes, 64 clients, 100-byte values:

| Change | Writes | 90% reads |
|---|---|---|
| baseline (both fsyncs, fsync before replicate) | 688 ops/s | 941 ops/s |
| + apply without fsync | 686 ops/s | 1,701 ops/s |
| + replicate before leader fsync | 1,076 ops/s | 1,865 ops/s |

## Consequences

* The simulator runs the new ready-loop order, so its safety checks cover
  it.
* The invariant that makes (1) safe now spans two modules (Raft log
  compaction and the state machine), and is documented in both and in
  CONSISTENCY.md.
* After power loss a node may briefly serve `Local` reads from an older
  state until it re-applies; linearizable reads are unaffected.
