# Consistency and durability guarantees

## Failure model

* Nodes fail by **crashing** (process kill, power loss) and may restart
  with their disk intact. A node whose disk is lost must rejoin empty.
* The network may **drop, delay, duplicate and reorder** messages and
  **partition** nodes arbitrarily.
* **Not covered:** Byzantine behaviour, malicious clients or peers (there is
  no authentication), disks that lie about fsync, clock-based assumptions
  (none are made — there are no leases).

A cluster of `2f + 1` nodes stays available while at most `f` nodes are
down or partitioned away from the majority.

## Guarantees

### Writes

* A write acknowledged with `OK` is **linearizable**: it took effect at a
  single point between request and response, consistent with every other
  operation.
* It is **durable**: it is in the fsynced Raft log of a majority and will
  be in the state of every node that later applies that index, whatever
  crashes happen afterwards.
* `NOT_LEADER` and `BUSY` mean the write **did not** take effect.
* `TIMEOUT`, a dropped connection or a client-side timeout mean the outcome
  is **unknown**. Retrying with the same `(client_id, seq)` is safe: the
  state machine applies each pair at most once (see *Sessions*). The bundled
  client does this automatically.

### Reads

| Mode | Guarantee | Cost |
|---|---|---|
| `Linearizable` (default) | Sees every write acknowledged before the read began | One heartbeat round to a majority; no disk write |
| `Local` | Sees some prefix of the committed history, possibly stale; monotonic on a single node | Local disk read only |

A leader isolated by a partition cannot serve linearizable reads (it cannot
reach a majority) and steps down after one election timeout (check-quorum),
so clients move on to the majority side.

### Sessions (exactly-once retries)

Each client gets a random 64-bit `client_id` and numbers its writes
`seq = 1, 2, 3, …`, keeping one write in flight. The state machine stores
the last applied `seq` per client, in the same atomic batch as the write,
and skips any entry with `seq ≤ last`. Consequences:

* A retry of a write that did commit (e.g. the leader crashed after
  committing but before replying) is ignored.
* Session records are replicated and included in snapshots, so this holds
  across leader changes and restarts.
* Session records are never expired (one small key per client id ever
  seen). Clients that reuse an id must keep `seq` increasing.

## Crash recovery rules

Storage engine (each enforced in [`storage/mod.rs`](../src/storage/mod.rs)):

1. A write is acknowledged only after its WAL record is written (and, with
   `sync_writes`, fsynced).
2. A WAL is fsynced before the next one is created, so **only the newest WAL
   can have a torn tail.**
3. An SSTable is fsynced before the manifest references it; a WAL is deleted
   only after a durable manifest no longer needs it.
4. The manifest is replaced atomically (temp + fsync + rename + dir fsync).
   Files it does not reference are orphans and are deleted on open.

Record logs (WAL and Raft log) classify the first invalid record:

| Situation | Classification | Action |
|---|---|---|
| Record extends past end of file | torn tail | keep valid prefix, truncate |
| Everything from the record to EOF is zeros | torn tail | keep valid prefix, truncate |
| Invalid record followed by more data | **corruption** | refuse to open |
| Torn tail in a sealed (not newest) WAL | **corruption** (rule 2) | refuse to open |

SSTables, the manifest and the Raft hard state carry checksums; a mismatch
is an error, never wrong data.

Raft node:

* `term` and `vote` are fsynced before any vote or RequestVote is sent.
* Log entries are fsynced before they are acknowledged to a leader and
  before the leader counts itself towards a majority.
* The state machine stores its applied `(index, term)` in the same atomic
  batch as the effects, so a restart resumes exactly where it stopped:
  nothing is applied twice, nothing is skipped.
* The storage engine runs without per-write fsync under Raft, because the
  Raft log is the durable record. If power loss rolls the engine back, the
  applied index rolls back with it and the missing entries are re-applied
  from the log — which is never compacted past a point the engine has
  fsynced (`StateMachine::sync` runs before every log compaction).
* Installing a snapshot writes the state machine first (fsynced), then
  resets the log. If the node crashes in between, startup sees a state
  machine ahead of its log and resets the log itself.

## Not guaranteed

* No multi-key transactions or range scans across the network (the engine
  supports atomic batches and ordered iteration locally).
* No bounded staleness for `Local` reads.
* Availability during a partition where no side has a majority.
* Membership changes (the cluster is static).
