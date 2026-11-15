# Architecture

This document follows a write from the socket to the disk, then a read,
then covers threads, backpressure and on-disk formats. The *why* behind
each choice lives in the [ADRs](adr/).

## The life of a write

`SET user:123 {...}` sent to a three-node cluster:

1. **Receive.** The client API task decodes the frame
   ([`protocol`](../src/protocol.rs)). Every length is bounds-checked; a
   malformed frame gets an `ERROR` response, never a panic. Keys are
   1..64 KiB, values up to 4 MiB.
2. **Validate and serialize.** The command engine
   ([`server/api.rs`](../src/server/api.rs)) rejects keys in the reserved
   `0x00` namespace, then encodes a `Command::Set { key, value, client_id,
   seq }`.
3. **Submit.** The command goes onto the Raft loop's bounded queue. Full
   queue → `BUSY`, immediately.
4. **Log.** The Raft loop thread calls `propose`: the entry is appended to
   the Raft log (written to the kernel, not yet fsynced). A follower would
   answer `NOT_LEADER` with the leader's client address.
5. **Replicate.** In the same loop iteration, *before* fsyncing, the leader
   sends `AppendEntries` to the followers (`take_early_messages`). Then it
   fsyncs its log (`persist`). Leader and follower fsyncs overlap.
6. **Commit.** Each follower appends, fsyncs, and only then acknowledges.
   When a majority (counting the leader, whose copy now is durable) holds
   the entry *and* it belongs to the current term, the commit index moves.
7. **Apply.** `apply` hands committed entries to the
   [KV state machine](../src/server/kv.rs), which turns all entries of
   this iteration into **one** storage `WriteBatch`: the mutations, the
   updated client sessions, and the new applied index — atomically.
8. **Store.** The [storage engine](../src/storage/mod.rs) writes the batch
   as one WAL record and inserts it into the memtable.
9. **Acknowledge.** The proposal's `(index, term)` shows up as applied, so
   the waiting client task receives `OK`. If a different term's entry was
   applied at that index, the write was overwritten by a new leader and the
   client is told to retry.

Group commit is a consequence of the loop structure: everything that
arrived while the previous iteration was fsyncing is handled together, so
one log fsync and one storage write cover many clients.

## The life of a read

* **Linearizable (default).** The leader records `read_index =
  max(commit_index, index of its no-op)`, broadcasts a heartbeat tagged with
  a sequence number, and waits until a majority has echoed that number
  (proving no newer leader existed when the read started) and until
  `applied ≥ read_index`. Then it reads the storage engine. No log write.
* **Local.** Any replica reads its own storage engine directly from the API
  task (on a blocking-pool thread). Fast, possibly stale.

Inside the engine: memtable → immutable memtable → SSTables newest to
oldest. Each SSTable lookup consults its bloom filter (absent keys cost
~200 ns) and then binary-searches the in-memory block index to read one
checksummed block with `pread`.

## Threads and shared state

| Thread | Owns | Talks to others via |
|---|---|---|
| tokio runtime | sockets, connections, peer senders | bounded `std::sync::mpsc` into the Raft loop; `oneshot` replies |
| `ironclad-raft` | `RaftCore`, Raft log, state machine | non-blocking `try_send` to per-peer queues |
| `ironclad-bg` | flush and compaction | `Mutex` + `Condvar` with writers |

* The Raft core needs no locks: one thread owns it. It does synchronous
  fsyncs, which is why it is not a tokio task (it would stall I/O).
* The storage engine's read path is lock-light: readers clone an
  `Arc<Version>` (memtable, immutable memtable, table list) under a briefly
  held `RwLock`, then proceed without blocking writers, flushes or
  compactions. SSTables are read with `pread`, so threads never share a file
  cursor.
* Writers serialize on the WAL — inherently sequential — and apply to the
  memtable under its write lock, which makes a batch visible atomically.
* Lock order in the engine is fixed: `writer → bg → version → manifest`.
* A compaction deletes input files while readers may still hold them; on
  POSIX the open descriptor keeps the data readable until released.

## Backpressure

| Where | Mechanism | Client sees |
|---|---|---|
| Connections | semaphore (1,024) gates `accept` | connection waits in backlog |
| Raft loop queue | bounded (16 Ki events), `try_send` | `BUSY` |
| Request deadline | 5 s per write/read | `TIMEOUT` (outcome unknown, retry with same `seq`) |
| Memtable | a writer that fills the memtable while the previous one is still flushing blocks | higher latency (`db_write_stalls` metric) |
| Peer queues | bounded, drop when full | nothing: Raft retransmits |

## On-disk layout

```
<data-dir>/
  raft/raft.state      term + vote, crc; replaced atomically
  raft/raft.log        record log: header(snapshot index, term), entries
  db/LOCK              exclusive flock: one process per directory
  db/MANIFEST          live SSTables (newest first) + WAL replay start, crc
  db/000042.wal        write-ahead log (record log of WriteBatches)
  db/000041.sst        immutable sorted table
```

Formats are specified where they are implemented:
[record log](../src/storage/wal.rs) (shared by the WAL and the Raft log),
[SSTable](../src/storage/sstable.rs), [manifest](../src/storage/manifest.rs),
[Raft log](../src/raft/log.rs), [client protocol](../src/protocol.rs).

## Observability

* `tracing` logs on stderr (`RUST_LOG=debug` for elections, connections and
  duplicate suppression).
* `STATUS` (`ironclad-cli status`) reports role, term, leader, commit /
  applied / last / snapshot index, pending requests, storage stats (tables,
  bytes, flushes, compactions, write stalls) and counters (requests, reads,
  writes, errors, busy, timeouts, dropped peer messages).
