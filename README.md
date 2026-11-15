<p align="center">
  <img src="docs/assets/logo.png" alt="Ironclad — distributed key-value store written in Rust" width="720">
</p>

# Ironclad

**A distributed key-value database built from scratch in Rust.**

Ironclad stores key-value pairs on a cluster of machines and keeps them
safe when machines crash, disks get corrupted or the network misbehaves.
It is a small cousin of systems like etcd, CockroachDB's storage layer or
TiKV: a custom LSM-tree storage engine at the bottom, the Raft consensus
algorithm in the middle, and a binary TCP protocol on top.

```console
$ ironclad-cli set user:123 '{"name":"Bernardo"}'
OK
$ ironclad-cli get user:123
{"name":"Bernardo"}
$ ironclad-cli del user:123
OK
```

The storage engine, the consensus algorithm and the wire protocol are all
implemented in this repository. External crates provide only plumbing:
the async runtime (tokio), CRC32 checksums, argument parsing and the
serialization of internal peer messages.

---

## What it does

* **Stores data durably.** A write that was acknowledged with `OK` is on
  disk on a majority of the nodes. It survives process crashes, `kill -9`
  and power loss.
* **Replicates across nodes.** Run 3 or 5 nodes. The cluster keeps working
  while a minority of them is down: a 5-node cluster tolerates 2 failures.
* **Fails over automatically.** When the leader dies, the remaining nodes
  elect a new one within one to two seconds, and clients follow it without any
  configuration change.
* **Gives strong consistency.** Writes and reads are *linearizable*: once a
  write is acknowledged, every later read sees it, no matter which node
  serves it. For lower latency, a client can opt into fast local reads that
  may be slightly stale.
* **Applies each write exactly once.** A client that retries a write
  (because a node crashed or the network dropped the reply) never causes it
  to be applied twice.
* **Detects corruption.** Every structure on disk carries a checksum.
  Damaged data produces an error. It is never returned as if it were valid.
* **Works embedded, too.** The storage engine is a library you can use on
  its own, like RocksDB or LevelDB, with no network and no cluster.

---

## How it does it

Ironclad has three layers. Each one solves one problem and relies on the
one below it.

```
              clients
                 │   binary protocol over TCP
┌────────────────▼─────────────────────────────────────┐
│  Client API        parse, validate, route, reply      │   "speak the protocol"
├───────────────────────────────────────────────────────┤
│  Raft consensus    elect a leader, replicate the log  │   "agree on the order"
├───────────────────────────────────────────────────────┤
│  Storage engine    WAL, MemTable, SSTables            │   "never lose a byte"
└───────────────────────────────────────────────────────┘
```

### 1. The storage engine: never lose a byte

Each node stores its data in an **LSM tree** (log-structured merge tree),
the design behind LevelDB, RocksDB and Cassandra. It turns every write into
a sequential append, the fastest thing a disk can do.

```
 write ──► WAL ──► MemTable ──(full)──► immutable MemTable ──flush──► SSTable
           disk     memory                   memory                   disk
                                                                        │
 read  ──► MemTable ─► immutable ─► SSTables, newest → oldest          compaction
                                    (bloom filter skips most)           │
                                                                        ▼
                                                             one merged SSTable
```

1. **Write-ahead log (WAL).** Every write first goes to an append-only file.
   Each record carries a CRC32 checksum and a length. A write is
   acknowledged only once its record is written (and, optionally, fsynced).
   A group of operations (a `WriteBatch`) is a single record, so either the
   whole batch survives a crash or none of it does.
2. **MemTable.** Writes are then inserted into a sorted in-memory map.
   Reads check it first, so recent writes are served from RAM.
3. **SSTables.** When the MemTable reaches 4 MiB, a background thread writes
   it to disk as a *sorted string table*: an immutable file of 4 KiB blocks,
   each with its own checksum, followed by a block index and a **bloom
   filter**. The bloom filter answers "is this key possibly here?" in about
   200 ns. Lookups for keys that don't exist therefore almost never touch
   the disk.
4. **Compaction.** Over time SSTables pile up and old versions and deleted
   keys (*tombstones*) waste space. When four tables exist, a background
   thread merges them into one, keeping only the newest version of each key
   and dropping tombstones.
5. **The manifest.** A small file lists which SSTables are live and which
   WALs still need to be replayed. It is never edited in place. A new
   version is written to a temporary file, fsynced, and atomically renamed
   over the old one.

**Surviving a crash** comes down to doing things in the right order:

* an SSTable is fsynced *before* the manifest mentions it;
* a WAL is deleted only *after* a manifest that no longer needs it is
  durable;
* a WAL is fsynced before the next one is started, so only the newest one
  can be incomplete.

On restart the engine reads the manifest, opens the SSTables it lists,
replays the WALs, and deletes any file the manifest doesn't know about
(leftovers of an interrupted flush). It also tells two cases apart. A
half-written record at the very end of the newest WAL is a normal
interrupted write and is discarded. A damaged record with valid data after
it is real corruption, so the engine refuses to start rather than silently
dropping data.

**Concurrency.** Readers never wait for flushes or compactions, and wait
for writers only for the instant it takes to insert one batch in memory.
The engine publishes an immutable *version* (current MemTable, the one
being flushed, the list of SSTables) behind a reference-counted pointer.
A reader grabs the current version and works from it while the background
thread prepares the next one. SSTables are read with positional I/O
(`pread`), so any number of threads can read the same file at once.

### 2. Raft: agree on the order of writes

Replication raises a harder problem: if three machines each hold a copy,
how do they agree on what the data is, especially while machines crash and
messages get lost? Ironclad uses **Raft**, the consensus algorithm behind
etcd, Consul and CockroachDB.

**One leader at a time.** At any moment at most one node is the leader;
the others are followers. Time is divided into numbered *terms*. If a
follower stops hearing heartbeats from the leader, it waits a randomized
timeout, becomes a candidate for the next term and asks the others for
votes. A node grants at most one vote per term, and only to a candidate
whose log is at least as complete as its own. Whoever collects a majority
becomes leader, so two leaders can never exist in the same term.

**A replicated log.** Every write becomes an entry in an ordered log. The
leader appends the entry and sends it to the followers. Each follower
writes it to its own disk and acknowledges it. Once a majority has the
entry on disk, it is *committed*: no future leader can be elected without
it. Only then is it applied to the storage engine and the client gets `OK`.

```
client ──SET──► leader ──AppendEntries──► follower A   (fsync, ack)
                  │    ──AppendEntries──► follower B   (fsync, ack)
                  │ fsync
                  ▼
      majority has it → committed → applied → OK to client
```

**Followers that fall behind** are repaired automatically. If a follower's
log disagrees with the leader's, the leader walks back to the last point
where they match and overwrites the rest. If the follower has been away so
long that the leader has already discarded those log entries (logs are
compacted once applied), the leader sends a **snapshot** of its whole state
instead.

**Linearizable reads without writing.** A leader can't simply answer reads
from its own copy, because it might have been replaced without knowing it,
for example on the losing side of a network partition. Before serving a
read, the leader confirms with one round of heartbeats that a majority
still accepts it as leader (Raft's *ReadIndex* technique). A leader that
can't reach a majority for a whole election period steps down by itself
(*check-quorum*).

**Exactly-once writes.** Each client numbers its writes. The replicated
state machine remembers the last number applied for each client and skips
anything it has already seen, so a retried write is harmless.

**Built for testing.** The Raft implementation is a pure state machine
with no network, no clock and no threads. The server drives it with real
sockets and a real timer. The test suite drives the *same code* with a
simulated network that drops, delays, reorders and partitions messages,
crashes nodes at random, and checks Raft's safety properties after every
step. Every simulated run can be replayed exactly from its seed.

### 3. The client API: speak the protocol

Clients talk to any node over a compact binary protocol: length-prefixed
frames, big-endian integers, explicit status codes such as `OK`, `VALUE`,
`NOT_FOUND`, `NOT_LEADER` (with the leader's address), `BUSY` and
`TIMEOUT`. Every length in an incoming frame is checked before use, so
malformed or hostile input gets an error reply and can never crash the
server. The bundled client discovers the leader, follows redirects and
retries transient failures automatically.

The server protects itself under load (*backpressure*):

* there is a cap on open connections;
* a bounded queue sits in front of the consensus thread, and when it is
  full clients get `BUSY` immediately instead of piling up;
* requests have deadlines;
* writers are briefly stalled if the storage engine can't flush fast
  enough.

### Putting it together: the life of a write

1. A client sends `SET user:123 …` to any node. A follower answers
   `NOT_LEADER` with the leader's address, and the client reconnects there.
2. The leader validates and encodes the command and places it on the
   consensus thread's queue.
3. The consensus thread appends it to the Raft log. It sends it to the
   followers *while* fsyncing its own copy, so leader and follower disk
   writes overlap.
4. Followers fsync the entry and acknowledge.
5. With a majority, the entry is committed. All entries committed in this
   round go into the storage engine as **one atomic batch**, together with
   the updated client sessions and the new "applied up to" position.
6. The client receives `OK`.

Because many clients' writes are handled per round, one fsync covers many
writes (*group commit*). Because the "applied up to" position is stored
atomically with the data, a node that restarts resumes exactly where it
stopped, without re-applying or skipping anything.

---

## Getting started

Requires Rust 1.89 or newer (edition 2024).

```bash
cargo build --release
```

**Single node:**

```bash
./target/release/ironclad-server --id 1 --data-dir ./data/single
```

```bash
./target/release/ironclad-cli set hello world
```

**Three-node cluster on localhost** (client ports 6001–6003):

```bash
scripts/local-cluster.sh 3
```

```bash
./target/release/ironclad-cli -a 127.0.0.1:6001,127.0.0.1:6002,127.0.0.1:6003 status
```

**Five nodes in Docker, with the failover demo:**

```bash
scripts/failover-demo.sh
```

The demo starts five nodes, writes thousands of records, kills the leader
with `SIGKILL`, waits for a new leader, restarts the old one with its old
data, and verifies that all five nodes converge to the same state:

```
== 3. killing the leader (node4) with SIGKILL
== 4. waiting for a new leader
new leader: node3 (term 2) after 1933 ms
== 5. restarting node4 with its old data
== 6. verifying convergence
applied_index per node: 4004 4004 4004 3002 4004
applied_index per node: 4004 4004 4004 4004 4004
all five nodes converged to the same state
```

**CLI reference:**

| Command | Description |
|---|---|
| `get <key> [--local]` | Linearizable read, or a fast local read with `--local` |
| `set <key> <value>` | Write a key |
| `del <key>` | Delete a key |
| `status` | Role, term, leader, log indexes, storage and request metrics of each node |
| `bench` | Load test reporting throughput and p50/p95/p99 latency |

**Embedded use** (no network, no cluster):

```rust
use ironclad::storage::{Db, Options, WriteBatch};

let db = Db::open("./data/embedded", Options::default())?;
db.put("user:123", r#"{"name":"Bernardo"}"#)?;

let mut batch = WriteBatch::new();   // atomic: all or nothing
batch.put("a", "1");
batch.delete("b");
db.write(batch)?;

assert_eq!(db.get(b"a")?.as_deref(), Some(&b"1"[..]));
for entry in db.iter() {             // consistent point-in-time view
    let (key, value) = entry?;
}
```

---

## How it is tested

`cargo test` runs the whole suite in about a minute.

| Suite | What it proves |
|---|---|
| [`tests/raft_sim.rs`](tests/raft_sim.rs) | **Deterministic fault simulation.** The real Raft code runs under 5% message loss, reordering, random partitions and node crashes: 60 seeds × 1,500 ticks on 3- and 5-node clusters. After every step it checks that there is at most one leader per term, that no two nodes apply different entries at the same position, that no acknowledged write is lost, and that no read returns stale data. At the end it heals the network and checks that all nodes converge. As a check on the checker, disabling one of Raft's voting rules makes it fail immediately. |
| [`tests/cluster.rs`](tests/cluster.rs) | **Real processes, real crashes.** Starts five server processes and writes 2,000 records. Kills the leader with `SIGKILL`, checks that a new leader is elected in a higher term, and keeps writing inserts, overwrites and deletes. Restarts the old leader, which catches up via a snapshot, and verifies every replica holds exactly the expected data. A second test kills a node in the middle of a stream of writes and checks that every acknowledged write is present after restart. |
| [`tests/storage.rs`](tests/storage.rs) | **Model-based property testing.** Random sequences of writes, deletes, atomic batches, flushes, compactions and restarts must behave exactly like an in-memory `BTreeMap`. Also: half-written WAL tails, bit flips inside the WAL, corrupted SSTables and manifests, leftover files from an interrupted flush, and concurrent readers that must never see half of a batch. |
| unit tests | File formats, bloom filter accuracy, Raft log persistence and compaction, protocol round trips and fuzzing of the decoder with random bytes. |

## Performance

Measured on a laptop whose disk takes **about 10 ms per fsync**. That
latency dominates every durable number below; on an NVMe drive they would
be much higher.

Storage engine (`cargo bench --bench storage`, 100-byte values):

| Operation | Result |
|---|---|
| read, key in an SSTable | 5.6 µs |
| read, missing key (bloom filter) | 194 ns |
| write, no fsync | ~12 µs |
| write, fsync every write | 10.4 ms (the disk) |
| 100 writes in one batch, one fsync | 17.6 ms (~5,700 writes/s) |

Three-node cluster on one machine (`ironclad-cli bench`, 64 concurrent
clients, 100-byte values):

| Workload | Throughput | p50 | p95 | p99 |
|---|---|---|---|---|
| 100% writes | 1,076 ops/s | 52.7 ms | 147 ms | 196 ms |
| 90% reads, 10% writes | 1,865 ops/s | 28.2 ms | 57.6 ms | 90.2 ms |

Two optimizations, each measured before and after, raised write throughput
from 686 to 1,076 ops/s and read-heavy throughput from 941 to
1,865 ops/s. Both are explained in
[ADR 0004](docs/adr/0004-raft-log-is-the-wal.md).

## Limitations

These are deliberate scope cuts, discussed in
[ADR 0005](docs/adr/0005-scope-and-known-limitations.md):

* cluster membership is fixed at startup;
* snapshots are sent as a single message;
* compaction merges all tables at once;
* there are no range scans over the network;
* there is no authentication or TLS.

## Further reading

* [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): threads, backpressure,
  on-disk layout, observability.
* [docs/CONSISTENCY.md](docs/CONSISTENCY.md): the exact guarantees, the
  failure model, and the crash-recovery rules.
* [docs/adr/](docs/adr/): architecture decision records explaining the
  main design choices and the alternatives that were rejected.

## Repository layout

```
src/storage/    LSM-tree storage engine
src/raft/       Raft consensus (pure state machine + durable log)
src/server/     node driver, KV state machine, peer transport, client API
src/protocol.rs client wire protocol
src/client.rs   async client library
src/bin/        ironclad-server, ironclad-cli
tests/          raft_sim, cluster, storage
benches/        criterion benchmarks
docs/           architecture, consistency, ADRs
scripts/        local-cluster.sh, failover-demo.sh
```

## License

MIT — see [LICENSE](LICENSE).
