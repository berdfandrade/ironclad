# Ironclad

A replicated, embeddable key-value store written from scratch in Rust: an
LSM-tree storage engine, Raft consensus, a binary TCP protocol, and the
tests to show they hold up under crashes, corruption and network faults.

```
$ ironclad-cli set user:123 '{"name":"Bernardo"}'
OK
$ ironclad-cli get user:123
{"name":"Bernardo"}
$ ironclad-cli del user:123
OK
```

The storage engine, the consensus algorithm and the wire protocol are all
written here; external crates provide only plumbing (async runtime,
checksums, CLI parsing, serialization of internal peer messages).

## What it guarantees

| Property | How |
|---|---|
| Writes are **linearizable** and survive the loss of a minority of nodes | Raft: acknowledged only after a majority has fsynced the entry |
| Reads are **linearizable** by default | ReadIndex: the leader confirms with a quorum that it is still leader |
| Optional fast **local reads** | Served by any replica; may be stale |
| Retried writes apply **exactly once** | Client sessions `(client_id, seq)` deduplicated inside the state machine |
| A **crash at any point** loses no acknowledged write | WAL + atomic manifest + ordering invariants (see [CONSISTENCY.md](docs/CONSISTENCY.md)) |
| On-disk **corruption is detected**, never returned as data | CRC32 on every WAL record, SSTable block, index, bloom filter, footer and manifest |

The full contract, including what is *not* guaranteed, is in
[docs/CONSISTENCY.md](docs/CONSISTENCY.md).

## Architecture

```
            clients
               │  binary protocol over TCP
┌──────────────▼───────────────────────────────────────────────────────┐
│ Client API (tokio)      decode → validate → route → encode            │
│   local reads ──────────────────────────────────────────────┐         │
│   writes / linearizable reads ── bounded queue (BUSY) ──┐    │         │
├─────────────────────────────────────────────────────────▼────┼───────┤
│ Raft loop (dedicated thread)                                 │       │
│   RaftCore: deterministic, no I/O except its durable log     │       │
│   ready loop: step → send appends → fsync → send acks → apply│       │
├────────────────────────────┬────────────────────────────────┼───────┤
│ Raft log (fsynced)         │ KV state machine               ▼       │
│ term/vote, entries,        │ commands + sessions + applied index    │
│ compaction, snapshots      │ committed in ONE atomic WriteBatch     │
├────────────────────────────┴─────────────────────────────────────────┤
│ Storage engine (LSM tree)                                             │
│   WAL → MemTable → immutable MemTable → SSTables (+bloom) → compaction│
└───────────────────────────────────────────────────────────────────────┘
        peers ◄── one-way Raft messages over TCP (best effort) ──► peers
```

Details: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). Design decisions and
their trade-offs: [docs/adr/](docs/adr/).

| Module | Contents |
|---|---|
| [`storage`](src/storage/mod.rs) | WAL, memtable, SSTables, bloom filters, manifest, merge iterator, flush/compaction thread, recovery |
| [`raft`](src/raft/mod.rs) | Leader election, log replication, commit rules, ReadIndex, check-quorum, log compaction, InstallSnapshot, durable log |
| [`protocol`](src/protocol.rs) | Client wire format with bounds-checked decoding |
| [`client`](src/client.rs) | Async client: leader discovery, redirects, retries, idempotent writes |
| [`server`](src/server/mod.rs) | Raft driver thread, KV state machine, peer transport, client API, metrics |

## Quick start

```bash
cargo build --release
```

Single node:

```bash
./target/release/ironclad-server --id 1 --data-dir ./data/single
```

```bash
./target/release/ironclad-cli set hello world
```

Three-node cluster on localhost (ports 6001–6003 for clients):

```bash
scripts/local-cluster.sh 3
```

```bash
./target/release/ironclad-cli -a 127.0.0.1:6001,127.0.0.1:6002,127.0.0.1:6003 status
```

Five nodes in Docker, plus the scripted failover demo (start five nodes,
write thousands of records, `SIGKILL` the leader, watch the election, bring
the old leader back, verify convergence):

```bash
scripts/failover-demo.sh
```

### Embedded use

The engine works on its own, without the network or Raft:

```rust
use ironclad::storage::{Db, Options, WriteBatch};

let db = Db::open("./data/embedded", Options::default())?;
db.put("user:123", r#"{"name":"Bernardo"}"#)?;
let mut batch = WriteBatch::new();          // atomic, one WAL record
batch.put("a", "1");
batch.delete("b");
db.write(batch)?;
assert_eq!(db.get(b"a")?.as_deref(), Some(&b"1"[..]));
for kv in db.iter() { let (key, value) = kv?; }   // consistent snapshot
```

## Testing

`cargo test` runs everything (about a minute; fsync-heavy).

| Suite | What it proves |
|---|---|
| [`tests/raft_sim.rs`](tests/raft_sim.rs) | **Deterministic fault simulation.** The real `RaftCore` under a simulated network with 5% message loss, reordering, random partitions and node crashes, 60 seeds × 1,500 ticks on 3- and 5-node clusters. Checked continuously: at most one leader per term; no two nodes apply different entries at the same index; every acknowledged write survives; linearizable reads observe every write acknowledged before them. At the end: heal, converge, compare. Mutation-checked: disabling the "candidate log is up to date" vote rule makes it fail. |
| [`tests/cluster.rs`](tests/cluster.rs) | **Real processes, real crashes.** Five `ironclad-server` processes: write 2,000 records, `SIGKILL` the leader, assert a new leader in a higher term, keep writing (inserts, overwrites, deletes), restart the old leader — which catches up through InstallSnapshot — and verify every replica holds exactly the expected state. Plus: writers hammer a node, it is `kill -9`'d mid-stream, and every acknowledged write is present after restart. |
| [`tests/storage.rs`](tests/storage.rs) | **Model-based property test**: random sequences of puts, deletes, atomic batches, flushes, compactions and restarts must behave exactly like a `BTreeMap`. Torn WAL tails, bit flips inside the WAL (refuses to open rather than silently dropping data), corrupted SSTable blocks and manifests, orphan files from interrupted flushes. Concurrent writers and readers never observe a torn batch. |
| unit tests | Record-log torn-tail classification at every truncation point, SSTable format, bloom false-positive rate, Raft log persistence/truncation/compaction, protocol round trips and fuzzing (`proptest`), session deduplication. |

## Benchmarks

Measured on a development laptop whose disk takes **~10 ms per fsync**,
which dominates every durable number below. Expect far higher figures on
NVMe.

Storage engine (`cargo bench --bench storage`, 100-byte values):

| Operation | Result |
|---|---|
| `get`, key in an SSTable | 5.6 µs (≈180k ops/s, single thread) |
| `get`, absent key (bloom filter rejects) | 194 ns (≈5.2M ops/s) |
| `put`, no fsync | ≈12 µs |
| `put`, fsync per write | 10.4 ms (bounded by the disk) |
| batch of 100 `put`s, one fsync | 17.6 ms (≈5.7k writes/s) |

Three-node cluster on one machine (`ironclad-cli bench`, 64 concurrent
clients, 100-byte values, 20,000 operations):

| Workload | Throughput | p50 | p95 | p99 |
|---|---|---|---|---|
| 100% writes | 1,076 ops/s | 52.7 ms | 147 ms | 196 ms |
| 90% linearizable reads / 10% writes | 1,865 ops/s | 28.2 ms | 57.6 ms | 90.2 ms |

Two optimizations measured along the way, both justified in
[ADR 0004](docs/adr/0004-raft-log-is-the-wal.md):

* not fsyncing the storage engine on apply (the Raft log already is the
  durable WAL): read-heavy throughput 941 → 1,701 ops/s;
* letting the leader send AppendEntries *before* its own fsync, overlapping
  leader and follower disk writes: write throughput 686 → 1,076 ops/s.

## Limitations

Deliberate scope cuts, each discussed in [ADR 0005](docs/adr/0005-scope-and-known-limitations.md):
static cluster membership; snapshots sent as a single message; size-tiered
"merge everything" compaction; no range scans over the network; no
authentication or TLS on either port.

## Layout

```
src/storage/   LSM engine            src/raft/      consensus
src/server/    node, API, transport  src/protocol.rs, src/client.rs
src/bin/       ironclad-server, ironclad-cli
tests/         raft_sim, cluster, storage
benches/       criterion benchmarks
docs/          ARCHITECTURE, CONSISTENCY, adr/
scripts/       local-cluster.sh, failover-demo.sh
```
