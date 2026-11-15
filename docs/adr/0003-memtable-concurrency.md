# ADR 0003 — MemTable concurrency: `RwLock<BTreeMap>` plus versioned swaps

**Status:** accepted

## Context

A concurrent memtable should not turn every access into contention on one
global mutex. The classic answer is a lock-free skip list.

## Decision

Keep the memtable a `BTreeMap` behind an `RwLock`, and make the rest of the
engine avoid shared locks on the read path:

* Readers take an `Arc<Version>` snapshot (memtable, immutable memtable,
  SSTable list) under a briefly held read lock and then work without
  blocking anyone. Flushes and compactions publish a new `Version`; they
  never block readers.
* Many readers share the memtable's read lock concurrently.
* Writers already serialize on the WAL append — the log is a single
  sequential file — so a lock-free memtable could not add write
  parallelism. A batch is inserted under one write lock, which is also what
  makes it visible atomically.
* Keys and values are `Bytes` (reference-counted), so copying entries out
  for a flush or an iterator does not copy payloads.

## Alternatives considered

* **Lock-free skip list** (crossbeam-skiplist or hand-written): readers
  never block even during inserts. A hand-written one is a large amount of
  `unsafe` code with memory reclamation (epochs or hazard pointers); the
  measured benefit is bounded because the writer side is serial anyway.
* **Sharded maps**: break ordered iteration, which flush and compaction
  need.

## Consequences

* Simple, safe code with no `unsafe`; atomic batch visibility for free.
* Readers can wait for the duration of a single batch insert. Under Raft a
  batch is one loop iteration's worth of commands (microseconds of work).
* `tests/storage.rs` verifies that concurrent readers never observe a torn
  batch while writers trigger background flushes.
* If profiling ever shows reader stalls on the memtable lock, the skip list
  can replace `MemTable` without touching the rest of the engine.
