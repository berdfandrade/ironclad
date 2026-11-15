# ADR 0001 — An LSM tree as the storage engine

**Status:** accepted

## Context

The node must persist a key-value map with low write latency, survive
crashes at any instruction, and detect corruption. Under Raft every write
arrives as an ordered stream of batches, and reads are mostly point
lookups.

## Decision

A log-structured merge tree built from scratch:

* **WAL**: a checksummed, append-only record log. One `WriteBatch` = one
  record, which makes batches atomic on recovery.
* **MemTable** absorbs writes in memory; when it reaches 4 MiB it becomes
  immutable and a background thread writes it out as an **SSTable**.
* **SSTables**: 4 KiB data blocks, each with its own CRC; an in-memory
  block index (last key per block); a **bloom filter** at 10 bits/key
  (~0.8% false positives), so lookups of absent keys skip disk entirely.
* **Manifest**: the single source of truth for live files, replaced
  atomically. Recovery = read manifest, open its tables, replay WALs newer
  than its `log_number`, delete everything else.
* **Compaction**: size-tiered "merge everything" when 4 tables exist.
  Since every table takes part, nothing older remains and tombstones can be
  dropped safely.

## Alternatives considered

* **B-tree with page cache** (LMDB/BoltDB style): better read amplification,
  but random in-place writes, and crash safety needs copy-on-write or a
  double-write buffer. Sequential writes suit the Raft-fed workload better.
* **Leveled compaction** (RocksDB default): bounded read and space
  amplification at scale, but substantially more machinery (key-range
  partitioning, per-level targets, overlap selection).
* **Existing engine** (RocksDB, sled): would remove the core learning
  objective of the project.

## Consequences

* Writes cost one sequential append; reads cost at most one block read per
  table that may hold the key — few, since compaction keeps the count ≤ 4.
* Full compaction rewrites the whole database each time: write
  amplification grows with data size. Fine for tens of GB, not beyond;
  leveled compaction is the natural next step and fits behind the same
  `Version` / manifest interfaces.
* No block cache: the OS page cache serves that role.
