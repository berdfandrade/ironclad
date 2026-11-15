//! Durable Raft log and hard state (current term, vote).
//!
//! On disk (`dir/`):
//!
//! * `raft.state` — `term:u64 vote:u64 crc32`, replaced atomically on every
//!   change, before any message that depends on it is sent.
//! * `raft.log`   — a [`wal`](crate::storage::wal) record log. The first
//!   record is a header `(snapshot_index, snapshot_term)`; every following
//!   record is one entry `index:u64 term:u64 data`. Compaction rewrites the
//!   file atomically with a new header and the remaining suffix.
//!
//! Entries are also kept in memory (bounded by compaction), so reads never
//! touch the disk. A memory-only log (no directory) is used by the
//! simulator.

use std::io;
use std::path::{Path, PathBuf};

use bytes::BufMut;

use super::{Entry, NodeId};
use crate::codec::Reader;
use crate::error::Error;
use crate::storage::wal::{LogWriter, encode_record, read_log, write_atomic};

const TAG_HEADER: u8 = 0;
const TAG_ENTRY: u8 = 1;

struct Disk {
    dir: PathBuf,
    writer: LogWriter,
    /// Byte offset where entry `i` ends.
    ends: Vec<u64>,
    /// Byte offset where the first entry starts (after the header).
    base: u64,
    dirty: bool,
}

pub struct RaftLog {
    snapshot_index: u64,
    snapshot_term: u64,
    /// `entries[i].index == snapshot_index + 1 + i`.
    entries: Vec<Entry>,
    term: u64,
    vote: Option<NodeId>,
    disk: Option<Disk>,
}

fn corrupt(msg: impl Into<String>) -> io::Error {
    Error::Corruption(msg.into()).into()
}

fn encode_header(buf: &mut Vec<u8>, index: u64, term: u64) {
    let mut p = Vec::with_capacity(17);
    p.put_u8(TAG_HEADER);
    p.put_u64(index);
    p.put_u64(term);
    encode_record(buf, &p);
}

fn encode_entry(buf: &mut Vec<u8>, e: &Entry) {
    let mut p = Vec::with_capacity(17 + e.data.len());
    p.put_u8(TAG_ENTRY);
    p.put_u64(e.index);
    p.put_u64(e.term);
    p.put_slice(&e.data);
    encode_record(buf, &p);
}

impl RaftLog {
    pub fn memory() -> Self {
        Self {
            snapshot_index: 0,
            snapshot_term: 0,
            entries: Vec::new(),
            term: 0,
            vote: None,
            disk: None,
        }
    }

    pub fn open(dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let mut log = Self::memory();

        match std::fs::read(dir.join("raft.state")) {
            Ok(data) => {
                if data.len() != 20
                    || crc32fast::hash(&data[..16])
                        != u32::from_be_bytes(data[16..].try_into().unwrap())
                {
                    return Err(corrupt("raft.state checksum mismatch"));
                }
                log.term = u64::from_be_bytes(data[0..8].try_into().unwrap());
                let vote = u64::from_be_bytes(data[8..16].try_into().unwrap());
                log.vote = (vote != 0).then_some(vote);
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }

        let path = dir.join("raft.log");
        if !path.exists() {
            let mut buf = Vec::new();
            encode_header(&mut buf, 0, 0);
            write_atomic(&path, &buf)?;
        }
        let contents = read_log(&path)?;
        if contents.torn_tail {
            tracing::warn!(
                valid_len = contents.valid_len,
                "discarding torn raft log tail"
            );
        }
        let mut records = contents.records.iter();
        let header = records
            .next()
            .ok_or_else(|| corrupt("raft.log missing header"))?;
        let mut r = Reader::new(header);
        if r.u8()? != TAG_HEADER {
            return Err(corrupt("raft.log first record is not a header"));
        }
        log.snapshot_index = r.u64()?;
        log.snapshot_term = r.u64()?;
        for record in records {
            let mut r = Reader::new(record);
            if r.u8()? != TAG_ENTRY {
                return Err(corrupt("unexpected record in raft.log"));
            }
            let index = r.u64()?;
            let term = r.u64()?;
            if index != log.last_index() + 1 {
                return Err(corrupt(format!(
                    "raft.log gap: expected {}, got {index}",
                    log.last_index() + 1
                )));
            }
            log.entries.push(Entry {
                index,
                term,
                data: r.bytes(r.remaining())?.to_vec(),
            });
        }
        log.disk = Some(Disk {
            dir: dir.to_path_buf(),
            writer: LogWriter::open(&path, contents.valid_len)?,
            base: contents.ends[0],
            ends: contents.ends[1..].to_vec(),
            dirty: false,
        });
        Ok(log)
    }

    pub fn hard_state(&self) -> (u64, Option<NodeId>) {
        (self.term, self.vote)
    }

    /// Durable on return.
    pub fn set_hard_state(&mut self, term: u64, vote: Option<NodeId>) -> io::Result<()> {
        if (term, vote) == (self.term, self.vote) {
            return Ok(());
        }
        if let Some(disk) = &self.disk {
            let mut buf = Vec::with_capacity(20);
            buf.put_u64(term);
            buf.put_u64(vote.unwrap_or(0));
            let c = crc32fast::hash(&buf);
            buf.put_u32(c);
            write_atomic(&disk.dir.join("raft.state"), &buf)?;
        }
        self.term = term;
        self.vote = vote;
        Ok(())
    }

    pub fn snapshot_index(&self) -> u64 {
        self.snapshot_index
    }

    pub fn snapshot_term(&self) -> u64 {
        self.snapshot_term
    }

    pub fn last_index(&self) -> u64 {
        self.snapshot_index + self.entries.len() as u64
    }

    pub fn last_term(&self) -> u64 {
        self.entries.last().map_or(self.snapshot_term, |e| e.term)
    }

    /// Term of the entry at `index`, if it is still known (not compacted
    /// away below the snapshot and not beyond the end).
    pub fn term(&self, index: u64) -> Option<u64> {
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        self.get(index).map(|e| e.term)
    }

    fn get(&self, index: u64) -> Option<&Entry> {
        if index <= self.snapshot_index {
            return None;
        }
        self.entries.get((index - self.snapshot_index - 1) as usize)
    }

    /// Entries starting at `from` (which must be above the snapshot): at
    /// most `max` of them and, beyond the first, at most `max_bytes` of
    /// payload.
    pub fn entries(&self, from: u64, max: usize, max_bytes: usize) -> Vec<Entry> {
        if from <= self.snapshot_index || from > self.last_index() {
            return Vec::new();
        }
        let start = (from - self.snapshot_index - 1) as usize;
        let mut bytes = 0;
        self.entries[start..]
            .iter()
            .take(max)
            .take_while(|e| {
                let first = bytes == 0;
                bytes += e.data.len() + 1;
                first || bytes <= max_bytes
            })
            .cloned()
            .collect()
    }

    /// Appends contiguous entries. Written to the kernel immediately but
    /// only durable after [`sync`](Self::sync).
    pub fn append(&mut self, entries: &[Entry]) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        for (i, e) in entries.iter().enumerate() {
            if e.index != self.last_index() + 1 + i as u64 {
                return Err(io::Error::other(format!(
                    "non-contiguous append at index {} (last {})",
                    e.index,
                    self.last_index()
                )));
            }
        }
        if let Some(disk) = &mut self.disk {
            let mut buf = Vec::new();
            let mut ends = Vec::with_capacity(entries.len());
            for e in entries {
                encode_entry(&mut buf, e);
                ends.push(disk.writer.len() + buf.len() as u64);
            }
            disk.writer.write(&buf)?;
            disk.ends.extend(ends);
            disk.dirty = true;
        }
        self.entries.extend_from_slice(entries);
        Ok(())
    }

    /// Removes the entry at `index` and everything after it. Durable on
    /// return.
    pub fn truncate_from(&mut self, index: u64) -> io::Result<()> {
        if index <= self.snapshot_index {
            return Err(io::Error::other("cannot truncate below the snapshot"));
        }
        let keep = (index - self.snapshot_index - 1) as usize;
        if keep >= self.entries.len() {
            return Ok(());
        }
        if let Some(disk) = &mut self.disk {
            let len = if keep == 0 {
                disk.base
            } else {
                disk.ends[keep - 1]
            };
            disk.writer.truncate(len)?;
            disk.ends.truncate(keep);
        }
        self.entries.truncate(keep);
        Ok(())
    }

    pub fn sync(&mut self) -> io::Result<()> {
        if let Some(disk) = &mut self.disk
            && disk.dirty
        {
            disk.writer.sync()?;
            disk.dirty = false;
        }
        Ok(())
    }

    /// Discards entries up to and including `index`, which must already be
    /// applied to a durable state machine.
    pub fn compact(&mut self, index: u64) -> io::Result<()> {
        if index <= self.snapshot_index {
            return Ok(());
        }
        let term = self
            .term(index)
            .ok_or_else(|| io::Error::other("compact beyond end of log"))?;
        let drop = (index - self.snapshot_index) as usize;
        let rest = self.entries.split_off(drop);
        self.rewrite(index, term, rest)
    }

    /// Discards the whole log and restarts it after a snapshot at
    /// `(index, term)`.
    pub fn reset(&mut self, index: u64, term: u64) -> io::Result<()> {
        self.rewrite(index, term, Vec::new())
    }

    fn rewrite(&mut self, index: u64, term: u64, entries: Vec<Entry>) -> io::Result<()> {
        if let Some(disk) = &mut self.disk {
            let mut buf = Vec::new();
            encode_header(&mut buf, index, term);
            let base = buf.len() as u64;
            let mut ends = Vec::with_capacity(entries.len());
            for e in &entries {
                encode_entry(&mut buf, e);
                ends.push(buf.len() as u64);
            }
            let path = disk.dir.join("raft.log");
            write_atomic(&path, &buf)?;
            disk.writer = LogWriter::open(&path, buf.len() as u64)?;
            disk.base = base;
            disk.ends = ends;
            disk.dirty = false;
        }
        self.snapshot_index = index;
        self.snapshot_term = term;
        self.entries = entries;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(index: u64, term: u64) -> Entry {
        Entry {
            index,
            term,
            data: format!("e{index}").into_bytes(),
        }
    }

    #[test]
    fn persists_appends_truncation_and_compaction() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut log = RaftLog::open(dir.path()).unwrap();
            log.set_hard_state(3, Some(2)).unwrap();
            log.append(&(1..=10).map(|i| entry(i, 1)).collect::<Vec<_>>())
                .unwrap();
            log.truncate_from(8).unwrap();
            log.append(&[entry(8, 2)]).unwrap();
            log.sync().unwrap();
        }
        {
            let mut log = RaftLog::open(dir.path()).unwrap();
            assert_eq!(log.hard_state(), (3, Some(2)));
            assert_eq!(log.last_index(), 8);
            assert_eq!(log.term(8), Some(2));
            assert_eq!(log.term(7), Some(1));
            log.compact(5).unwrap();
            log.append(&[entry(9, 2)]).unwrap();
            log.sync().unwrap();
        }
        let log = RaftLog::open(dir.path()).unwrap();
        assert_eq!(log.snapshot_index(), 5);
        assert_eq!(log.term(5), Some(1));
        assert_eq!(log.term(4), None);
        assert_eq!(log.last_index(), 9);
        assert_eq!(log.entries(6, 100, usize::MAX).len(), 4);
        assert_eq!(log.entries(6, 100, 1).len(), 1);
    }

    #[test]
    fn torn_tail_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut log = RaftLog::open(dir.path()).unwrap();
            log.append(&[entry(1, 1), entry(2, 1)]).unwrap();
            log.sync().unwrap();
        }
        let path = dir.path().join("raft.log");
        let len = std::fs::metadata(&path).unwrap().len();
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(len - 3).unwrap();
        let mut log = RaftLog::open(dir.path()).unwrap();
        assert_eq!(log.last_index(), 1);
        log.append(&[entry(2, 2)]).unwrap();
        log.sync().unwrap();
        drop(log);
        assert_eq!(RaftLog::open(dir.path()).unwrap().term(2), Some(2));
    }

    #[test]
    fn rejects_gaps() {
        let mut log = RaftLog::memory();
        assert!(log.append(&[entry(2, 1)]).is_err());
    }
}
