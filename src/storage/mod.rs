//! Embedded LSM-tree key-value storage engine.
//!
//! ```text
//!  write ─► WAL (append + fsync) ─► MemTable ──(full)──► immutable MemTable
//!                                                            │ background flush
//!                                                            ▼
//!  read ──► MemTable ─► immutable ─► SSTables (newest → oldest, bloom-filtered)
//!                                         │ background compaction
//!                                         ▼
//!                                  one merged SSTable (tombstones dropped)
//! ```
//!
//! # Durability and recovery invariants
//!
//! 1. A write is acknowledged only after its WAL record is written (and
//!    fsynced when `sync_writes` is on).
//! 2. A WAL is fsynced before the next one is created, so only the newest
//!    WAL can have a torn tail.
//! 3. An SSTable is fsynced *before* the manifest references it, and a WAL
//!    is deleted only *after* a manifest that no longer needs it is durable.
//! 4. The manifest is replaced atomically; files it does not reference are
//!    orphans and are deleted on open.
//!
//! Together these mean that after a crash at any instruction, `open`
//! reconstructs exactly the set of acknowledged writes (plus possibly
//! writes that were in flight, which is allowed).

mod batch;
mod bloom;
mod manifest;
mod memtable;
mod merge;
mod sstable;
pub mod wal;

use std::collections::HashSet;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::JoinHandle;

use bytes::Bytes;

pub use batch::{Value, WriteBatch};
use manifest::Manifest;
use memtable::MemTable;
use merge::{MergeIter, Source};
use sstable::{Table, TableWriter, table_path};
use wal::{LogWriter, encode_record, read_log};

use crate::error::{Error, Result};

#[derive(Clone, Debug)]
pub struct Options {
    /// Memtable size that triggers a flush to an SSTable.
    pub memtable_size: usize,
    /// fsync the WAL on every write. Off means a process crash is still
    /// safe (writes reach the kernel) but power loss may drop recent writes.
    pub sync_writes: bool,
    pub block_size: usize,
    pub bloom_bits_per_key: usize,
    /// Number of SSTables that triggers a compaction.
    pub compaction_trigger: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            memtable_size: 4 * 1024 * 1024,
            sync_writes: true,
            block_size: 4096,
            bloom_bits_per_key: 10,
            compaction_trigger: 4,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct DbStats {
    pub tables: usize,
    pub table_bytes: u64,
    /// Entries across tables, including shadowed versions and tombstones.
    pub table_entries: u64,
    pub memtable_bytes: usize,
    pub flushes: u64,
    pub compactions: u64,
    pub write_stalls: u64,
}

/// Immutable view of the read path. Swapped atomically on rotation, flush
/// and compaction; readers clone the `Arc` and never block writers.
struct Version {
    mem: Arc<MemTable>,
    imm: Option<Arc<MemTable>>,
    tables: Vec<Arc<Table>>, // newest first
}

struct Writer {
    wal: LogWriter,
    wal_id: u64,
}

#[derive(Default)]
struct BgState {
    /// WAL id backing the immutable memtable, while one exists.
    imm_wal: Option<u64>,
    compact_requested: bool,
    shutdown: bool,
    error: Option<String>,
    flushes: u64,
    compactions: u64,
    write_stalls: u64,
}

struct Shared {
    dir: PathBuf,
    opts: Options,
    // Lock order: writer -> bg -> version -> manifest.
    writer: Mutex<Writer>,
    bg: Mutex<BgState>,
    version: RwLock<Arc<Version>>,
    manifest: Mutex<Manifest>,
    bg_work: Condvar,
    bg_done: Condvar,
    next_file_id: AtomicU64,
    _lock: File,
}

/// A handle to an open database. Cheap to share across threads via `Arc`.
pub struct Db {
    shared: Arc<Shared>,
    bg_thread: Option<JoinHandle<()>>,
}

enum FileKind {
    Table(u64),
    Wal(u64),
    Temp,
}

fn classify(name: &str) -> Option<FileKind> {
    if name.ends_with(".tmp") {
        return Some(FileKind::Temp);
    }
    let (stem, ext) = name.split_once('.')?;
    let id = stem.parse().ok()?;
    match ext {
        "sst" => Some(FileKind::Table(id)),
        "wal" => Some(FileKind::Wal(id)),
        _ => None,
    }
}

fn wal_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:06}.wal"))
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
        _ => Ok(()),
    }
}

fn write_table(
    dir: &Path,
    id: u64,
    opts: &Options,
    entries: impl Iterator<Item = Result<(Bytes, Value)>>,
) -> Result<Option<Arc<Table>>> {
    let path = table_path(dir, id);
    let mut w = TableWriter::create(&path, opts.block_size, opts.bloom_bits_per_key)?;
    for e in entries {
        let (k, v) = e?;
        w.add(&k, &v)?;
    }
    if w.entry_count() == 0 {
        drop(w);
        remove_if_exists(&path)?;
        return Ok(None);
    }
    w.finish()?;
    Ok(Some(Arc::new(Table::open(dir, id)?)))
}

impl Db {
    pub fn open(dir: impl AsRef<Path>, opts: Options) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let lock = File::create(dir.join("LOCK"))?;
        lock.try_lock().map_err(|_| {
            Error::InvalidArgument(format!(
                "{} is already open by another process",
                dir.display()
            ))
        })?;

        let mut manifest = Manifest::load(&dir)?.unwrap_or_default();
        let mut tables = manifest
            .tables
            .iter()
            .map(|&id| Table::open(&dir, id).map(Arc::new))
            .collect::<Result<Vec<_>>>()?;

        let mut max_id = manifest.log_number;
        max_id = max_id.max(manifest.tables.iter().copied().max().unwrap_or(0));
        let mut wals = Vec::new();
        let mut orphans = Vec::new();
        let live: HashSet<u64> = manifest.tables.iter().copied().collect();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            match classify(&name) {
                Some(FileKind::Wal(id)) => {
                    max_id = max_id.max(id);
                    if id >= manifest.log_number {
                        wals.push(id);
                    } else {
                        orphans.push(entry.path());
                    }
                }
                Some(FileKind::Table(id)) => {
                    max_id = max_id.max(id);
                    if !live.contains(&id) {
                        orphans.push(entry.path());
                    }
                }
                Some(FileKind::Temp) => orphans.push(entry.path()),
                None => {}
            }
        }
        wals.sort_unstable();

        // Replay WALs oldest-first into a fresh memtable.
        let recovered = MemTable::new();
        for (i, &id) in wals.iter().enumerate() {
            let path = wal_path(&dir, id);
            let contents = read_log(&path)?;
            if contents.torn_tail && i + 1 != wals.len() {
                // Invariant 2: sealed WALs are fsynced, so a damaged one is
                // real corruption, not an interrupted append.
                return Err(Error::Corruption(format!(
                    "sealed WAL {} has a damaged tail",
                    path.display()
                )));
            }
            if contents.torn_tail {
                tracing::warn!(wal = %path.display(), valid_len = contents.valid_len, "discarding torn WAL tail");
            }
            for record in &contents.records {
                recovered.apply(&WriteBatch::decode(record)?);
            }
        }

        let next_file_id = AtomicU64::new(max_id + 1);
        let alloc = || next_file_id.fetch_add(1, Ordering::SeqCst);

        // Persist recovered data as a table so we can start from an empty
        // WAL instead of appending to a possibly torn one.
        if !recovered.is_empty() {
            let entries = recovered.entries().into_iter().map(Ok);
            if let Some(t) = write_table(&dir, alloc(), &opts, entries)? {
                tables.insert(0, t);
            }
        }
        let wal_id = alloc();
        let wal = LogWriter::create(&wal_path(&dir, wal_id))?;
        manifest = Manifest {
            log_number: wal_id,
            tables: tables.iter().map(|t| t.id).collect(),
        };
        manifest.store(&dir)?;
        for id in wals {
            remove_if_exists(&wal_path(&dir, id))?;
        }
        for path in orphans {
            tracing::info!(file = %path.display(), "removing orphan file");
            remove_if_exists(&path)?;
        }

        let compact_requested = tables.len() >= opts.compaction_trigger;
        let shared = Arc::new(Shared {
            dir,
            opts,
            writer: Mutex::new(Writer { wal, wal_id }),
            bg: Mutex::new(BgState {
                compact_requested,
                ..Default::default()
            }),
            version: RwLock::new(Arc::new(Version {
                mem: Arc::new(MemTable::new()),
                imm: None,
                tables,
            })),
            manifest: Mutex::new(manifest),
            bg_work: Condvar::new(),
            bg_done: Condvar::new(),
            next_file_id,
            _lock: lock,
        });
        let bg_shared = shared.clone();
        let bg_thread = std::thread::Builder::new()
            .name("ironclad-bg".into())
            .spawn(move || bg_shared.background_loop())?;
        Ok(Self {
            shared,
            bg_thread: Some(bg_thread),
        })
    }

    pub fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
        let mut b = WriteBatch::new();
        b.put(key, value);
        self.write(b)
    }

    pub fn delete(&self, key: impl Into<Bytes>) -> Result<()> {
        let mut b = WriteBatch::new();
        b.delete(key);
        self.write(b)
    }

    /// Applies a batch atomically and durably.
    pub fn write(&self, batch: WriteBatch) -> Result<()> {
        self.shared.write(batch)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let v = self.shared.current();
        if let Some(value) = v.mem.get(key) {
            return Ok(value.into_option());
        }
        if let Some(value) = v.imm.as_ref().and_then(|imm| imm.get(key)) {
            return Ok(value.into_option());
        }
        for table in &v.tables {
            if let Some(value) = table.get(key)? {
                return Ok(value.into_option());
            }
        }
        Ok(None)
    }

    /// Iterates over all live key-value pairs in key order, as of the
    /// moment of the call (a consistent point-in-time view).
    pub fn iter(&self) -> impl Iterator<Item = Result<(Bytes, Bytes)>> + Send + use<> {
        let v = self.shared.current();
        let mut sources: Vec<Source> = vec![Box::new(v.mem.entries().into_iter().map(Ok))];
        if let Some(imm) = &v.imm {
            sources.push(Box::new(imm.entries().into_iter().map(Ok)));
        }
        for t in &v.tables {
            sources.push(Box::new(t.iter()));
        }
        MergeIter::new(sources).filter_map(|r| match r {
            Ok((k, Value::Put(v))) => Some(Ok((k, v))),
            Ok((_, Value::Delete)) => None,
            Err(e) => Some(Err(e)),
        })
    }

    /// fsyncs the WAL, making every write so far durable. Only needed when
    /// `sync_writes` is off.
    pub fn sync(&self) -> Result<()> {
        let mut w = self.shared.writer.lock().unwrap();
        self.shared.check_error()?;
        w.wal.sync().inspect_err(|e| self.shared.poison(e))
    }

    /// Forces the current memtable to disk and waits for it.
    pub fn flush(&self) -> Result<()> {
        let s = &self.shared;
        let mut w = s.writer.lock().unwrap();
        if !s.current().mem.is_empty() {
            s.rotate(&mut w)?;
        }
        drop(w);
        let mut bg = s.bg.lock().unwrap();
        while bg.imm_wal.is_some() && bg.error.is_none() {
            bg = s.bg_done.wait(bg).unwrap();
        }
        bg.error
            .as_ref()
            .map_or(Ok(()), |e| Err(Error::Poisoned(e.clone())))
    }

    /// Flushes, then merges all tables into one and waits for it.
    pub fn compact(&self) -> Result<()> {
        self.flush()?;
        let s = &self.shared;
        let mut bg = s.bg.lock().unwrap();
        bg.compact_requested = true;
        s.bg_work.notify_one();
        while bg.compact_requested && bg.error.is_none() {
            bg = s.bg_done.wait(bg).unwrap();
        }
        bg.error
            .as_ref()
            .map_or(Ok(()), |e| Err(Error::Poisoned(e.clone())))
    }

    pub fn stats(&self) -> DbStats {
        let v = self.shared.current();
        let bg = self.shared.bg.lock().unwrap();
        DbStats {
            tables: v.tables.len(),
            table_bytes: v.tables.iter().map(|t| t.file_size()).sum(),
            table_entries: v.tables.iter().map(|t| t.entry_count()).sum(),
            memtable_bytes: v.mem.approximate_size()
                + v.imm.as_ref().map_or(0, |m| m.approximate_size()),
            flushes: bg.flushes,
            compactions: bg.compactions,
            write_stalls: bg.write_stalls,
        }
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        // No flush on close: the WAL already makes every write durable, and
        // skipping it keeps "drop" equivalent to "crash" for testing.
        self.shared.bg.lock().unwrap().shutdown = true;
        self.shared.bg_work.notify_all();
        if let Some(t) = self.bg_thread.take() {
            let _ = t.join();
        }
    }
}

impl Shared {
    fn current(&self) -> Arc<Version> {
        self.version.read().unwrap().clone()
    }

    fn check_error(&self) -> Result<()> {
        match &self.bg.lock().unwrap().error {
            Some(e) => Err(Error::Poisoned(e.clone())),
            None => Ok(()),
        }
    }

    fn poison(&self, e: &Error) {
        tracing::error!(error = %e, "storage engine entering failed state");
        let mut bg = self.bg.lock().unwrap();
        bg.error.get_or_insert_with(|| e.to_string());
        self.bg_done.notify_all();
    }

    fn write(&self, batch: WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let payload = batch.encode();
        if payload.len() > wal::MAX_RECORD_LEN {
            return Err(Error::InvalidArgument("write batch too large".into()));
        }
        let mut record = Vec::with_capacity(payload.len() + wal::HEADER_LEN);
        encode_record(&mut record, &payload);

        let mut w = self.writer.lock().unwrap();
        self.check_error()?;
        // After a failed write or fsync the on-disk state is unknown
        // (fsync errors may drop dirty pages), so fail-stop rather than
        // keep accepting writes.
        let durable = w.wal.write(&record).and_then(|_| {
            if self.opts.sync_writes {
                w.wal.sync()
            } else {
                Ok(())
            }
        });
        if let Err(e) = durable {
            self.poison(&e);
            return Err(e);
        }
        let mem = self.current().mem.clone();
        mem.apply(&batch);
        // The write is already durable; a failed rotation poisons the
        // engine and surfaces on the next write instead of misreporting
        // this one as failed.
        if mem.approximate_size() >= self.opts.memtable_size {
            let _ = self.rotate(&mut w);
        }
        Ok(())
    }

    /// Turns the active memtable into the immutable one and starts a new
    /// WAL. Blocks (backpressure) while a previous flush is still running.
    fn rotate(&self, w: &mut Writer) -> Result<()> {
        let mut bg = self.bg.lock().unwrap();
        if bg.imm_wal.is_some() {
            bg.write_stalls += 1;
            while bg.imm_wal.is_some() && bg.error.is_none() {
                bg = self.bg_done.wait(bg).unwrap();
            }
        }
        if let Some(e) = &bg.error {
            return Err(Error::Poisoned(e.clone()));
        }
        let result = (|| {
            w.wal.sync()?; // Invariant 2.
            let id = self.next_file_id.fetch_add(1, Ordering::SeqCst);
            let wal = LogWriter::create(&wal_path(&self.dir, id))?;
            Ok::<_, Error>((id, wal))
        })();
        let (id, wal) = match result {
            Ok(r) => r,
            Err(e) => {
                drop(bg);
                self.poison(&e);
                return Err(e);
            }
        };
        let old_wal = std::mem::replace(&mut w.wal_id, id);
        w.wal = wal;
        {
            let mut v = self.version.write().unwrap();
            *v = Arc::new(Version {
                mem: Arc::new(MemTable::new()),
                imm: Some(v.mem.clone()),
                tables: v.tables.clone(),
            });
        }
        bg.imm_wal = Some(old_wal);
        self.bg_work.notify_one();
        Ok(())
    }

    fn background_loop(&self) {
        enum Job {
            Flush(u64),
            Compact,
        }
        loop {
            let job = {
                let mut bg = self.bg.lock().unwrap();
                loop {
                    if bg.shutdown || bg.error.is_some() {
                        return;
                    }
                    if let Some(id) = bg.imm_wal {
                        break Job::Flush(id);
                    }
                    if bg.compact_requested {
                        break Job::Compact;
                    }
                    bg = self.bg_work.wait(bg).unwrap();
                }
            };
            let result = match job {
                Job::Flush(wal_id) => self.flush_imm(wal_id),
                Job::Compact => self.compact_tables(),
            };
            if let Err(e) = result {
                self.poison(&e);
                return;
            }
        }
    }

    fn flush_imm(&self, wal_id: u64) -> Result<()> {
        let imm = self
            .current()
            .imm
            .clone()
            .expect("flush without immutable memtable");
        let id = self.next_file_id.fetch_add(1, Ordering::SeqCst);
        // Tombstones are kept: older tables may still hold the key.
        let table = write_table(&self.dir, id, &self.opts, imm.entries().into_iter().map(Ok))?;

        let tables = {
            let mut tables = self.current().tables.clone();
            if let Some(t) = table {
                tables.insert(0, t);
            }
            tables
        };
        {
            let mut m = self.manifest.lock().unwrap();
            let next = Manifest {
                log_number: wal_id + 1,
                tables: tables.iter().map(|t| t.id).collect(),
            };
            next.store(&self.dir)?; // Invariant 3: before the WAL goes away.
            *m = next;
        }
        let ntables = tables.len();
        {
            let mut v = self.version.write().unwrap();
            *v = Arc::new(Version {
                mem: v.mem.clone(),
                imm: None,
                tables,
            });
        }
        remove_if_exists(&wal_path(&self.dir, wal_id))?;

        let mut bg = self.bg.lock().unwrap();
        bg.imm_wal = None;
        bg.flushes += 1;
        if ntables >= self.opts.compaction_trigger {
            bg.compact_requested = true;
        }
        self.bg_done.notify_all();
        Ok(())
    }

    /// Size-tiered "merge everything" compaction. Because every table
    /// participates, nothing older than the output exists and tombstones
    /// can be dropped.
    fn compact_tables(&self) -> Result<()> {
        let inputs = self.current().tables.clone();
        if inputs.len() >= 2 {
            let id = self.next_file_id.fetch_add(1, Ordering::SeqCst);
            let sources: Vec<Source> = inputs
                .iter()
                .map(|t| Box::new(t.iter()) as Source)
                .collect();
            let live = MergeIter::new(sources).filter(|r| !matches!(r, Ok((_, Value::Delete))));
            let output = write_table(&self.dir, id, &self.opts, live)?;

            let compacted: HashSet<u64> = inputs.iter().map(|t| t.id).collect();
            let mut tables: Vec<_> = self
                .current()
                .tables
                .iter()
                .filter(|t| !compacted.contains(&t.id))
                .cloned()
                .collect();
            tables.extend(output);
            {
                let mut m = self.manifest.lock().unwrap();
                let next = Manifest {
                    log_number: m.log_number,
                    tables: tables.iter().map(|t| t.id).collect(),
                };
                next.store(&self.dir)?;
                *m = next;
            }
            {
                let mut v = self.version.write().unwrap();
                *v = Arc::new(Version {
                    mem: v.mem.clone(),
                    imm: v.imm.clone(),
                    tables,
                });
            }
            // Readers holding the old Version keep their file descriptors;
            // on POSIX the data stays readable until they are closed.
            for t in &inputs {
                remove_if_exists(&table_path(&self.dir, t.id))?;
            }
        }
        let mut bg = self.bg.lock().unwrap();
        bg.compact_requested = false;
        bg.compactions += 1;
        self.bg_done.notify_all();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small_opts() -> Options {
        Options {
            memtable_size: 4 * 1024,
            block_size: 512,
            sync_writes: false,
            ..Options::default()
        }
    }

    #[test]
    fn basic_crud() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path(), Options::default()).unwrap();
        db.put("a", "1").unwrap();
        db.put("b", "2").unwrap();
        db.delete("a").unwrap();
        assert_eq!(db.get(b"a").unwrap(), None);
        assert_eq!(db.get(b"b").unwrap().as_deref(), Some(b"2".as_ref()));
    }

    #[test]
    fn data_survives_reopen_without_flush() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Db::open(dir.path(), Options::default()).unwrap();
            for i in 0..100 {
                db.put(format!("k{i}"), format!("v{i}")).unwrap();
            }
            db.delete("k5").unwrap();
        }
        let db = Db::open(dir.path(), Options::default()).unwrap();
        assert_eq!(db.get(b"k99").unwrap().as_deref(), Some(b"v99".as_ref()));
        assert_eq!(db.get(b"k5").unwrap(), None);
    }

    #[test]
    fn flush_and_compaction_preserve_data_and_drop_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path(), small_opts()).unwrap();
        for i in 0..2000u32 {
            db.put(format!("key{i:05}"), vec![b'x'; 50]).unwrap();
        }
        for i in (0..2000u32).step_by(2) {
            db.delete(format!("key{i:05}")).unwrap();
        }
        db.compact().unwrap();
        let stats = db.stats();
        assert_eq!(stats.tables, 1);
        assert!(stats.flushes > 0 && stats.compactions > 0);
        let live: Vec<_> = db.iter().collect::<Result<_>>().unwrap();
        assert_eq!(live.len(), 1000);
        assert_eq!(db.get(b"key00000").unwrap(), None);
        assert!(db.get(b"key00001").unwrap().is_some());
    }

    #[test]
    fn second_open_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let _db = Db::open(dir.path(), Options::default()).unwrap();
        assert!(Db::open(dir.path(), Options::default()).is_err());
    }
}
