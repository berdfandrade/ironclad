//! Storage engine: model-based property tests, crash/corruption recovery,
//! and concurrency.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use ironclad::Error;
use ironclad::storage::{Db, Options, WriteBatch};
use proptest::prelude::*;

fn tiny() -> Options {
    Options {
        memtable_size: 1024,
        block_size: 256,
        compaction_trigger: 3,
        sync_writes: false,
        ..Options::default()
    }
}

fn files_with_ext(dir: &Path, ext: &str) -> Vec<std::path::PathBuf> {
    let mut v: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == ext))
        .collect();
    v.sort();
    v
}

#[derive(Debug, Clone)]
enum Op {
    Put(u8, Vec<u8>),
    Delete(u8),
    Batch(Vec<(u8, Option<Vec<u8>>)>),
    Flush,
    Compact,
    Reopen,
}

fn op() -> impl Strategy<Value = Op> {
    let value = proptest::collection::vec(any::<u8>(), 0..200);
    prop_oneof![
        8 => (0u8..40, value.clone()).prop_map(|(k, v)| Op::Put(k, v)),
        3 => (0u8..40).prop_map(Op::Delete),
        2 => proptest::collection::vec((0u8..40, proptest::option::of(value)), 1..6).prop_map(Op::Batch),
        1 => Just(Op::Flush),
        1 => Just(Op::Compact),
        1 => Just(Op::Reopen),
    ]
}

fn key(k: u8) -> Bytes {
    Bytes::from(format!("key-{k:03}"))
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 32, ..ProptestConfig::default() })]

    /// The engine behaves exactly like a `BTreeMap` across any sequence of
    /// writes, flushes, compactions and restarts.
    #[test]
    fn matches_model(ops in proptest::collection::vec(op(), 1..120)) {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::open(dir.path(), tiny()).unwrap();
        let mut model: BTreeMap<Bytes, Bytes> = BTreeMap::new();
        for op in ops {
            match op {
                Op::Put(k, v) => {
                    db.put(key(k), v.clone()).unwrap();
                    model.insert(key(k), v.into());
                }
                Op::Delete(k) => {
                    db.delete(key(k)).unwrap();
                    model.remove(&key(k));
                }
                Op::Batch(ops) => {
                    let mut b = WriteBatch::new();
                    for (k, v) in ops {
                        match v {
                            Some(v) => { b.put(key(k), v.clone()); model.insert(key(k), v.into()); }
                            None => { b.delete(key(k)); model.remove(&key(k)); }
                        }
                    }
                    db.write(b).unwrap();
                }
                Op::Flush => db.flush().unwrap(),
                Op::Compact => db.compact().unwrap(),
                Op::Reopen => {
                    drop(db);
                    db = Db::open(dir.path(), tiny()).unwrap();
                }
            }
            for k in 0u8..40 {
                prop_assert_eq!(db.get(&key(k)).unwrap(), model.get(&key(k)).cloned());
            }
        }
        let all: Vec<(Bytes, Bytes)> = db.iter().collect::<Result<_, _>>().unwrap();
        let expected: Vec<(Bytes, Bytes)> = model.into_iter().collect();
        prop_assert_eq!(all, expected);
    }
}

#[test]
fn torn_wal_tail_loses_only_the_torn_record() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), Options::default()).unwrap();
        for i in 0..100 {
            db.put(format!("k{i:03}"), format!("v{i}")).unwrap();
        }
    }
    // Simulate a crash in the middle of appending the last record.
    let wal = files_with_ext(dir.path(), "wal").pop().unwrap();
    let len = std::fs::metadata(&wal).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&wal)
        .unwrap()
        .set_len(len - 3)
        .unwrap();

    let db = Db::open(dir.path(), Options::default()).unwrap();
    for i in 0..99 {
        assert_eq!(
            db.get(format!("k{i:03}").as_bytes()).unwrap().unwrap(),
            format!("v{i}")
        );
    }
    assert_eq!(db.get(b"k099").unwrap(), None);
    // And it keeps working after recovery.
    db.put("after", "crash").unwrap();
    drop(db);
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_eq!(db.get(b"after").unwrap().unwrap(), "crash");
    assert_eq!(db.get(b"k098").unwrap().unwrap(), "v98");
}

#[test]
fn corruption_inside_the_wal_is_reported_not_skipped() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), Options::default()).unwrap();
        for i in 0..100 {
            db.put(format!("k{i:03}"), format!("v{i}")).unwrap();
        }
    }
    let wal = files_with_ext(dir.path(), "wal").pop().unwrap();
    let mut data = std::fs::read(&wal).unwrap();
    data[20] ^= 0x40; // inside the first record, with valid records after it
    std::fs::write(&wal, data).unwrap();
    assert!(matches!(
        Db::open(dir.path(), Options::default()),
        Err(Error::Corruption(_))
    ));
}

#[test]
fn corrupted_sstable_is_detected_on_read() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), Options::default()).unwrap();
        for i in 0..500 {
            db.put(format!("k{i:03}"), vec![b'x'; 100]).unwrap();
        }
        db.flush().unwrap();
    }
    let sst = files_with_ext(dir.path(), "sst").pop().unwrap();
    let mut data = std::fs::read(&sst).unwrap();
    data[100] ^= 0xff;
    std::fs::write(&sst, data).unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    // Never wrong data: either the right value (other blocks) or an error.
    let mut errors = 0;
    for i in 0..500 {
        match db.get(format!("k{i:03}").as_bytes()) {
            Ok(v) => assert_eq!(v.unwrap().as_ref(), &[b'x'; 100][..]),
            Err(Error::Corruption(_)) => errors += 1,
            Err(e) => panic!("unexpected error {e}"),
        }
    }
    assert!(errors > 0);
}

#[test]
fn corrupted_manifest_refuses_to_open() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), Options::default()).unwrap();
        db.put("a", "b").unwrap();
        db.flush().unwrap();
    }
    let path = dir.path().join("MANIFEST");
    let mut data = std::fs::read(&path).unwrap();
    data[10] ^= 1;
    std::fs::write(&path, data).unwrap();
    assert!(matches!(
        Db::open(dir.path(), Options::default()),
        Err(Error::Corruption(_))
    ));
}

#[test]
fn orphans_from_an_interrupted_flush_are_removed() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), Options::default()).unwrap();
        db.put("a", "1").unwrap();
    }
    // A table written by a flush that crashed before the manifest update,
    // and a half-written manifest temp file.
    std::fs::write(dir.path().join("999999.sst"), b"garbage").unwrap();
    std::fs::write(dir.path().join("MANIFEST.tmp"), b"garbage").unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_eq!(db.get(b"a").unwrap().unwrap(), "1");
    assert!(!dir.path().join("999999.sst").exists());
    assert!(!dir.path().join("MANIFEST.tmp").exists());
}

#[test]
fn concurrent_writers_and_readers_see_atomic_batches() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        memtable_size: 16 * 1024,
        ..tiny()
    };
    let db = Arc::new(Db::open(dir.path(), opts).unwrap());
    let done = Arc::new(AtomicBool::new(false));

    let writers: Vec<_> = (0..4)
        .map(|w| {
            let db = db.clone();
            std::thread::spawn(move || {
                for i in 0..500u32 {
                    // Each batch keeps the pair (a, b) equal.
                    let mut b = WriteBatch::new();
                    b.put(format!("pair{w}:a"), i.to_be_bytes().to_vec());
                    b.put(format!("pair{w}:b"), i.to_be_bytes().to_vec());
                    b.put(format!("w{w}:{i:04}"), vec![b'v'; 32]);
                    db.write(b).unwrap();
                }
            })
        })
        .collect();

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let db = db.clone();
            let done = done.clone();
            std::thread::spawn(move || {
                let mut checks = 0;
                while !done.load(Ordering::Relaxed) {
                    let snapshot: BTreeMap<Bytes, Bytes> = db.iter().map(|r| r.unwrap()).collect();
                    for w in 0..4 {
                        let a = snapshot.get(format!("pair{w}:a").as_bytes());
                        let b = snapshot.get(format!("pair{w}:b").as_bytes());
                        assert_eq!(a, b, "torn batch observed");
                    }
                    checks += 1;
                }
                checks
            })
        })
        .collect();

    for w in writers {
        w.join().unwrap();
    }
    done.store(true, Ordering::Relaxed);
    let checks: usize = readers.into_iter().map(|r| r.join().unwrap()).sum();
    assert!(checks > 0);

    let stats = db.stats();
    assert!(stats.flushes > 0, "test should exercise background flushes");
    for w in 0..4 {
        for i in 0..500 {
            assert!(db.get(format!("w{w}:{i:04}").as_bytes()).unwrap().is_some());
        }
    }
}
