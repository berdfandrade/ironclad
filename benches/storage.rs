//! Storage engine micro-benchmarks: `cargo bench --bench storage`.

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use ironclad::storage::{Db, Options, WriteBatch};
use std::hint::black_box;

fn key(i: u64) -> Vec<u8> {
    format!("key:{:016x}", i.wrapping_mul(0x9e37_79b9_7f4a_7c15)).into_bytes()
}

fn bench_writes(c: &mut Criterion) {
    let mut g = c.benchmark_group("write");
    g.throughput(Throughput::Elements(1));
    for (name, sync) in [("put_100B_nosync", false), ("put_100B_fsync", true)] {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(
            dir.path(),
            Options {
                sync_writes: sync,
                ..Options::default()
            },
        )
        .unwrap();
        let value = vec![b'x'; 100];
        let mut i = 0u64;
        g.bench_function(name, |b| {
            b.iter(|| {
                i += 1;
                db.put(key(i), value.clone()).unwrap();
            })
        });
    }
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    let mut i = 0u64;
    g.throughput(Throughput::Elements(100));
    g.bench_function("batch_100x100B_fsync", |b| {
        b.iter_batched(
            || {
                let mut batch = WriteBatch::new();
                for _ in 0..100 {
                    i += 1;
                    batch.put(key(i), vec![b'x'; 100]);
                }
                batch
            },
            |batch| db.write(batch).unwrap(),
            BatchSize::SmallInput,
        )
    });
    g.finish();
}

fn bench_reads(c: &mut Criterion) {
    const N: u64 = 200_000;
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(
        dir.path(),
        Options {
            sync_writes: false,
            ..Options::default()
        },
    )
    .unwrap();
    for chunk in (0..N).collect::<Vec<_>>().chunks(1000) {
        let mut batch = WriteBatch::new();
        for &i in chunk {
            batch.put(key(i), vec![b'v'; 100]);
        }
        db.write(batch).unwrap();
    }
    db.compact().unwrap();

    let mut g = c.benchmark_group("read");
    g.throughput(Throughput::Elements(1));
    let mut i = 0u64;
    g.bench_function("get_hit_sstable", |b| {
        b.iter(|| {
            i = (i + 7919) % N;
            black_box(db.get(&key(i)).unwrap().unwrap());
        })
    });
    g.bench_function("get_miss_bloom", |b| {
        b.iter(|| {
            i += 1;
            black_box(db.get(&key(N + i)).unwrap());
        })
    });
    g.finish();
}

criterion_group!(benches, bench_writes, bench_reads);
criterion_main!(benches);
