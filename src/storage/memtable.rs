//! In-memory write buffer.
//!
//! A `BTreeMap` behind an `RwLock`. Writers are already serialized by the
//! WAL (one sequential log), so a lock-free skip list would not add write
//! parallelism; what matters is that readers never wait on each other and
//! never wait on a flush. See ADR 0003 for the trade-off.

use std::collections::BTreeMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;

use super::batch::{Value, WriteBatch};

#[derive(Default)]
pub struct MemTable {
    map: RwLock<BTreeMap<Bytes, Value>>,
    size: AtomicUsize,
}

impl MemTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies the whole batch under one write lock so it becomes visible
    /// atomically.
    pub fn apply(&self, batch: &WriteBatch) {
        let mut map = self.map.write().unwrap();
        for (key, value) in &batch.ops {
            map.insert(key.clone(), value.clone());
        }
        self.size
            .fetch_add(batch.approximate_size(), Ordering::Relaxed);
    }

    /// `Some(Value::Delete)` means "deleted here, stop searching".
    pub fn get(&self, key: &[u8]) -> Option<Value> {
        self.map.read().unwrap().get(key).cloned()
    }

    pub fn approximate_size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.map.read().unwrap().is_empty()
    }

    /// Sorted copy of the contents. Cheap: keys and values are `Bytes`
    /// (reference counted), so only the vector itself is allocated.
    pub fn entries(&self) -> Vec<(Bytes, Value)> {
        self.map
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}
