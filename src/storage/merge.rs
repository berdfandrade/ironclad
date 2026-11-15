//! K-way merge of sorted sources with newest-wins semantics.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use bytes::Bytes;

use super::batch::Value;
use crate::error::Result;

pub type Source = Box<dyn Iterator<Item = Result<(Bytes, Value)>> + Send>;

/// Merges sources that are each sorted by key. `sources[0]` is the newest;
/// when several sources hold the same key, only the newest version is
/// yielded. Tombstones are yielded too — the caller decides whether they
/// may be dropped.
pub struct MergeIter {
    sources: Vec<Source>,
    heads: Vec<Option<Value>>,
    heap: BinaryHeap<Reverse<(Bytes, usize)>>,
    error: Option<crate::error::Error>,
}

impl MergeIter {
    pub fn new(sources: Vec<Source>) -> Self {
        let n = sources.len();
        let mut it = Self {
            sources,
            heads: vec![None; n],
            heap: BinaryHeap::with_capacity(n),
            error: None,
        };
        for i in 0..n {
            it.advance(i);
        }
        it
    }

    fn advance(&mut self, i: usize) {
        match self.sources[i].next() {
            Some(Ok((key, value))) => {
                self.heads[i] = Some(value);
                self.heap.push(Reverse((key, i)));
            }
            Some(Err(e)) => self.error = Some(e),
            None => self.heads[i] = None,
        }
    }
}

impl Iterator for MergeIter {
    type Item = Result<(Bytes, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(e) = self.error.take() {
            return Some(Err(e));
        }
        // Heap order is (key, source index): the first pop for a key comes
        // from the newest source holding it.
        let Reverse((key, src)) = self.heap.pop()?;
        let value = self.heads[src].take().unwrap();
        self.advance(src);
        while let Some(Reverse((k, _))) = self.heap.peek() {
            if *k != key {
                break;
            }
            let Reverse((_, older)) = self.heap.pop().unwrap();
            self.heads[older] = None;
            self.advance(older);
        }
        if let Some(e) = self.error.take() {
            return Some(Err(e));
        }
        Some(Ok((key, value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn src(items: &[(&'static str, Option<&'static str>)]) -> Source {
        let v: Vec<_> = items
            .iter()
            .map(|(k, v)| {
                Ok((
                    Bytes::from_static(k.as_bytes()),
                    match v {
                        Some(v) => Value::Put(Bytes::from_static(v.as_bytes())),
                        None => Value::Delete,
                    },
                ))
            })
            .collect();
        Box::new(v.into_iter())
    }

    #[test]
    fn newest_wins() {
        let merged: Vec<_> = MergeIter::new(vec![
            src(&[("b", Some("new")), ("d", None)]),
            src(&[("a", Some("old")), ("b", Some("old")), ("d", Some("old"))]),
            src(&[("c", Some("oldest")), ("d", Some("oldest"))]),
        ])
        .map(|r| r.unwrap())
        .collect();
        assert_eq!(
            merged,
            vec![
                (Bytes::from("a"), Value::Put("old".into())),
                (Bytes::from("b"), Value::Put("new".into())),
                (Bytes::from("c"), Value::Put("oldest".into())),
                (Bytes::from("d"), Value::Delete),
            ]
        );
    }
}
