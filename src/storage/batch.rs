use bytes::{BufMut, Bytes};

use crate::codec::Reader;
use crate::error::{Error, Result};

const TAG_PUT: u8 = 1;
const TAG_DELETE: u8 = 2;

/// A value as stored inside the engine: either live data or a tombstone
/// that shadows older versions of the key in lower layers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Put(Bytes),
    Delete,
}

impl Value {
    pub(crate) fn tag(&self) -> u8 {
        match self {
            Value::Put(_) => TAG_PUT,
            Value::Delete => TAG_DELETE,
        }
    }

    pub(crate) fn from_tag(tag: u8, value: &[u8]) -> Result<Self> {
        match tag {
            TAG_PUT => Ok(Value::Put(Bytes::copy_from_slice(value))),
            TAG_DELETE => Ok(Value::Delete),
            t => Err(Error::Corruption(format!("unknown value tag {t}"))),
        }
    }

    pub(crate) fn payload(&self) -> &[u8] {
        match self {
            Value::Put(v) => v,
            Value::Delete => &[],
        }
    }

    pub fn into_option(self) -> Option<Bytes> {
        match self {
            Value::Put(v) => Some(v),
            Value::Delete => None,
        }
    }
}

/// A group of mutations applied atomically: one WAL record, one memtable
/// update. Readers observe all of a batch or none of it.
#[derive(Clone, Debug, Default)]
pub struct WriteBatch {
    pub(crate) ops: Vec<(Bytes, Value)>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, key: impl Into<Bytes>, value: impl Into<Bytes>) {
        self.ops.push((key.into(), Value::Put(value.into())));
    }

    pub fn delete(&mut self, key: impl Into<Bytes>) {
        self.ops.push((key.into(), Value::Delete));
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Approximate in-memory footprint, used for memtable accounting.
    pub(crate) fn approximate_size(&self) -> usize {
        self.ops
            .iter()
            .map(|(k, v)| k.len() + v.payload().len() + 32)
            .sum()
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.approximate_size());
        buf.put_u32(self.ops.len() as u32);
        for (key, value) in &self.ops {
            buf.put_u8(value.tag());
            buf.put_u32(key.len() as u32);
            buf.put_slice(key);
            if let Value::Put(v) = value {
                buf.put_u32(v.len() as u32);
                buf.put_slice(v);
            }
        }
        buf
    }

    pub(crate) fn decode(data: &[u8]) -> Result<Self> {
        let mut r = Reader::new(data);
        let count = r.u32()? as usize;
        // Every op needs at least 5 bytes, so a huge count is corruption,
        // not a reason to allocate.
        if count > r.remaining() / 5 {
            return Err(Error::Corruption(format!("batch count {count} too large")));
        }
        let mut ops = Vec::with_capacity(count);
        for _ in 0..count {
            let tag = r.u8()?;
            let key = Bytes::copy_from_slice(r.len_prefixed()?);
            let value = match tag {
                TAG_PUT => Value::Put(Bytes::copy_from_slice(r.len_prefixed()?)),
                TAG_DELETE => Value::Delete,
                t => return Err(Error::Corruption(format!("unknown op tag {t}"))),
            };
            ops.push((key, value));
        }
        if !r.is_empty() {
            return Err(Error::Corruption("trailing bytes after batch".into()));
        }
        Ok(Self { ops })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let mut b = WriteBatch::new();
        b.put("a", "1");
        b.delete("b");
        b.put("", "");
        let decoded = WriteBatch::decode(&b.encode()).unwrap();
        assert_eq!(decoded.ops, b.ops);
    }

    #[test]
    fn rejects_garbage() {
        assert!(WriteBatch::decode(&[0xff, 0xff, 0xff, 0xff]).is_err());
        assert!(WriteBatch::decode(&[0, 0, 0, 1, 9, 0, 0, 0, 0]).is_err());
    }
}
