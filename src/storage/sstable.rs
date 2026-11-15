//! Immutable sorted string tables.
//!
//! File layout (integers big-endian):
//!
//! ```text
//! [data block 0] ... [data block n-1] [index block] [bloom block] [footer]
//!
//! data block : entry* crc32
//!   entry    : tag:u8 klen:u32 vlen:u32 key value
//! index block: count:u32 (klen:u32 last_key offset:u64 len:u32)* crc32
//! bloom block: k:u32 nbytes:u32 bits crc32
//! footer     : index_off:u64 index_len:u32 bloom_off:u64 bloom_len:u32
//!              entry_count:u64 magic:u64 crc32        (44 bytes)
//! ```
//!
//! Every block carries its own checksum and is verified on every read, so
//! corruption is detected at the block that is damaged.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::{BufMut, Bytes};

use super::batch::Value;
use super::bloom::BloomFilter;
use crate::codec::Reader;
use crate::error::{Error, Result};

const MAGIC: u64 = 0x4952_4f4e_434c_4144; // "IRONCLAD"
const FOOTER_LEN: usize = 44;

pub fn table_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:06}.sst"))
}

fn crc(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

fn seal(buf: &mut Vec<u8>) {
    let c = crc(buf);
    buf.put_u32(c);
}

fn unseal<'a>(block: &'a [u8], what: &str) -> Result<&'a [u8]> {
    if block.len() < 4 {
        return Err(Error::Corruption(format!("{what} too short")));
    }
    let (body, tail) = block.split_at(block.len() - 4);
    if crc(body) != u32::from_be_bytes(tail.try_into().unwrap()) {
        return Err(Error::Corruption(format!("{what} checksum mismatch")));
    }
    Ok(body)
}

struct IndexEntry {
    last_key: Bytes,
    offset: u64,
    len: u32,
}

/// Builds a table from entries supplied in strictly increasing key order.
pub struct TableWriter {
    path: PathBuf,
    out: BufWriter<File>,
    offset: u64,
    block_size: usize,
    bits_per_key: usize,
    block: Vec<u8>,
    last_key: Option<Bytes>,
    index: Vec<IndexEntry>,
    keys: Vec<Bytes>,
}

impl TableWriter {
    pub fn create(path: &Path, block_size: usize, bits_per_key: usize) -> Result<Self> {
        Ok(Self {
            path: path.to_path_buf(),
            out: BufWriter::with_capacity(256 * 1024, File::create(path)?),
            offset: 0,
            block_size,
            bits_per_key,
            block: Vec::with_capacity(block_size + 1024),
            last_key: None,
            index: Vec::new(),
            keys: Vec::new(),
        })
    }

    pub fn add(&mut self, key: &Bytes, value: &Value) -> Result<()> {
        if let Some(last) = &self.last_key
            && key <= last
        {
            return Err(Error::InvalidArgument(
                "table keys must be strictly increasing".into(),
            ));
        }
        let payload = value.payload();
        self.block.put_u8(value.tag());
        self.block.put_u32(key.len() as u32);
        self.block.put_u32(payload.len() as u32);
        self.block.put_slice(key);
        self.block.put_slice(payload);
        self.last_key = Some(key.clone());
        self.keys.push(key.clone());
        if self.block.len() >= self.block_size {
            self.finish_block()?;
        }
        Ok(())
    }

    fn finish_block(&mut self) -> Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        seal(&mut self.block);
        self.out.write_all(&self.block)?;
        self.index.push(IndexEntry {
            last_key: self.last_key.clone().unwrap(),
            offset: self.offset,
            len: self.block.len() as u32,
        });
        self.offset += self.block.len() as u64;
        self.block.clear();
        Ok(())
    }

    pub fn entry_count(&self) -> usize {
        self.keys.len()
    }

    /// Writes index, bloom filter and footer, then fsyncs the file. The
    /// table is durable but not yet *live*: it only becomes part of the
    /// database once the manifest references it.
    pub fn finish(mut self) -> Result<PathBuf> {
        self.finish_block()?;

        let mut index = Vec::new();
        index.put_u32(self.index.len() as u32);
        for e in &self.index {
            index.put_u32(e.last_key.len() as u32);
            index.put_slice(&e.last_key);
            index.put_u64(e.offset);
            index.put_u32(e.len);
        }
        seal(&mut index);
        let index_off = self.offset;
        self.out.write_all(&index)?;

        let mut bloom = Vec::new();
        BloomFilter::build(self.keys.iter().map(|k| k.as_ref()), self.bits_per_key)
            .encode(&mut bloom);
        seal(&mut bloom);
        let bloom_off = index_off + index.len() as u64;
        self.out.write_all(&bloom)?;

        let mut footer = Vec::with_capacity(FOOTER_LEN);
        footer.put_u64(index_off);
        footer.put_u32(index.len() as u32);
        footer.put_u64(bloom_off);
        footer.put_u32(bloom.len() as u32);
        footer.put_u64(self.keys.len() as u64);
        footer.put_u64(MAGIC);
        seal(&mut footer);
        debug_assert_eq!(footer.len(), FOOTER_LEN);
        self.out.write_all(&footer)?;

        let file = self.out.into_inner().map_err(|e| e.into_error())?;
        file.sync_all()?;
        Ok(self.path)
    }
}

/// An open, immutable table. Safe to share between threads: reads use
/// positional I/O (`pread`) and never move a shared file cursor.
pub struct Table {
    pub id: u64,
    file: File,
    index: Vec<IndexEntry>,
    bloom: BloomFilter,
    entry_count: u64,
    file_size: u64,
}

impl Table {
    pub fn open(dir: &Path, id: u64) -> Result<Self> {
        let path = table_path(dir, id);
        let file = File::open(&path)?;
        let file_size = file.metadata()?.len();
        let ctx = |e: Error| match e {
            Error::Corruption(m) => Error::Corruption(format!("{}: {m}", path.display())),
            other => other,
        };
        if file_size < FOOTER_LEN as u64 {
            return Err(ctx(Error::Corruption("file shorter than footer".into())));
        }
        let mut footer = vec![0u8; FOOTER_LEN];
        file.read_exact_at(&mut footer, file_size - FOOTER_LEN as u64)?;
        let body = unseal(&footer, "footer").map_err(ctx)?;
        let mut r = Reader::new(body);
        let index_off = r.u64()?;
        let index_len = r.u32()?;
        let bloom_off = r.u64()?;
        let bloom_len = r.u32()?;
        let entry_count = r.u64()?;
        if r.u64()? != MAGIC {
            return Err(ctx(Error::Corruption("bad magic".into())));
        }
        if index_off + index_len as u64 > file_size || bloom_off + bloom_len as u64 > file_size {
            return Err(ctx(Error::Corruption("footer offsets out of range".into())));
        }

        let mut buf = vec![0u8; index_len as usize];
        file.read_exact_at(&mut buf, index_off)?;
        let index = Self::decode_index(unseal(&buf, "index block").map_err(ctx)?).map_err(ctx)?;

        let mut buf = vec![0u8; bloom_len as usize];
        file.read_exact_at(&mut buf, bloom_off)?;
        let bloom = BloomFilter::decode(unseal(&buf, "bloom block").map_err(ctx)?).map_err(ctx)?;

        Ok(Self {
            id,
            file,
            index,
            bloom,
            entry_count,
            file_size,
        })
    }

    fn decode_index(body: &[u8]) -> Result<Vec<IndexEntry>> {
        let mut r = Reader::new(body);
        let count = r.u32()? as usize;
        let mut index = Vec::with_capacity(count.min(body.len() / 16));
        for _ in 0..count {
            index.push(IndexEntry {
                last_key: Bytes::copy_from_slice(r.len_prefixed()?),
                offset: r.u64()?,
                len: r.u32()?,
            });
        }
        Ok(index)
    }

    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    fn read_block(&self, i: usize) -> Result<Vec<(Bytes, Value)>> {
        let e = &self.index[i];
        let mut buf = vec![0u8; e.len as usize];
        self.file.read_exact_at(&mut buf, e.offset)?;
        let body = unseal(&buf, &format!("table {} block {i}", self.id))?;
        let mut r = Reader::new(body);
        let mut entries = Vec::new();
        while !r.is_empty() {
            let tag = r.u8()?;
            let klen = r.u32()? as usize;
            let vlen = r.u32()? as usize;
            let key = Bytes::copy_from_slice(r.bytes(klen)?);
            let value = Value::from_tag(tag, r.bytes(vlen)?)?;
            entries.push((key, value));
        }
        Ok(entries)
    }

    /// `Ok(None)` if the key is not in this table; `Ok(Some(Value::Delete))`
    /// if this table holds a tombstone for it.
    pub fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        if !self.bloom.may_contain(key) {
            return Ok(None);
        }
        let i = self.index.partition_point(|e| e.last_key.as_ref() < key);
        if i == self.index.len() {
            return Ok(None);
        }
        Ok(self
            .read_block(i)?
            .into_iter()
            .find(|(k, _)| k.as_ref() == key)
            .map(|(_, v)| v))
    }

    /// Full sequential scan, one block in memory at a time.
    pub fn iter(self: &Arc<Self>) -> TableIter {
        TableIter {
            table: self.clone(),
            next_block: 0,
            buffered: Vec::new().into_iter(),
        }
    }
}

pub struct TableIter {
    table: Arc<Table>,
    next_block: usize,
    buffered: std::vec::IntoIter<(Bytes, Value)>,
}

impl Iterator for TableIter {
    type Item = Result<(Bytes, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(e) = self.buffered.next() {
                return Some(Ok(e));
            }
            if self.next_block >= self.table.index.len() {
                return None;
            }
            match self.table.read_block(self.next_block) {
                Ok(entries) => {
                    self.next_block += 1;
                    self.buffered = entries.into_iter();
                }
                Err(e) => {
                    self.next_block = self.table.index.len();
                    return Some(Err(e));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(dir: &Path, n: u32) -> Arc<Table> {
        let mut w = TableWriter::create(&table_path(dir, 1), 256, 10).unwrap();
        for i in 0..n {
            let key = Bytes::from(format!("key{i:05}"));
            let value = if i % 7 == 0 {
                Value::Delete
            } else {
                Value::Put(Bytes::from(format!("value{i}")))
            };
            w.add(&key, &value).unwrap();
        }
        w.finish().unwrap();
        Arc::new(Table::open(dir, 1).unwrap())
    }

    #[test]
    fn point_lookups_and_scan() {
        let dir = tempfile::tempdir().unwrap();
        let t = build(dir.path(), 1000);
        assert_eq!(t.entry_count(), 1000);
        assert_eq!(
            t.get(b"key00001").unwrap(),
            Some(Value::Put("value1".into()))
        );
        assert_eq!(t.get(b"key00007").unwrap(), Some(Value::Delete));
        assert_eq!(t.get(b"key99999").unwrap(), None);
        assert_eq!(t.get(b"a").unwrap(), None);
        let all: Vec<_> = t.iter().collect::<Result<_>>().unwrap();
        assert_eq!(all.len(), 1000);
        assert!(all.windows(2).all(|w| w[0].0 < w[1].0));
    }

    #[test]
    fn rejects_unsorted_keys() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = TableWriter::create(&table_path(dir.path(), 1), 256, 10).unwrap();
        w.add(&Bytes::from("b"), &Value::Delete).unwrap();
        assert!(w.add(&Bytes::from("a"), &Value::Delete).is_err());
    }

    #[test]
    fn detects_corrupted_block() {
        let dir = tempfile::tempdir().unwrap();
        build(dir.path(), 1000);
        let path = table_path(dir.path(), 1);
        let mut data = std::fs::read(&path).unwrap();
        data[10] ^= 0xff;
        std::fs::write(&path, data).unwrap();
        let t = Table::open(dir.path(), 1).unwrap();
        assert!(matches!(t.get(b"key00001"), Err(Error::Corruption(_))));
    }

    #[test]
    fn detects_corrupted_footer() {
        let dir = tempfile::tempdir().unwrap();
        build(dir.path(), 10);
        let path = table_path(dir.path(), 1);
        let mut data = std::fs::read(&path).unwrap();
        let n = data.len();
        data[n - 10] ^= 0xff;
        std::fs::write(&path, data).unwrap();
        assert!(matches!(
            Table::open(dir.path(), 1),
            Err(Error::Corruption(_))
        ));
    }
}
