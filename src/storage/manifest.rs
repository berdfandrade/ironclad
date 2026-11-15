//! The manifest is the single source of truth for which files are live.
//!
//! ```text
//! magic:u64 log_number:u64 count:u32 table_id:u64* crc32
//! ```
//!
//! * `tables` are ordered newest first.
//! * `log_number`: WAL files with id >= this still hold data that is not in
//!   any table and must be replayed on open. Older WALs are garbage.
//!
//! It is always replaced atomically (temp file + fsync + rename + dir
//! fsync), so after a crash it is either the old or the new version, and
//! any file it does not reference is an orphan that can be deleted.

use std::path::{Path, PathBuf};

use bytes::BufMut;

use super::wal::write_atomic;
use crate::codec::Reader;
use crate::error::{Error, Result};

const MAGIC: u64 = 0x4d41_4e49_4645_5354; // "MANIFEST"

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Manifest {
    pub log_number: u64,
    pub tables: Vec<u64>,
}

pub fn manifest_path(dir: &Path) -> PathBuf {
    dir.join("MANIFEST")
}

impl Manifest {
    pub fn load(dir: &Path) -> Result<Option<Self>> {
        let data = match std::fs::read(manifest_path(dir)) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        if data.len() < 4 {
            return Err(Error::Corruption("MANIFEST too short".into()));
        }
        let (body, tail) = data.split_at(data.len() - 4);
        if crc32fast::hash(body) != u32::from_be_bytes(tail.try_into().unwrap()) {
            return Err(Error::Corruption("MANIFEST checksum mismatch".into()));
        }
        let mut r = Reader::new(body);
        if r.u64()? != MAGIC {
            return Err(Error::Corruption("MANIFEST bad magic".into()));
        }
        let log_number = r.u64()?;
        let count = r.u32()? as usize;
        let mut tables = Vec::with_capacity(count.min(r.remaining() / 8));
        for _ in 0..count {
            tables.push(r.u64()?);
        }
        Ok(Some(Self { log_number, tables }))
    }

    pub fn store(&self, dir: &Path) -> Result<()> {
        let mut buf = Vec::new();
        buf.put_u64(MAGIC);
        buf.put_u64(self.log_number);
        buf.put_u32(self.tables.len() as u32);
        for id in &self.tables {
            buf.put_u64(*id);
        }
        let c = crc32fast::hash(&buf);
        buf.put_u32(c);
        write_atomic(&manifest_path(dir), &buf)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_corruption() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(Manifest::load(dir.path()).unwrap(), None);
        let m = Manifest {
            log_number: 7,
            tables: vec![6, 3, 1],
        };
        m.store(dir.path()).unwrap();
        assert_eq!(Manifest::load(dir.path()).unwrap(), Some(m));

        let path = manifest_path(dir.path());
        let mut data = std::fs::read(&path).unwrap();
        data[9] ^= 1;
        std::fs::write(&path, data).unwrap();
        assert!(Manifest::load(dir.path()).is_err());
    }
}
