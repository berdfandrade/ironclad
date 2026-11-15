//! Append-only record log with per-record checksums.
//!
//! Used both as the storage engine's write-ahead log and as the Raft log.
//!
//! Record format (all integers big-endian):
//!
//! ```text
//! +-----------+-----------+------------------+
//! | crc32: u32| len: u32  | payload: [u8;len]|
//! +-----------+-----------+------------------+
//! crc32 covers the len field and the payload.
//! ```
//!
//! # Recovery rule
//!
//! A crash can leave a partially written last record ("torn tail"). When
//! reading, the first invalid record is classified as:
//!
//! * **torn tail** — the record runs past end-of-file, or everything from the
//!   record to EOF is zero bytes. The valid prefix is kept; the caller
//!   truncates the tail before appending again.
//! * **corruption** — an invalid record followed by more data. This cannot be
//!   produced by a crash during an append, so it is reported as an error and
//!   nothing is silently discarded.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use bytes::BufMut;

use crate::error::{Error, Result};

pub const HEADER_LEN: usize = 8;
/// Upper bound on a single record; larger lengths are treated as damage.
pub const MAX_RECORD_LEN: usize = 512 * 1024 * 1024;

pub fn encode_record(buf: &mut Vec<u8>, payload: &[u8]) {
    let len = (payload.len() as u32).to_be_bytes();
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&len);
    hasher.update(payload);
    buf.put_u32(hasher.finalize());
    buf.put_slice(&len);
    buf.put_slice(payload);
}

/// Result of scanning a log file.
pub struct LogContents {
    pub records: Vec<Vec<u8>>,
    /// Byte offset of the end of each record (`ends[i]` is where record
    /// `i + 1` starts).
    pub ends: Vec<u64>,
    /// Length of the valid prefix.
    pub valid_len: u64,
    /// True if a torn tail was found after `valid_len`.
    pub torn_tail: bool,
}

pub fn read_log(path: &Path) -> Result<LogContents> {
    let mut data = Vec::new();
    File::open(path)?.read_to_end(&mut data)?;
    parse_log(&data).map_err(|e| match e {
        Error::Corruption(msg) => Error::Corruption(format!("{}: {msg}", path.display())),
        other => other,
    })
}

pub fn parse_log(data: &[u8]) -> Result<LogContents> {
    let mut records = Vec::new();
    let mut ends = Vec::new();
    let mut pos = 0usize;
    while pos < data.len() {
        match check_record(&data[pos..]) {
            Some(len) => {
                records.push(data[pos + HEADER_LEN..pos + HEADER_LEN + len].to_vec());
                pos += HEADER_LEN + len;
                ends.push(pos as u64);
            }
            None => {
                let rest = &data[pos..];
                let runs_past_eof = rest.len() < HEADER_LEN || {
                    let len = u32::from_be_bytes(rest[4..8].try_into().unwrap()) as usize;
                    HEADER_LEN + len >= rest.len()
                };
                if runs_past_eof || rest.iter().all(|&b| b == 0) {
                    return Ok(LogContents {
                        records,
                        ends,
                        valid_len: pos as u64,
                        torn_tail: true,
                    });
                }
                return Err(Error::Corruption(format!(
                    "invalid record at offset {pos} followed by {} more bytes",
                    rest.len()
                )));
            }
        }
    }
    Ok(LogContents {
        records,
        ends,
        valid_len: pos as u64,
        torn_tail: false,
    })
}

/// Returns the payload length if a complete, checksum-valid record starts
/// at the beginning of `data`.
fn check_record(data: &[u8]) -> Option<usize> {
    if data.len() < HEADER_LEN {
        return None;
    }
    let crc = u32::from_be_bytes(data[0..4].try_into().unwrap());
    let len = u32::from_be_bytes(data[4..8].try_into().unwrap()) as usize;
    if len > MAX_RECORD_LEN || data.len() < HEADER_LEN + len {
        return None;
    }
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&data[4..HEADER_LEN + len]);
    (hasher.finalize() == crc).then_some(len)
}

/// Appends pre-encoded records to a log file.
///
/// Every `write` goes straight to the kernel (no userspace buffering), so a
/// process crash never loses an acknowledged write; `sync` additionally
/// survives power loss.
pub struct LogWriter {
    file: File,
    path: PathBuf,
    len: u64,
}

impl LogWriter {
    /// Opens (or creates) `path` for appending, truncating it to `len`
    /// first. Pass the `valid_len` from [`read_log`] to drop a torn tail.
    pub fn open(path: &Path, len: u64) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        if file.metadata()?.len() != len {
            file.set_len(len)?;
            file.sync_all()?;
        }
        let mut w = Self {
            file,
            path: path.to_path_buf(),
            len,
        };
        w.seek_end()?;
        Ok(w)
    }

    pub fn create(path: &Path) -> Result<Self> {
        let w = Self::open(path, 0)?;
        sync_parent_dir(path)?;
        Ok(w)
    }

    fn seek_end(&mut self) -> io::Result<()> {
        use std::io::Seek;
        self.file.seek(io::SeekFrom::Start(self.len))?;
        Ok(())
    }

    pub fn write(&mut self, encoded: &[u8]) -> Result<()> {
        if let Err(e) = self.file.write_all(encoded) {
            // Roll back a partial write so the file never contains a torn
            // record followed by later valid records.
            let _ = self.file.set_len(self.len);
            let _ = self.seek_end();
            return Err(e.into());
        }
        self.len += encoded.len() as u64;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Discards everything after `len` (used by the Raft log to drop a
    /// conflicting suffix). Durable on return.
    pub fn truncate(&mut self, len: u64) -> Result<()> {
        self.file.set_len(len)?;
        self.file.sync_all()?;
        self.len = len;
        self.seek_end()?;
        Ok(())
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// fsync a directory so that file creations, renames and deletions in it
/// are durable.
pub fn sync_parent_dir(path: &Path) -> io::Result<()> {
    match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => File::open(dir)?.sync_all(),
        _ => File::open(".")?.sync_all(),
    }
}

/// Atomically replaces `path` with `contents`: write a temp file, fsync it,
/// rename over the target, fsync the directory.
pub fn write_atomic(path: &Path, contents: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(contents)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    sync_parent_dir(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log_of(payloads: &[&[u8]]) -> Vec<u8> {
        let mut buf = Vec::new();
        for p in payloads {
            encode_record(&mut buf, p);
        }
        buf
    }

    #[test]
    fn roundtrip() {
        let data = log_of(&[b"one", b"", b"three"]);
        let c = parse_log(&data).unwrap();
        assert_eq!(c.records, vec![b"one".to_vec(), vec![], b"three".to_vec()]);
        assert!(!c.torn_tail);
        assert_eq!(c.valid_len, data.len() as u64);
    }

    #[test]
    fn every_truncation_point_is_a_torn_tail() {
        let data = log_of(&[b"alpha", b"beta", b"gamma"]);
        let first = HEADER_LEN + 5;
        let second = first + HEADER_LEN + 4;
        for cut in 0..data.len() {
            let c = parse_log(&data[..cut]).unwrap();
            let expected = if cut >= second {
                2
            } else if cut >= first {
                1
            } else {
                0
            };
            assert_eq!(c.records.len(), expected, "cut at {cut}");
            assert_eq!(c.torn_tail, cut != first && cut != second && cut != 0);
        }
    }

    #[test]
    fn zero_filled_tail_is_torn() {
        let mut data = log_of(&[b"alpha"]);
        data.extend_from_slice(&[0u8; 64]);
        let c = parse_log(&data).unwrap();
        assert_eq!(c.records.len(), 1);
        assert!(c.torn_tail);
    }

    #[test]
    fn bit_flip_in_middle_is_corruption() {
        let mut data = log_of(&[b"alpha", b"beta", b"gamma"]);
        data[HEADER_LEN + 1] ^= 0x01; // inside "alpha"
        assert!(matches!(parse_log(&data), Err(Error::Corruption(_))));
    }

    #[test]
    fn bit_flip_in_last_record_is_torn_tail() {
        let mut data = log_of(&[b"alpha", b"beta"]);
        let last = data.len() - 1;
        data[last] ^= 0x01;
        let c = parse_log(&data).unwrap();
        assert_eq!(c.records.len(), 1);
        assert!(c.torn_tail);
    }
}
