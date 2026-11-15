//! The replicated key-value state machine, backed by the storage engine.
//!
//! Reserved keys (clients cannot use keys starting with `0x00`):
//!
//! * `\0meta/applied`      — `index:u64 term:u64` of the last applied entry
//! * `\0session/<client>`  — last applied `seq` for a client id
//!
//! Each `apply` call is one `WriteBatch`: the commands, the session updates
//! and the applied position commit together or not at all. That is what
//! makes restart exactly-once: after a crash we resume from the stored
//! position, never re-applying or skipping an entry.
//!
//! The engine normally runs without per-write fsync here: the Raft log is
//! already the fsynced write-ahead log of the replicated system. If power
//! loss drops recently applied batches, the stored position rolls back with
//! them and those entries are re-applied from the log, which is never
//! compacted past what `sync` has made durable.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use bytes::{BufMut, Bytes};
use serde::{Deserialize, Serialize};

use crate::raft::{Entry, StateMachine};
use crate::storage::{Db, WriteBatch};

pub const RESERVED_PREFIX: u8 = 0;
const APPLIED_KEY: &[u8] = b"\0meta/applied";
const SESSION_PREFIX: &[u8] = b"\0session/";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        client_id: u64,
        seq: u64,
    },
    Delete {
        key: Vec<u8>,
        client_id: u64,
        seq: u64,
    },
}

impl Command {
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).expect("command serialization cannot fail")
    }

    fn decode(data: &[u8]) -> io::Result<Self> {
        bincode::deserialize(data)
            .map_err(|e| io::Error::other(format!("corrupt command in log: {e}")))
    }

    fn session(&self) -> (u64, u64) {
        match self {
            Command::Set { client_id, seq, .. } | Command::Delete { client_id, seq, .. } => {
                (*client_id, *seq)
            }
        }
    }
}

fn session_key(client_id: u64) -> Vec<u8> {
    let mut k = SESSION_PREFIX.to_vec();
    k.put_u64(client_id);
    k
}

fn decode_u64(v: &[u8]) -> io::Result<u64> {
    v.try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| io::Error::other("corrupt u64 metadata"))
}

pub struct KvStateMachine {
    db: Arc<Db>,
    applied: (u64, u64),
}

impl KvStateMachine {
    pub fn open(db: Arc<Db>) -> io::Result<Self> {
        let applied = match db.get(APPLIED_KEY)? {
            Some(v) if v.len() == 16 => (decode_u64(&v[..8])?, decode_u64(&v[8..])?),
            Some(_) => return Err(io::Error::other("corrupt applied index")),
            None => (0, 0),
        };
        Ok(Self { db, applied })
    }

    fn applied_value(index: u64, term: u64) -> Vec<u8> {
        let mut v = Vec::with_capacity(16);
        v.put_u64(index);
        v.put_u64(term);
        v
    }
}

impl StateMachine for KvStateMachine {
    fn applied(&self) -> (u64, u64) {
        self.applied
    }

    fn apply(&mut self, entries: &[Entry]) -> io::Result<()> {
        let Some(last) = entries.last() else {
            return Ok(());
        };
        let mut batch = WriteBatch::new();
        let mut sessions: HashMap<u64, u64> = HashMap::new();
        for e in entries.iter().filter(|e| !e.data.is_empty()) {
            let cmd = Command::decode(&e.data)?;
            let (client_id, seq) = cmd.session();
            if client_id != 0 {
                let last_seq = match sessions.get(&client_id) {
                    Some(&s) => s,
                    None => self
                        .db
                        .get(&session_key(client_id))?
                        .map_or(Ok(0), |v| decode_u64(&v))?,
                };
                if seq <= last_seq {
                    tracing::debug!(client_id, seq, "skipping duplicate command");
                    continue;
                }
                sessions.insert(client_id, seq);
            }
            match cmd {
                Command::Set { key, value, .. } => batch.put(key, value),
                Command::Delete { key, .. } => batch.delete(key),
            }
        }
        for (client_id, seq) in sessions {
            batch.put(session_key(client_id), seq.to_be_bytes().to_vec());
        }
        batch.put(APPLIED_KEY, Self::applied_value(last.index, last.term));
        self.db.write(batch)?;
        self.applied = (last.index, last.term);
        Ok(())
    }

    fn snapshot(&self) -> io::Result<Vec<u8>> {
        let pairs = self
            .db
            .iter()
            .filter(|r| !matches!(r, Ok((k, _)) if k.as_ref() == APPLIED_KEY))
            .map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())))
            .collect::<crate::Result<Vec<_>>>()?;
        bincode::serialize(&pairs).map_err(io::Error::other)
    }

    fn restore(&mut self, index: u64, term: u64, data: &[u8]) -> io::Result<()> {
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = bincode::deserialize(data)
            .map_err(|e| io::Error::other(format!("corrupt snapshot: {e}")))?;
        // One atomic batch: wipe, load, and record the new position.
        let mut batch = WriteBatch::new();
        for r in self.db.iter() {
            batch.delete(r?.0);
        }
        for (k, v) in pairs {
            batch.put(k, v);
        }
        batch.put(APPLIED_KEY, Self::applied_value(index, term));
        self.db.write(batch)?;
        // The Raft log is reset right after this; the state must not be
        // lost even when the engine runs without per-write fsync.
        self.db.sync()?;
        self.applied = (index, term);
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        Ok(self.db.sync()?)
    }
}

/// Client keys must be non-empty and must not use the reserved prefix.
pub fn validate_key(key: &Bytes) -> Result<(), String> {
    if key.first() == Some(&RESERVED_PREFIX) {
        return Err("keys starting with 0x00 are reserved".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Options;

    fn entry(index: u64, cmd: Command) -> Entry {
        Entry {
            index,
            term: 1,
            data: cmd.encode(),
        }
    }

    #[test]
    fn applies_once_and_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let set = |seq, v: &str| Command::Set {
            key: b"k".to_vec(),
            value: v.as_bytes().to_vec(),
            client_id: 42,
            seq,
        };
        {
            let db = Arc::new(Db::open(dir.path(), Options::default()).unwrap());
            let mut sm = KvStateMachine::open(db.clone()).unwrap();
            // seq 1, then a retry of seq 1 committed later, then seq 2.
            sm.apply(&[entry(1, set(1, "a")), entry(2, set(1, "stale-retry"))])
                .unwrap();
            sm.apply(&[entry(3, set(2, "b")), entry(4, set(1, "very-stale"))])
                .unwrap();
            assert_eq!(db.get(b"k").unwrap().as_deref(), Some(b"b".as_ref()));
        }
        let db = Arc::new(Db::open(dir.path(), Options::default()).unwrap());
        let sm = KvStateMachine::open(db.clone()).unwrap();
        assert_eq!(sm.applied(), (4, 1));
        assert_eq!(db.get(b"k").unwrap().as_deref(), Some(b"b".as_ref()));
    }

    #[test]
    fn snapshot_restore_replaces_state() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let db_a = Arc::new(Db::open(a.path(), Options::default()).unwrap());
        let db_b = Arc::new(Db::open(b.path(), Options::default()).unwrap());
        let mut sm_a = KvStateMachine::open(db_a).unwrap();
        let mut sm_b = KvStateMachine::open(db_b.clone()).unwrap();
        db_b.put("only-in-b", "x").unwrap();
        sm_a.apply(&[entry(
            1,
            Command::Set {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                client_id: 0,
                seq: 0,
            },
        )])
        .unwrap();
        let snap = sm_a.snapshot().unwrap();
        sm_b.restore(1, 1, &snap).unwrap();
        assert_eq!(db_b.get(b"only-in-b").unwrap(), None);
        assert_eq!(db_b.get(b"k").unwrap().as_deref(), Some(b"v".as_ref()));
        assert_eq!(sm_b.applied(), (1, 1));
    }
}
