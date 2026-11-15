//! Client wire protocol.
//!
//! Every message is a frame: `length:u32` (big-endian, excluding itself)
//! followed by the body. Bodies (integers big-endian):
//!
//! ```text
//! request : version:u8=1  request_id:u64  op:u8  args
//!   GET    op=1  consistency:u8 (0=linearizable, 1=local)  key
//!   SET    op=2  client_id:u64 seq:u64  key  value
//!   DELETE op=3  client_id:u64 seq:u64  key
//!   PING   op=4
//!   STATUS op=5
//!
//! response: version:u8=1  request_id:u64  status:u8  payload
//!   0 OK            —
//!   1 VALUE         value
//!   2 NOT_FOUND     —
//!   3 NOT_LEADER    has_hint:u8 [addr: string]
//!   4 BUSY          —     (backpressure: retry later)
//!   5 TIMEOUT       —     (outcome unknown: retry with the same seq)
//!   6 ERROR         message: string
//!   7 STATUS        text: string (key=value lines)
//!
//! key, value : len:u32 bytes
//! string     : len:u32 utf-8
//! ```
//!
//! `(client_id, seq)` make writes idempotent: a retry with the same pair is
//! applied at most once. `client_id = 0` opts out.

use bytes::{BufMut, Bytes};

use crate::codec::Reader;
use crate::error::{Error, Result};

pub const VERSION: u8 = 1;
pub const MAX_KEY_LEN: usize = 64 * 1024;
pub const MAX_VALUE_LEN: usize = 4 * 1024 * 1024;
pub const MAX_FRAME_LEN: usize = MAX_VALUE_LEN + MAX_KEY_LEN + 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Consistency {
    /// Served by the leader after confirming leadership (ReadIndex).
    Linearizable,
    /// Served from the local replica; may be stale.
    Local,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    Get {
        key: Bytes,
        consistency: Consistency,
    },
    Set {
        key: Bytes,
        value: Bytes,
        client_id: u64,
        seq: u64,
    },
    Delete {
        key: Bytes,
        client_id: u64,
        seq: u64,
    },
    Ping,
    Status,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Response {
    Ok,
    Value(Bytes),
    NotFound,
    NotLeader(Option<String>),
    Busy,
    Timeout,
    Error(String),
    Status(String),
}

fn put_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    buf.put_u32(b.len() as u32);
    buf.put_slice(b);
}

fn key(r: &mut Reader) -> Result<Bytes> {
    let k = r.len_prefixed()?;
    if k.is_empty() || k.len() > MAX_KEY_LEN {
        return Err(Error::InvalidArgument(format!(
            "key length must be 1..={MAX_KEY_LEN}"
        )));
    }
    Ok(Bytes::copy_from_slice(k))
}

fn value(r: &mut Reader) -> Result<Bytes> {
    let v = r.len_prefixed()?;
    if v.len() > MAX_VALUE_LEN {
        return Err(Error::InvalidArgument(format!(
            "value longer than {MAX_VALUE_LEN}"
        )));
    }
    Ok(Bytes::copy_from_slice(v))
}

fn string(r: &mut Reader) -> Result<String> {
    String::from_utf8(r.len_prefixed()?.to_vec())
        .map_err(|_| Error::InvalidArgument("invalid utf-8".into()))
}

fn header(r: &mut Reader) -> Result<u64> {
    let v = r.u8()?;
    if v != VERSION {
        return Err(Error::InvalidArgument(format!(
            "unsupported protocol version {v}"
        )));
    }
    r.u64()
}

fn finish<T>(r: Reader, value: T) -> Result<T> {
    if r.is_empty() {
        Ok(value)
    } else {
        Err(Error::InvalidArgument("trailing bytes in frame".into()))
    }
}

pub fn encode_request(id: u64, req: &Request) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    buf.put_u8(VERSION);
    buf.put_u64(id);
    match req {
        Request::Get { key, consistency } => {
            buf.put_u8(1);
            buf.put_u8(match consistency {
                Consistency::Linearizable => 0,
                Consistency::Local => 1,
            });
            put_bytes(&mut buf, key);
        }
        Request::Set {
            key,
            value,
            client_id,
            seq,
        } => {
            buf.put_u8(2);
            buf.put_u64(*client_id);
            buf.put_u64(*seq);
            put_bytes(&mut buf, key);
            put_bytes(&mut buf, value);
        }
        Request::Delete {
            key,
            client_id,
            seq,
        } => {
            buf.put_u8(3);
            buf.put_u64(*client_id);
            buf.put_u64(*seq);
            put_bytes(&mut buf, key);
        }
        Request::Ping => buf.put_u8(4),
        Request::Status => buf.put_u8(5),
    }
    buf
}

/// Parses an untrusted request frame. Returns the request id whenever the
/// header was readable, so an error can still be correlated.
pub fn decode_request(frame: &[u8]) -> (Option<u64>, Result<Request>) {
    let mut r = Reader::new(frame);
    let id = match header(&mut r) {
        Ok(id) => id,
        Err(e) => return (None, Err(e)),
    };
    let req = (|| {
        let req = match r.u8()? {
            1 => {
                let consistency = match r.u8()? {
                    0 => Consistency::Linearizable,
                    1 => Consistency::Local,
                    c => return Err(Error::InvalidArgument(format!("unknown consistency {c}"))),
                };
                Request::Get {
                    key: key(&mut r)?,
                    consistency,
                }
            }
            2 => Request::Set {
                client_id: r.u64()?,
                seq: r.u64()?,
                key: key(&mut r)?,
                value: value(&mut r)?,
            },
            3 => Request::Delete {
                client_id: r.u64()?,
                seq: r.u64()?,
                key: key(&mut r)?,
            },
            4 => Request::Ping,
            5 => Request::Status,
            op => return Err(Error::InvalidArgument(format!("unknown op {op}"))),
        };
        finish(r, req)
    })();
    (Some(id), req)
}

pub fn encode_response(id: u64, resp: &Response) -> Vec<u8> {
    let mut buf = Vec::with_capacity(32);
    buf.put_u8(VERSION);
    buf.put_u64(id);
    match resp {
        Response::Ok => buf.put_u8(0),
        Response::Value(v) => {
            buf.put_u8(1);
            put_bytes(&mut buf, v);
        }
        Response::NotFound => buf.put_u8(2),
        Response::NotLeader(hint) => {
            buf.put_u8(3);
            match hint {
                Some(addr) => {
                    buf.put_u8(1);
                    put_bytes(&mut buf, addr.as_bytes());
                }
                None => buf.put_u8(0),
            }
        }
        Response::Busy => buf.put_u8(4),
        Response::Timeout => buf.put_u8(5),
        Response::Error(msg) => {
            buf.put_u8(6);
            put_bytes(&mut buf, msg.as_bytes());
        }
        Response::Status(text) => {
            buf.put_u8(7);
            put_bytes(&mut buf, text.as_bytes());
        }
    }
    buf
}

pub fn decode_response(frame: &[u8]) -> Result<(u64, Response)> {
    let mut r = Reader::new(frame);
    let id = header(&mut r)?;
    let resp = match r.u8()? {
        0 => Response::Ok,
        1 => Response::Value(Bytes::copy_from_slice(r.len_prefixed()?)),
        2 => Response::NotFound,
        3 => Response::NotLeader(match r.u8()? {
            0 => None,
            _ => Some(string(&mut r)?),
        }),
        4 => Response::Busy,
        5 => Response::Timeout,
        6 => Response::Error(string(&mut r)?),
        7 => Response::Status(string(&mut r)?),
        s => return Err(Error::InvalidArgument(format!("unknown status {s}"))),
    };
    finish(r, (id, resp))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn arb_request() -> impl Strategy<Value = Request> {
        let key = proptest::collection::vec(any::<u8>(), 1..64).prop_map(Bytes::from);
        let value = proptest::collection::vec(any::<u8>(), 0..256).prop_map(Bytes::from);
        prop_oneof![
            (key.clone(), any::<bool>()).prop_map(|(key, local)| Request::Get {
                key,
                consistency: if local {
                    Consistency::Local
                } else {
                    Consistency::Linearizable
                },
            }),
            (key.clone(), value, any::<u64>(), any::<u64>()).prop_map(
                |(key, value, client_id, seq)| Request::Set {
                    key,
                    value,
                    client_id,
                    seq
                }
            ),
            (key, any::<u64>(), any::<u64>()).prop_map(|(key, client_id, seq)| Request::Delete {
                key,
                client_id,
                seq
            }),
            Just(Request::Ping),
            Just(Request::Status),
        ]
    }

    proptest! {
        #[test]
        fn request_roundtrip(id in any::<u64>(), req in arb_request()) {
            let (got_id, got) = decode_request(&encode_request(id, &req));
            prop_assert_eq!(got_id, Some(id));
            prop_assert_eq!(got.unwrap(), req);
        }

        #[test]
        fn decoding_garbage_never_panics(data in proptest::collection::vec(any::<u8>(), 0..128)) {
            let _ = decode_request(&data);
            let _ = decode_response(&data);
        }
    }

    #[test]
    fn response_roundtrip() {
        for resp in [
            Response::Ok,
            Response::Value(Bytes::from("v")),
            Response::NotFound,
            Response::NotLeader(Some("10.0.0.1:6001".into())),
            Response::NotLeader(None),
            Response::Busy,
            Response::Timeout,
            Response::Error("boom".into()),
            Response::Status("role=leader".into()),
        ] {
            assert_eq!(
                decode_response(&encode_response(9, &resp)).unwrap(),
                (9, resp)
            );
        }
    }

    #[test]
    fn rejects_empty_key_and_bad_version() {
        let mut frame = encode_request(
            1,
            &Request::Get {
                key: Bytes::from("k"),
                consistency: Consistency::Local,
            },
        );
        frame[0] = 2;
        assert!(decode_request(&frame).1.is_err());
        let mut buf = vec![VERSION];
        buf.extend_from_slice(&7u64.to_be_bytes());
        buf.extend_from_slice(&[1, 0, 0, 0, 0, 0]);
        let (id, req) = decode_request(&buf);
        assert_eq!(id, Some(7));
        assert!(req.is_err());
    }
}
