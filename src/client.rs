//! Async client with leader discovery, retries and idempotent writes.
//!
//! A `Client` keeps one connection and one request in flight (methods take
//! `&mut self`); use one client per task for concurrency. Each write gets a
//! fresh `seq`, reused across retries, so a write whose outcome was lost
//! (leader crashed after committing, connection dropped, timeout) can be
//! retried without being applied twice.

use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::protocol::{
    Consistency, MAX_FRAME_LEN, Request, Response, decode_response, encode_request,
};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("server error: {0}")]
    Server(String),
    #[error("cluster unavailable after {attempts} attempts: {last}")]
    Unavailable { attempts: u32, last: String },
    #[error("protocol error: {0}")]
    Protocol(String),
}

pub type ClientResult<T> = std::result::Result<T, ClientError>;

type Conn = Framed<TcpStream, LengthDelimitedCodec>;

pub struct Client {
    addrs: Vec<String>,
    current: usize,
    conn: Option<Conn>,
    next_request_id: u64,
    client_id: u64,
    seq: u64,
    /// Total time budget per operation, across retries.
    pub op_timeout: Duration,
}

fn random_u64() -> u64 {
    use std::hash::{BuildHasher, RandomState};
    // RandomState is seeded from OS randomness per process; mixing in the
    // time and a counter keeps ids distinct for clients in one process.
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    RandomState::new().hash_one((n, t)) | 1
}

impl Client {
    /// `addrs` are client addresses of any subset of the cluster.
    pub fn new(addrs: Vec<String>) -> Self {
        assert!(!addrs.is_empty(), "at least one address is required");
        Self {
            addrs,
            current: 0,
            conn: None,
            next_request_id: 1,
            client_id: random_u64(),
            seq: 0,
            op_timeout: Duration::from_secs(10),
        }
    }

    pub async fn get(
        &mut self,
        key: impl Into<Bytes>,
        consistency: Consistency,
    ) -> ClientResult<Option<Bytes>> {
        match self
            .call(Request::Get {
                key: key.into(),
                consistency,
            })
            .await?
        {
            Response::Value(v) => Ok(Some(v)),
            Response::NotFound => Ok(None),
            other => Err(ClientError::Protocol(format!(
                "unexpected response {other:?}"
            ))),
        }
    }

    pub async fn set(
        &mut self,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
    ) -> ClientResult<()> {
        self.seq += 1;
        let req = Request::Set {
            key: key.into(),
            value: value.into(),
            client_id: self.client_id,
            seq: self.seq,
        };
        self.call(req).await.map(drop)
    }

    pub async fn delete(&mut self, key: impl Into<Bytes>) -> ClientResult<()> {
        self.seq += 1;
        let req = Request::Delete {
            key: key.into(),
            client_id: self.client_id,
            seq: self.seq,
        };
        self.call(req).await.map(drop)
    }

    pub async fn ping(&mut self) -> ClientResult<()> {
        self.call(Request::Ping).await.map(drop)
    }

    /// Status text of the node this client is currently connected to.
    pub async fn status(&mut self) -> ClientResult<String> {
        match self.call(Request::Status).await? {
            Response::Status(s) => Ok(s),
            other => Err(ClientError::Protocol(format!(
                "unexpected response {other:?}"
            ))),
        }
    }

    async fn connect(&mut self) -> std::io::Result<&mut Conn> {
        if self.conn.is_none() {
            let addr = &self.addrs[self.current];
            let stream = tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(addr))
                .await
                .map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!("connect to {addr} timed out"),
                    )
                })??;
            stream.set_nodelay(true)?;
            let codec = LengthDelimitedCodec::builder()
                .max_frame_length(MAX_FRAME_LEN)
                .new_codec();
            self.conn = Some(Framed::new(stream, codec));
        }
        Ok(self.conn.as_mut().unwrap())
    }

    async fn roundtrip(&mut self, req: &Request) -> std::io::Result<Response> {
        let id = self.next_request_id;
        self.next_request_id += 1;
        let conn = self.connect().await?;
        conn.send(Bytes::from(encode_request(id, req))).await?;
        let frame = conn.next().await.ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "connection closed")
        })??;
        let (rid, resp) = decode_response(&frame).map_err(std::io::Error::from)?;
        if rid != id {
            return Err(std::io::Error::other(format!(
                "response id {rid} != request id {id}"
            )));
        }
        Ok(resp)
    }

    fn rotate(&mut self) {
        self.conn = None;
        self.current = (self.current + 1) % self.addrs.len();
    }

    async fn call(&mut self, req: Request) -> ClientResult<Response> {
        let deadline = Instant::now() + self.op_timeout;
        let mut attempts = 0;
        let mut backoff = Duration::from_millis(20);
        loop {
            attempts += 1;
            let last = match tokio::time::timeout_at(deadline.into(), self.roundtrip(&req)).await {
                Err(_) => {
                    // A response may still arrive on this connection; drop
                    // it so ids never get out of step.
                    self.conn = None;
                    return Err(ClientError::Unavailable {
                        attempts,
                        last: "operation timed out".into(),
                    });
                }
                Ok(Err(e)) => {
                    self.rotate();
                    e.to_string()
                }
                Ok(Ok(resp)) => match resp {
                    Response::NotLeader(Some(addr)) => {
                        self.conn = None;
                        self.current = match self.addrs.iter().position(|a| *a == addr) {
                            Some(i) => i,
                            None => {
                                self.addrs.push(addr);
                                self.addrs.len() - 1
                            }
                        };
                        // Follow a redirect immediately, unless hints keep
                        // bouncing us around (stale leader information).
                        if attempts <= 3 {
                            continue;
                        }
                        "redirected too many times".to_string()
                    }
                    Response::NotLeader(None) => {
                        self.rotate();
                        "no leader (election in progress)".to_string()
                    }
                    Response::Busy => "server busy".to_string(),
                    // Outcome unknown: retrying with the same seq is safe.
                    Response::Timeout => "request timed out on server".to_string(),
                    Response::Error(e) => return Err(ClientError::Server(e)),
                    other => return Ok(other),
                },
            };
            if Instant::now() + backoff >= deadline {
                return Err(ClientError::Unavailable { attempts, last });
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_millis(500));
        }
    }
}
