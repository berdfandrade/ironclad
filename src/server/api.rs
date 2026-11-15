//! Client-facing TCP server and command engine.
//!
//! Pipeline per request: decode frame → validate → route
//! (local read / linearizable read / replicated write) → encode response.
//!
//! Requests on one connection are executed strictly in order, which keeps
//! a client's writes in its `seq` order (needed for deduplication).
//! Concurrency comes from many connections; batching across them happens
//! in the Raft loop.
//!
//! Backpressure, outermost first:
//! 1. a connection limit (excess connections wait in the accept backlog),
//! 2. a bounded event queue into the Raft loop (full → `BUSY`),
//! 3. a per-request deadline (expired → `TIMEOUT`, outcome unknown).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{SyncSender, TrySendError};
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, oneshot};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use super::Metrics;
use super::kv::{Command, validate_key};
use super::node::{Event, ReadOutcome, WriteOutcome};
use crate::protocol::{
    Consistency, MAX_FRAME_LEN, Request, Response, decode_request, encode_response,
};
use crate::raft::NodeId;
use crate::storage::Db;

pub struct ApiContext {
    pub events: SyncSender<Event>,
    pub db: Arc<Db>,
    /// Client address of every member, for NOT_LEADER redirects.
    pub client_addrs: HashMap<NodeId, String>,
    pub request_timeout: Duration,
    pub metrics: Arc<Metrics>,
}

pub async fn serve(listener: TcpListener, ctx: Arc<ApiContext>, max_connections: usize) {
    let limit = Arc::new(Semaphore::new(max_connections));
    loop {
        let permit = limit
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed");
        let (stream, remote) = match listener.accept().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "client accept failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let ctx = ctx.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, &ctx).await {
                tracing::debug!(%remote, error = %e, "client connection closed");
            }
            drop(permit);
        });
    }
}

async fn handle_connection(stream: TcpStream, ctx: &ApiContext) -> std::io::Result<()> {
    stream.set_nodelay(true)?;
    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME_LEN)
        .new_codec();
    let mut conn = Framed::new(stream, codec);
    while let Some(frame) = conn.next().await {
        let frame = frame?;
        ctx.metrics.requests.fetch_add(1, Ordering::Relaxed);
        let (id, request) = decode_request(&frame);
        let response = match request {
            Ok(req) => execute(req, ctx).await,
            Err(e) => {
                ctx.metrics.errors.fetch_add(1, Ordering::Relaxed);
                let resp = Response::Error(e.to_string());
                // Without a readable header we cannot correlate a reply.
                if id.is_none() {
                    conn.send(Bytes::from(encode_response(0, &resp))).await?;
                    return Ok(());
                }
                resp
            }
        };
        conn.send(Bytes::from(encode_response(id.unwrap_or(0), &response)))
            .await?;
    }
    Ok(())
}

async fn execute(req: Request, ctx: &ApiContext) -> Response {
    let key = match &req {
        Request::Get { key, .. } | Request::Set { key, .. } | Request::Delete { key, .. } => {
            Some(key)
        }
        Request::Ping | Request::Status => None,
    };
    if let Some(Err(e)) = key.map(validate_key) {
        ctx.metrics.errors.fetch_add(1, Ordering::Relaxed);
        return Response::Error(e);
    }

    match req {
        Request::Ping => Response::Ok,
        Request::Status => {
            let (reply, rx) = oneshot::channel();
            match submit(ctx, Event::Status { reply }, rx).await {
                Ok(text) => Response::Status(text),
                Err(resp) => resp,
            }
        }
        Request::Get {
            key,
            consistency: Consistency::Local,
        } => {
            ctx.metrics.reads.fetch_add(1, Ordering::Relaxed);
            let db = ctx.db.clone();
            match tokio::task::spawn_blocking(move || db.get(&key)).await {
                Ok(Ok(Some(v))) => Response::Value(v),
                Ok(Ok(None)) => Response::NotFound,
                Ok(Err(e)) => Response::Error(e.to_string()),
                Err(e) => Response::Error(e.to_string()),
            }
        }
        Request::Get {
            key,
            consistency: Consistency::Linearizable,
        } => {
            ctx.metrics.reads.fetch_add(1, Ordering::Relaxed);
            let (reply, rx) = oneshot::channel();
            match submit(ctx, Event::Read { key, reply }, rx).await {
                Ok(ReadOutcome::Value(Some(v))) => Response::Value(v),
                Ok(ReadOutcome::Value(None)) => Response::NotFound,
                Ok(ReadOutcome::NotLeader(l)) => not_leader(ctx, l),
                Ok(ReadOutcome::Aborted(_)) => Response::NotLeader(None),
                Err(resp) => resp,
            }
        }
        Request::Set {
            key,
            value,
            client_id,
            seq,
        } => {
            let cmd = Command::Set {
                key: key.to_vec(),
                value: value.to_vec(),
                client_id,
                seq,
            };
            write(ctx, cmd).await
        }
        Request::Delete {
            key,
            client_id,
            seq,
        } => {
            let cmd = Command::Delete {
                key: key.to_vec(),
                client_id,
                seq,
            };
            write(ctx, cmd).await
        }
    }
}

async fn write(ctx: &ApiContext, cmd: Command) -> Response {
    ctx.metrics.writes.fetch_add(1, Ordering::Relaxed);
    let (reply, rx) = oneshot::channel();
    match submit(
        ctx,
        Event::Propose {
            data: cmd.encode(),
            reply,
        },
        rx,
    )
    .await
    {
        Ok(WriteOutcome::Committed) => Response::Ok,
        Ok(WriteOutcome::NotLeader(l)) => not_leader(ctx, l),
        // Definitely not applied: safe for the client to retry elsewhere.
        Ok(WriteOutcome::Aborted(_)) => Response::NotLeader(None),
        Err(resp) => resp,
    }
}

fn not_leader(ctx: &ApiContext, leader: Option<NodeId>) -> Response {
    Response::NotLeader(leader.and_then(|l| ctx.client_addrs.get(&l).cloned()))
}

async fn submit<T>(
    ctx: &ApiContext,
    event: Event,
    rx: oneshot::Receiver<T>,
) -> Result<T, Response> {
    match ctx.events.try_send(event) {
        Ok(()) => {}
        Err(TrySendError::Full(_)) => {
            ctx.metrics.busy.fetch_add(1, Ordering::Relaxed);
            return Err(Response::Busy);
        }
        Err(TrySendError::Disconnected(_)) => {
            return Err(Response::Error("node is shutting down".into()));
        }
    }
    match tokio::time::timeout(ctx.request_timeout, rx).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(_)) => Err(Response::Error("node is shutting down".into())),
        Err(_) => {
            ctx.metrics.timeouts.fetch_add(1, Ordering::Relaxed);
            Err(Response::Timeout)
        }
    }
}
