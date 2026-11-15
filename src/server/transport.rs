//! Peer-to-peer transport for Raft messages.
//!
//! One-way messages over long-lived TCP connections: each node dials every
//! peer for its outbound traffic and accepts inbound connections for
//! incoming traffic. Frames are `len:u32 | bincode(Envelope)`.
//!
//! Delivery is best effort by design: queues are bounded, full queues drop,
//! broken connections drop whatever was queued. Raft retransmits on its own
//! (heartbeats, rejections), so the transport never needs to be reliable —
//! and never applies backpressure to the consensus thread.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc::SyncSender;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use super::Metrics;
use super::node::Event;
use crate::raft::{Envelope, NodeId};

/// Snapshots travel as a single message, so peer frames can be large.
const MAX_PEER_FRAME: usize = 512 * 1024 * 1024;

fn codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_PEER_FRAME)
        .new_codec()
}

/// Delivers queued messages to one peer, reconnecting with backoff.
pub async fn run_sender(peer: NodeId, addr: String, mut queue: mpsc::Receiver<Envelope>) {
    let mut backoff = Duration::from_millis(50);
    loop {
        let conn =
            match tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(&addr)).await {
                Ok(Ok(stream)) => {
                    let _ = stream.set_nodelay(true);
                    backoff = Duration::from_millis(50);
                    Framed::new(stream, codec())
                }
                _ => {
                    // While the peer is unreachable, discard traffic rather than
                    // replaying a stale backlog once it comes back.
                    while queue.try_recv().is_ok() {}
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(1));
                    continue;
                }
            };
        tracing::debug!(peer, %addr, "connected to peer");
        let mut conn = conn;
        loop {
            let Some(env) = queue.recv().await else {
                return; // node shutting down
            };
            let frame = match bincode::serialize(&env) {
                Ok(f) => f,
                Err(e) => {
                    tracing::error!(error = %e, "failed to encode raft message");
                    continue;
                }
            };
            // Batch whatever else is already queued into one flush.
            let mut result = conn.feed(Bytes::from(frame)).await;
            while result.is_ok() {
                match queue.try_recv() {
                    Ok(env) => match bincode::serialize(&env) {
                        Ok(f) => result = conn.feed(Bytes::from(f)).await,
                        Err(_) => continue,
                    },
                    Err(_) => break,
                }
            }
            if result.is_ok() {
                result = SinkExt::<Bytes>::flush(&mut conn).await;
            }
            if let Err(e) = result {
                tracing::debug!(peer, error = %e, "peer connection lost");
                break;
            }
        }
    }
}

/// Accepts peer connections and forwards their messages to the Raft loop.
pub async fn serve(
    listener: TcpListener,
    me: NodeId,
    members: Vec<NodeId>,
    events: SyncSender<Event>,
    metrics: Arc<Metrics>,
) {
    let members: Arc<[NodeId]> = members.into();
    loop {
        let (stream, remote) = match listener.accept().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "peer accept failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        let events = events.clone();
        let members = members.clone();
        let metrics = metrics.clone();
        tokio::spawn(async move {
            let mut conn = Framed::new(stream, codec());
            while let Some(frame) = conn.next().await {
                let Ok(frame) = frame else { break };
                let env: Envelope = match bincode::deserialize(&frame) {
                    Ok(env) => env,
                    Err(e) => {
                        tracing::warn!(%remote, error = %e, "malformed peer message; closing");
                        break;
                    }
                };
                if env.to != me || !members.contains(&env.from) {
                    tracing::warn!(%remote, from = env.from, to = env.to, "misaddressed peer message; closing");
                    break;
                }
                if events.try_send(Event::Peer(env)).is_err() {
                    metrics
                        .peer_messages_dropped
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    }
}
