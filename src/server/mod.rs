//! A networked Ironclad node: storage engine + Raft + client API.
//!
//! ```text
//!   clients ──TCP──► api (tokio) ──bounded queue──► raft loop (thread) ──► peers (tokio)
//!                     │ local reads                    │ apply                ▲
//!                     └──────────────► Db ◄────────────┘        peers ──TCP──┘
//! ```

pub mod api;
pub mod kv;
pub mod node;
pub mod transport;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::mpsc;

use crate::raft::{self, NodeId, RaftCore, RaftLog};
use crate::storage::{self, Db};

#[derive(Default)]
pub struct Metrics {
    pub requests: AtomicU64,
    pub reads: AtomicU64,
    pub writes: AtomicU64,
    pub errors: AtomicU64,
    pub busy: AtomicU64,
    pub timeouts: AtomicU64,
    pub peer_messages_dropped: AtomicU64,
}

impl Metrics {
    pub fn snapshot(&self) -> Vec<(&'static str, u64)> {
        let l = |a: &AtomicU64| a.load(Ordering::Relaxed);
        vec![
            ("requests", l(&self.requests)),
            ("reads", l(&self.reads)),
            ("writes", l(&self.writes)),
            ("errors", l(&self.errors)),
            ("busy", l(&self.busy)),
            ("timeouts", l(&self.timeouts)),
            ("peer_messages_dropped", l(&self.peer_messages_dropped)),
        ]
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member {
    pub id: NodeId,
    /// Advertised address clients connect to.
    pub client_addr: String,
    /// Advertised address peers connect to.
    pub peer_addr: String,
}

impl std::str::FromStr for Member {
    type Err = String;

    /// `id=client_addr=peer_addr`, e.g. `1=10.0.0.1:6001=10.0.0.1:7001`.
    fn from_str(s: &str) -> Result<Self, String> {
        let parts: Vec<&str> = s.split('=').collect();
        let [id, client_addr, peer_addr] = parts[..] else {
            return Err(format!("expected id=client_addr=peer_addr, got {s:?}"));
        };
        let id: NodeId = id.parse().map_err(|_| format!("invalid node id {id:?}"))?;
        if id == 0 {
            return Err("node id 0 is reserved".into());
        }
        Ok(Member {
            id,
            client_addr: client_addr.into(),
            peer_addr: peer_addr.into(),
        })
    }
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub id: NodeId,
    pub data_dir: PathBuf,
    pub client_listen: String,
    pub peer_listen: String,
    /// Every member including this node. Static: membership changes are
    /// not supported (see ADR 0005).
    pub members: Vec<Member>,
    pub tick: Duration,
    pub request_timeout: Duration,
    pub max_connections: usize,
    pub event_queue: usize,
    pub raft: raft::Config,
    pub storage: storage::Options,
}

impl ServerConfig {
    pub fn new(id: NodeId, data_dir: PathBuf, members: Vec<Member>) -> Self {
        let me = members.iter().find(|m| m.id == id);
        Self {
            client_listen: me.map_or("127.0.0.1:6001".into(), |m| m.client_addr.clone()),
            peer_listen: me.map_or("127.0.0.1:7001".into(), |m| m.peer_addr.clone()),
            id,
            data_dir,
            members,
            tick: Duration::from_millis(50),
            request_timeout: Duration::from_secs(5),
            max_connections: 1024,
            event_queue: 16 * 1024,
            raft: raft::Config::default(),
            // The Raft log is fsynced; see `kv` for why the engine needn't be.
            storage: storage::Options {
                sync_writes: false,
                ..storage::Options::default()
            },
        }
    }
}

/// Opens storage, recovers Raft state, and serves until a fatal error.
pub async fn run(cfg: ServerConfig) -> std::io::Result<()> {
    let ids: Vec<NodeId> = cfg.members.iter().map(|m| m.id).collect();
    if !ids.contains(&cfg.id) {
        return Err(std::io::Error::other(format!(
            "node {} is not in the member list",
            cfg.id
        )));
    }
    let peers: Vec<NodeId> = ids.iter().copied().filter(|&p| p != cfg.id).collect();

    let db = Arc::new(Db::open(cfg.data_dir.join("db"), cfg.storage.clone())?);
    let sm = kv::KvStateMachine::open(db.clone())?;
    let log = RaftLog::open(&cfg.data_dir.join("raft"))?;
    let mut raft_cfg = cfg.raft.clone();
    if raft_cfg.seed == 0 {
        raft_cfg.seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
    }
    let core = RaftCore::new(cfg.id, peers.clone(), log, sm, raft_cfg)?;
    let status = core.status();
    tracing::info!(
        id = cfg.id,
        term = status.term,
        applied = status.applied_index,
        last = status.last_index,
        "recovered node state"
    );

    let client_listener = TcpListener::bind(&cfg.client_listen).await?;
    let peer_listener = TcpListener::bind(&cfg.peer_listen).await?;
    tracing::info!(client = %cfg.client_listen, peer = %cfg.peer_listen, "listening");

    let metrics = Arc::new(Metrics::default());
    let (events_tx, events_rx) = std::sync::mpsc::sync_channel(cfg.event_queue);

    let mut outbound = HashMap::new();
    for m in cfg.members.iter().filter(|m| m.id != cfg.id) {
        let (tx, rx) = mpsc::channel(4096);
        outbound.insert(m.id, tx);
        tokio::spawn(transport::run_sender(m.id, m.peer_addr.clone(), rx));
    }

    let raft_loop = node::RaftLoop::new(
        core,
        db.clone(),
        events_rx,
        outbound,
        cfg.tick,
        metrics.clone(),
    );
    let (fatal_tx, fatal_rx) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("ironclad-raft".into())
        .spawn(move || {
            let _ = fatal_tx.send(raft_loop.run());
        })?;

    tokio::spawn(transport::serve(
        peer_listener,
        cfg.id,
        ids,
        events_tx.clone(),
        metrics.clone(),
    ));
    let ctx = Arc::new(api::ApiContext {
        events: events_tx,
        db,
        client_addrs: cfg
            .members
            .iter()
            .map(|m| (m.id, m.client_addr.clone()))
            .collect(),
        request_timeout: cfg.request_timeout,
        metrics,
    });
    tokio::spawn(api::serve(client_listener, ctx, cfg.max_connections));

    let err = fatal_rx
        .await
        .unwrap_or_else(|_| std::io::Error::other("raft thread panicked"));
    tracing::error!(error = %err, "fatal error in raft loop; stopping (fail-stop)");
    Err(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_members() {
        let m: Member = "3=node3:6001=node3:7001".parse().unwrap();
        assert_eq!(m.id, 3);
        assert_eq!(m.client_addr, "node3:6001");
        assert_eq!(m.peer_addr, "node3:7001");
        assert!("0=a=b".parse::<Member>().is_err());
        assert!("1=a".parse::<Member>().is_err());
    }
}
