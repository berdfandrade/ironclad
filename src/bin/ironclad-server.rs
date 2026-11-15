use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use ironclad::server::{Member, ServerConfig, run};

/// Ironclad node.
///
/// Single node:   ironclad-server --id 1 --data-dir ./data
/// Cluster node:  ironclad-server --id 1 --data-dir ./data/1 \
///                  --member 1=127.0.0.1:6001=127.0.0.1:7001 \
///                  --member 2=127.0.0.1:6002=127.0.0.1:7002 \
///                  --member 3=127.0.0.1:6003=127.0.0.1:7003
#[derive(Parser, Debug)]
#[command(version, verbatim_doc_comment)]
struct Args {
    /// This node's id (non-zero, unique in the cluster).
    #[arg(long)]
    id: u64,

    /// Directory for this node's data (storage engine + Raft log).
    #[arg(long)]
    data_dir: PathBuf,

    /// Cluster member as id=client_addr=peer_addr (advertised addresses).
    /// Repeat for every member, including this node. Omit for single-node.
    #[arg(long = "member")]
    members: Vec<Member>,

    /// Address to accept client connections on [default: own client_addr].
    #[arg(long)]
    listen: Option<String>,

    /// Address to accept peer connections on [default: own peer_addr].
    #[arg(long)]
    peer_listen: Option<String>,

    /// Raft tick interval in milliseconds.
    #[arg(long, default_value_t = 50)]
    tick_ms: u64,

    /// Election timeout in ticks (randomized between N and 2N).
    #[arg(long, default_value_t = 10)]
    election_ticks: u32,

    /// Compact the Raft log after this many applied entries.
    #[arg(long, default_value_t = 10_000)]
    compact_threshold: u64,

    /// Also fsync the storage engine on every applied batch. Not needed for
    /// durability (the Raft log is fsynced and replayed after power loss);
    /// costs one extra fsync per batch.
    #[arg(long)]
    storage_fsync: bool,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_target(false)
        .with_writer(std::io::stderr)
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .init();

    let args = Args::parse();
    let members = if args.members.is_empty() {
        vec![Member {
            id: args.id,
            client_addr: args
                .listen
                .clone()
                .unwrap_or_else(|| "127.0.0.1:6001".into()),
            peer_addr: args
                .peer_listen
                .clone()
                .unwrap_or_else(|| "127.0.0.1:7001".into()),
        }]
    } else {
        args.members
    };
    let mut cfg = ServerConfig::new(args.id, args.data_dir, members);
    if let Some(l) = args.listen {
        cfg.client_listen = l;
    }
    if let Some(l) = args.peer_listen {
        cfg.peer_listen = l;
    }
    cfg.tick = Duration::from_millis(args.tick_ms.max(1));
    cfg.raft.election_ticks = args.election_ticks;
    cfg.raft.compact_threshold = args.compact_threshold;
    cfg.storage.sync_writes = args.storage_fsync;

    if let Err(e) = run(cfg).await {
        eprintln!("ironclad-server: {e}");
        std::process::exit(1);
    }
}
