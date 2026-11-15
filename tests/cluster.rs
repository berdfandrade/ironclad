//! End-to-end tests against real `ironclad-server` processes.
//!
//! These exercise the full stack — TCP protocol, Raft over the network,
//! fsync'd storage — and real crashes (`SIGKILL`, no shutdown hooks).

use std::collections::{BTreeMap, HashMap};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use ironclad::client::Client;
use ironclad::protocol::Consistency;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Node {
    id: u64,
    dir: PathBuf,
    client_addr: String,
    peer_addr: String,
    child: Option<Child>,
}

struct Cluster {
    _root: tempfile::TempDir,
    nodes: Vec<Node>,
    extra_args: Vec<String>,
}

impl Cluster {
    fn new(n: u64, extra_args: &[&str]) -> Self {
        let root = tempfile::tempdir().unwrap();
        let nodes = (1..=n)
            .map(|id| Node {
                id,
                dir: root.path().join(format!("node{id}")),
                client_addr: format!("127.0.0.1:{}", free_port()),
                peer_addr: format!("127.0.0.1:{}", free_port()),
                child: None,
            })
            .collect();
        let mut c = Self {
            _root: root,
            nodes,
            extra_args: extra_args.iter().map(|s| s.to_string()).collect(),
        };
        for id in 1..=n {
            c.start(id);
        }
        c
    }

    fn node(&mut self, id: u64) -> &mut Node {
        self.nodes.iter_mut().find(|n| n.id == id).unwrap()
    }

    fn start(&mut self, id: u64) {
        let members: Vec<String> = self
            .nodes
            .iter()
            .map(|n| format!("{}={}={}", n.id, n.client_addr, n.peer_addr))
            .collect();
        let extra = self.extra_args.clone();
        let node = self.node(id);
        std::fs::create_dir_all(&node.dir).unwrap();
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(node.dir.join("server.log"))
            .unwrap();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_ironclad-server"));
        cmd.arg("--id")
            .arg(id.to_string())
            .arg("--data-dir")
            .arg(node.dir.join("data"))
            .args(["--tick-ms", "20"])
            .args(&extra)
            .env("RUST_LOG", "info")
            .stdout(Stdio::null())
            .stderr(log);
        for m in &members {
            cmd.arg("--member").arg(m);
        }
        node.child = Some(cmd.spawn().unwrap());
    }

    /// SIGKILL: no destructors, no flushes — a real crash.
    fn kill(&mut self, id: u64) {
        if let Some(mut child) = self.node(id).child.take() {
            child.kill().unwrap();
            child.wait().unwrap();
        }
    }

    fn addrs(&self) -> Vec<String> {
        self.nodes.iter().map(|n| n.client_addr.clone()).collect()
    }

    fn live_addrs(&self) -> Vec<(u64, String)> {
        self.nodes
            .iter()
            .filter(|n| n.child.is_some())
            .map(|n| (n.id, n.client_addr.clone()))
            .collect()
    }

    fn dump_logs(&self) {
        for n in &self.nodes {
            let log = std::fs::read_to_string(n.dir.join("server.log")).unwrap_or_default();
            eprintln!(
                "===== node {} =====\n{}",
                n.id,
                log.lines()
                    .rev()
                    .take(30)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect::<Vec<_>>()
                    .join("\n")
            );
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.dump_logs();
        }
        for n in &mut self.nodes {
            if let Some(mut c) = n.child.take() {
                let _ = c.kill();
                let _ = c.wait();
            }
        }
    }
}

async fn status(addr: &str) -> Option<HashMap<String, String>> {
    let mut c = Client::new(vec![addr.to_string()]);
    c.op_timeout = Duration::from_millis(500);
    let text = c.status().await.ok()?;
    Some(
        text.lines()
            .filter_map(|l| l.split_once('='))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    )
}

/// Waits until a live node reports itself leader; returns (id, term).
async fn wait_for_leader(cluster: &Cluster, timeout: Duration) -> (u64, u64) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let mut best = None;
        for (id, addr) in cluster.live_addrs() {
            if let Some(s) = status(&addr).await
                && s["role"] == "leader"
            {
                let term: u64 = s["term"].parse().unwrap();
                if best.is_none_or(|(_, t)| term > t) {
                    best = Some((id, term));
                }
            }
        }
        if let Some(b) = best {
            return b;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no leader elected within {timeout:?}");
}

async fn write_all(addrs: Vec<String>, writes: Vec<(String, Option<String>)>, workers: usize) {
    let chunk = writes.len().div_ceil(workers);
    let mut tasks = Vec::new();
    for part in writes.chunks(chunk) {
        let part = part.to_vec();
        let addrs = addrs.clone();
        tasks.push(tokio::spawn(async move {
            let mut c = Client::new(addrs);
            for (k, v) in part {
                match v {
                    Some(v) => c.set(k.clone(), v).await,
                    None => c.delete(k.clone()).await,
                }
                .unwrap_or_else(|e| panic!("write {k} failed: {e}"));
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_failover_and_convergence() {
    // Small compaction threshold so the restarted node is far enough behind
    // to need an InstallSnapshot.
    let mut cluster = Cluster::new(5, &["--compact-threshold", "300"]);
    let mut expected: BTreeMap<String, Option<String>> = BTreeMap::new();

    // 1. Elect a leader and write a few thousand records.
    let (old_leader, old_term) = wait_for_leader(&cluster, Duration::from_secs(10)).await;
    let batch: Vec<_> = (0..2000)
        .map(|i| (format!("key:{i:05}"), Some(format!("v1-{i}"))))
        .collect();
    expected.extend(batch.iter().cloned());
    write_all(cluster.addrs(), batch, 16).await;

    // 2. Kill the leader abruptly.
    cluster.kill(old_leader);

    // 3. A new leader is elected among the survivors, in a later term.
    let (new_leader, new_term) = wait_for_leader(&cluster, Duration::from_secs(10)).await;
    assert_ne!(new_leader, old_leader);
    assert!(
        new_term > old_term,
        "term did not advance: {old_term} -> {new_term}"
    );

    // 4. Keep writing while the old leader is down: new keys, overwrites
    //    and deletes.
    let mut batch: Vec<_> = (2000..3000)
        .map(|i| (format!("key:{i:05}"), Some(format!("v2-{i}"))))
        .collect();
    batch.extend(
        (0..500)
            .step_by(5)
            .map(|i| (format!("key:{i:05}"), Some(format!("v3-{i}")))),
    );
    batch.extend((1..500).step_by(5).map(|i| (format!("key:{i:05}"), None)));
    for (k, v) in &batch {
        expected.insert(k.clone(), v.clone());
    }
    write_all(cluster.addrs(), batch, 8).await;

    // 5. Bring the old leader back with its old data directory.
    cluster.start(old_leader);

    // 6. Every node converges to the same applied index...
    let deadline = Instant::now() + Duration::from_secs(30);
    let applied = loop {
        let mut indexes = Vec::new();
        for (_, addr) in cluster.live_addrs() {
            indexes.push(
                status(&addr)
                    .await
                    .and_then(|s| s["applied_index"].parse::<u64>().ok()),
            );
        }
        if indexes.len() == 5 && indexes.iter().all(|i| i.is_some() && *i == indexes[0]) {
            break indexes[0].unwrap();
        }
        assert!(
            Instant::now() < deadline,
            "nodes did not converge: {indexes:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(applied > 3000);

    // ...and to exactly the expected state, checked on each replica locally.
    for (id, addr) in cluster.live_addrs() {
        let mut c = Client::new(vec![addr]);
        for (k, v) in &expected {
            let got = c.get(k.clone(), Consistency::Local).await.unwrap();
            assert_eq!(
                got.as_deref()
                    .map(|b| String::from_utf8_lossy(b).into_owned()),
                *v,
                "node {id} key {k}"
            );
        }
    }

    // The rejoined node caught up through a snapshot transfer.
    let log = std::fs::read_to_string(cluster.node(old_leader).dir.join("server.log")).unwrap();
    assert!(
        log.contains("installing snapshot"),
        "expected snapshot install on node {old_leader}"
    );

    // Linearizable reads through any node still work after all of this.
    let mut c = Client::new(cluster.addrs());
    assert_eq!(
        c.get("key:00000", Consistency::Linearizable)
            .await
            .unwrap()
            .as_deref(),
        Some(b"v3-0".as_ref())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acknowledged_writes_survive_kill_9() {
    let mut cluster = Cluster::new(1, &[]);
    wait_for_leader(&cluster, Duration::from_secs(10)).await;
    let addrs = cluster.addrs();

    // Writers record every acknowledged write until the node dies under them.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let mut tasks = Vec::new();
    for w in 0..8 {
        let addrs = addrs.clone();
        let tx = tx.clone();
        tasks.push(tokio::spawn(async move {
            let mut c = Client::new(addrs);
            c.op_timeout = Duration::from_millis(500);
            for i in 0.. {
                let key = format!("w{w}:{i}");
                if c.set(key.clone(), format!("value-{i}")).await.is_err() {
                    return;
                }
                let _ = tx.send(key);
            }
        }));
    }
    drop(tx);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    cluster.kill(1);
    for t in tasks {
        t.await.unwrap();
    }
    let mut acked = Vec::new();
    while let Ok(k) = rx.try_recv() {
        acked.push(k);
    }
    assert!(
        acked.len() > 50,
        "too few writes before the crash: {}",
        acked.len()
    );

    cluster.start(1);
    wait_for_leader(&cluster, Duration::from_secs(10)).await;
    let mut c = Client::new(addrs);
    for key in &acked {
        let i = key.rsplit(':').next().unwrap();
        assert_eq!(
            c.get(key.clone(), Consistency::Linearizable)
                .await
                .unwrap()
                .as_deref(),
            Some(format!("value-{i}").as_bytes()),
            "acknowledged write {key} lost after crash"
        );
    }
}
