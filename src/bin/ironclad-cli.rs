use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use ironclad::client::Client;
use ironclad::protocol::Consistency;

/// Command-line client and load generator for Ironclad.
#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    /// Comma-separated client addresses of cluster members.
    #[arg(long, short, default_value = "127.0.0.1:6001", value_delimiter = ',')]
    addr: Vec<String>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Read a key.
    Get {
        key: String,
        /// Read from the connected replica without contacting the leader
        /// (fast, possibly stale).
        #[arg(long)]
        local: bool,
    },
    /// Write a key.
    Set {
        key: String,
        value: String,
    },
    /// Delete a key.
    Del {
        key: String,
    },
    Ping,
    /// Show the status of every node given in --addr.
    Status,
    /// Closed-loop load test reporting throughput and latency percentiles.
    Bench {
        #[arg(long, default_value_t = 20_000)]
        ops: usize,
        #[arg(long, default_value_t = 32)]
        concurrency: usize,
        #[arg(long, default_value_t = 100)]
        value_size: usize,
        /// Fraction of operations that are (linearizable) reads.
        #[arg(long, default_value_t = 0.0)]
        read_ratio: f64,
        #[arg(long, default_value_t = 10_000)]
        keys: usize,
    },
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let i = ((sorted.len() as f64 * p).ceil() as usize).clamp(1, sorted.len()) - 1;
    sorted[i]
}

async fn bench(
    addrs: Vec<String>,
    ops: usize,
    concurrency: usize,
    value_size: usize,
    read_ratio: f64,
    keys: usize,
) {
    let value = vec![b'x'; value_size];
    let start = Instant::now();
    let mut tasks = Vec::new();
    for worker in 0..concurrency {
        let addrs = addrs.clone();
        let value = value.clone();
        let n = ops / concurrency + usize::from(worker < ops % concurrency);
        tasks.push(tokio::spawn(async move {
            let mut client = Client::new(addrs);
            let mut latencies = Vec::with_capacity(n);
            let mut errors = 0usize;
            // Deterministic per-worker key/op mix (no RNG dependency).
            let mut x = (worker as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            for _ in 0..n {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let key = format!("bench:{}", x % keys.max(1) as u64);
                let is_read = (x >> 11) as f64 / (1u64 << 53) as f64 % 1.0 < read_ratio;
                let t = Instant::now();
                let r = if is_read {
                    client.get(key, Consistency::Linearizable).await.map(drop)
                } else {
                    client.set(key, value.clone()).await
                };
                match r {
                    Ok(()) => latencies.push(t.elapsed()),
                    Err(_) => errors += 1,
                }
            }
            (latencies, errors)
        }));
    }
    let mut all = Vec::with_capacity(ops);
    let mut errors = 0;
    for t in tasks {
        let (l, e) = t.await.unwrap();
        all.extend(l);
        errors += e;
    }
    let elapsed = start.elapsed();
    all.sort_unstable();
    println!("operations   {} ok, {} failed", all.len(), errors);
    println!("concurrency  {concurrency}");
    println!("value size   {value_size} B, read ratio {read_ratio}");
    println!("elapsed      {:.2?}", elapsed);
    println!(
        "throughput   {:.0} ops/s",
        all.len() as f64 / elapsed.as_secs_f64()
    );
    for (name, p) in [
        ("p50", 0.50),
        ("p95", 0.95),
        ("p99", 0.99),
        ("p99.9", 0.999),
    ] {
        println!("{name:<12} {:.2?}", percentile(&all, p));
    }
    println!(
        "max          {:.2?}",
        all.last().copied().unwrap_or_default()
    );
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let mut client = Client::new(args.addr.clone());
    let result = match args.cmd {
        Cmd::Get { key, local } => {
            let consistency = if local {
                Consistency::Local
            } else {
                Consistency::Linearizable
            };
            client.get(key, consistency).await.map(|v| match v {
                Some(v) => println!("{}", String::from_utf8_lossy(&v)),
                None => println!("(nil)"),
            })
        }
        Cmd::Set { key, value } => client.set(key, value).await.map(|_| println!("OK")),
        Cmd::Del { key } => client.delete(key).await.map(|_| println!("OK")),
        Cmd::Ping => client.ping().await.map(|_| println!("PONG")),
        Cmd::Status => {
            for addr in &args.addr {
                let mut c = Client::new(vec![addr.clone()]);
                c.op_timeout = Duration::from_secs(2);
                println!("--- {addr}");
                match c.status().await {
                    Ok(s) => print!("{s}"),
                    Err(e) => println!("unreachable: {e}"),
                }
            }
            Ok(())
        }
        Cmd::Bench {
            ops,
            concurrency,
            value_size,
            read_ratio,
            keys,
        } => {
            bench(
                args.addr,
                ops,
                concurrency.max(1),
                value_size,
                read_ratio,
                keys,
            )
            .await;
            Ok(())
        }
    };
    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
