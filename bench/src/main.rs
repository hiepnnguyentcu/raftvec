//! Open-loop load generator: fires Search requests at a fixed target rate
//! and reports achieved QPS, latency percentiles, and error count.
//! Open-loop (scheduling regardless of in-flight requests) is what exposes
//! latency degradation; a closed-loop client self-throttles and hides it.
//!
//! Usage: bench --addr http://127.0.0.1:50060 --dim 384 --qps 200 --duration-secs 60

use clap::Parser;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use raftvec_proto::raft_vec_client::RaftVecClient;
use raftvec_proto::SearchRequest;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time::interval;

#[derive(Parser, Debug)]
#[command(name = "bench")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:50060")]
    addr: String,

    #[arg(long, default_value = "docs")]
    collection: String,

    /// Must match the target collection's dimension.
    #[arg(long)]
    dim: usize,

    #[arg(long, default_value_t = 200)]
    qps: u64,

    #[arg(long, default_value_t = 60)]
    duration_secs: u64,

    #[arg(long, default_value_t = 10)]
    k: u32,
}

#[derive(Default)]
struct Results {
    latencies_ms: Mutex<Vec<f64>>,
    successes: AtomicU64,
    errors: AtomicU64,
}

fn percentile(sorted_ms: &[f64], p: f64) -> f64 {
    if sorted_ms.is_empty() {
        return 0.0;
    }
    let idx = ((sorted_ms.len() as f64 - 1.0) * p).round() as usize;
    sorted_ms[idx]
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    println!(
        "load test: {} qps for {}s against {} (collection={}, k={})",
        args.qps, args.duration_secs, args.addr, args.collection, args.k
    );

    // One real connection, cloned per request (cheap -- shares the
    // underlying HTTP/2 channel). Reconnecting per-request would measure
    // handshake overhead, not query latency.
    const MAX_MESSAGE_SIZE: usize = 128 * 1024 * 1024; // see raftvec-node's MAX_MESSAGE_SIZE
    let base_client = RaftVecClient::connect(args.addr.clone())
        .await?
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_MESSAGE_SIZE);

    let results = Arc::new(Results::default());
    let period = Duration::from_secs_f64(1.0 / args.qps as f64);
    let mut ticker = interval(period);
    let deadline = Instant::now() + Duration::from_secs(args.duration_secs);

    let mut handles = Vec::new();
    let mut rng = StdRng::seed_from_u64(42);

    let run_start = Instant::now();
    while Instant::now() < deadline {
        ticker.tick().await;

        let mut client = base_client.clone();
        let collection = args.collection.clone();
        let k = args.k;
        let query_vector: Vec<f32> = (0..args.dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
        let results = results.clone();

        handles.push(tokio::spawn(async move {
            let start = Instant::now();
            let outcome = client
                .search(SearchRequest {
                    collection,
                    query_vector,
                    k,
                })
                .await;
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

            match outcome {
                Ok(_) => {
                    results.successes.fetch_add(1, Ordering::Relaxed);
                    results.latencies_ms.lock().unwrap().push(elapsed_ms);
                }
                Err(_) => {
                    results.errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }
    let wall_secs = run_start.elapsed().as_secs_f64();

    let mut latencies = results.latencies_ms.lock().unwrap().clone();
    latencies.sort_by(|a, b| a.total_cmp(b));
    let successes = results.successes.load(Ordering::Relaxed);
    let errors = results.errors.load(Ordering::Relaxed);
    let achieved_qps = (successes + errors) as f64 / wall_secs;

    let p50 = percentile(&latencies, 0.50);
    let p99 = percentile(&latencies, 0.99);

    println!(
        "\nQPS: {achieved_qps:.0}  p50: {p50:.1}ms  p99: {p99:.1}ms  errors: {errors}  ({successes} ok, {} total, {wall_secs:.1}s)",
        successes + errors
    );

    Ok(())
}
