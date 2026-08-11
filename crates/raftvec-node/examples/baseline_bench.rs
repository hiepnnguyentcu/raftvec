//! Single-node scan latency baseline: brute-force search measured
//! in-process against a synthetic 500K-vector, 384-dim collection, so the
//! number reflects the scan itself rather than gRPC/network overhead.
//!
//! Run: cargo run --release -p raftvec-node --example baseline_bench

use raftvec_core::VectorRecord;
use raftvec_node::store::Store;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::time::Instant;

const N: u64 = 500_000;
const DIM: usize = 384;
const K: usize = 10;
const QUERIES: usize = 200;

fn random_vector(rng: &mut StdRng, dim: usize) -> Vec<f32> {
    (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect()
}

fn percentile(sorted_ms: &[f64], p: f64) -> f64 {
    let idx = ((sorted_ms.len() as f64 - 1.0) * p).round() as usize;
    sorted_ms[idx]
}

fn main() {
    let mut rng = StdRng::seed_from_u64(1);

    println!("generating {N} synthetic vectors (dim={DIM})...");
    let store = Store::new();
    store.create_collection("bench", DIM, 1).unwrap();

    let insert_start = Instant::now();
    const BATCH: u64 = 10_000;
    let mut id = 0u64;
    while id < N {
        let batch_end = (id + BATCH).min(N);
        let batch: Vec<VectorRecord> = (id..batch_end)
            .map(|i| VectorRecord::new(i, random_vector(&mut rng, DIM), HashMap::new()))
            .collect();
        store.insert("bench", batch).unwrap();
        id = batch_end;
    }
    println!("inserted {N} vectors in {:.2}s", insert_start.elapsed().as_secs_f64());

    // Warm up (first query pays allocator/cache warmup cost).
    let warmup_query = random_vector(&mut rng, DIM);
    let _ = store.search("bench", &warmup_query, K).unwrap();

    let mut latencies_ms = Vec::with_capacity(QUERIES);
    for _ in 0..QUERIES {
        let query = random_vector(&mut rng, DIM);
        let start = Instant::now();
        let results = store.search("bench", &query, K).unwrap();
        latencies_ms.push(start.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(results.len(), K);
    }

    latencies_ms.sort_by(|a, b| a.total_cmp(b));
    let p50 = percentile(&latencies_ms, 0.50);
    let p99 = percentile(&latencies_ms, 0.99);
    let mean: f64 = latencies_ms.iter().sum::<f64>() / latencies_ms.len() as f64;

    println!("\n--- single-node scan baseline ({N} vectors, dim={DIM}, k={K}, {QUERIES} queries) ---");
    println!("mean: {mean:.2}ms  p50: {p50:.2}ms  p99: {p99:.2}ms");
}
