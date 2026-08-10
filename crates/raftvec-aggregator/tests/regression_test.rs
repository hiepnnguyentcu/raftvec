//! The M2 exit criterion (product spec §9): "sharded cluster returns
//! identical results to the single-node baseline." This spins up real
//! shard nodes (actual gRPC servers over localhost TCP, not in-process
//! function calls) behind a ShardRouter, and asserts every result exactly
//! matches an independent single-node Store holding the same data -- the
//! same oracle-equality discipline as M1's tests/oracle_test.rs, extended
//! across the network boundary sharding introduces.

use openraft::{BasicNode, Config};
use raftvec_aggregator::router::ShardRouter;
use raftvec_core::VectorRecord as CoreRecord;
use raftvec_node::raft::{LogStore, NodeId, ShardStateMachine};
use raftvec_node::raft_network::{GrpcNetwork, ShardRaftService};
use raftvec_node::service::NodeService;
use raftvec_node::store::Store;
use raftvec_proto::raft_vec_server::RaftVecServer;
use raftvec_proto::shard_raft_server::ShardRaftServer;
use raftvec_proto::VectorRecord as ProtoRecord;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

const SHARD_COUNT: usize = 4;
const DEADLINE: Duration = Duration::from_secs(5);

fn random_vector(rng: &mut StdRng, dim: usize) -> Vec<f32> {
    (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect()
}

/// Starts one real shard node (gRPC server over localhost TCP) as a
/// single-member Raft group of its own, and returns its address. A lone
/// node in a 1-node group elects itself leader immediately, so this is
/// enough to exercise the real NodeService write/read path (M3) while
/// keeping this test's actual subject -- the aggregator's fan-out/merge --
/// unchanged from M2. Binding "127.0.0.1:0" avoids fixed-port collisions.
async fn spawn_shard_node(node_id: NodeId) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let self_addr = format!("http://{addr}");

    let store = Arc::new(Store::new());
    let log_store = LogStore::default();
    let state_machine = Arc::new(ShardStateMachine::new(store.clone()));
    let network = GrpcNetwork::default();
    let config = Arc::new(
        Config {
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let raft = openraft::Raft::new(node_id, config, network, log_store, state_machine)
        .await
        .unwrap();

    let node_service = NodeService::new(store, raft.clone(), node_id);
    let raft_service = ShardRaftService::new(raft.clone());

    tokio::spawn(async move {
        Server::builder()
            .add_service(RaftVecServer::new(node_service))
            .add_service(ShardRaftServer::new(raft_service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut members = BTreeMap::new();
    members.insert(node_id, BasicNode { addr: self_addr.clone() });
    raft.initialize(members).await.unwrap();

    // The router's first request would otherwise race the (near-instant,
    // but not synchronous) self-election and hit an "unknown leader"
    // rejection.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if raft.metrics().borrow().current_leader == Some(node_id) {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "node-{node_id} never became its own leader");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    self_addr
}

async fn spawn_router() -> ShardRouter {
    let mut addrs = Vec::with_capacity(SHARD_COUNT);
    for shard_id in 0..SHARD_COUNT {
        addrs.push(spawn_shard_node(shard_id as NodeId + 1).await);
    }
    ShardRouter::connect(&addrs, DEADLINE).await.unwrap()
}

#[tokio::test]
async fn sharded_cluster_matches_oracle_on_random_corpus() {
    let dim = 24;
    let n = 5_000u64;
    let k = 10;

    let router = spawn_router().await;
    let oracle = Store::new();
    oracle.create_collection("docs", dim, 1).unwrap();
    router.create_collection("docs", dim as u32).await.unwrap();

    let mut rng = StdRng::seed_from_u64(123);
    let mut proto_records = Vec::with_capacity(n as usize);
    let mut core_records = Vec::with_capacity(n as usize);
    for id in 0..n {
        let emb = random_vector(&mut rng, dim);
        proto_records.push(ProtoRecord {
            id,
            embedding: emb.clone(),
            metadata: HashMap::new(),
        });
        core_records.push(CoreRecord::new(id, emb, HashMap::new()));
    }

    oracle.insert("docs", core_records).unwrap();
    let inserted = router.insert("docs", proto_records).await.unwrap();
    assert_eq!(inserted, n as u32);

    for _ in 0..15 {
        let query = random_vector(&mut rng, dim);
        let (cluster_results, shards_queried, shards_failed) =
            router.search("docs", query.clone(), k).await;
        assert_eq!(shards_queried, SHARD_COUNT as u32);
        assert_eq!(shards_failed, 0, "no shard should fail in a healthy cluster");

        let oracle_results = oracle.search("docs", &query, k as usize).unwrap();

        assert_eq!(cluster_results.len(), oracle_results.len());
        for (c, o) in cluster_results.iter().zip(oracle_results.iter()) {
            assert_eq!(c.id, o.id, "ranked id mismatch");
            assert!((c.score - o.score).abs() < 1e-5, "score mismatch for id {}", c.id);
        }
    }
}

#[tokio::test]
async fn sharded_cluster_matches_oracle_after_deletes() {
    let dim = 16;
    let n = 2_000u64;
    let k = 5;

    let router = spawn_router().await;
    let oracle = Store::new();
    oracle.create_collection("docs", dim, 1).unwrap();
    router.create_collection("docs", dim as u32).await.unwrap();

    let mut rng = StdRng::seed_from_u64(456);
    let mut proto_records = Vec::with_capacity(n as usize);
    let mut core_records = Vec::with_capacity(n as usize);
    for id in 0..n {
        let emb = random_vector(&mut rng, dim);
        proto_records.push(ProtoRecord {
            id,
            embedding: emb.clone(),
            metadata: HashMap::new(),
        });
        core_records.push(CoreRecord::new(id, emb, HashMap::new()));
    }
    oracle.insert("docs", core_records).unwrap();
    router.insert("docs", proto_records).await.unwrap();

    // Delete every third id through the aggregator (routed to the owning
    // shard) and through the oracle directly; both should now agree on the
    // shrunken corpus.
    let deleted_ids: Vec<u64> = (0..n).step_by(3).collect();
    oracle.delete("docs", &deleted_ids).unwrap();
    router.delete("docs", deleted_ids).await.unwrap();

    for _ in 0..10 {
        let query = random_vector(&mut rng, dim);
        let (cluster_results, _, shards_failed) = router.search("docs", query.clone(), k).await;
        assert_eq!(shards_failed, 0);

        let oracle_results = oracle.search("docs", &query, k as usize).unwrap();

        assert_eq!(cluster_results.len(), oracle_results.len());
        for (c, o) in cluster_results.iter().zip(oracle_results.iter()) {
            assert_eq!(c.id, o.id);
            assert!((c.score - o.score).abs() < 1e-5);
        }
    }
}

#[tokio::test]
async fn sharded_cluster_ties_break_the_same_way_as_the_oracle() {
    // Identical embeddings placed on different shards force cross-shard
    // score ties -- exercising that the aggregator's merge (raftvec_core's
    // ScoredId ordering) breaks ties (lower id wins) the same way the
    // single-node scan does, not just that per-shard results are locally
    // sorted correctly.
    let dim = 4;
    let router = spawn_router().await;
    let oracle = Store::new();
    oracle.create_collection("docs", dim, 1).unwrap();
    router.create_collection("docs", dim as u32).await.unwrap();

    // ids chosen so fxhash(id) % 4 spreads them across all 4 shards; if the
    // hash distribution changes this may need adjusting, but the test
    // fails loudly (length/id mismatch) rather than silently passing on
    // one shard.
    let ids = [0u64, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
    let embedding = vec![1.0, 0.0, 0.0, 0.0];

    let proto_records: Vec<ProtoRecord> = ids
        .iter()
        .map(|&id| ProtoRecord {
            id,
            embedding: embedding.clone(),
            metadata: HashMap::new(),
        })
        .collect();
    let core_records: Vec<CoreRecord> = ids
        .iter()
        .map(|&id| CoreRecord::new(id, embedding.clone(), HashMap::new()))
        .collect();

    oracle.insert("docs", core_records).unwrap();
    router.insert("docs", proto_records).await.unwrap();

    let query = vec![1.0, 0.0, 0.0, 0.0];
    let k = ids.len() as u32;
    let (cluster_results, _, shards_failed) = router.search("docs", query.clone(), k).await;
    assert_eq!(shards_failed, 0);
    let oracle_results = oracle.search("docs", &query, k as usize).unwrap();

    let cluster_ids: Vec<u64> = cluster_results.iter().map(|r| r.id).collect();
    let oracle_ids: Vec<u64> = oracle_results.iter().map(|r| r.id).collect();
    assert_eq!(cluster_ids, oracle_ids, "tie-break order must match the single-node oracle exactly");
}
