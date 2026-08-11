//! Same replication + leader-failover scenario as raft.rs's unit test,
//! but over real gRPC/TCP between separate server tasks — a distributed
//! system that only works in one process proves nothing.

use openraft::{BasicNode, Config};
use raftvec_core::VectorRecord;
use raftvec_node::raft::{LogStore, NodeId, Raft, ShardCommand, ShardStateMachine};
use raftvec_node::raft_network::{GrpcNetwork, ShardRaftService};
use raftvec_node::store::Store;
use raftvec_proto::shard_raft_server::ShardRaftServer;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

fn rec(id: u64, embedding: Vec<f32>) -> VectorRecord {
    VectorRecord::new(id, embedding, HashMap::new())
}

/// Starts one real shard replica: its own Raft core plus a real gRPC
/// server exposing ShardRaft on an OS-assigned localhost port.
async fn spawn_shard_replica(node_id: NodeId, config: Arc<openraft::Config>) -> (String, Raft, Arc<Store>) {
    let store = Arc::new(Store::new());
    store.create_collection("docs", 4, 1).unwrap();

    let log_store = LogStore::default();
    let sm = Arc::new(ShardStateMachine::new(store.clone()));
    let network = GrpcNetwork::default();

    let raft = openraft::Raft::new(node_id, config, network, log_store, sm)
        .await
        .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = ShardRaftService::new(raft.clone());

    tokio::spawn(async move {
        Server::builder()
            .add_service(ShardRaftServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    (format!("http://{addr}"), raft, store)
}

/// `client_write` returning success guarantees majority commit + apply on
/// the leader, but a follower applies asynchronously after learning the
/// new commit index -- a slower follower can still be mid-flight for a
/// few milliseconds after the write "succeeds". Real gRPC has enough
/// latency for that gap to be observable (an in-process loopback network
/// does not), so convergence must be polled for, not asserted immediately.
async fn wait_for_replication(
    stores: &HashMap<NodeId, Arc<Store>>,
    collection: &str,
    query: &[f32],
    expected_id: u64,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let converged = stores.values().all(|store| {
            store
                .search(collection, query, 1)
                .ok()
                .and_then(|r| r.first().map(|s| s.id))
                == Some(expected_id)
        });
        if converged {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            for (id, store) in stores {
                eprintln!("node-{id}: {:?}", store.search(collection, query, 1));
            }
            panic!("replication of id {expected_id} did not converge within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_leader_change(
    rafts: &HashMap<NodeId, Raft>,
    probe: NodeId,
    old_leader: Option<NodeId>,
    timeout: Duration,
) -> NodeId {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let metrics = rafts.get(&probe).unwrap().metrics().borrow().clone();
        if let Some(leader) = metrics.current_leader {
            if Some(leader) != old_leader {
                return leader;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for a leader change away from {old_leader:?} on node-{probe}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn shard_replica_group_replicates_over_real_grpc() {
    let config = Arc::new(
        Config {
            heartbeat_interval: 100,
            election_timeout_min: 300,
            election_timeout_max: 600,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );

    let mut rafts: HashMap<NodeId, Raft> = HashMap::new();
    let mut stores: HashMap<NodeId, Arc<Store>> = HashMap::new();
    let mut addrs: HashMap<NodeId, String> = HashMap::new();

    for id in 1..=3u64 {
        let (addr, raft, store) = spawn_shard_replica(id, config.clone()).await;
        rafts.insert(id, raft);
        stores.insert(id, store);
        addrs.insert(id, addr);
    }

    let mut members = BTreeMap::new();
    for id in 1..=3u64 {
        members.insert(id, BasicNode { addr: addrs[&id].clone() });
    }
    rafts.get(&1).unwrap().initialize(members).await.unwrap();

    let leader_id = wait_for_leader_change(&rafts, 1, None, Duration::from_secs(10)).await;

    rafts
        .get(&leader_id)
        .unwrap()
        .client_write(ShardCommand::Upsert {
            collection: "docs".to_string(),
            records: vec![rec(1, vec![1.0, 0.0, 0.0, 0.0])],
        })
        .await
        .unwrap();

    wait_for_replication(&stores, "docs", &[1.0, 0.0, 0.0, 0.0], 1, Duration::from_secs(2)).await;

    rafts.remove(&leader_id).unwrap().shutdown().await.unwrap();
    stores.remove(&leader_id);

    let remaining: Vec<u64> = rafts.keys().copied().collect();
    let new_leader_id =
        wait_for_leader_change(&rafts, remaining[0], Some(leader_id), Duration::from_secs(10)).await;
    assert_ne!(new_leader_id, leader_id);

    rafts
        .get(&new_leader_id)
        .unwrap()
        .client_write(ShardCommand::Upsert {
            collection: "docs".to_string(),
            records: vec![rec(2, vec![0.0, 1.0, 0.0, 0.0])],
        })
        .await
        .unwrap();

    wait_for_replication(&stores, "docs", &[0.0, 1.0, 0.0, 0.0], 2, Duration::from_secs(2)).await;
}
