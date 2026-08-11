//! Regression test: `RaftMetrics.current_leader` is a replica's own
//! cached belief, and an isolated ex-leader keeps believing it leads —
//! without a live quorum check it would serve increasingly stale reads
//! forever. The fix is `raft.ensure_linearizable()` before serving reads
//! (see NodeService::search).

use openraft::{BasicNode, Config};
use raftvec_node::raft::{LogStore, NodeId, Raft, ShardStateMachine};
use raftvec_node::raft_network::{GrpcNetwork, ShardRaftService};
use raftvec_node::service::NodeService;
use raftvec_node::store::Store;
use raftvec_proto::raft_vec_client::RaftVecClient;
use raftvec_proto::raft_vec_server::RaftVecServer;
use raftvec_proto::shard_raft_server::ShardRaftServer;
use raftvec_proto::SearchRequest;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// Same as raft_grpc_test.rs's helper, but also serves the client-facing
/// RaftVec API (NodeService), since this test needs to call Search through
/// the real gRPC path -- the bug lived in NodeService::search, not in the
/// Raft core itself.
async fn spawn_shard_replica(node_id: NodeId, config: Arc<openraft::Config>) -> (String, Raft) {
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

    (format!("http://{addr}"), raft)
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
            panic!(
                "timed out waiting for a leader change away from {old_leader:?} on node-{probe}"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn isolated_former_leader_refuses_reads_after_losing_quorum() {
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

    let mut rafts: HashMap<NodeId, Raft> = HashMap::new();
    let mut addrs: HashMap<NodeId, String> = HashMap::new();
    for id in 1..=3u64 {
        let (addr, raft) = spawn_shard_replica(id, config.clone()).await;
        rafts.insert(id, raft);
        addrs.insert(id, addr);
    }

    let mut members = BTreeMap::new();
    for id in 1..=3u64 {
        members.insert(
            id,
            BasicNode {
                addr: addrs[&id].clone(),
            },
        );
    }
    rafts.get(&1).unwrap().initialize(members).await.unwrap();

    let leader_id = wait_for_leader_change(&rafts, 1, None, Duration::from_secs(5)).await;
    let leader_addr = addrs[&leader_id].clone();

    // Isolate the leader by shutting down both followers -- it keeps
    // running, unaware anything happened, exactly like a network
    // partition from the leader's point of view.
    for (id, raft) in rafts {
        if id != leader_id {
            raft.shutdown().await.unwrap();
        }
    }

    // Give the leader a moment to notice its heartbeats are failing (it
    // won't proactively step down for reads -- that's the whole bug --
    // but ensure_linearizable should refuse regardless of timing).
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut client = RaftVecClient::connect(leader_addr).await.unwrap();
    let result = client
        .search(SearchRequest {
            collection: "docs".to_string(),
            query_vector: vec![1.0, 0.0, 0.0, 0.0],
            k: 1,
        })
        .await;

    let status = result.expect_err("an isolated former leader must not serve reads");
    assert_eq!(
        status.code(),
        tonic::Code::Unavailable,
        "expected Unavailable (lost quorum), got: {status:?}"
    );
}
