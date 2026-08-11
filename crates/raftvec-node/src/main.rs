use anyhow::Context;
use clap::Parser;
use metrics_exporter_prometheus::PrometheusBuilder;
use openraft::{BasicNode, Config};
use raftvec_node::metrics::{spawn_leader_election_watcher, spawn_replication_lag_gauge, spawn_vector_count_gauge};
use raftvec_node::raft::{LogStore, NodeId, ShardStateMachine};
use raftvec_node::raft_network::{GrpcNetwork, ShardRaftService, MAX_MESSAGE_SIZE};
use raftvec_node::service::NodeService;
use raftvec_node::store::Store;
use raftvec_proto::raft_vec_server::RaftVecServer;
use raftvec_proto::shard_raft_server::ShardRaftServer;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Server;

/// raftvec-node (M3): one replica of one shard's Raft group. Serves the
/// client-facing RaftVec API (writes go through Raft, reads served only
/// when this replica believes itself the leader) and the internal
/// ShardRaft transport (AppendEntries/Vote/InstallSnapshot) on the same
/// port.
#[derive(Parser, Debug)]
#[command(name = "raftvec-node")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:50051")]
    listen: SocketAddr,

    /// This replica's Raft node id, unique within its shard's group.
    #[arg(long)]
    node_id: NodeId,

    /// Every replica in this shard's Raft group, including this node
    /// itself, as `id=addr` pairs (comma-separated). Addresses double as
    /// both the ShardRaft transport target and the value handed back to
    /// the aggregator as a leader_hint.
    #[arg(long, value_delimiter = ',', required = true)]
    peers: Vec<String>,

    /// Calls raft.initialize() with the full peer set shortly after
    /// startup. Exactly one node in a fresh shard group should set this;
    /// on an already-initialized cluster it's a harmless no-op error.
    #[arg(long, default_value_t = false)]
    bootstrap: bool,

    /// Address the Prometheus /metrics endpoint is served on.
    #[arg(long, default_value = "0.0.0.0:9100")]
    metrics_listen: SocketAddr,
}

fn normalize(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

fn parse_peers(peers: &[String]) -> anyhow::Result<BTreeMap<NodeId, BasicNode>> {
    let mut members = BTreeMap::new();
    for p in peers {
        let (id_str, addr) = p
            .split_once('=')
            .with_context(|| format!("peer '{p}' must be in id=addr form"))?;
        let id: NodeId = id_str.parse().with_context(|| format!("bad peer id in '{p}'"))?;
        members.insert(id, BasicNode { addr: normalize(addr) });
    }
    Ok(members)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let members = parse_peers(&args.peers)?;

    PrometheusBuilder::new()
        .with_http_listener(args.metrics_listen)
        .install()
        .context("installing Prometheus exporter")?;
    tracing::info!(addr = %args.metrics_listen, "serving /metrics");

    // openraft uses `heartbeat_interval` as the AppendEntries RPC timeout,
    // and AppendEntries carries replicated payload (a catch-up batch can be
    // tens of MB) — so it must cover realistic transfer time, not just
    // round-trip latency. Widening `election_timeout_max` costs recovery
    // twice: failure detection, then the leader lease openraft ties to the
    // same bound. Values are from measured behavior under Docker
    // networking; see README "Known limitations".
    let config = Arc::new(
        Config {
            heartbeat_interval: 250,
            election_timeout_min: 750,
            election_timeout_max: 1500,
            ..Default::default()
        }
        .validate()?,
    );

    let store = Arc::new(Store::new());
    let log_store = LogStore::default();
    let state_machine = Arc::new(ShardStateMachine::new(store.clone()));
    let network = GrpcNetwork::default();

    let raft = openraft::Raft::new(args.node_id, config, network, log_store, state_machine).await?;

    spawn_leader_election_watcher(raft.clone());
    spawn_replication_lag_gauge(raft.clone());
    spawn_vector_count_gauge(store.clone(), args.node_id);

    if args.bootstrap {
        let raft = raft.clone();
        tokio::spawn(async move {
            // Give peers a moment to come up and start listening before
            // proposing the initial membership config against them.
            tokio::time::sleep(Duration::from_millis(500)).await;
            if let Err(e) = raft.initialize(members).await {
                tracing::warn!(error = %e, "cluster initialize skipped (already initialized?)");
            }
        });
    }

    let node_service = NodeService::new(store, raft.clone(), args.node_id);
    let raft_service = ShardRaftService::new(raft);

    tracing::info!(addr = %args.listen, node_id = args.node_id, "raftvec-node listening");

    Server::builder()
        .add_service(
            RaftVecServer::new(node_service)
                .max_decoding_message_size(MAX_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_MESSAGE_SIZE),
        )
        .add_service(
            ShardRaftServer::new(raft_service)
                .max_decoding_message_size(MAX_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_MESSAGE_SIZE),
        )
        .serve(args.listen)
        .await?;

    Ok(())
}
