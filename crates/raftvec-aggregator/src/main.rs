use anyhow::Context;
use clap::Parser;
use metrics_exporter_prometheus::PrometheusBuilder;
use raftvec_aggregator::router::ShardRouter;
use raftvec_aggregator::service::AggregatorService;
use raftvec_proto::raft_vec_server::RaftVecServer;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Server;

/// Stateless fan-out/routing layer in front of a fixed set of shards,
/// each a fixed set of Raft replicas. All durable state lives on the
/// replicas, so the aggregator can be killed and restarted freely.
#[derive(Parser, Debug)]
#[command(name = "raftvec-aggregator")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:50060")]
    listen: SocketAddr,

    /// One shard's replica addresses, comma-separated, in the order the
    /// shard's own nodes were started with. Repeat once per shard, in
    /// shard order (first --shard is shard 0, etc):
    ///   --shard 127.0.0.1:51001,127.0.0.1:51002,127.0.0.1:51003
    ///   --shard 127.0.0.1:51004,127.0.0.1:51005,127.0.0.1:51006
    #[arg(long = "shard", required = true)]
    shards: Vec<String>,

    /// Per-shard search deadline in milliseconds before that shard is
    /// dropped from the merge and reported in `shards_failed`.
    #[arg(long, default_value_t = 2000)]
    deadline_ms: u64,

    /// Address the Prometheus /metrics endpoint is served on.
    #[arg(long, default_value = "0.0.0.0:9101")]
    metrics_listen: SocketAddr,
}

fn normalize(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    PrometheusBuilder::new()
        .with_http_listener(args.metrics_listen)
        .install()
        .context("installing Prometheus exporter")?;
    tracing::info!(addr = %args.metrics_listen, "serving /metrics");

    let shard_replica_addrs: Vec<Vec<String>> = args
        .shards
        .iter()
        .map(|shard| shard.split(',').map(normalize).collect())
        .collect();

    tracing::info!(shards = ?shard_replica_addrs, "connecting to shard replicas");
    let router = ShardRouter::connect(
        &shard_replica_addrs,
        Duration::from_millis(args.deadline_ms),
    )
    .await?;
    let service = AggregatorService::new(Arc::new(router));

    tracing::info!(addr = %args.listen, shard_count = shard_replica_addrs.len(), "raftvec-aggregator listening");

    Server::builder()
        .add_service(
            RaftVecServer::new(service)
                .max_decoding_message_size(raftvec_aggregator::router::MAX_MESSAGE_SIZE)
                .max_encoding_message_size(raftvec_aggregator::router::MAX_MESSAGE_SIZE),
        )
        .serve(args.listen)
        .await?;

    Ok(())
}
