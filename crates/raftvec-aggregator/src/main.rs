use clap::Parser;
use raftvec_aggregator::router::ShardRouter;
use raftvec_aggregator::service::AggregatorService;
use raftvec_proto::raft_vec_server::RaftVecServer;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Server;

/// raftvec-aggregator (M2): stateless fan-out/routing layer in front of a
/// fixed set of shard nodes. No local state -- everything durable lives on
/// the shard nodes it talks to (technical design §2.3).
#[derive(Parser, Debug)]
#[command(name = "raftvec-aggregator")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:50060")]
    listen: SocketAddr,

    /// Shard node addresses, in shard order (shard 0 first, shard 1 next, ...).
    #[arg(long, value_delimiter = ',', required = true)]
    shards: Vec<String>,

    /// Per-shard search deadline in milliseconds before that shard is
    /// dropped from the merge (technical design §5.3).
    #[arg(long, default_value_t = 2000)]
    deadline_ms: u64,
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
    let shard_addrs: Vec<String> = args.shards.iter().map(|a| normalize(a)).collect();

    tracing::info!(shards = ?shard_addrs, "connecting to shard nodes");
    let router = ShardRouter::connect(&shard_addrs, Duration::from_millis(args.deadline_ms)).await?;
    let service = AggregatorService::new(Arc::new(router));

    tracing::info!(addr = %args.listen, shard_count = shard_addrs.len(), "raftvec-aggregator listening");

    Server::builder()
        .add_service(RaftVecServer::new(service))
        .serve(args.listen)
        .await?;

    Ok(())
}
