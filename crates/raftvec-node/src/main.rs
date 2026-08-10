use clap::Parser;
use raftvec_node::service::NodeService;
use raftvec_node::store::Store;
use raftvec_proto::raft_vec_server::RaftVecServer;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;

/// Standalone raftvec-node (M1): in-memory vector store served over gRPC.
/// From M2 this binary gains shard/replica identity; from M3 it joins a
/// per-shard Raft group. None of that exists yet — this is a single node
/// serving the full client-facing RaftVec API directly.
#[derive(Parser, Debug)]
#[command(name = "raftvec-node")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:50051")]
    listen: SocketAddr,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let store = Arc::new(Store::new());
    let service = NodeService::new(store);

    tracing::info!(addr = %args.listen, "raftvec-node listening");

    Server::builder()
        .add_service(RaftVecServer::new(service))
        .serve(args.listen)
        .await?;

    Ok(())
}
