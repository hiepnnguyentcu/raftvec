//! Regression test for a bug found only by measuring latency *after* a
//! chaos run rather than just during it.
//!
//! When the replica at the router's cached leader index stops responding,
//! the gRPC call against it does not fail fast -- the connection is still
//! established, the peer just never answers. Originally the only bound on
//! that was the caller's overall fan-out deadline, so the first attempt
//! consumed the entire budget and no other replica was ever tried. Worse,
//! the cached index was only persisted on *success*, so when the outer
//! deadline cancelled the retry loop the advancement was lost and the
//! next query started at the same unresponsive replica again.
//!
//! Live effect: a ~3s leader election turned into a steady 2002ms p50 --
//! permanently, long after the cluster itself had recovered.
//!
//! Note the failure mode being simulated: a replica that *hangs*, not one
//! that refuses connections. A refused connection errors immediately and
//! the original code handled it correctly -- an earlier version of this
//! test shut the server down instead, and passed even with the fix
//! reverted, which is exactly the kind of test that provides false
//! assurance. This version verifies against the fix being removed.

use raftvec_aggregator::router::ShardRouter;
use raftvec_proto::raft_vec_server::{RaftVec, RaftVecServer};
use raftvec_proto::{
    ClusterStatusRequest, ClusterStatusResponse, CreateCollectionRequest, CreateCollectionResponse,
    DeleteRequest, DeleteResponse, InsertRequest, InsertResponse, ScoredRecord, SearchRequest,
    SearchResponse,
};
use std::time::{Duration, Instant};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

/// The router's overall per-shard deadline. The whole point of the fix is
/// that an unresponsive replica costs materially less than this.
const FAN_OUT_DEADLINE: Duration = Duration::from_secs(2);

/// A replica that completes the gRPC handshake normally -- so the router
/// connects to it successfully at startup -- but never answers a Search.
/// This is what a killed-but-not-yet-reaped peer looks like from the
/// client side, and it is the case the per-attempt timeout exists for.
struct HangingReplica;

#[tonic::async_trait]
impl RaftVec for HangingReplica {
    async fn search(&self, _: Request<SearchRequest>) -> Result<Response<SearchResponse>, Status> {
        std::future::pending::<()>().await;
        unreachable!()
    }

    async fn create_collection(
        &self,
        _: Request<CreateCollectionRequest>,
    ) -> Result<Response<CreateCollectionResponse>, Status> {
        Ok(Response::new(CreateCollectionResponse { created: true }))
    }
    async fn insert(&self, _: Request<InsertRequest>) -> Result<Response<InsertResponse>, Status> {
        std::future::pending::<()>().await;
        unreachable!()
    }
    async fn delete(&self, _: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        std::future::pending::<()>().await;
        unreachable!()
    }
    async fn cluster_status(
        &self,
        _: Request<ClusterStatusRequest>,
    ) -> Result<Response<ClusterStatusResponse>, Status> {
        Ok(Response::new(ClusterStatusResponse {
            collections: vec![],
            vector_count: 0,
        }))
    }
}

/// A replica that answers Search immediately and claims leadership
/// (empty `leader_hint` == "I am the leader, here are your results").
struct HealthyReplica;

#[tonic::async_trait]
impl RaftVec for HealthyReplica {
    async fn search(&self, _: Request<SearchRequest>) -> Result<Response<SearchResponse>, Status> {
        Ok(Response::new(SearchResponse {
            results: vec![ScoredRecord {
                id: 42,
                score: 1.0,
                metadata: Default::default(),
            }],
            shards_queried: 1,
            shards_failed: 0,
            leader_hint: String::new(),
        }))
    }

    async fn create_collection(
        &self,
        _: Request<CreateCollectionRequest>,
    ) -> Result<Response<CreateCollectionResponse>, Status> {
        Ok(Response::new(CreateCollectionResponse { created: true }))
    }
    async fn insert(&self, _: Request<InsertRequest>) -> Result<Response<InsertResponse>, Status> {
        Ok(Response::new(InsertResponse {
            inserted: 0,
            leader_hint: String::new(),
        }))
    }
    async fn delete(&self, _: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        Ok(Response::new(DeleteResponse {
            deleted: 0,
            leader_hint: String::new(),
        }))
    }
    async fn cluster_status(
        &self,
        _: Request<ClusterStatusRequest>,
    ) -> Result<Response<ClusterStatusResponse>, Status> {
        Ok(Response::new(ClusterStatusResponse {
            collections: vec!["docs".to_string()],
            vector_count: 1,
        }))
    }
}

async fn spawn<S>(service: S) -> String
where
    S: RaftVec,
{
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        Server::builder()
            .add_service(RaftVecServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    addr
}

#[tokio::test]
async fn search_skips_an_unresponsive_replica_instead_of_burning_the_whole_deadline() {
    // Replica order matters: the router's cached leader index starts at 0,
    // so the hanging replica is the one it reaches for first -- exactly
    // the situation after a shard's leader is killed.
    let hanging = spawn(HangingReplica).await;
    let healthy = spawn(HealthyReplica).await;

    let router = ShardRouter::connect(&[vec![hanging, healthy]], FAN_OUT_DEADLINE)
        .await
        .unwrap();

    // Each of these must complete well inside the fan-out deadline.
    // Pre-fix, the first consumed the entire 2s budget *and* every
    // subsequent one did too, because the cached index never advanced
    // past the replica that hung.
    for attempt in 1..=3 {
        let started = Instant::now();
        let (results, shards_queried, shards_failed) =
            router.search("docs", vec![1.0, 0.0, 0.0, 0.0], 1).await;
        let elapsed = started.elapsed();

        assert_eq!(shards_queried, 1);
        assert_eq!(
            shards_failed, 0,
            "attempt {attempt}: should have been served by the healthy replica, \
             got shards_failed={shards_failed}"
        );
        assert_eq!(results.len(), 1, "attempt {attempt}: expected the healthy replica's result");
        assert!(
            elapsed < FAN_OUT_DEADLINE,
            "attempt {attempt}: took {elapsed:?} against a {FAN_OUT_DEADLINE:?} deadline -- \
             the unresponsive replica consumed the whole budget instead of one bounded attempt"
        );
    }
}
