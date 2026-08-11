use crate::router::{RouterError, ShardRouter};
use raftvec_proto::raft_vec_server::RaftVec;
use raftvec_proto::{
    ClusterStatusRequest, ClusterStatusResponse, CreateCollectionRequest, CreateCollectionResponse,
    DeleteRequest, DeleteResponse, InsertRequest, InsertResponse, SearchRequest, SearchResponse,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// Implements the same client-facing RaftVec service raftvec-node does,
/// delegating to the ShardRouter instead of a local store — vecctl can
/// point at either without client-side changes.
pub struct AggregatorService {
    router: Arc<ShardRouter>,
}

impl AggregatorService {
    pub fn new(router: Arc<ShardRouter>) -> Self {
        Self { router }
    }
}

impl From<RouterError> for Status {
    fn from(err: RouterError) -> Self {
        match err {
            RouterError::Connect { .. } => Status::unavailable(err.to_string()),
            RouterError::Rpc(status) => status,
            RouterError::ShardUnreachable => Status::unavailable(err.to_string()),
        }
    }
}

#[tonic::async_trait]
impl RaftVec for AggregatorService {
    async fn create_collection(
        &self,
        request: Request<CreateCollectionRequest>,
    ) -> Result<Response<CreateCollectionResponse>, Status> {
        let req = request.into_inner();
        self.router.create_collection(&req.name, req.dim).await?;
        Ok(Response::new(CreateCollectionResponse { created: true }))
    }

    async fn insert(
        &self,
        request: Request<InsertRequest>,
    ) -> Result<Response<InsertResponse>, Status> {
        let req = request.into_inner();
        // The router resolves leadership internally, so there is nothing
        // to hint the aggregator's own caller about.
        let inserted = self.router.insert(&req.collection, req.records).await?;
        Ok(Response::new(InsertResponse {
            inserted,
            leader_hint: String::new(),
        }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        let deleted = self.router.delete(&req.collection, req.ids).await?;
        Ok(Response::new(DeleteResponse {
            deleted,
            leader_hint: String::new(),
        }))
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let start = std::time::Instant::now();
        let req = request.into_inner();
        let (results, shards_queried, shards_failed) = self
            .router
            .search(&req.collection, req.query_vector, req.k)
            .await;

        // Labeled by result status so degraded windows are visible as
        // their own series rather than averaged into steady-state latency.
        let status = if shards_failed > 0 { "degraded" } else { "ok" };
        metrics::histogram!("raftvec_query_duration_seconds", "status" => status)
            .record(start.elapsed().as_secs_f64());

        Ok(Response::new(SearchResponse {
            results,
            shards_queried,
            shards_failed,
            leader_hint: String::new(),
        }))
    }

    async fn cluster_status(
        &self,
        _request: Request<ClusterStatusRequest>,
    ) -> Result<Response<ClusterStatusResponse>, Status> {
        let (collections, vector_count) = self.router.cluster_status().await;
        Ok(Response::new(ClusterStatusResponse {
            collections,
            vector_count,
        }))
    }
}
