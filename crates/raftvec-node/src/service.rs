use crate::store::{Store, StoreError};
use raftvec_core::VectorRecord;
use raftvec_proto::raft_vec_server::RaftVec;
use raftvec_proto::{
    ClusterStatusRequest, ClusterStatusResponse, CreateCollectionRequest, CreateCollectionResponse,
    DeleteRequest, DeleteResponse, InsertRequest, InsertResponse, ScoredRecord, SearchRequest,
    SearchResponse,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct NodeService {
    store: Arc<Store>,
}

impl NodeService {
    pub fn new(store: Arc<Store>) -> Self {
        Self { store }
    }
}

impl From<StoreError> for Status {
    fn from(err: StoreError) -> Self {
        match err {
            StoreError::CollectionExists(_) => Status::already_exists(err.to_string()),
            StoreError::CollectionNotFound(_) => Status::not_found(err.to_string()),
            StoreError::DimensionMismatch { .. } => Status::invalid_argument(err.to_string()),
        }
    }
}

#[tonic::async_trait]
impl RaftVec for NodeService {
    async fn create_collection(
        &self,
        request: Request<CreateCollectionRequest>,
    ) -> Result<Response<CreateCollectionResponse>, Status> {
        let req = request.into_inner();
        self.store
            .create_collection(&req.name, req.dim as usize, req.shard_count)?;
        Ok(Response::new(CreateCollectionResponse { created: true }))
    }

    async fn insert(&self, request: Request<InsertRequest>) -> Result<Response<InsertResponse>, Status> {
        let req = request.into_inner();
        let records: Vec<VectorRecord> = req
            .records
            .into_iter()
            .map(|r| VectorRecord::new(r.id, r.embedding, r.metadata))
            .collect();

        let store = self.store.clone();
        let collection = req.collection;
        let inserted = tokio::task::spawn_blocking(move || store.insert(&collection, records))
            .await
            .map_err(|e| Status::internal(e.to_string()))??;

        Ok(Response::new(InsertResponse {
            inserted: inserted as u32,
        }))
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        let deleted = self.store.delete(&req.collection, &req.ids)?;
        Ok(Response::new(DeleteResponse {
            deleted: deleted as u32,
        }))
    }

    async fn search(&self, request: Request<SearchRequest>) -> Result<Response<SearchResponse>, Status> {
        let req = request.into_inner();

        // brute_force_top_k is a rayon-parallel scan; run it off the tokio
        // worker thread so a large collection's search doesn't stall other
        // in-flight requests on this node.
        let store = self.store.clone();
        let collection = req.collection.clone();
        let query = req.query_vector;
        let k = req.k as usize;
        let scored = tokio::task::spawn_blocking(move || store.search(&collection, &query, k))
            .await
            .map_err(|e| Status::internal(e.to_string()))??;

        let results = scored
            .into_iter()
            .map(|s| ScoredRecord {
                id: s.id,
                score: s.score,
                metadata: self
                    .store
                    .get_metadata(&req.collection, s.id)
                    .unwrap_or_default(),
            })
            .collect();

        Ok(Response::new(SearchResponse {
            results,
            shards_queried: 1,
            shards_failed: 0,
        }))
    }

    async fn cluster_status(
        &self,
        _request: Request<ClusterStatusRequest>,
    ) -> Result<Response<ClusterStatusResponse>, Status> {
        let (collections, vector_count) = self.store.cluster_status();
        Ok(Response::new(ClusterStatusResponse {
            collections,
            vector_count,
        }))
    }
}
