use crate::raft::{NodeId, Raft, ShardCommand};
use crate::store::{Store, StoreError};
use openraft::error::{ClientWriteError, RaftError};
use openraft::BasicNode;
use raftvec_core::VectorRecord;
use raftvec_proto::raft_vec_server::RaftVec;
use raftvec_proto::{
    ClusterStatusRequest, ClusterStatusResponse, CreateCollectionRequest, CreateCollectionResponse,
    DeleteRequest, DeleteResponse, InsertRequest, InsertResponse, ScoredRecord, SearchRequest,
    SearchResponse,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// One replica of one shard's Raft group. Vector mutations reach `store`
/// only via committed Raft log entries (`raft.client_write`); collection
/// creation is applied out-of-band on every replica, and reads are served
/// only after confirming leadership with a quorum.
pub struct NodeService {
    store: Arc<Store>,
    raft: Raft,
    node_id: NodeId,
}

enum Leadership {
    IAmLeader,
    Redirect(String),
    Unknown,
}

impl NodeService {
    pub fn new(store: Arc<Store>, raft: Raft, node_id: NodeId) -> Self {
        Self { store, raft, node_id }
    }

    /// Leader address resolved from openraft's own membership metrics, so
    /// it cannot drift from what the Raft core believes.
    async fn leadership(&self) -> Leadership {
        let metrics = self.raft.metrics().borrow().clone();
        match metrics.current_leader {
            Some(id) if id == self.node_id => Leadership::IAmLeader,
            Some(id) => match metrics.membership_config.membership().get_node(&id) {
                Some(node) => Leadership::Redirect(node.addr.clone()),
                None => Leadership::Unknown,
            },
            None => Leadership::Unknown,
        }
    }

    /// `client_write` failed; either translate it into a leader address to
    /// hand back to the caller, or a genuine error if none is known.
    #[allow(clippy::result_large_err)]
    fn write_error_to_hint(
        err: RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>,
    ) -> Result<String, Status> {
        match err {
            RaftError::APIError(ClientWriteError::ForwardToLeader(fwd)) => match fwd.leader_node {
                Some(node) => Ok(node.addr),
                None => Err(Status::unavailable("no leader currently known for this shard")),
            },
            other => Err(Status::internal(other.to_string())),
        }
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
        let count = records.len() as u32;

        match self
            .raft
            .client_write(ShardCommand::Upsert {
                collection: req.collection,
                records,
            })
            .await
        {
            Ok(_) => Ok(Response::new(InsertResponse {
                inserted: count,
                leader_hint: String::new(),
            })),
            Err(e) => match Self::write_error_to_hint(e) {
                Ok(hint) => Ok(Response::new(InsertResponse {
                    inserted: 0,
                    leader_hint: hint,
                })),
                Err(status) => Err(status),
            },
        }
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        let count = req.ids.len() as u32;

        match self
            .raft
            .client_write(ShardCommand::Delete {
                collection: req.collection,
                ids: req.ids,
            })
            .await
        {
            Ok(_) => Ok(Response::new(DeleteResponse {
                deleted: count,
                leader_hint: String::new(),
            })),
            Err(e) => match Self::write_error_to_hint(e) {
                Ok(hint) => Ok(Response::new(DeleteResponse {
                    deleted: 0,
                    leader_hint: hint,
                })),
                Err(status) => Err(status),
            },
        }
    }

    async fn search(&self, request: Request<SearchRequest>) -> Result<Response<SearchResponse>, Status> {
        match self.leadership().await {
            Leadership::Redirect(hint) => {
                return Ok(Response::new(SearchResponse {
                    results: vec![],
                    shards_queried: 0,
                    shards_failed: 0,
                    leader_hint: hint,
                }));
            }
            Leadership::Unknown => {
                return Err(Status::unavailable("no leader currently known for this shard"));
            }
            Leadership::IAmLeader => {}
        }

        // `leadership()` is only this replica's cached belief — an isolated
        // ex-leader keeps believing it leads and would serve stale reads.
        // ensure_linearizable() is a ReadIndex-style check that succeeds
        // only if a majority still acknowledges this replica as leader.
        if let Err(e) = self.raft.ensure_linearizable().await {
            return Err(Status::unavailable(format!(
                "cannot confirm current leadership (lost quorum?): {e}"
            )));
        }

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
            leader_hint: String::new(),
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
