use futures::future::join_all;
use raftvec_core::{merge_top_k, shard_id, ScoredId};
use raftvec_proto::raft_vec_client::RaftVecClient;
use raftvec_proto::{
    ClusterStatusRequest, CreateCollectionRequest, DeleteRequest, InsertRequest, ScoredRecord,
    SearchRequest, VectorRecord,
};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use thiserror::Error;
use tonic::transport::Channel;
use tonic::{Response, Status};

#[derive(Debug, Error)]
pub enum RouterError {
    #[error("failed to connect to shard replica at {addr}: {source}")]
    Connect {
        addr: String,
        #[source]
        source: tonic::transport::Error,
    },
    #[error("shard rpc failed: {0}")]
    Rpc(#[from] Status),
    #[error("no replica of this shard responded")]
    ShardUnreachable,
}

/// One shard's Raft group: all its replicas, plus a cached guess at which
/// one is currently the leader. There is no MetaRaft (out of scope, see
/// technical design's non-goals) -- the aggregator's own static config IS
/// the shard->replica-address topology; only *leadership within* a shard's
/// fixed replica set is discovered dynamically, via `leader_hint`.
struct ShardReplicaSet {
    clients: Vec<RaftVecClient<Channel>>,
    addrs: Vec<String>,
    /// Index into `clients`/`addrs` last known (or guessed) to be leader.
    /// Relaxed ordering is enough: this is a best-effort cache to avoid
    /// wasted round trips, not a correctness-load-bearing value -- a stale
    /// guess just costs one extra retry, never a wrong answer.
    leader_hint: AtomicUsize,
}

/// Stateless routing/fan-out over a fixed set of shards, each a fixed set
/// of Raft replicas (technical design §2.2, §5.1).
pub struct ShardRouter {
    shards: Vec<ShardReplicaSet>,
    /// Per-shard call deadline for search fan-out (technical design §5.3):
    /// a slow/dead shard degrades the result instead of hanging the query.
    deadline: Duration,
}

const CONNECT_MAX_ATTEMPTS: u32 = 10;
const CONNECT_RETRY_DELAY: Duration = Duration::from_millis(500);
/// One attempt per replica plus one extra to follow a hint received on the
/// last replica tried.
const MAX_LEADER_ATTEMPTS: usize = 4;

/// Implemented by every response type that carries a `leader_hint` field
/// (InsertResponse/DeleteResponse/SearchResponse), so `call_with_retry` can
/// stay generic over which RPC it's retrying.
trait CarriesLeaderHint {
    fn leader_hint(&self) -> &str;
}

impl CarriesLeaderHint for raftvec_proto::InsertResponse {
    fn leader_hint(&self) -> &str {
        &self.leader_hint
    }
}
impl CarriesLeaderHint for raftvec_proto::DeleteResponse {
    fn leader_hint(&self) -> &str {
        &self.leader_hint
    }
}
impl CarriesLeaderHint for raftvec_proto::SearchResponse {
    fn leader_hint(&self) -> &str {
        &self.leader_hint
    }
}

impl ShardRouter {
    /// Connects to every replica of every shard, retrying each with a
    /// fixed backoff before giving up (docker-compose startup: a replica's
    /// listener may not be up yet when the aggregator container starts).
    pub async fn connect(shard_replica_addrs: &[Vec<String>], deadline: Duration) -> Result<Self, RouterError> {
        let mut shards = Vec::with_capacity(shard_replica_addrs.len());
        for addrs in shard_replica_addrs {
            let mut clients = Vec::with_capacity(addrs.len());
            for addr in addrs {
                clients.push(connect_with_retry(addr).await?);
            }
            shards.push(ShardReplicaSet {
                clients,
                addrs: addrs.clone(),
                leader_hint: AtomicUsize::new(0),
            });
        }
        Ok(Self { shards, deadline })
    }

    pub fn shard_count(&self) -> u32 {
        self.shards.len() as u32
    }

    /// Fans CreateCollection out to every replica of every shard; each
    /// replica independently creates its own local slice of the collection
    /// under the same name (not Raft-replicated -- see raftvec-node's
    /// ShardCommand doc comment for why).
    pub async fn create_collection(&self, name: &str, dim: u32) -> Result<(), RouterError> {
        let shard_count = self.shard_count();
        let calls = self.shards.iter().flat_map(|shard| shard.clients.iter().cloned()).map(|mut client| {
            let req = CreateCollectionRequest {
                name: name.to_string(),
                dim,
                shard_count,
            };
            async move { client.create_collection(req).await }
        });

        for result in join_all(calls).await {
            result?;
        }
        Ok(())
    }

    /// Groups records by `shard_id` and sends each group only to its
    /// shard's current leader (retrying/following `leader_hint` as needed)
    /// -- the write path is the opposite of fan-out: exactly one shard per
    /// record, not all of them.
    pub async fn insert(&self, collection: &str, records: Vec<VectorRecord>) -> Result<u32, RouterError> {
        let groups = self.group_by_shard(records, |r| r.id);

        let calls = self
            .shards
            .iter()
            .zip(groups)
            .filter(|(_, group)| !group.is_empty())
            .map(|(shard, group)| {
                let collection = collection.to_string();
                async move {
                    call_with_retry(shard, move |mut client| {
                        let req = InsertRequest {
                            collection: collection.clone(),
                            records: group.clone(),
                        };
                        async move { client.insert(req).await }
                    })
                    .await
                }
            });

        let mut inserted = 0u32;
        for result in join_all(calls).await {
            inserted += result?.inserted;
        }
        Ok(inserted)
    }

    pub async fn delete(&self, collection: &str, ids: Vec<u64>) -> Result<u32, RouterError> {
        let groups = self.group_by_shard(ids, |id| *id);

        let calls = self
            .shards
            .iter()
            .zip(groups)
            .filter(|(_, group)| !group.is_empty())
            .map(|(shard, group)| {
                let collection = collection.to_string();
                async move {
                    call_with_retry(shard, move |mut client| {
                        let req = DeleteRequest {
                            collection: collection.clone(),
                            ids: group.clone(),
                        };
                        async move { client.delete(req).await }
                    })
                    .await
                }
            });

        let mut deleted = 0u32;
        for result in join_all(calls).await {
            deleted += result?.deleted;
        }
        Ok(deleted)
    }

    /// Fans the query out to every shard's current leader in parallel and
    /// merges the local top-k lists into an exact global top-k (technical
    /// design §5.2). A shard whose leader can't be reached/found within
    /// the deadline is dropped from the merge and counted in
    /// `shards_failed` instead of failing the whole query (§5.3).
    pub async fn search(&self, collection: &str, query_vector: Vec<f32>, k: u32) -> (Vec<ScoredRecord>, u32, u32) {
        let deadline = self.deadline;
        let calls = self.shards.iter().map(|shard| {
            let collection = collection.to_string();
            let query_vector = query_vector.clone();
            async move {
                tokio::time::timeout(
                    deadline,
                    call_with_retry(shard, move |mut client| {
                        let req = SearchRequest {
                            collection: collection.clone(),
                            query_vector: query_vector.clone(),
                            k,
                        };
                        async move { client.search(req).await }
                    }),
                )
                .await
            }
        });

        let responses = join_all(calls).await;
        let shards_queried = responses.len() as u32;
        let mut shards_failed = 0u32;
        let mut shard_lists: Vec<Vec<ScoredRecord>> = Vec::new();

        for r in responses {
            match r {
                Ok(Ok(resp)) => shard_lists.push(resp.results),
                _ => shards_failed += 1, // timed out finding a leader, or every replica errored
            }
        }

        let merged = merge_scored_records(shard_lists, k as usize);
        (merged, shards_queried, shards_failed)
    }

    pub async fn cluster_status(&self) -> (Vec<String>, u32) {
        // Any replica's local count is representative enough for this
        // observability endpoint (technical design §7 FR8) -- no need to
        // route specifically to leaders here. But it must still try more
        // than one replica: always querying index 0 silently under-counts
        // the moment that one replica is down, which is exactly when this
        // endpoint is most useful.
        let calls = self.shards.iter().map(|shard| async move {
            for mut client in shard.clients.iter().cloned() {
                if let Ok(resp) = client.cluster_status(ClusterStatusRequest {}).await {
                    return Some(resp);
                }
            }
            None
        });

        let mut collections: HashSet<String> = HashSet::new();
        let mut total = 0u32;
        for resp in join_all(calls).await.into_iter().flatten() {
            let resp = resp.into_inner();
            collections.extend(resp.collections);
            total += resp.vector_count;
        }
        (collections.into_iter().collect(), total)
    }

    /// Buckets `items` into one `Vec` per shard, using `shard_id(key(item), shard_count)`
    /// as the bucket index -- the same hash function every shard replica
    /// also runs on insert, so routing and storage agree on where a record lives.
    fn group_by_shard<T>(&self, items: Vec<T>, key: impl Fn(&T) -> u64) -> Vec<Vec<T>> {
        let shard_count = self.shard_count();
        let mut groups: Vec<Vec<T>> = (0..shard_count).map(|_| Vec::new()).collect();
        for item in items {
            let shard = shard_id(key(&item), shard_count) as usize;
            groups[shard].push(item);
        }
        groups
    }
}

/// Calls `make_request` against `shard`'s cached leader guess; if the
/// replica says it isn't leader (`leader_hint` non-empty in the response)
/// or is unreachable, follows the hint (or just advances to the next
/// replica) and retries, up to `MAX_LEADER_ATTEMPTS` times (technical
/// design §5.1: bounded retry rather than retrying forever).
async fn call_with_retry<F, Fut, R>(shard: &ShardReplicaSet, mut make_request: F) -> Result<R, RouterError>
where
    F: FnMut(RaftVecClient<Channel>) -> Fut,
    Fut: Future<Output = Result<Response<R>, Status>>,
    R: CarriesLeaderHint,
{
    let mut idx = shard.leader_hint.load(Ordering::Relaxed) % shard.clients.len();
    let mut last_err: Option<RouterError> = None;

    for _ in 0..MAX_LEADER_ATTEMPTS {
        let client = shard.clients[idx].clone();
        match make_request(client).await {
            Ok(response) => {
                let response = response.into_inner();
                if response.leader_hint().is_empty() {
                    shard.leader_hint.store(idx, Ordering::Relaxed);
                    return Ok(response);
                }
                idx = shard
                    .addrs
                    .iter()
                    .position(|a| a == response.leader_hint())
                    .unwrap_or((idx + 1) % shard.clients.len());
            }
            Err(status) => {
                last_err = Some(RouterError::Rpc(status));
                idx = (idx + 1) % shard.clients.len();
            }
        }
    }

    Err(last_err.unwrap_or(RouterError::ShardUnreachable))
}

async fn connect_with_retry(addr: &str) -> Result<RaftVecClient<Channel>, RouterError> {
    let mut last_err = None;
    for attempt in 0..CONNECT_MAX_ATTEMPTS {
        match RaftVecClient::connect(addr.to_string()).await {
            Ok(client) => return Ok(client),
            Err(e) => {
                last_err = Some(e);
                if attempt + 1 < CONNECT_MAX_ATTEMPTS {
                    tokio::time::sleep(CONNECT_RETRY_DELAY).await;
                }
            }
        }
    }
    Err(RouterError::Connect {
        addr: addr.to_string(),
        source: last_err.expect("loop runs at least once"),
    })
}

/// Merges several shards' local top-k lists (each already ranked) into one
/// exact global top-k, reusing raftvec_core's ordering/tie-break rule so
/// the merge agrees with the single-node oracle's ranking.
fn merge_scored_records(shard_lists: Vec<Vec<ScoredRecord>>, k: usize) -> Vec<ScoredRecord> {
    let mut metadata: HashMap<u64, HashMap<String, String>> = HashMap::new();
    let scored_lists: Vec<Vec<ScoredId>> = shard_lists
        .into_iter()
        .map(|list| {
            list.into_iter()
                .map(|r| {
                    metadata.insert(r.id, r.metadata);
                    ScoredId {
                        id: r.id,
                        score: r.score,
                    }
                })
                .collect()
        })
        .collect();

    merge_top_k(scored_lists, k)
        .into_iter()
        .map(|s| ScoredRecord {
            id: s.id,
            score: s.score,
            metadata: metadata.remove(&s.id).unwrap_or_default(),
        })
        .collect()
}
