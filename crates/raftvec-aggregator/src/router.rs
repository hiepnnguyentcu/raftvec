use futures::future::join_all;
use raftvec_core::{merge_top_k, shard_id, ScoredId};
use raftvec_proto::raft_vec_client::RaftVecClient;
use raftvec_proto::{
    ClusterStatusRequest, CreateCollectionRequest, DeleteRequest, InsertRequest, ScoredRecord,
    SearchRequest, VectorRecord,
};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use thiserror::Error;
use tonic::transport::Channel;
use tonic::Status;

#[derive(Debug, Error)]
pub enum RouterError {
    #[error("failed to connect to shard node at {addr}: {source}")]
    Connect {
        addr: String,
        #[source]
        source: tonic::transport::Error,
    },
    #[error("shard rpc failed: {0}")]
    Rpc(#[from] Status),
}

/// Stateless routing/fan-out over a fixed set of shard nodes. Shard count
/// is simply the number of configured shard addresses -- there is no
/// MetaRaft yet (that's M3), so the aggregator's own static shard list IS
/// the topology (technical design §2.2, §3: shard count fixed at
/// collection creation, static hash routing).
///
/// `shard_clients[i]` must be shard `i`: `connect` builds the list in the
/// same order the caller supplies addresses in, and every routing decision
/// below (`shard_id(id, shard_count) as usize`) indexes into it directly.
pub struct ShardRouter {
    shard_clients: Vec<RaftVecClient<Channel>>,
    /// Per-shard call deadline for search fan-out (technical design §5.3):
    /// a slow/dead shard degrades the result instead of hanging the query.
    deadline: Duration,
}

const CONNECT_MAX_ATTEMPTS: u32 = 10;
const CONNECT_RETRY_DELAY: Duration = Duration::from_millis(500);

impl ShardRouter {
    /// Connects to every shard, retrying each with a fixed backoff before
    /// giving up. Needed for docker-compose startup: the aggregator
    /// container can win the race and start before a shard's listener is
    /// up, and a hard failure on the first attempt would make every
    /// `docker compose up` flaky rather than just slow.
    pub async fn connect(addrs: &[String], deadline: Duration) -> Result<Self, RouterError> {
        let mut shard_clients = Vec::with_capacity(addrs.len());
        for addr in addrs {
            shard_clients.push(connect_with_retry(addr).await?);
        }
        Ok(Self {
            shard_clients,
            deadline,
        })
    }

    pub fn shard_count(&self) -> u32 {
        self.shard_clients.len() as u32
    }

    /// Fans CreateCollection out to every shard; each shard independently
    /// creates its own local slice of the collection under the same name.
    pub async fn create_collection(&self, name: &str, dim: u32) -> Result<(), RouterError> {
        let shard_count = self.shard_count();
        let calls = self.shard_clients.iter().cloned().map(|mut client| {
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

    /// Groups records by `shard_id` and sends each group only to its shard
    /// -- the write path is the opposite of fan-out: exactly one shard per
    /// record, not all of them.
    pub async fn insert(
        &self,
        collection: &str,
        records: Vec<VectorRecord>,
    ) -> Result<u32, RouterError> {
        let groups = self.group_by_shard(records, |r| r.id);

        let calls = self
            .shard_clients
            .iter()
            .cloned()
            .zip(groups)
            .filter(|(_, group)| !group.is_empty())
            .map(|(mut client, group)| {
                let collection = collection.to_string();
                async move {
                    client
                        .insert(InsertRequest {
                            collection,
                            records: group,
                        })
                        .await
                }
            });

        let mut inserted = 0u32;
        for result in join_all(calls).await {
            inserted += result?.into_inner().inserted;
        }
        Ok(inserted)
    }

    pub async fn delete(&self, collection: &str, ids: Vec<u64>) -> Result<u32, RouterError> {
        let groups = self.group_by_shard(ids, |id| *id);

        let calls = self
            .shard_clients
            .iter()
            .cloned()
            .zip(groups)
            .filter(|(_, group)| !group.is_empty())
            .map(|(mut client, group)| {
                let collection = collection.to_string();
                async move { client.delete(DeleteRequest { collection, ids: group }).await }
            });

        let mut deleted = 0u32;
        for result in join_all(calls).await {
            deleted += result?.into_inner().deleted;
        }
        Ok(deleted)
    }

    /// Fans the query out to every shard in parallel and merges the local
    /// top-k lists into an exact global top-k (technical design §5.2: safe
    /// because shards partition the corpus disjointly, so the true global
    /// top-k is necessarily a subset of the union of local top-k sets). A
    /// shard that doesn't answer within the deadline is dropped from the
    /// merge and counted in `shards_failed` instead of failing the whole
    /// query (§5.3).
    pub async fn search(
        &self,
        collection: &str,
        query_vector: Vec<f32>,
        k: u32,
    ) -> (Vec<ScoredRecord>, u32, u32) {
        let deadline = self.deadline;
        let calls = self.shard_clients.iter().cloned().map(|mut client| {
            let collection = collection.to_string();
            let query_vector = query_vector.clone();
            async move {
                tokio::time::timeout(
                    deadline,
                    client.search(SearchRequest {
                        collection,
                        query_vector,
                        k,
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
                Ok(Ok(resp)) => shard_lists.push(resp.into_inner().results),
                _ => shards_failed += 1, // timed out, or the shard call itself errored
            }
        }

        let merged = merge_scored_records(shard_lists, k as usize);
        (merged, shards_queried, shards_failed)
    }

    pub async fn cluster_status(&self) -> (Vec<String>, u32) {
        let calls = self
            .shard_clients
            .iter()
            .cloned()
            .map(|mut client| async move { client.cluster_status(ClusterStatusRequest {}).await });

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
    /// as the bucket index -- the same hash function every shard node also
    /// runs on insert, so routing and storage agree on where a record lives.
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
