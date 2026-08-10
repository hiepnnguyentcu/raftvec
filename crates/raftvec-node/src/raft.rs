//! openraft integration for a single shard's Raft group (technical design
//! §6). Every replica of a shard runs one of these; `ShardStateMachine`
//! wraps the same `Store` the M1/M2 gRPC service reads/writes, so a
//! committed log entry and a direct local read see identical data.
//!
//! Log storage and network trait shapes are adapted from the openraft
//! project's own reference example (examples/memstore,
//! examples/raft-kv-memstore at v0.9.25) -- verified first in isolation in
//! `crates/raft-spike` before being pointed at real `VectorRecord` data
//! here. `RaftNetwork` here is still a loopback (test-only); real gRPC
//! transport between replicas is the next step.

use crate::store::Store;
use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, LogState, RaftLogId, RaftLogReader, RaftSnapshotBuilder,
    RaftTypeConfig, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use raftvec_core::VectorRecord;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

pub type NodeId = u64;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = ShardCommand,
        R = ShardCommandResponse,
);

pub type Raft = openraft::Raft<TypeConfig>;

/// The unit of replication for a shard's Raft log (technical design §6).
/// Carries its own `collection` name because a single shard-replica
/// process hosts the same multi-collection `Store` the M1/M2 gRPC path
/// does; only vector mutations are Raft-replicated -- `CreateCollection`
/// stays an out-of-band call applied identically to every replica (M2
/// pattern), since it is rare and not the property FR5 is about.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ShardCommand {
    Upsert { collection: String, record: VectorRecord },
    Delete { collection: String, id: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardCommandResponse {
    pub applied: bool,
}

// ---------------------------------------------------------------------
// Log storage: in-memory (documented non-goal: no disk persistence,
// product spec §5). Shape verified in crates/raft-spike.
// ---------------------------------------------------------------------

#[derive(Clone, Default)]
pub struct LogStore {
    inner: Arc<Mutex<LogStoreInner>>,
}

#[derive(Default)]
struct LogStoreInner {
    last_purged_log_id: Option<LogId<NodeId>>,
    log: BTreeMap<u64, Entry<TypeConfig>>,
    committed: Option<LogId<NodeId>>,
    vote: Option<Vote<NodeId>>,
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        Ok(inner.log.range(range).map(|(_, v)| v.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        let last = inner
            .log
            .iter()
            .next_back()
            .map(|(_, e)| *e.get_log_id())
            .or(inner.last_purged_log_id);
        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id: last,
        })
    }

    async fn save_committed(&mut self, committed: Option<LogId<NodeId>>) -> Result<(), StorageError<NodeId>> {
        self.inner.lock().await.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().await.committed)
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.inner.lock().await.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().await.vote)
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut inner = self.inner.lock().await;
        for entry in entries {
            inner.log.insert(entry.get_log_id().index, entry);
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        let keys: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.last_purged_log_id = Some(log_id);
        let keys: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

// ---------------------------------------------------------------------
// State machine: applies committed ShardCommands to the real Store.
// ---------------------------------------------------------------------

pub struct ShardStateMachine {
    pub store: Arc<Store>,
    last_applied_log: RwLock<Option<LogId<NodeId>>>,
    last_membership: RwLock<StoredMembership<NodeId, BasicNode>>,
}

impl ShardStateMachine {
    pub fn new(store: Arc<Store>) -> Self {
        Self {
            store,
            last_applied_log: RwLock::new(None),
            last_membership: RwLock::new(StoredMembership::default()),
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<ShardStateMachine> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let bytes =
            serde_json::to_vec(&self.store.snapshot()).map_err(|e| StorageIOError::read_state_machine(&e))?;
        let last_applied_log = *self.last_applied_log.read().await;
        let last_membership = self.last_membership.read().await.clone();
        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id: format!("{last_applied_log:?}"),
        };
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<ShardStateMachine> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>> {
        Ok((
            *self.last_applied_log.read().await,
            self.last_membership.read().await.clone(),
        ))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ShardCommandResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut res = Vec::new();
        for entry in entries {
            *self.last_applied_log.write().await = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(ShardCommandResponse { applied: false }),
                EntryPayload::Normal(cmd) => {
                    // A rejection here (unknown collection, dim mismatch) is
                    // an application-level outcome, not a storage failure --
                    // every replica applies the same committed entry and
                    // must reach the same (non-fatal) outcome deterministically,
                    // rather than erroring the whole apply() call.
                    let applied = match cmd {
                        ShardCommand::Upsert { collection, record } => {
                            self.store.insert(&collection, vec![record]).is_ok()
                        }
                        ShardCommand::Delete { collection, id } => {
                            self.store.delete(&collection, &[id]).is_ok()
                        }
                    };
                    res.push(ShardCommandResponse { applied });
                }
                EntryPayload::Membership(mem) => {
                    *self.last_membership.write().await = StoredMembership::new(Some(entry.log_id), mem);
                    res.push(ShardCommandResponse { applied: false });
                }
            }
        }
        Ok(res)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes = snapshot.into_inner();
        let store_snapshot = serde_json::from_slice(&bytes)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        self.store.restore(store_snapshot);
        *self.last_applied_log.write().await = meta.last_log_id;
        *self.last_membership.write().await = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        // No persisted snapshot cache: `build_snapshot` re-serializes the
        // live Store on demand, which is cheap enough at this project's
        // scale and avoids tracking a second copy of the data in sync.
        Ok(None)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError};
    use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
    use openraft::raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
        VoteRequest, VoteResponse,
    };
    use openraft::Config;
    use std::collections::{BTreeMap, HashMap};
    use std::time::Duration;

    type Registry = Arc<Mutex<HashMap<NodeId, Raft>>>;

    #[derive(Clone, Default)]
    struct LoopbackNetwork {
        registry: Registry,
    }

    impl RaftNetworkFactory<TypeConfig> for LoopbackNetwork {
        type Network = LoopbackConnection;

        async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
            LoopbackConnection {
                registry: self.registry.clone(),
                target,
            }
        }
    }

    struct LoopbackConnection {
        registry: Registry,
        target: NodeId,
    }

    impl LoopbackConnection {
        async fn target_raft(&self) -> Result<Raft, openraft::error::Unreachable> {
            self.registry
                .lock()
                .await
                .get(&self.target)
                .cloned()
                .ok_or_else(|| openraft::error::Unreachable::new(&std::io::Error::other("target not registered")))
        }
    }

    impl RaftNetwork<TypeConfig> for LoopbackConnection {
        async fn append_entries(
            &mut self,
            req: AppendEntriesRequest<TypeConfig>,
            _option: RPCOption,
        ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
            let raft = self.target_raft().await?;
            raft.append_entries(req)
                .await
                .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
        }

        async fn install_snapshot(
            &mut self,
            req: InstallSnapshotRequest<TypeConfig>,
            _option: RPCOption,
        ) -> Result<InstallSnapshotResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>>
        {
            let raft = self.target_raft().await?;
            raft.install_snapshot(req)
                .await
                .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
        }

        async fn vote(
            &mut self,
            req: VoteRequest<NodeId>,
            _option: RPCOption,
        ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
            let raft = self.target_raft().await?;
            raft.vote(req)
                .await
                .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
        }
    }

    async fn wait_for_leader_change(
        rafts: &HashMap<NodeId, Raft>,
        probe: NodeId,
        old_leader: Option<NodeId>,
        timeout: Duration,
    ) -> NodeId {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let metrics = rafts.get(&probe).unwrap().metrics().borrow().clone();
            if let Some(leader) = metrics.current_leader {
                if Some(leader) != old_leader {
                    return leader;
                }
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("timed out waiting for a leader change away from {old_leader:?} on node-{probe}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn rec(id: u64, embedding: Vec<f32>) -> VectorRecord {
        VectorRecord::new(id, embedding, HashMap::new())
    }

    /// 3-node shard replica group, real VectorRecord data through the real
    /// Store on every node: proves the ShardStateMachine (not the spike's
    /// toy KV) replicates correctly, survives a leader kill, and keeps
    /// serving/replicating writes with the new leader.
    #[tokio::test]
    async fn shard_replica_group_replicates_and_survives_leader_failure() {
        let config = Arc::new(
            Config {
                heartbeat_interval: 100,
                election_timeout_min: 300,
                election_timeout_max: 600,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );

        let registry: Registry = Arc::new(Mutex::new(HashMap::new()));
        let mut rafts: HashMap<NodeId, Raft> = HashMap::new();
        let mut stores: HashMap<NodeId, Arc<Store>> = HashMap::new();

        for node_id in 1..=3u64 {
            let store = Arc::new(Store::new());
            store.create_collection("docs", 2, 1).unwrap();

            let log_store = LogStore::default();
            let sm = Arc::new(ShardStateMachine::new(store.clone()));
            let network = LoopbackNetwork {
                registry: registry.clone(),
            };
            let raft = openraft::Raft::new(node_id, config.clone(), network, log_store, sm)
                .await
                .unwrap();

            rafts.insert(node_id, raft);
            stores.insert(node_id, store);
        }

        {
            let mut reg = registry.lock().await;
            for (id, raft) in &rafts {
                reg.insert(*id, raft.clone());
            }
        }

        let mut members = BTreeMap::new();
        for id in 1..=3u64 {
            members.insert(id, BasicNode { addr: format!("shard-replica-{id}") });
        }
        rafts.get(&1).unwrap().initialize(members).await.unwrap();

        let leader_id = wait_for_leader_change(&rafts, 1, None, Duration::from_secs(5)).await;

        rafts
            .get(&leader_id)
            .unwrap()
            .client_write(ShardCommand::Upsert {
                collection: "docs".to_string(),
                record: rec(1, vec![1.0, 0.0]),
            })
            .await
            .unwrap();

        for (id, store) in &stores {
            let results = store.search("docs", &[1.0, 0.0], 1).unwrap();
            assert_eq!(results.len(), 1, "node-{id} missing replicated record");
            assert_eq!(results[0].id, 1);
        }

        rafts.remove(&leader_id).unwrap().shutdown().await.unwrap();
        registry.lock().await.remove(&leader_id);
        stores.remove(&leader_id);

        let remaining: Vec<u64> = rafts.keys().copied().collect();
        let new_leader_id =
            wait_for_leader_change(&rafts, remaining[0], Some(leader_id), Duration::from_secs(5)).await;
        assert_ne!(new_leader_id, leader_id);

        rafts
            .get(&new_leader_id)
            .unwrap()
            .client_write(ShardCommand::Upsert {
                collection: "docs".to_string(),
                record: rec(2, vec![0.0, 1.0]),
            })
            .await
            .unwrap();

        for (id, store) in &stores {
            let results = store.search("docs", &[0.0, 1.0], 1).unwrap();
            assert_eq!(results.len(), 1, "node-{id} missing post-failover record");
            assert_eq!(results[0].id, 2, "node-{id} did not replicate post-failover write");
        }
    }
}
