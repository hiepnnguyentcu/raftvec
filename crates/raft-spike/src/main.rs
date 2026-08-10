//! De-risking spike (product spec §10 risk register: "openraft API learning
//! curve consumes Week 3" -> "spike a minimal 3-node counter state machine
//! before the real integration"). Proves out the openraft 0.9 trait shapes
//! -- RaftLogStorage, RaftStateMachine, RaftNetworkFactory/RaftNetwork,
//! cluster init, client writes, leader failover -- with a trivial
//! in-memory key/value state machine, BEFORE wiring any of this into
//! raftvec-node's real VectorRecord state machine or real gRPC transport.
//!
//! Deliberately NOT using real network I/O: nodes call each other's Raft
//! handles directly through a shared in-process registry. This violates
//! this project's own "real network boundaries" design principle (technical
//! design §1) on purpose -- that principle applies to the real system, not
//! to a throwaway spike whose only job is to prove the state-machine
//! mechanics compile and behave. Standalone crate, not part of the main
//! workspace, not wired into any other crate.

#![allow(clippy::result_large_err)]

use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Config, Entry, EntryPayload, LogId, LogState, Raft, RaftLogId, RaftLogReader,
    RaftSnapshotBuilder, RaftTypeConfig, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};

type NodeId = u64;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Command,
        R = CommandResponse,
);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    Set { key: String, value: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResponse {
    value: Option<u64>,
}

// ---------------------------------------------------------------------
// Log storage: in-memory, adapted from openraft's own reference memstore
// example (examples/memstore in the openraft repo, v0.9.25).
// ---------------------------------------------------------------------

#[derive(Clone, Default)]
struct LogStore {
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
            .or_else(|| inner.last_purged_log_id);
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
// State machine: applies committed Commands to a plain in-memory map.
// This is the piece that becomes ShardStateMachine (VectorRecord map) in
// the real integration -- everything here proves the apply/snapshot
// lifecycle, not the domain logic.
// ---------------------------------------------------------------------

#[derive(Default, Clone)]
struct StateMachineData {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    data: BTreeMap<String, u64>,
}

#[derive(Default)]
struct StateMachineStore {
    state_machine: RwLock<StateMachineData>,
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let sm = self.state_machine.read().await;
        let bytes = serde_json::to_vec(&sm.data).map_err(|e| StorageIOError::read_state_machine(&e))?;
        let meta = SnapshotMeta {
            last_log_id: sm.last_applied_log,
            last_membership: sm.last_membership.clone(),
            snapshot_id: format!("{:?}", sm.last_applied_log),
        };
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>> {
        let sm = self.state_machine.read().await;
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<CommandResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut res = Vec::new();
        let mut sm = self.state_machine.write().await;
        for entry in entries {
            sm.last_applied_log = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Blank => res.push(CommandResponse { value: None }),
                EntryPayload::Normal(Command::Set { key, value }) => {
                    sm.data.insert(key, value);
                    res.push(CommandResponse { value: Some(value) });
                }
                EntryPayload::Membership(mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem);
                    res.push(CommandResponse { value: None });
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
        let data: BTreeMap<String, u64> =
            serde_json::from_slice(&bytes).map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        let mut sm = self.state_machine.write().await;
        sm.data = data;
        sm.last_applied_log = meta.last_log_id;
        sm.last_membership = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        // Spike only: no persisted snapshot cache needed to prove the
        // apply/failover mechanism.
        Ok(None)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}

// ---------------------------------------------------------------------
// Network: in-process loopback. Each "connection" looks the target node's
// live Raft handle up in a shared registry and calls it directly -- no
// serialization, no sockets. The real integration replaces this with a
// tonic client hitting AppendEntries/Vote/InstallSnapshot over gRPC.
// ---------------------------------------------------------------------

type Registry = Arc<Mutex<HashMap<NodeId, Raft<TypeConfig>>>>;

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
    async fn target_raft(&self) -> Result<Raft<TypeConfig>, openraft::error::Unreachable> {
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
    ) -> Result<InstallSnapshotResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>> {
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

// ---------------------------------------------------------------------
// Spike scenario: bring up 3 nodes, initialize the cluster, write through
// the leader, confirm replication to followers, kill the leader, confirm
// the remaining two elect a new leader and keep accepting writes.
// ---------------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = Arc::new(
        Config {
            heartbeat_interval: 250,
            election_timeout_min: 800,
            election_timeout_max: 1500,
            ..Default::default()
        }
        .validate()?,
    );

    let registry: Registry = Arc::new(Mutex::new(HashMap::new()));

    let mut rafts: HashMap<NodeId, Raft<TypeConfig>> = HashMap::new();
    let mut state_machines: HashMap<NodeId, Arc<StateMachineStore>> = HashMap::new();

    for node_id in 1..=3u64 {
        let log_store = LogStore::default();
        let sm = Arc::new(StateMachineStore::default());
        let network = LoopbackNetwork {
            registry: registry.clone(),
        };
        let raft = Raft::new(node_id, config.clone(), network, log_store, sm.clone()).await?;
        rafts.insert(node_id, raft);
        state_machines.insert(node_id, sm);
    }

    {
        let mut reg = registry.lock().await;
        for (id, raft) in &rafts {
            reg.insert(*id, raft.clone());
        }
    }

    let mut members = BTreeMap::new();
    for id in 1..=3u64 {
        members.insert(id, BasicNode { addr: format!("node-{id}") });
    }
    rafts.get(&1).unwrap().initialize(members).await?;
    println!("cluster initialized with 3 nodes");

    let leader_id = wait_for_leader_change(&rafts, 1, None, Duration::from_secs(5)).await;
    println!("leader elected: node-{leader_id}");

    rafts
        .get(&leader_id)
        .unwrap()
        .client_write(Command::Set {
            key: "x".to_string(),
            value: 42,
        })
        .await?;
    println!("wrote x=42 through leader node-{leader_id}");

    tokio::time::sleep(Duration::from_millis(500)).await;

    for id in 1..=3u64 {
        let sm = state_machines.get(&id).unwrap();
        let value = sm.state_machine.read().await.data.get("x").copied();
        println!("node-{id} sees x = {value:?}");
        assert_eq!(value, Some(42), "replication did not reach node-{id}");
    }
    println!("replication confirmed on all 3 nodes\n");

    println!("killing leader node-{leader_id}...");
    rafts.remove(&leader_id).unwrap().shutdown().await?;
    registry.lock().await.remove(&leader_id);

    let remaining: Vec<u64> = rafts.keys().copied().collect();
    let new_leader_id =
        wait_for_leader_change(&rafts, remaining[0], Some(leader_id), Duration::from_secs(5)).await;
    println!("new leader elected: node-{new_leader_id}");
    assert_ne!(new_leader_id, leader_id, "a new leader must be elected");

    rafts
        .get(&new_leader_id)
        .unwrap()
        .client_write(Command::Set {
            key: "y".to_string(),
            value: 7,
        })
        .await?;
    println!("wrote y=7 through new leader node-{new_leader_id}");

    tokio::time::sleep(Duration::from_millis(500)).await;

    for &id in &remaining {
        let sm = state_machines.get(&id).unwrap();
        let value = sm.state_machine.read().await.data.get("y").copied();
        println!("node-{id} sees y = {value:?}");
        assert_eq!(value, Some(7), "post-failover write did not reach node-{id}");
    }

    println!("\nspike passed: cluster survived a leader kill, elected a new leader, and kept accepting/replicating writes.");
    Ok(())
}

/// Polls `probe`'s metrics until it reports a leader different from
/// `old_leader`. Checking for "any Some value" is not enough: openraft's
/// `current_leader` metric holds the *last known* leader and is not
/// necessarily cleared to None while a new election is in flight, so right
/// after killing the leader this can still read back the dead node's id.
async fn wait_for_leader_change(
    rafts: &HashMap<NodeId, Raft<TypeConfig>>,
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
            panic!(
                "timed out waiting for a leader change away from {old_leader:?}; probe node-{probe} metrics: {metrics:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
