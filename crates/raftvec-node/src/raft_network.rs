//! gRPC transport for a shard's Raft group: `GrpcNetwork`/`GrpcConnection`
//! implement openraft's client-side network traits; `ShardRaftService`
//! serves the receiving end and hands requests to the local `Raft` handle.
//!
//! openraft's request/response types are version-sensitive generics, so
//! rather than hand-modeling them as proto messages, each RPC carries the
//! bincode-serialized request/response as opaque bytes (`RaftMessage`).

use crate::raft::{NodeId, Raft, TypeConfig};
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use raftvec_proto::shard_raft_client::ShardRaftClient;
use raftvec_proto::shard_raft_server::ShardRaft;
use raftvec_proto::RaftMessage;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

// ---------------------------------------------------------------------
// Client side: RaftNetworkFactory / RaftNetwork over tonic.
// ---------------------------------------------------------------------

/// Connect timeout per peer. A healthy same-network peer connects in
/// single-digit milliseconds; this bound only matters for dead peers,
/// where every election round otherwise burns the full budget waiting on
/// a member that will never answer.
const CONNECT_TIMEOUT: Duration = Duration::from_millis(100);

/// tonic's default 4MiB message cap is too small for AppendEntries
/// carrying batched vector payloads (a catch-up batch can reach tens of
/// MB); hitting it makes replication to a healthy follower fail in a way
/// that is indistinguishable from a stuck election.
pub const MAX_MESSAGE_SIZE: usize = 128 * 1024 * 1024;

#[derive(Clone, Default)]
pub struct GrpcNetwork {
    /// Connections cached per target node and reused across RPCs; a fresh
    /// TCP + HTTP/2 handshake per heartbeat would dominate latency.
    clients: Arc<Mutex<HashMap<NodeId, ShardRaftClient<Channel>>>>,
}

impl GrpcNetwork {
    /// The lock covers only the cache lookup/insert, never the
    /// `.connect().await` itself — otherwise one slow connect to a dead
    /// peer serializes connects to healthy peers behind it.
    async fn get_or_connect(
        &self,
        target: NodeId,
        addr: &str,
    ) -> Result<ShardRaftClient<Channel>, tonic::transport::Error> {
        {
            let clients = self.clients.lock().await;
            if let Some(client) = clients.get(&target) {
                return Ok(client.clone());
            }
        }

        let channel = Endpoint::from_shared(addr.to_string())?
            .connect_timeout(CONNECT_TIMEOUT)
            .connect()
            .await?;
        let client = ShardRaftClient::new(channel)
            .max_decoding_message_size(MAX_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_MESSAGE_SIZE);

        let mut clients = self.clients.lock().await;
        clients.insert(target, client.clone());
        Ok(client)
    }
}

impl RaftNetworkFactory<TypeConfig> for GrpcNetwork {
    type Network = GrpcConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        GrpcConnection {
            network: self.clone(),
            target,
            addr: node.addr.clone(),
        }
    }
}

pub struct GrpcConnection {
    network: GrpcNetwork,
    target: NodeId,
    addr: String,
}

enum RaftRpc {
    AppendEntries,
    Vote,
    InstallSnapshot,
}

impl GrpcConnection {
    /// Sends `req`; the peer replies with the serialized
    /// `Result<Resp, RaftError<..>>` its local Raft call produced, relayed
    /// byte-for-byte rather than reconstructed.
    async fn call<Req, Resp, E>(
        &mut self,
        rpc: RaftRpc,
        req: Req,
    ) -> Result<Resp, RPCError<NodeId, BasicNode, RaftError<NodeId, E>>>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
        E: std::error::Error + DeserializeOwned,
    {
        let payload = bincode::serialize(&req).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let mut client = self
            .network
            .get_or_connect(self.target, &self.addr)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let message = RaftMessage { payload };
        let response = match rpc {
            RaftRpc::AppendEntries => client.append_entries(message).await,
            RaftRpc::Vote => client.vote(message).await,
            RaftRpc::InstallSnapshot => client.install_snapshot(message).await,
        };

        let response = response.map_err(|status: Status| {
            if status.code() == tonic::Code::Unavailable {
                RPCError::Unreachable(Unreachable::new(&status))
            } else {
                RPCError::Network(NetworkError::new(&status))
            }
        })?;

        let bytes = response.into_inner().payload;
        let result: Result<Resp, RaftError<NodeId, E>> =
            bincode::deserialize(&bytes).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        result.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

impl RaftNetwork<TypeConfig> for GrpcConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.call(RaftRpc::AppendEntries, req).await
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>>
    {
        self.call(RaftRpc::InstallSnapshot, req).await
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.call(RaftRpc::Vote, req).await
    }
}

// ---------------------------------------------------------------------
// Server side: receives ShardRaft gRPC calls, hands them to the local
// Raft handle, relays back whatever Result it produced.
// ---------------------------------------------------------------------

pub struct ShardRaftService {
    raft: Raft,
}

impl ShardRaftService {
    pub fn new(raft: Raft) -> Self {
        Self { raft }
    }
}

#[allow(clippy::result_large_err)]
fn encode<T: Serialize>(result: &T) -> Result<Response<RaftMessage>, Status> {
    let payload = bincode::serialize(result).map_err(|e| Status::internal(e.to_string()))?;
    Ok(Response::new(RaftMessage { payload }))
}

#[allow(clippy::result_large_err)]
fn decode<T: DeserializeOwned>(request: Request<RaftMessage>) -> Result<T, Status> {
    bincode::deserialize(&request.into_inner().payload)
        .map_err(|e| Status::invalid_argument(format!("bad Raft message payload: {e}")))
}

#[tonic::async_trait]
impl ShardRaft for ShardRaftService {
    async fn append_entries(&self, request: Request<RaftMessage>) -> Result<Response<RaftMessage>, Status> {
        let req: AppendEntriesRequest<TypeConfig> = decode(request)?;
        let result = self.raft.append_entries(req).await;
        encode(&result)
    }

    async fn vote(&self, request: Request<RaftMessage>) -> Result<Response<RaftMessage>, Status> {
        let req: VoteRequest<NodeId> = decode(request)?;
        let result = self.raft.vote(req).await;
        encode(&result)
    }

    async fn install_snapshot(&self, request: Request<RaftMessage>) -> Result<Response<RaftMessage>, Status> {
        let req: InstallSnapshotRequest<TypeConfig> = decode(request)?;
        let result = self.raft.install_snapshot(req).await;
        encode(&result)
    }
}
