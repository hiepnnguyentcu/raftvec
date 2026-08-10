//! Real gRPC transport for a shard's Raft group, replacing the in-process
//! loopback used in raft.rs's own tests. Two halves of one wire contract:
//! `GrpcNetwork`/`GrpcConnection` (client side, implements openraft's
//! `RaftNetworkFactory`/`RaftNetwork`) and `ShardRaftService` (server side,
//! implements the generated `ShardRaft` gRPC trait and hands incoming
//! requests to this node's local `Raft` handle).
//!
//! openraft's request/response types are generic Rust structs whose exact
//! shape can shift across versions; rather than hand-modeling every field
//! as its own proto message, each RPC carries the request/response
//! (including the `Result<_, RaftError<..>>` on responses) JSON-encoded as
//! opaque bytes (technical design §4.2's transport RPCs, `raftvec.proto`'s
//! `RaftMessage`).

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
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

// ---------------------------------------------------------------------
// Client side: RaftNetworkFactory / RaftNetwork over tonic.
// ---------------------------------------------------------------------

#[derive(Clone, Default)]
pub struct GrpcNetwork {
    /// Connections are cached per target node id and reused across RPCs
    /// (a fresh TCP + HTTP/2 handshake per heartbeat would dominate
    /// latency and defeat the point of a 50-150ms election timeout).
    clients: Arc<Mutex<HashMap<NodeId, ShardRaftClient<Channel>>>>,
}

impl GrpcNetwork {
    async fn get_or_connect(
        &self,
        target: NodeId,
        addr: &str,
    ) -> Result<ShardRaftClient<Channel>, tonic::transport::Error> {
        let mut clients = self.clients.lock().await;
        if let Some(client) = clients.get(&target) {
            return Ok(client.clone());
        }
        let client = ShardRaftClient::connect(addr.to_string()).await?;
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
    /// Sends `req`, expecting the peer to respond with a JSON-encoded
    /// `Result<Resp, RaftError<NodeId, E>>` -- i.e. the exact `Result` its
    /// own local `raft.append_entries()`/`vote()`/`install_snapshot()edge`
    /// call produced, relayed byte-for-byte rather than reconstructed.
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
        let payload = serde_json::to_vec(&req).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

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
            serde_json::from_slice(&bytes).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

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
    let payload = serde_json::to_vec(result).map_err(|e| Status::internal(e.to_string()))?;
    Ok(Response::new(RaftMessage { payload }))
}

#[allow(clippy::result_large_err)]
fn decode<T: DeserializeOwned>(request: Request<RaftMessage>) -> Result<T, Status> {
    serde_json::from_slice(&request.into_inner().payload)
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
