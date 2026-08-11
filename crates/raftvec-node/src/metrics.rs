//! Background tasks translating this replica's live state into Prometheus
//! metrics. None of this is on the request path — it is periodic or
//! event-driven observation of state other modules own.

use crate::raft::{NodeId, Raft};
use crate::store::Store;
use std::sync::Arc;
use std::time::Duration;

const VECTOR_COUNT_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// raftvec_raft_leader_elections_total: increments each time this replica
/// observes its shard's leader change. Watches openraft's metrics channel
/// rather than polling, so it fires exactly on change.
pub fn spawn_leader_election_watcher(raft: Raft) {
    tokio::spawn(async move {
        let mut rx = raft.metrics();
        let mut last_leader = rx.borrow().current_leader;
        while rx.changed().await.is_ok() {
            let current = rx.borrow().current_leader;
            if current != last_leader && current.is_some() {
                metrics::counter!("raftvec_raft_leader_elections_total").increment(1);
            }
            last_leader = current;
        }
    });
}

/// raftvec_vectors_total, labeled by node_id: shard balance sanity check.
/// Polled rather than updated inline, because a follower's count changes
/// via Raft apply, not through its own insert/delete handlers.
pub fn spawn_vector_count_gauge(store: Arc<Store>, node_id: NodeId) {
    tokio::spawn(async move {
        loop {
            let (_, total) = store.cluster_status();
            metrics::gauge!("raftvec_vectors_total", "node_id" => node_id.to_string()).set(total as f64);
            tokio::time::sleep(VECTOR_COUNT_POLL_INTERVAL).await;
        }
    });
}

/// raftvec_raft_replication_lag_entries, labeled by follower node_id.
/// Only available while this replica is the leader, since only the leader
/// tracks follower log progress.
pub fn spawn_replication_lag_gauge(raft: Raft) {
    tokio::spawn(async move {
        let mut rx = raft.metrics();
        loop {
            if rx.changed().await.is_err() {
                break;
            }
            let metrics = rx.borrow().clone();
            let Some(leader_log_index) = metrics.last_log_index else {
                continue;
            };
            let Some(replication) = metrics.replication else {
                continue; // not currently leader
            };
            for (follower_id, follower_log_id) in replication {
                let follower_index = follower_log_id.map(|l| l.index).unwrap_or(0);
                let lag = leader_log_index.saturating_sub(follower_index);
                metrics::gauge!(
                    "raftvec_raft_replication_lag_entries",
                    "follower_node_id" => follower_id.to_string()
                )
                .set(lag as f64);
            }
        }
    });
}
