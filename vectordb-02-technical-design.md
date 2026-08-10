# Technical Design — RaftVec

**Working name:** RaftVec
**Author:** Hiep Nguyen
**Doc:** 2 of 2 (see also: `vectordb-01-product-spec.md`)
**Status:** Draft v1
**Date:** August 2026
**Stack:** Rust (stable), tokio, tonic + prost (gRPC), openraft, Docker Compose, Prometheus + Grafana

---

## 1. Design Goals & Constraints

Derived from the product spec:

- **Correctness is the primary output.** Every design choice that trades correctness for speed is rejected in v1.
- **Real network boundaries.** Nodes are separate OS processes in separate containers, communicating over gRPC. No in-process shortcuts — a distributed system that only works in one process proves nothing.
- **Consensus via library, not from scratch.** Raft is implemented with `openraft`. Hand-rolling Raft was already done in Go (MIT 6.5840); production Rust systems integrate a battle-tested crate, and doing that correctly is its own distinct skill.
- **Fit in 4 weeks.** Every component is the simplest thing that is still correct.

---

## 2. Architecture

### 2.1 Component Overview

Two data-plane roles plus a control plane. The aggregator/leaf split mirrors MuopDB's own terminology; the per-shard Raft group model mirrors CockroachDB and TiKV (one Raft group per data range).

```
                         ┌──────────────┐
            client ────▶ │  Aggregator  │ ◀──── metrics scrape
                         └──────┬───────┘
                                │  parallel gRPC fan-out
        ┌───────────────────────┼───────────────────────┐
        ▼                       ▼                       ▼
 ┌─────────────┐         ┌─────────────┐         ┌─────────────┐
 │  Shard 0    │         │  Shard 1    │         │  Shard 2    │
 │  Raft group │         │  Raft group │         │  Raft group │
 │ ┌─┐ ┌─┐ ┌─┐ │         │ ┌─┐ ┌─┐ ┌─┐ │         │ ┌─┐ ┌─┐ ┌─┐ │
 │ │L│ │F│ │F│ │         │ │F│ │L│ │F│ │         │ │F│ │F│ │L│ │
 │ └─┘ └─┘ └─┘ │         │ └─┘ └─┘ └─┘ │         │ └─┘ └─┘ └─┘ │
 └─────────────┘         └─────────────┘         └─────────────┘
        ▲
        │  shard→node assignment, current leader per shard
 ┌──────┴────────┐
 │   MetaRaft    │   control plane: 3-node Raft group
 │  ┌─┐ ┌─┐ ┌─┐  │
 │  │L│ │F│ │F│  │
 │  └─┘ └─┘ └─┘  │
 └───────────────┘
```

L = Raft leader, F = follower.

### 2.2 Roles

**Aggregator (`raftvec-aggregator`)**
Stateless, horizontally scalable. Responsibilities:
- Terminate client gRPC
- Route writes to the correct shard leader (`shard_id = hash(vector_id) % shard_count`)
- Fan out reads to all shard leaders concurrently; merge partial top-k into global top-k
- Cache the shard→leader map from MetaRaft; refresh on `NotLeader` errors
- Export Prometheus metrics

**Shard node (`raftvec-node`)**
Stateful. Holds one replica of one shard. Responsibilities:
- Participate in its shard's Raft group via `openraft`
- Apply committed log entries to its local in-memory index (the Raft state machine)
- Serve `ShardSearch` when it is the leader
- Serve openraft's internal transport RPCs (AppendEntries, Vote, InstallSnapshot)

**MetaRaft**
A 3-node Raft group holding cluster topology. Conceptually identical to MIT 6.5840 Lab 5's `shardctrler`: a small, separately-replicated config service that the data plane consults. Kept separate from shard groups so a shard outage cannot take down topology reads, and so topology changes are themselves consensus-backed.

### 2.3 Why This Split

| Decision | Alternative considered | Rationale |
|---|---|---|
| One Raft group per shard | One Raft group for the whole cluster | A single group serializes all writes cluster-wide and cannot scale; per-shard groups let shards accept writes independently. Matches CockroachDB/TiKV. |
| Separate MetaRaft group | Store topology in shard 0's group | Coupling topology availability to one shard's health is a needless SPOF. |
| Stateless aggregator | Aggregator as a Raft member | Keeping it stateless means it can be killed/restarted freely and scaled out; all durable state lives in Raft groups. |

---

## 3. Data Model

```rust
/// Cluster-level config, replicated via MetaRaft
struct CollectionMeta {
    name: String,
    dim: usize,
    shard_count: u32,
    replication_factor: u32,   // 3 in v1
}

struct ShardAssignment {
    shard_id: u32,
    replicas: Vec<NodeId>,
    leader: Option<NodeId>,    // advisory cache; authoritative answer comes from the node itself
}

/// The unit of data, replicated via a shard's Raft log
struct VectorRecord {
    id: u64,
    embedding: Vec<f32>,               // length == CollectionMeta.dim
    metadata: HashMap<String, String>, // unindexed passthrough
}
```

**Shard state machine (per shard node):**

```rust
struct ShardStateMachine {
    vectors: HashMap<u64, VectorRecord>,   // v1: flat map; ANN index slots in here later
    last_applied: LogId,
}
```

**Sharding function:** `shard_id = fxhash(vector_id) % shard_count`, fixed at collection creation. Consistent hashing is noted as the upgrade path if dynamic resharding is ever added, but is not needed for a fixed shard count.

---

## 4. API Design

### 4.1 Client-facing (Aggregator)

```protobuf
service RaftVec {
  rpc CreateCollection(CreateCollectionRequest) returns (CreateCollectionResponse);
  rpc Insert(InsertRequest) returns (InsertResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
  rpc Search(SearchRequest)  returns (SearchResponse);
  rpc ClusterStatus(ClusterStatusRequest) returns (ClusterStatusResponse);
}

message InsertRequest {
  string collection = 1;
  repeated VectorRecord records = 2;   // batched
}

message SearchRequest {
  string collection = 1;
  repeated float query_vector = 2;
  uint32 k = 3;
}

message SearchResponse {
  repeated ScoredRecord results = 1;   // sorted desc by score, len <= k
  uint32 shards_queried = 2;           // observability: did we hit every shard?
  uint32 shards_failed  = 3;           // non-zero => degraded/incomplete result
}
```

`shards_failed` is deliberately surfaced to the client. A partial result silently presented as complete is the exact failure mode this project exists to prevent.

### 4.2 Internal (Shard node)

```protobuf
service ShardNode {
  rpc ShardSearch(ShardSearchRequest) returns (ShardSearchResponse);
  rpc ShardWrite (ShardWriteRequest)  returns (ShardWriteResponse);
  rpc NodeStatus (NodeStatusRequest)  returns (NodeStatusResponse);

  // openraft transport
  rpc AppendEntries  (RaftAppendRequest)   returns (RaftAppendResponse);
  rpc Vote           (RaftVoteRequest)     returns (RaftVoteResponse);
  rpc InstallSnapshot(RaftSnapshotRequest) returns (RaftSnapshotResponse);
}
```

`ShardWriteResponse` carries an optional `leader_hint` so a misrouted write tells the aggregator where to go, avoiding a round trip to MetaRaft.

---

## 5. Request Paths

### 5.1 Write Path

```
Client ──Insert──▶ Aggregator
                     │ 1. group records by shard_id
                     │ 2. look up leader per shard (cached)
                     ▼
                  Shard leader
                     │ 3. openraft: append to log, replicate
                     │ 4. wait for majority commit  ◀── the durability guarantee
                     │ 5. apply to local state machine
                     ▼
                  ack ──▶ Aggregator ──▶ Client
```

Failure handling at step 2/3: if the target is no longer leader, openraft returns a `ForwardToLeader` error carrying the new leader's id. The aggregator updates its cache and retries once. If that also fails, it refreshes from MetaRaft and retries with backoff, up to a bounded retry count.

**Guarantee:** an `InsertResponse` is only returned after the write is committed to a majority of that shard's replicas. Losing any single node therefore cannot lose an acknowledged write (NFR3).

### 5.2 Read Path

```
Client ──Search──▶ Aggregator
                      │ 1. fan out to ALL shard leaders in parallel (tokio::join_all)
                      ▼
       ┌──────────────┼──────────────┐
       ▼              ▼              ▼
   Shard 0        Shard 1        Shard 2
   local top-k    local top-k    local top-k     ◀── brute-force cosine, rayon-parallel
       │              │              │
       └──────────────┼──────────────┘
                      ▼
                  2. merge into global top-k (bounded min-heap)
                      ▼
                   Client
```

**Correctness argument for merge:** each shard returns its true local top-k. Since shards partition the corpus disjointly, the global top-k is necessarily a subset of the union of local top-k sets. Merging by score and truncating to k is therefore exact — not an approximation. This is what makes NFR1 (identical to single-node baseline) achievable.

v1 reads from the leader only. Follower reads would require a read-index or lease mechanism to avoid stale results; deferred to stretch goals with an explicit staleness bound.

### 5.3 Timeouts and Partial Results

Each fan-out call has a per-shard deadline. If a shard fails to respond, the aggregator returns results from surviving shards **with `shards_failed > 0`**, rather than either hanging or silently returning an incomplete list. The client can then decide whether a degraded answer is acceptable.

---

## 6. Raft Integration (openraft)

Three traits must be implemented per Raft group:

| Trait | Implementation |
|---|---|
| `RaftLogStorage` | In-memory `Vec<Entry>` + `last_purged` marker. v1 has no disk persistence (documented non-goal). |
| `RaftStateMachine` | `ShardStateMachine` above; `apply()` mutates the vector map. Snapshot = serialized map. |
| `RaftNetwork` | tonic client wrapping the three transport RPCs. Includes connection pooling and timeouts. |

**Application types:**

```rust
#[derive(Serialize, Deserialize)]
enum ShardCommand {
    Upsert { record: VectorRecord },
    Delete { id: u64 },
}
```

Every mutation is a `ShardCommand` appended to the log. Reads bypass the log entirely (leader serves from applied state).

**Configuration:** election timeout 150-300ms, heartbeat 50ms. These are tuned to meet NFR2 (<2s recovery) with headroom — expected election time is well under 1s, leaving room for aggregator retry and cache refresh.

**Snapshotting:** triggered every N applied entries. Needed because a restarted or lagging replica must catch up without replaying the full log, and because in-memory log growth is otherwise unbounded during the load test.

---

## 7. Failure Scenarios & Expected Behavior

| Scenario | Expected behavior | How it's verified |
|---|---|---|
| Shard **follower** dies | No client-visible impact; group retains quorum (2/3) | Chaos test asserts zero failed requests |
| Shard **leader** dies | In-flight requests to it fail; remaining replicas elect a new leader; aggregator retries and succeeds. Bounded latency spike. | Chaos test: kill leader mid-load, assert recovery <2s and post-audit integrity |
| Two of three replicas die | That shard loses quorum → unavailable for writes; reads return `shards_failed > 0` | Manual test; documented as expected (correct behavior, not a bug) |
| MetaRaft leader dies | New MetaRaft leader elected. In-flight query/write traffic unaffected (uses cached topology). Collection creation briefly blocked. | Chaos test variant |
| Aggregator dies | Stateless — restart or route to another instance. No data impact. | Manual test |
| Network partition isolating a leader | Old leader steps down (cannot reach majority); majority side elects a new leader. No split-brain writes. | `iptables`-based partition test |

The two-of-three case is worth stating explicitly in the README: the system correctly becoming *unavailable* rather than *incorrect* under quorum loss is the intended CP-side tradeoff, not a limitation to apologize for.

---

## 8. Testing Strategy

Four layers, weakest to strongest:

1. **Unit tests** — top-k merge logic, shard routing hash distribution, cosine similarity correctness.
2. **Correctness oracle (the key test)** — a single-node brute-force implementation is ground truth. An automated test runs the same query set against the oracle and the cluster and asserts *exact* equality of returned ids and scores. This runs in three states: healthy cluster, during leader failover, and after recovery.
3. **Chaos tests** — scripted container kills during sustained load. Post-conditions asserted:
   - every acknowledged write is present exactly once (no loss, no duplication)
   - cluster results still match the oracle
   - recovery occurred within the NFR2 bound
4. **Load tests** — sustained QPS with latency histograms, run in healthy and degraded states, producing the results table for the README.

Deliberately **not** doing formal verification or full Jepsen. The oracle + chaos combination is the honest, achievable level of rigor for a 4-week project, and it directly tests the properties claimed.

---

## 9. Repository Layout

```
raftvec/
├── crates/
│   ├── raftvec-proto/       # .proto files + prost/tonic codegen
│   ├── raftvec-core/        # VectorRecord, cosine, top-k merge, sharding fn
│   ├── raftvec-node/        # shard node: state machine, openraft impls, gRPC server
│   ├── raftvec-aggregator/  # fan-out, merge, routing, leader cache, metrics
│   ├── raftvec-meta/        # MetaRaft control-plane group
│   └── vecctl/              # CLI client
├── bench/                   # load generator, results tables
├── chaos/                   # chaos.sh, partition.sh, audit.sh
├── scripts/                 # embed_corpus.py — dataset → embeddings
├── dashboards/              # grafana.json
├── docker-compose.yml
└── README.md
```

Cargo workspace. `raftvec-core` holds all logic that must be identical between the oracle and the distributed path — that shared code is what makes the equality assertion meaningful rather than tautological.

---

## 10. Observability

| Metric | Type | Purpose |
|---|---|---|
| `raftvec_query_duration_seconds` | histogram | End-to-end p50/p99, labeled by result status |
| `raftvec_shard_query_duration_seconds` | histogram | Per-shard latency; exposes stragglers in the fan-out |
| `raftvec_shards_failed_total` | counter | Degraded-result rate — the key correctness-adjacent metric |
| `raftvec_raft_leader_elections_total` | counter | Spikes visibly at chaos-injection time |
| `raftvec_raft_replication_lag_entries` | gauge | Follower health |
| `raftvec_vectors_total` | gauge, by shard | Shard balance sanity check |

The Grafana dashboard is designed so that the moment of the chaos injection is visually obvious — an election-counter step, a p99 spike, and a return to baseline. That screenshot is the single most communicative artifact of the whole project.

---

## 11. Milestones, Deliverables & Demos

Same four milestones as the product spec, expressed as engineering tasks.

### M1 — Single-node core (Week 1)

**Build:** `raftvec-core` (cosine, top-k, sharding fn) · `raftvec-node` standalone mode · `raftvec-proto` · `vecctl` · `scripts/embed_corpus.py`

**Key tasks**
- Define proto schema and generate Rust bindings
- Flat `HashMap` store; rayon-parallel brute-force scan
- Bounded min-heap top-k
- Generate 500K embeddings from a public corpus
- Write the correctness oracle and its test harness

**Deliverable:** single node answering correct semantic queries over 500K vectors, with baseline latency recorded.

**Demo**
```
$ vecctl search --query "distributed consensus algorithms" -k 5
1. 0.891  arxiv:1305.xxxx  "In Search of an Understandable Consensus Algorithm"
...
$ cargo test --release        # oracle equality passes
```

**Exit criteria:** oracle test green; p50/p99 baseline recorded.

---

### M2 — Sharding + fan-out (Week 2)

**Build:** `raftvec-aggregator` · multi-node `docker-compose.yml` · `bench/`

**Key tasks**
- Shard routing by hash; static assignment from a config file
- Parallel fan-out with per-shard deadlines
- Global merge; populate `shards_queried` / `shards_failed`
- Load generator with latency histograms
- Regression test: cluster results == M1 oracle

**Deliverable:** 4-shard cluster in Docker returning results identical to the single-node baseline, with measured throughput.

**Demo**
```
$ docker compose up -d
$ vecctl search --query "..." -k 5     # byte-identical to M1 output
$ ./bench --qps 200 --duration 60s
  QPS 214 | p50 12ms | p99 38ms | errors 0 | shards_failed 0
```

**Exit criteria:** equality regression test green against the sharded cluster.

> **Parallel track (evenings):** spike a minimal 3-node `openraft` counter to de-risk M3.

---

### M3 — Raft replication + fault tolerance (Week 3)

**Build:** openraft integration in `raftvec-node` · `raftvec-meta` · leader-aware routing · `chaos/`

**Key tasks**
- Implement `RaftLogStorage`, `RaftStateMachine`, `RaftNetwork`
- Convert write path to `ShardCommand` log entries
- Stand up MetaRaft; aggregator caches and refreshes topology
- Handle `ForwardToLeader` with bounded retry + backoff
- Snapshotting
- `chaos.sh` (kill leader), `partition.sh` (iptables), `audit.sh` (integrity check)

**Deliverable:** 4 shards × 3 replicas + 3 MetaRaft nodes, surviving leader loss under load.

**Demo**
```
$ ./bench --qps 200 --duration 60s &
$ ./chaos.sh --kill-leader shard-2
  [t=15.0s] killed shard-2 leader (node-7)
  [t=15.4s] new leader elected: node-9        ← 400ms
  [t=60s]   QPS 208 | p99 41ms | failed requests 0

$ ./audit.sh
  ✓ 500,000/500,000 acknowledged writes present
  ✓ 0 duplicates
  ✓ cluster results match oracle (1,000/1,000 queries)
```

**Exit criteria:** success criteria 1-3 from the product spec all pass under fault injection.

---

### M4 — Observability + polish (Week 4)

**Build:** metrics · Grafana dashboard · benchmark results · README · recording

**Key tasks**
- Instrument aggregator and nodes with the metric set in §10
- Build and export the dashboard JSON
- Run the full benchmark matrix (healthy vs. degraded) and write up the table
- README: architecture diagram, quickstart, Design Decisions & Tradeoffs (citing MuopDB's open Raft roadmap item and the LinkedIn paper), known limitations
- Record the chaos demo

**Deliverable:** a repo a stranger can clone and fully demo in <10 minutes.

**Demo:** the README walkthrough end-to-end, with Grafana showing the election spike and recovery in real time.

**Exit criteria:** clean-machine reproduction succeeds; all five product-spec success criteria documented as met.

---

## 12. Key Design Decisions (summary table)

| Decision | Chosen | Rejected alternative | Why |
|---|---|---|---|
| Consensus | `openraft` | Hand-rolled Raft | Already hand-rolled Raft in Go (6.5840). Correct library integration is the production-relevant skill and frees 2+ weeks for the properties actually being demonstrated. |
| Raft granularity | Per-shard groups | One cluster-wide group | Per-shard groups allow independent parallel writes; matches CockroachDB/TiKV. |
| Search algorithm | Brute-force cosine | HNSW/IVF | Exactness makes the oracle test meaningful. ANN is orthogonal to the distributed thesis and is a documented stretch goal. |
| Persistence | In-memory | WAL + mmap | Storage-engine work would consume the timeline without strengthening the consensus story. Explicit non-goal. |
| Reads | Leader-only | Follower reads | Avoids read-index/lease complexity; keeps NFR1 (exact equality) trivially provable. |
| Sharding | Static hash | Consistent hashing | Shard count is fixed in v1; consistent hashing only pays off with dynamic resharding. |
| RPC | tonic/gRPC | REST, custom TCP | Production-standard, matches MuopDB, and openraft transport rides the same stack. |

---

## References

- [openraft documentation](https://github.com/databendlabs/openraft)
- [Raft paper — In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- [Semantic Search At LinkedIn (arXiv:2602.07309)](https://arxiv.org/abs/2602.07309)
- [MuopDB](https://github.com/hicder/muopdb)
- [MIT 6.5840](https://pdos.csail.mit.edu/6.824/)
