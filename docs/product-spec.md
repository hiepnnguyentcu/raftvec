# Product Spec — Raft-Replicated Distributed Vector Search Engine

**Working name:** RaftVec
**Author:** Hiep Nguyen
**Doc:** 1 of 2 (see also: `vectordb-02-technical-design.md`)
**Status:** Draft v1
**Date:** August 2026
**Timeline:** 4 weeks

---

## 1. Summary

RaftVec is a distributed vector search engine written in Rust. Vectors are partitioned across shards for scale, and each shard is replicated across three nodes using the Raft consensus protocol, so the cluster continues serving correct search results when a node fails.

The project's purpose is to demonstrate distributed-systems engineering — consensus, replication, failure recovery, and provable correctness under fault injection — in a domain (vector search) where those properties are increasingly required but frequently under-implemented.

---

## 2. Background & Motivation

Vector search is core infrastructure for semantic search, RAG, and recommendation retrieval. LinkedIn's 2026 paper *Semantic Search At LinkedIn* (arXiv:2602.07309) describes a production system doing exhaustive GPU-based embedding retrieval over 1.3B documents, feeding a downstream ranking stage — a two-stage architecture now standard across the industry.

Most open-source vector databases implement the **search** half well (ANN indexing, sharding, query fan-out) and treat **distributed correctness** as future work. [MuopDB](https://github.com/hicder/muopdb) is a concrete, current example: an actively developed Rust vector DB with a working "doc-sharding & query fan-out with aggregator-leaf architecture," where *"MuopDB with consensus protocol (Raft)"* sits unchecked in Phase 5 of its public roadmap.

That gap is the product opportunity for this project. Sharding alone gives you scale but not durability: if a shard node dies, its slice of the index is simply gone, and every query silently returns incomplete results. Replication with consensus is what turns a partitioned index into a fault-tolerant system.

---

## 3. Target User & Use Case

**Primary persona:** a backend/ML engineer who needs semantic search over a corpus too large for a single node, and who cannot tolerate silent result degradation when a machine fails.

**Representative user story:**
> As a service owner, when one of my vector search nodes crashes at 3am, I need my search endpoint to keep returning complete, correct results — automatically, without paging a human and without silently dropping a shard's worth of documents from every query.

**Concrete demo scenario used throughout this project:** semantic search over a corpus of ~500K technical documents (arXiv abstracts or job postings), embedded with a local sentence-transformer model. A query returns the top-k most semantically similar documents, and continues to do so while a node is being killed.

---

## 4. Goals

| # | Goal | Why it matters |
|---|---|---|
| G1 | Partition a vector corpus across N shards and serve queries via parallel fan-out + merge | Baseline capability — scale beyond one node |
| G2 | Replicate each shard 3x with Raft consensus | The core differentiator; turns partitioning into fault tolerance |
| G3 | Survive shard-leader failure with automatic recovery, no data loss, no incorrect results | The property the whole project exists to prove |
| G4 | Prove correctness empirically, not by assertion | Chaos test + correctness oracle, not "it seemed to work" |
| G5 | Be reproducible by a stranger in <10 minutes | A portfolio project nobody can run is not a portfolio project |

---

## 5. Non-Goals (v1)

These are deliberate scope cuts, documented so they read as decisions rather than gaps.

| Non-goal | Rationale |
|---|---|
| ANN indexing (HNSW/IVF/SPANN) | Brute-force search per shard is correct and sufficient at demo scale. ANN is a solved, well-documented problem orthogonal to the distributed-systems question being tested. Interface designed to allow swapping it in later. |
| Dynamic resharding / online shard migration | A hard problem in its own right; would consume the entire timeline. Shard count is fixed at collection creation. |
| On-disk persistence / WAL | Cluster is in-memory; full-cluster restart loses data. Removes storage-engine complexity from the critical path so effort goes toward replication correctness. |
| Hybrid text+vector search, multi-tenancy, quantization | Features MuopDB already has. Not what this project is demonstrating. |
| Reranking / L2 ranking stage | Separate concern; possible follow-on project. |
| Authentication, multi-region, cloud deployment | Not relevant to the core thesis. |

---

## 6. Functional Requirements

| ID | Requirement | Priority |
|---|---|---|
| FR1 | Create a collection with configurable dimension and shard count | Must |
| FR2 | Insert a vector (id, embedding, optional metadata) | Must |
| FR3 | Delete a vector by id | Must |
| FR4 | Search: given a query vector and k, return global top-k by cosine similarity | Must |
| FR5 | Writes are replicated to a majority of a shard's replicas before acknowledgement | Must |
| FR6 | Client requests to a non-leader are transparently redirected/retried | Must |
| FR7 | Cluster topology (shard→node assignment, current leaders) is queryable | Should |
| FR8 | Metrics endpoint exposing latency, throughput, and Raft health | Should |

---

## 7. Non-Functional Requirements

| ID | Requirement | Target |
|---|---|---|
| NFR1 | Query correctness | Merged cluster top-k is **identical** to single-node brute-force baseline, at all times |
| NFR2 | Recovery time after leader loss | < 2 seconds to resume serving |
| NFR3 | Data loss on single-node failure | Zero acknowledged writes lost |
| NFR4 | Query latency | p99 documented on 500K-vector corpus; no hard SLA, but must be measured with/without node down |
| NFR5 | Setup time for a new user | < 10 minutes from `git clone` to running chaos demo |

---

## 8. Success Criteria

The project is successful if all five hold:

1. **Correctness under partition** — an automated test asserts cluster results equal single-node baseline results across the full query set.
2. **Correctness under failure** — the same assertion holds after a shard leader is killed mid-load-test and the cluster recovers.
3. **No lost writes** — post-chaos audit confirms every acknowledged write is present and no write is duplicated.
4. **Measured, not claimed** — a results table reports QPS and p50/p99 latency in both healthy and degraded (one node down) states.
5. **Reproducible** — `docker compose up` + one script reproduces the entire demo on a clean machine.

---

## 9. Timeline & Deliverables

Four weeks, one milestone per week. Each milestone ends in a runnable artifact — no milestone is "research" or "design only."

### Week 1 — M1: Single-node vector store

**Deliverables**
- `vectordb-node` binary: in-memory vector store, brute-force cosine top-k, gRPC service
- CLI client (`vecctl`) for insert/delete/search
- Embedding-generation script producing a 500K-vector corpus from a public text dataset
- Correctness oracle: a reference brute-force implementation used as ground truth in tests

**Expected demo**
```
$ vecctl search --query "distributed consensus algorithms" -k 5
1. 0.891  "In Search of an Understandable Consensus Algorithm (Raft)"
2. 0.874  "Paxos Made Simple"
...
```
Plus: `cargo test` showing results match the oracle exactly.

**Exit criteria:** search results are provably correct on a single node; latency measured and recorded as the baseline.

---

### Week 2 — M2: Sharding and fan-out

**Deliverables**
- `aggregator` service: shard routing, parallel fan-out, top-k merge
- 4-shard cluster orchestrated by `docker-compose.yml`
- Regression test asserting cluster results == M1 single-node results
- Load-test harness reporting QPS and p50/p99

**Expected demo**
```
$ docker compose up -d          # aggregator + 4 shard nodes
$ vecctl search --query "..." -k 5   # identical results to M1
$ ./bench --qps 200 --duration 60s
  QPS: 214  p50: 12ms  p99: 38ms  errors: 0
```

**Exit criteria:** sharded cluster returns identical results to the single-node baseline, with measured throughput.

---

### Week 3 — M3: Raft replication and fault tolerance

**Deliverables**
- `openraft` integrated: each shard is a 3-node Raft group (12 shard nodes total for 4 shards)
- Metadata Raft group tracking shard→node assignment and current leaders
- Write path routed through the Raft log; leader redirect handling in the aggregator
- `chaos.sh`: kills a shard leader mid-load-test and audits the outcome

**Expected demo**
```
$ ./bench --qps 200 --duration 60s &
$ ./chaos.sh --kill-leader shard-2
  [t=15s] killed shard-2 leader (node-7)
  [t=15.4s] new leader elected: node-9
  [t=60s] bench complete — QPS: 208  p99: 41ms  failed requests: 0
$ ./audit.sh
  ✓ all 500,000 acknowledged writes present
  ✓ zero duplicates
  ✓ cluster results match single-node oracle
```

**Exit criteria:** all three success criteria on correctness and durability pass under fault injection.

---

### Week 4 — M4: Observability, benchmarking, documentation

**Deliverables**
- Prometheus metrics: query latency histograms, per-shard latency, Raft replication lag, leader-election counter
- Grafana dashboard (JSON committed to repo)
- Benchmark results table: healthy vs. degraded cluster
- README: architecture diagram, quickstart, **Design Decisions & Tradeoffs** section citing MuopDB and the LinkedIn paper
- Screen recording of the full chaos demo

**Expected demo**
The README walkthrough end-to-end on a clean machine, with the Grafana dashboard visibly showing the latency spike and recovery at the moment the leader is killed.

**Exit criteria:** a stranger following the README alone reproduces the chaos demo in under 10 minutes.

---

## 10. Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| `openraft` API learning curve consumes Week 3 | Medium | Spike a minimal 3-node counter state machine in Week 2 evenings, before the real integration |
| 12 containers overwhelm a laptop | Medium | Reduce to 2 shards × 3 replicas (6 nodes); the thesis holds identically |
| Chaos test is flaky / timing-dependent | Medium | Assert on post-conditions (data integrity, eventual recovery) rather than exact timing windows |
| Scope creep into ANN indexing | High | Explicitly listed as non-goal; only touch it if M4 finishes early |

---

## 11. Stretch Goals

Ordered by value, only if ahead of schedule:

1. Swap brute-force for a real ANN index (`hnsw_rs` / `instant-distance`) behind the existing shard interface
2. On-disk persistence (WAL + snapshot), enabling survival of full-cluster restart
3. Follower reads for read scaling, with a documented staleness bound
4. Dynamic resharding on node join/leave

---

## 12. Positioning

One-paragraph version for a README or resume conversation:

> RaftVec is a sharded vector search engine in Rust where each shard is a Raft-replicated group. I built it against a real reference point: MuopDB, an open-source Rust vector DB with the same aggregator-leaf sharding architecture, whose public roadmap lists Raft consensus as unstarted. The system survives a shard leader being killed mid-traffic with zero lost writes and automatic sub-2-second recovery, verified by an automated chaos test and a correctness oracle — with load-test numbers for both healthy and degraded cluster states.

---

## References

- [Semantic Search At LinkedIn (arXiv:2602.07309)](https://arxiv.org/abs/2602.07309)
- [Reimagining LinkedIn's search tech stack](https://www.linkedin.com/blog/engineering/search/reimagining-linkedins-search-stack)
- [MuopDB — Rust vector database](https://github.com/hicder/muopdb)
- [MIT 6.5840 Distributed Systems](https://pdos.csail.mit.edu/6.824/)
- [openraft](https://github.com/databendlabs/openraft)
