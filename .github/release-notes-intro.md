A distributed vector search engine in Rust where every shard is a Raft-replicated group (2 shards × 3 [openraft](https://github.com/databendlabs/openraft) replicas by default). Kill a shard leader mid-traffic and the cluster keeps returning complete, correct results — writes commit to a majority before acknowledgement, reads are served only after a quorum-confirmed leadership check, and partial results are never presented as complete (`shards_failed > 0` is surfaced, not hidden).

**Try it:**
```bash
docker compose up -d   # pulls the images below
docker compose run --rm --no-deps vecctl --addr http://aggregator:50060 \
    create-collection --collection docs --dim 384 --shard-count 2
```
Then run `./chaos/chaos.sh --kill-leader shard0` against a loaded collection and watch it recover. Full walkthrough in the [README](https://github.com/hiepnnguyentcu/raftvec#readme).

**In this release:**
- Sharded, Raft-replicated cluster: aggregator (stateless fan-out/merge) + shard replicas (Raft state machine + gRPC transport), exact top-k merge proven against a correctness oracle
- Four layers of correctness testing (unit → oracle equality → cluster equality → real-process failure injection), including regression tests for two bugs that only showed up under real chaos testing — see the README's "What real failure testing caught"
- Prometheus metrics + a pre-provisioned Grafana dashboard; `chaos/` scripts for scripted leader kills and post-chaos integrity audits
- Measured, not claimed: benchmark table (healthy / mid-failure / degraded-steady-state) and a documented, honest miss on the original <2s leader-election target, with root cause

**Images** (also available via `docker compose up -d`):
- `ghcr.io/hiepnnguyentcu/raftvec-node:0.1.0`
- `ghcr.io/hiepnnguyentcu/raftvec-aggregator:0.1.0`
- `ghcr.io/hiepnnguyentcu/raftvec-vecctl:0.1.0`

