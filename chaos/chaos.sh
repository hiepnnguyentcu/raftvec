#!/usr/bin/env bash
# Kills the current Raft leader of a shard and reports how long the
# cluster took to elect a new leader and resume serving that shard
# (product spec §9 M3 demo; NFR2 target: <2s recovery).
#
# Usage: ./chaos/chaos.sh --kill-leader shard0 [--dim 4] [--collection docs]
#
# Requires: docker compose stack already running (docker compose up -d),
# a collection already created, and the "docker.io/fullstorydev/grpcurl"
# image (pulled on first use) -- shard replica ports are intentionally not
# published to the host (only the aggregator is externally reachable), so
# probing an individual replica's leadership status has to happen from a
# container on the same compose network.
set -euo pipefail

SHARD=""
DIM=4
COLLECTION="docs"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --kill-leader) SHARD="$2"; shift 2 ;;
    --dim) DIM="$2"; shift 2 ;;
    --collection) COLLECTION="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$SHARD" ]]; then
  echo "usage: chaos.sh --kill-leader <shard0|shard1|...> [--dim N] [--collection NAME]" >&2
  exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROTO_DIR="$ROOT/crates/raftvec-proto/proto"
PROJECT="$(basename "$ROOT" | tr 'A-Z' 'a-z' | tr -cd 'a-z0-9-')"
NETWORK="${PROJECT}_default"

# All-zero query vector of the right length: cosine_similarity special-cases
# a zero vector to score 0.0 for everything (raftvec-core/src/similarity.rs)
# rather than dividing by zero, so this is a safe no-op probe that still
# passes the dimension check and reaches the leadership check inside
# NodeService::search (which runs before any dimension validation).
zero_vector() {
  python3 -c "print(','.join(['0'] * $DIM))"
}

grpc_search() {
  local target="$1"
  docker run --rm --network "$NETWORK" -v "$PROTO_DIR:/proto" fullstorydev/grpcurl \
    -plaintext -max-time 2 -import-path /proto -proto raftvec.proto \
    -d "{\"collection\":\"$COLLECTION\",\"query_vector\":[$(zero_vector)],\"k\":1}" \
    "${target}:50051" raftvec.RaftVec/Search 2>/dev/null || true
}

# Prints the replica service name currently believed to be this shard's
# leader (a replica that answered without a leader_hint), or nothing if
# none could be determined.
find_leader() {
  local shard="$1"
  for replica in "${shard}-r1" "${shard}-r2" "${shard}-r3"; do
    local resp
    resp=$(grpc_search "$replica")
    if [[ -n "$resp" ]] && ! grep -q leaderHint <<<"$resp"; then
      echo "$replica"
      return 0
    fi
  done
  return 1
}

# `date +%s.%N` is a GNU coreutils extension -- BSD/macOS date has no
# sub-second resolution, so timestamps go through python3 instead (already
# a project dependency) for portability.
now() { python3 -c "import time; print(time.time())"; }
elapsed_since() { python3 -c "import time; print(f'{time.time() - $1:.1f}')"; }

START=$(now)

echo "locating current leader of $SHARD..."
LEADER=$(find_leader "$SHARD") || { echo "could not determine a leader for $SHARD" >&2; exit 1; }
echo "[t=$(elapsed_since "$START")s] $SHARD leader is $LEADER"

docker compose --project-directory "$ROOT" kill -s SIGKILL "$LEADER" >/dev/null
KILL_TIME=$(now)
echo "[t=$(elapsed_since "$START")s] killed $LEADER"

# Recovery time is measured from the kill itself, not from script start --
# the leader-discovery probe above is setup, not part of what NFR2 bounds.
DEADLINE=$(python3 -c "import time; print(time.time() + 10)")
NEW_LEADER=""
while python3 -c "import time,sys; sys.exit(0 if time.time() < $DEADLINE else 1)"; do
  CANDIDATE=$(find_leader "$SHARD" || true)
  if [[ -n "$CANDIDATE" && "$CANDIDATE" != "$LEADER" ]]; then
    NEW_LEADER="$CANDIDATE"
    break
  fi
  sleep 0.1
done

if [[ -z "$NEW_LEADER" ]]; then
  echo "[t=$(elapsed_since "$START")s] FAILED: no new leader elected within 10s" >&2
  exit 1
fi

echo "[t=$(elapsed_since "$START")s] new leader elected: $NEW_LEADER"
echo ""
echo "recovery time: $(elapsed_since "$KILL_TIME")s (NFR2 target: <2s)"
