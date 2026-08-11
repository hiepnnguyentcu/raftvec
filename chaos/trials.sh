#!/usr/bin/env bash
# Runs the leader-kill chaos scenario N times against a fresh cluster each
# time and reports the distribution of recovery times. Recovery time is
# timing-dependent by nature (election timeouts are randomized, and the
# leader lease has to expire first), so a single run is not a measurement
# -- this exists so the README's NFR2 numbers come from a distribution
# rather than one lucky or unlucky trial.
#
# Usage: ./chaos/trials.sh [N] [corpus.jsonl]
set -euo pipefail

TRIALS="${1:-5}"
CORPUS="${2:-/tmp/bench_corpus.jsonl}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

results=()
for i in $(seq 1 "$TRIALS"); do
  echo "=== trial $i/$TRIALS ==="
  docker compose down >/dev/null 2>&1
  docker compose up -d >/dev/null 2>&1
  sleep 4
  docker compose run --rm --no-deps vecctl --addr http://aggregator:50060 \
    create-collection --collection docs --dim 384 --shard-count 2 >/dev/null 2>&1
  docker compose run --rm --no-deps -v "$(dirname "$CORPUS")":/data vecctl \
    --addr http://aggregator:50060 insert --collection docs \
    --file "/data/$(basename "$CORPUS")" >/dev/null 2>&1

  out=$(./chaos/chaos.sh --kill-leader shard0 --dim 384 --collection docs 2>&1) || {
    echo "  FAILED to recover"
    results+=("FAIL")
    continue
  }
  rt=$(grep -o 'recovery time: [0-9.]*' <<<"$out" | awk '{print $3}')
  echo "  recovery: ${rt}s"
  results+=("$rt")
done

echo ""
echo "=== recovery times across $TRIALS trials ==="
printf '%s\n' "${results[@]}"
printf '%s\n' "${results[@]}" | grep -v FAIL | python3 -c "
import sys
vals = [float(l) for l in sys.stdin if l.strip()]
if vals:
    vals.sort()
    print(f'min {min(vals):.1f}s  median {vals[len(vals)//2]:.1f}s  max {max(vals):.1f}s  (n={len(vals)})')
"
