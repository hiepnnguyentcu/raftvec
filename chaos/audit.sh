#!/usr/bin/env bash
# Post-chaos integrity check (product spec §9 M3 exit criteria):
#   - every acknowledged write is present exactly once (vector_count
#     matches the corpus size -- neither lost nor duplicated)
#   - a random sample of records are still retrievable by exact self-match
#     (proves the count isn't just numerically right by coincidence)
#
# Usage: ./chaos/audit.sh --file corpus.jsonl [--collection docs] [--samples 20]
#
# Run this after chaos.sh, once the cluster has recovered, against the
# same corpus file that was inserted before the chaos run.
set -euo pipefail

FILE=""
COLLECTION="docs"
SAMPLES=20
ADDR="http://aggregator:50060"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --file) FILE="$2"; shift 2 ;;
    --collection) COLLECTION="$2"; shift 2 ;;
    --samples) SAMPLES="$2"; shift 2 ;;
    --addr) ADDR="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$FILE" ]]; then
  echo "usage: audit.sh --file corpus.jsonl [--collection docs] [--samples 20] [--addr http://aggregator:50060]" >&2
  exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# --no-deps matters: without it, `docker compose run` resurrects any
# dependency (including a shard replica chaos.sh just killed) to satisfy
# the aggregator's depends_on, which would silently undo the chaos
# scenario mid-audit.
vecctl() {
  # </dev/null matters: without it, `docker compose run` attaches this
  # loop's own stdin, and inside the `while read` sampling loop below that
  # silently steals the remaining sample lines after the first iteration.
  docker compose --project-directory "$ROOT" run --rm --no-deps vecctl --addr "$ADDR" "$@" </dev/null
}

EXPECTED=$(grep -c . "$FILE")
echo "expected records: $EXPECTED"

STATUS=$(vecctl status)
ACTUAL=$(echo "$STATUS" | grep vector_count | awk '{print $2}' | tr -d '\r')
echo "actual vector_count: $ACTUAL"

FAILED=0

if [[ "$ACTUAL" == "$EXPECTED" ]]; then
  echo "✓ $ACTUAL/$EXPECTED acknowledged writes present, no duplicates"
else
  echo "✗ MISMATCH: expected $EXPECTED, found $ACTUAL"
  FAILED=1
fi

echo "spot-checking $SAMPLES records for correct retrieval..."
FAILURES=0
CHECKED=0
while IFS=$'\t' read -r id embedding; do
  CHECKED=$((CHECKED + 1))
  RESULT=$(vecctl search --collection "$COLLECTION" --query-vector "$embedding" -k 1 2>&1)
  TOP_ID=$(echo "$RESULT" | head -1 | grep -oE 'id=[0-9]+' | cut -d= -f2 || true)
  if [[ "$TOP_ID" != "$id" ]]; then
    echo "  ✗ id $id: expected itself as top match, got id=${TOP_ID:-<none>}"
    FAILURES=$((FAILURES + 1))
  fi
done < <(python3 "$ROOT/chaos/sample_records.py" "$FILE" "$SAMPLES")

if [[ $FAILURES -eq 0 ]]; then
  echo "✓ all $CHECKED sampled records retrievable with exact self-match"
else
  echo "✗ $FAILURES/$CHECKED sampled records failed retrieval check"
  FAILED=1
fi

echo ""
if [[ $FAILED -eq 0 ]]; then
  echo "audit passed"
else
  echo "audit FAILED"
  exit 1
fi
