#!/usr/bin/env python3
"""Picks N random records from a corpus JSONL file for audit.sh's
retrieval spot-check. Prints "id<TAB>comma,separated,embedding" per line.
"""
import json
import random
import sys

path, n = sys.argv[1], int(sys.argv[2])
with open(path) as f:
    lines = [line for line in f if line.strip()]

random.seed(42)  # deterministic sample across audit runs
sample = random.sample(lines, min(n, len(lines)))

for line in sample:
    rec = json.loads(line)
    embedding_csv = ",".join(str(x) for x in rec["embedding"])
    print(f"{rec['id']}\t{embedding_csv}")
