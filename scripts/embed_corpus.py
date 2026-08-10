#!/usr/bin/env python3
"""Embeds a public text corpus (or a single ad hoc query) with a
sentence-transformer, for RaftVec M1 (see vectordb-01-product-spec.md §9).

Batch mode (default) produces the JSONL file `vecctl insert --file` loads:
    {"id": 0, "embedding": [...], "metadata": {"text": "..."}}

Query mode (--embed-query) embeds one string and prints a JSON float array
to stdout. `vecctl search --query "..."` shells out to this mode so the
query is embedded with the exact same model used to build the corpus --
comparing vectors from two different models would make cosine similarity
meaningless.

Usage:
    python3 scripts/embed_corpus.py --output corpus.jsonl --limit 5000
    python3 scripts/embed_corpus.py --output corpus.jsonl --limit 500000  # full M1 corpus
    python3 scripts/embed_corpus.py --embed-query "distributed consensus algorithms"
"""
import argparse
import json
import sys

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"  # 384-dim
DATASET_NAME = "armanc/scientific_papers"
DATASET_CONFIG = "arxiv"

# Hand-picked fallback used when the Hugging Face dataset can't be reached
# (offline machine, dataset renamed/removed upstream). Keeps
# `vecctl search --query "distributed consensus algorithms"` demoable
# without a network dependency at demo time -- NFR5 requires a stranger to
# reproduce the demo in under 10 minutes, which a flaky dataset download
# would put at risk.
FALLBACK_CORPUS = [
    "In Search of an Understandable Consensus Algorithm (Raft)",
    "Paxos Made Simple",
    "Impossibility of Distributed Consensus with One Faulty Process",
    "The Part-Time Parliament",
    "Viewstamped Replication: A New Primary Copy Method",
    "The Google File System",
    "MapReduce: Simplified Data Processing on Large Clusters",
    "Bigtable: A Distributed Storage System for Structured Data",
    "Dynamo: Amazon's Highly Available Key-value Store",
    "Spanner: Google's Globally-Distributed Database",
    "ZooKeeper: Wait-free Coordination for Internet-scale Systems",
    "CockroachDB: The Resilient Geo-Distributed SQL Database",
    "TiKV: A Distributed and Transactional Key-Value Database",
    "Chubby: The Lock Service for Loosely-Coupled Distributed Systems",
    "Kafka: A Distributed Messaging System for Log Processing",
    "Cassandra: A Decentralized Structured Storage System",
    "Amazon Aurora: Design Considerations for High Throughput Cloud-Native Relational Databases",
    "Calvin: Fast Distributed Transactions for Partitioned Database Systems",
    "Consistent Hashing and Random Trees: Distributed Caching Protocols",
    "Time, Clocks, and the Ordering of Events in a Distributed System",
    "Practical Byzantine Fault Tolerance",
    "The Byzantine Generals Problem",
    "Attention Is All You Need",
    "BERT: Pre-training of Deep Bidirectional Transformers for Language Understanding",
    "Efficient Estimation of Word Representations in Vector Space",
    "Sentence-BERT: Sentence Embeddings using Siamese BERT-Networks",
    "Approximate Nearest Neighbor Search: HNSW",
    "Product Quantization for Nearest Neighbor Search",
    "Billion-scale Similarity Search with GPUs",
    "Semantic Search At LinkedIn",
    "A Guide to Approximate Nearest Neighbor Algorithms",
    "Deep Learning based Recommender Systems: A Survey",
    "Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks",
    "Reading Wikipedia to Answer Open-Domain Questions",
    "The Anatomy of a Large-Scale Hypertextual Web Search Engine",
    "Inverted Index Compression and Query Processing with Optimized Document Ordering",
    "Raft Refloated: Do We Have Consensus?",
    "In Search of an Understandable Consensus Algorithm (Extended Version)",
    "Paxos Made Live: An Engineering Perspective",
    "There Is More Consensus in Egalitarian Parliaments",
    "Fast Paxos",
]


def load_model():
    from sentence_transformers import SentenceTransformer

    return SentenceTransformer(MODEL_NAME)


def load_corpus_texts(limit: int) -> list[str]:
    try:
        from datasets import load_dataset

        ds = load_dataset(DATASET_NAME, DATASET_CONFIG, split="train", streaming=True)
        texts = []
        for row in ds:
            abstract = (row.get("abstract") or "").strip()
            if abstract:
                texts.append(" ".join(abstract.split()))
            if len(texts) >= limit:
                break
        if texts:
            return texts
        print("warning: dataset returned no usable rows, using fallback corpus", file=sys.stderr)
    except Exception as e:  # network down, dataset renamed, etc.
        print(f"warning: could not load '{DATASET_NAME}' ({e}); using fallback corpus", file=sys.stderr)

    texts = []
    i = 0
    while len(texts) < min(limit, 10_000):
        texts.append(FALLBACK_CORPUS[i % len(FALLBACK_CORPUS)])
        i += 1
        if i >= len(FALLBACK_CORPUS) * 50:  # avoid an infinite loop if limit is huge
            break
    return texts


def cmd_embed_query(text: str) -> None:
    model = load_model()
    vec = model.encode(text, normalize_embeddings=False)
    print(json.dumps(vec.tolist()))


def cmd_batch(output: str, limit: int) -> None:
    texts = load_corpus_texts(limit)
    model = load_model()
    embeddings = model.encode(texts, batch_size=64, show_progress_bar=True)

    with open(output, "w") as f:
        for i, (text, emb) in enumerate(zip(texts, embeddings)):
            record = {"id": i, "embedding": emb.tolist(), "metadata": {"text": text[:300]}}
            f.write(json.dumps(record) + "\n")

    dim = len(embeddings[0]) if len(embeddings) else 0
    print(f"wrote {len(texts)} vectors (dim={dim}) to {output}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output", default="corpus.jsonl", help="JSONL output path (batch mode)")
    parser.add_argument("--limit", type=int, default=2000, help="number of documents to embed (spec target: 500000)")
    parser.add_argument("--embed-query", metavar="TEXT", help="embed a single string and print its vector as JSON")
    args = parser.parse_args()

    if args.embed_query is not None:
        cmd_embed_query(args.embed_query)
    else:
        cmd_batch(args.output, args.limit)


if __name__ == "__main__":
    main()
