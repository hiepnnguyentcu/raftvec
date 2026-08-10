use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use raftvec_proto::raft_vec_client::RaftVecClient;
use raftvec_proto::{
    ClusterStatusRequest, CreateCollectionRequest, DeleteRequest, InsertRequest, SearchRequest,
    VectorRecord,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::Command;

#[derive(Parser)]
#[command(name = "vecctl", about = "CLI client for a raftvec node/cluster")]
struct Cli {
    #[arg(long, global = true, default_value = "http://127.0.0.1:50051")]
    addr: String,

    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Create a collection with a fixed dimension and shard count.
    CreateCollection {
        #[arg(long)]
        collection: String,
        #[arg(long)]
        dim: u32,
        #[arg(long, default_value_t = 1)]
        shard_count: u32,
    },
    /// Insert vectors. Either a single record via flags, or a batch from a
    /// JSON-lines file: {"id": 1, "embedding": [...], "metadata": {...}}
    Insert {
        #[arg(long)]
        collection: String,
        #[arg(long)]
        file: Option<PathBuf>,
        #[arg(long)]
        id: Option<u64>,
        #[arg(long, value_delimiter = ',')]
        embedding: Option<Vec<f32>>,
    },
    /// Delete vectors by id.
    Delete {
        #[arg(long)]
        collection: String,
        #[arg(long, value_delimiter = ',')]
        ids: Vec<u64>,
    },
    /// Search for the top-k nearest vectors to a query.
    Search {
        #[arg(long)]
        collection: String,
        /// Query text; embedded via `python3 scripts/embed_corpus.py --embed-query`.
        #[arg(long, conflicts_with = "query_vector")]
        query: Option<String>,
        /// Raw query vector as comma-separated floats.
        #[arg(long, value_delimiter = ',', conflicts_with = "query")]
        query_vector: Option<Vec<f32>>,
        #[arg(short, long, default_value_t = 5)]
        k: u32,
    },
    /// Print cluster/collection status.
    Status,
}

#[derive(Deserialize)]
struct JsonlRecord {
    id: u64,
    embedding: Vec<f32>,
    #[serde(default)]
    metadata: HashMap<String, String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut client = RaftVecClient::connect(cli.addr.clone())
        .await
        .with_context(|| format!("connecting to {}", cli.addr))?;

    match cli.command {
        Cmd::CreateCollection {
            collection,
            dim,
            shard_count,
        } => {
            client
                .create_collection(CreateCollectionRequest {
                    name: collection.clone(),
                    dim,
                    shard_count,
                })
                .await?;
            println!("created collection '{collection}' (dim={dim}, shard_count={shard_count})");
        }

        Cmd::Insert {
            collection,
            file,
            id,
            embedding,
        } => {
            let records = if let Some(path) = file {
                load_jsonl(&path)?
            } else {
                let id = id.context("--id is required when not using --file")?;
                let embedding = embedding.context("--embedding is required when not using --file")?;
                vec![VectorRecord {
                    id,
                    embedding,
                    metadata: HashMap::new(),
                }]
            };

            let total = records.len();
            const BATCH_SIZE: usize = 1000;
            let mut inserted = 0u32;
            for chunk in records.chunks(BATCH_SIZE) {
                let resp = client
                    .insert(InsertRequest {
                        collection: collection.clone(),
                        records: chunk.to_vec(),
                    })
                    .await?;
                inserted += resp.into_inner().inserted;
            }
            println!("inserted {inserted}/{total} records into '{collection}'");
        }

        Cmd::Delete { collection, ids } => {
            let resp = client.delete(DeleteRequest { collection, ids }).await?;
            println!("deleted {} records", resp.into_inner().deleted);
        }

        Cmd::Search {
            collection,
            query,
            query_vector,
            k,
        } => {
            let query_vector = match (query, query_vector) {
                (Some(text), None) => embed_query_text(&text)?,
                (None, Some(v)) => v,
                _ => bail!("pass exactly one of --query or --query-vector"),
            };

            let resp = client
                .search(SearchRequest {
                    collection,
                    query_vector,
                    k,
                })
                .await?
                .into_inner();

            if resp.shards_failed > 0 {
                eprintln!(
                    "warning: {}/{} shards failed to respond — results may be incomplete",
                    resp.shards_failed, resp.shards_queried
                );
            }

            for (i, r) in resp.results.iter().enumerate() {
                let title = r.metadata.get("text").or_else(|| r.metadata.get("title"));
                match title {
                    Some(t) => println!("{}. {:.3}  id={}  {:?}", i + 1, r.score, r.id, t),
                    None => println!("{}. {:.3}  id={}", i + 1, r.score, r.id),
                }
            }
        }

        Cmd::Status => {
            let resp = client.cluster_status(ClusterStatusRequest {}).await?.into_inner();
            println!("collections: {:?}", resp.collections);
            println!("vector_count: {}", resp.vector_count);
        }
    }

    Ok(())
}

fn load_jsonl(path: &PathBuf) -> Result<Vec<VectorRecord>> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut records = Vec::new();
    for (lineno, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let rec: JsonlRecord = serde_json::from_str(&line)
            .with_context(|| format!("{}:{}: invalid JSON record", path.display(), lineno + 1))?;
        records.push(VectorRecord {
            id: rec.id,
            embedding: rec.embedding,
            metadata: rec.metadata,
        });
    }
    Ok(records)
}

/// Shells out to the offline embedding script so `vecctl search --query "text"`
/// embeds with the same model used to build the corpus (scripts/embed_corpus.py).
/// Prefers the project venv (`.venv/bin/python3`), which is where
/// sentence-transformers is installed, falling back to whatever `python3`
/// is on PATH.
fn embed_query_text(text: &str) -> Result<Vec<f32>> {
    let venv_python = PathBuf::from(".venv/bin/python3");
    let python = if venv_python.exists() {
        venv_python.to_string_lossy().into_owned()
    } else {
        "python3".to_string()
    };

    let output = Command::new(&python)
        .args(["scripts/embed_corpus.py", "--embed-query", text])
        .output()
        .with_context(|| format!("running scripts/embed_corpus.py via {python}"))?;

    if !output.status.success() {
        bail!(
            "embed_corpus.py --embed-query failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let vector: Vec<f32> = serde_json::from_slice(&output.stdout)
        .context("parsing embed_corpus.py --embed-query output as a JSON float array")?;
    Ok(vector)
}
