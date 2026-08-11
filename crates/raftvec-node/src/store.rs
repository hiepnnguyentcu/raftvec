use raftvec_core::{brute_force_top_k, norm_sq, shard_id, ScoredId, VectorRecord};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum StoreError {
    #[error("collection '{0}' already exists")]
    CollectionExists(String),
    #[error("collection '{0}' not found")]
    CollectionNotFound(String),
    #[error("embedding has dimension {actual}, expected {expected}")]
    DimensionMismatch { expected: usize, actual: usize },
}

/// A record plus its squared L2 norm, cached at insert so the scan pays
/// one accumulation per element (the dot product) instead of three.
struct StoredVector {
    record: VectorRecord,
    norm_sq: f32,
}

/// One collection's data. This same structure is the Raft state machine's
/// applied state: committed log entries mutate it via insert/delete.
struct Collection {
    dim: usize,
    shard_count: u32,
    vectors: RwLock<HashMap<u64, StoredVector>>,
}

pub struct Store {
    collections: RwLock<HashMap<String, Collection>>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            collections: RwLock::new(HashMap::new()),
        }
    }

    pub fn create_collection(
        &self,
        name: &str,
        dim: usize,
        shard_count: u32,
    ) -> Result<(), StoreError> {
        let mut collections = self.collections.write().unwrap();
        if collections.contains_key(name) {
            return Err(StoreError::CollectionExists(name.to_string()));
        }
        collections.insert(
            name.to_string(),
            Collection {
                dim,
                shard_count: shard_count.max(1),
                vectors: RwLock::new(HashMap::new()),
            },
        );
        Ok(())
    }

    pub fn insert(
        &self,
        collection: &str,
        records: Vec<VectorRecord>,
    ) -> Result<usize, StoreError> {
        let collections = self.collections.read().unwrap();
        let coll = collections
            .get(collection)
            .ok_or_else(|| StoreError::CollectionNotFound(collection.to_string()))?;

        for r in &records {
            if r.embedding.len() != coll.dim {
                return Err(StoreError::DimensionMismatch {
                    expected: coll.dim,
                    actual: r.embedding.len(),
                });
            }
        }

        let mut vectors = coll.vectors.write().unwrap();
        let count = records.len();
        for record in records {
            debug_assert!(shard_id(record.id, coll.shard_count) < coll.shard_count);
            let norm_sq = norm_sq(&record.embedding);
            vectors.insert(record.id, StoredVector { record, norm_sq });
        }
        Ok(count)
    }

    pub fn delete(&self, collection: &str, ids: &[u64]) -> Result<usize, StoreError> {
        let collections = self.collections.read().unwrap();
        let coll = collections
            .get(collection)
            .ok_or_else(|| StoreError::CollectionNotFound(collection.to_string()))?;

        let mut vectors = coll.vectors.write().unwrap();
        let mut deleted = 0;
        for id in ids {
            if vectors.remove(id).is_some() {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    pub fn search(
        &self,
        collection: &str,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<ScoredId>, StoreError> {
        let collections = self.collections.read().unwrap();
        let coll = collections
            .get(collection)
            .ok_or_else(|| StoreError::CollectionNotFound(collection.to_string()))?;

        if query.len() != coll.dim {
            return Err(StoreError::DimensionMismatch {
                expected: coll.dim,
                actual: query.len(),
            });
        }

        let vectors = coll.vectors.read().unwrap();
        let records: Vec<(&VectorRecord, f32)> =
            vectors.values().map(|s| (&s.record, s.norm_sq)).collect();

        Ok(brute_force_top_k(&records, query, k))
    }

    pub fn get_metadata(&self, collection: &str, id: u64) -> Option<HashMap<String, String>> {
        let collections = self.collections.read().unwrap();
        let coll = collections.get(collection)?;
        let vectors = coll.vectors.read().unwrap();
        vectors.get(&id).map(|s| s.record.metadata.clone())
    }

    pub fn cluster_status(&self) -> (Vec<String>, u32) {
        let collections = self.collections.read().unwrap();
        let names: Vec<String> = collections.keys().cloned().collect();
        let total: u32 = collections
            .values()
            .map(|c| c.vectors.read().unwrap().len() as u32)
            .sum();
        (names, total)
    }

    /// Full-store snapshot for Raft snapshot install: a lagging or new
    /// replica catches up from this instead of replaying the whole log.
    pub fn snapshot(&self) -> StoreSnapshot {
        let collections = self.collections.read().unwrap();
        let snapshot = collections
            .iter()
            .map(|(name, coll)| {
                (
                    name.clone(),
                    CollectionSnapshot {
                        dim: coll.dim,
                        shard_count: coll.shard_count,
                        vectors: coll
                            .vectors
                            .read()
                            .unwrap()
                            .values()
                            .map(|s| (s.record.id, s.record.clone()))
                            .collect(),
                    },
                )
            })
            .collect();
        StoreSnapshot {
            collections: snapshot,
        }
    }

    pub fn restore(&self, snapshot: StoreSnapshot) {
        let mut collections = self.collections.write().unwrap();
        collections.clear();
        for (name, snap) in snapshot.collections {
            // Norms are derived state: recomputed on restore rather than
            // shipped in the snapshot, keeping the wire format minimal.
            let vectors = snap
                .vectors
                .into_values()
                .map(|record| {
                    let norm_sq = norm_sq(&record.embedding);
                    (record.id, StoredVector { record, norm_sq })
                })
                .collect();
            collections.insert(
                name,
                Collection {
                    dim: snap.dim,
                    shard_count: snap.shard_count,
                    vectors: RwLock::new(vectors),
                },
            );
        }
    }
}

#[derive(Serialize, Deserialize)]
struct CollectionSnapshot {
    dim: usize,
    shard_count: u32,
    vectors: HashMap<u64, VectorRecord>,
}

#[derive(Serialize, Deserialize, Default)]
pub struct StoreSnapshot {
    collections: HashMap<String, CollectionSnapshot>,
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as Map;

    fn rec(id: u64, embedding: Vec<f32>) -> VectorRecord {
        VectorRecord::new(id, embedding, Map::new())
    }

    #[test]
    fn insert_then_search_finds_exact_match() {
        let store = Store::new();
        store.create_collection("docs", 2, 1).unwrap();
        store
            .insert("docs", vec![rec(1, vec![1.0, 0.0]), rec(2, vec![0.0, 1.0])])
            .unwrap();

        let results = store.search("docs", &[1.0, 0.0], 1).unwrap();
        assert_eq!(results[0].id, 1);
    }

    #[test]
    fn dimension_mismatch_rejected() {
        let store = Store::new();
        store.create_collection("docs", 3, 1).unwrap();
        let err = store
            .insert("docs", vec![rec(1, vec![1.0, 0.0])])
            .unwrap_err();
        assert_eq!(
            err,
            StoreError::DimensionMismatch {
                expected: 3,
                actual: 2
            }
        );
    }

    #[test]
    fn delete_removes_from_search_results() {
        let store = Store::new();
        store.create_collection("docs", 2, 1).unwrap();
        store.insert("docs", vec![rec(1, vec![1.0, 0.0])]).unwrap();
        assert_eq!(store.delete("docs", &[1]).unwrap(), 1);
        let results = store.search("docs", &[1.0, 0.0], 5).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn duplicate_collection_rejected() {
        let store = Store::new();
        store.create_collection("docs", 2, 1).unwrap();
        let err = store.create_collection("docs", 2, 1).unwrap_err();
        assert_eq!(err, StoreError::CollectionExists("docs".to_string()));
    }

    #[test]
    fn unknown_collection_errors_on_search() {
        let store = Store::new();
        let err = store.search("nope", &[1.0], 1).unwrap_err();
        assert_eq!(err, StoreError::CollectionNotFound("nope".to_string()));
    }

    #[test]
    fn snapshot_restore_round_trip_preserves_data() {
        let store = Store::new();
        store.create_collection("docs", 2, 3).unwrap();
        store
            .insert("docs", vec![rec(1, vec![1.0, 0.0]), rec(2, vec![0.0, 1.0])])
            .unwrap();

        let snapshot = store.snapshot();

        let restored = Store::new();
        restored.restore(snapshot);

        let results = restored.search("docs", &[1.0, 0.0], 2).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, 1);
        let (names, count) = restored.cluster_status();
        assert_eq!(names, vec!["docs".to_string()]);
        assert_eq!(count, 2);
    }
}
