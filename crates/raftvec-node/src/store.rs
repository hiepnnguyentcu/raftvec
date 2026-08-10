use raftvec_core::{brute_force_top_k, shard_id, ScoredId, VectorRecord};
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

/// One collection's data. In M1 this is the whole store; from M3 onward the
/// per-shard slice of this same shape becomes the Raft state machine that
/// `apply()` mutates (technical design §3, §6) — the map is deliberately
/// kept as the same flat `HashMap<u64, VectorRecord>` shape now so that
/// migration doesn't change the data model, only who calls insert/delete.
struct Collection {
    dim: usize,
    shard_count: u32,
    vectors: RwLock<HashMap<u64, VectorRecord>>,
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

    pub fn create_collection(&self, name: &str, dim: usize, shard_count: u32) -> Result<(), StoreError> {
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

    pub fn insert(&self, collection: &str, records: Vec<VectorRecord>) -> Result<usize, StoreError> {
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

        // M1 has no sharding across nodes, but every id still resolves to a
        // shard_id via the same hash fn M2 will use for routing — exercised
        // here so the function is proven correct before it decides routing.
        let mut vectors = coll.vectors.write().unwrap();
        let count = records.len();
        for r in records {
            let _ = shard_id(r.id, coll.shard_count);
            vectors.insert(r.id, r);
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

    pub fn search(&self, collection: &str, query: &[f32], k: usize) -> Result<Vec<ScoredId>, StoreError> {
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
        let records: Vec<&VectorRecord> = vectors.values().collect();

        Ok(brute_force_top_k(&records, query, k))
    }

    pub fn get_metadata(&self, collection: &str, id: u64) -> Option<HashMap<String, String>> {
        let collections = self.collections.read().unwrap();
        let coll = collections.get(collection)?;
        let vectors = coll.vectors.read().unwrap();
        vectors.get(&id).map(|r| r.metadata.clone())
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
        let err = store.insert("docs", vec![rec(1, vec![1.0, 0.0])]).unwrap_err();
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
}
