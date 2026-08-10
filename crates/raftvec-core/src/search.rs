use crate::record::{ScoredId, VectorRecord};
use crate::similarity::cosine_similarity;
use crate::topk::bounded_top_k;
use rayon::prelude::*;

/// Rayon-parallel brute-force cosine scan over `records`, returning the
/// exact top-k. This is the one scan implementation shared by raftvec-node
/// and the correctness oracle, so an equality test between them is testing
/// the store/apply path, not two independently-written ranking algorithms.
///
/// Takes references rather than owned records so a caller scanning its
/// whole in-memory store (500K+ records) doesn't have to deep-clone every
/// embedding and metadata map just to run one query.
pub fn brute_force_top_k(records: &[&VectorRecord], query: &[f32], k: usize) -> Vec<ScoredId> {
    let scored: Vec<ScoredId> = records
        .par_iter()
        .map(|r| ScoredId {
            id: r.id,
            score: cosine_similarity(&r.embedding, query),
        })
        .collect();
    bounded_top_k(scored, k)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn rec(id: u64, embedding: Vec<f32>) -> VectorRecord {
        VectorRecord::new(id, embedding, HashMap::new())
    }

    #[test]
    fn finds_exact_match_first() {
        let records = [
            rec(1, vec![1.0, 0.0]),
            rec(2, vec![0.0, 1.0]),
            rec(3, vec![0.9, 0.1]),
        ];
        let refs: Vec<&VectorRecord> = records.iter().collect();
        let top = brute_force_top_k(&refs, &[1.0, 0.0], 2);
        assert_eq!(top[0].id, 1);
        assert!((top[0].score - 1.0).abs() < 1e-6);
    }
}
