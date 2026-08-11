use crate::record::{ScoredId, VectorRecord};
use crate::similarity::{cosine_from_norm_sq, norm_sq};
use crate::topk::TopK;
use rayon::prelude::*;

/// Rayon-parallel exact brute-force cosine scan. Each entry pairs a record
/// with its precomputed squared norm (cached at insert — a vector's norm
/// never changes). Each worker folds its slice into a bounded k-heap and
/// the heaps are merged, so peak extra memory is O(threads * k), not O(n).
///
/// This is the one scan implementation shared by raftvec-node and the
/// correctness oracle's target path, so the oracle equality test exercises
/// the store/apply path rather than two independently-written rankers.
pub fn brute_force_top_k(
    records: &[(&VectorRecord, f32)],
    query: &[f32],
    k: usize,
) -> Vec<ScoredId> {
    let query_norm_sq = norm_sq(query);
    records
        .par_iter()
        .fold(
            || TopK::new(k),
            |mut topk, (r, r_norm_sq)| {
                topk.push(ScoredId {
                    id: r.id,
                    score: cosine_from_norm_sq(&r.embedding, query, *r_norm_sq, query_norm_sq),
                });
                topk
            },
        )
        .reduce(|| TopK::new(k), TopK::merge)
        .into_sorted_vec()
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
        let refs: Vec<(&VectorRecord, f32)> =
            records.iter().map(|r| (r, norm_sq(&r.embedding))).collect();
        let top = brute_force_top_k(&refs, &[1.0, 0.0], 2);
        assert_eq!(top[0].id, 1);
        assert!((top[0].score - 1.0).abs() < 1e-6);
    }

    #[test]
    fn cached_norm_scan_is_bit_identical_to_direct_cosine() {
        use crate::similarity::cosine_similarity;
        let records: Vec<VectorRecord> = (0..100)
            .map(|i| rec(i, (0..64).map(|j| ((i * 64 + j) as f32).sin()).collect()))
            .collect();
        let query: Vec<f32> = (0..64).map(|j| (j as f32).cos()).collect();
        let refs: Vec<(&VectorRecord, f32)> =
            records.iter().map(|r| (r, norm_sq(&r.embedding))).collect();

        for scored in brute_force_top_k(&refs, &query, 100) {
            let direct = cosine_similarity(&records[scored.id as usize].embedding, &query);
            // Exact bit equality, not tolerance: the cached-norm path must
            // not change results at all.
            assert_eq!(scored.score.to_bits(), direct.to_bits());
        }
    }
}
