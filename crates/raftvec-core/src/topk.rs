use crate::record::ScoredId;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

/// Bounded min-heap top-k: keeps only the k best-scoring items seen so far,
/// evicting the current worst whenever a better one arrives. O(n log k)
/// instead of sorting all n candidates.
pub fn bounded_top_k<I: IntoIterator<Item = ScoredId>>(items: I, k: usize) -> Vec<ScoredId> {
    if k == 0 {
        return Vec::new();
    }

    let mut heap: BinaryHeap<Reverse<ScoredId>> = BinaryHeap::with_capacity(k);
    for item in items {
        if heap.len() < k {
            heap.push(Reverse(item));
        } else if let Some(Reverse(worst)) = heap.peek() {
            if item > *worst {
                heap.pop();
                heap.push(Reverse(item));
            }
        }
    }

    let mut result: Vec<ScoredId> = heap.into_iter().map(|Reverse(x)| x).collect();
    result.sort_by(|a, b| b.cmp(a)); // best first
    result
}

/// Merges several already-sorted (best-first) local top-k lists into one
/// global top-k. Exact, not approximate: since each input list is a shard's
/// true local top-k over a disjoint partition of the corpus, the global
/// top-k must be a subset of their union (see technical design §5.2).
pub fn merge_top_k(lists: impl IntoIterator<Item = Vec<ScoredId>>, k: usize) -> Vec<ScoredId> {
    let merged = lists.into_iter().flatten();
    bounded_top_k(merged, k)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sid(id: u64, score: f32) -> ScoredId {
        ScoredId { id, score }
    }

    #[test]
    fn returns_k_best_sorted_descending() {
        let items = vec![sid(1, 0.1), sid(2, 0.9), sid(3, 0.5), sid(4, 0.7)];
        let top2 = bounded_top_k(items, 2);
        assert_eq!(top2, vec![sid(2, 0.9), sid(4, 0.7)]);
    }

    #[test]
    fn k_larger_than_input_returns_all() {
        let items = vec![sid(1, 0.1), sid(2, 0.9)];
        let top = bounded_top_k(items, 10);
        assert_eq!(top.len(), 2);
    }

    #[test]
    fn k_zero_returns_empty() {
        let items = vec![sid(1, 0.1)];
        assert!(bounded_top_k(items, 0).is_empty());
    }

    #[test]
    fn ties_break_by_lower_id_first() {
        let items = vec![sid(5, 0.5), sid(2, 0.5), sid(8, 0.5)];
        let top = bounded_top_k(items, 3);
        assert_eq!(top, vec![sid(2, 0.5), sid(5, 0.5), sid(8, 0.5)]);
    }

    #[test]
    fn merge_of_disjoint_shard_results_matches_single_scan() {
        let shard_a = vec![sid(1, 0.9), sid(2, 0.3)];
        let shard_b = vec![sid(3, 0.8), sid(4, 0.1)];
        let merged = merge_top_k(vec![shard_a, shard_b], 2);
        assert_eq!(merged, vec![sid(1, 0.9), sid(3, 0.8)]);
    }
}
