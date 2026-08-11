use crate::record::ScoredId;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

/// A bounded min-heap holding the k best-scoring items seen so far. The
/// heap root is the current worst of the k, so a new item either beats it
/// (evict + push) or is discarded in O(1).
pub struct TopK {
    heap: BinaryHeap<Reverse<ScoredId>>,
    k: usize,
}

impl TopK {
    pub fn new(k: usize) -> Self {
        Self {
            heap: BinaryHeap::with_capacity(k),
            k,
        }
    }

    #[inline]
    pub fn push(&mut self, item: ScoredId) {
        if self.k == 0 {
            return;
        }
        if self.heap.len() < self.k {
            self.heap.push(Reverse(item));
        } else if let Some(Reverse(worst)) = self.heap.peek() {
            if item > *worst {
                self.heap.pop();
                self.heap.push(Reverse(item));
            }
        }
    }

    /// Merges another TopK into this one.
    pub fn merge(mut self, other: TopK) -> Self {
        for Reverse(item) in other.heap {
            self.push(item);
        }
        self
    }

    /// Consumes the heap, returning the top-k sorted best-first.
    pub fn into_sorted_vec(self) -> Vec<ScoredId> {
        let mut result: Vec<ScoredId> = self.heap.into_iter().map(|Reverse(x)| x).collect();
        result.sort_by(|a, b| b.cmp(a));
        result
    }
}

/// Exact top-k over an iterator: O(n log k), no O(n) intermediate storage.
pub fn bounded_top_k<I: IntoIterator<Item = ScoredId>>(items: I, k: usize) -> Vec<ScoredId> {
    let mut topk = TopK::new(k);
    for item in items {
        topk.push(item);
    }
    topk.into_sorted_vec()
}

/// Merges per-shard local top-k lists into one global top-k. Exact, not
/// approximate: shards partition the corpus disjointly, so the global
/// top-k is necessarily a subset of the union of the local top-k sets.
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
