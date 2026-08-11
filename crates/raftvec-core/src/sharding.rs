use rustc_hash::FxHasher;
use std::hash::{Hash, Hasher};

/// shard_id = fxhash(vector_id) % shard_count, fixed at collection
/// creation. Static hashing suffices because shard count never changes at
/// runtime (dynamic resharding is a documented non-goal).
pub fn shard_id(vector_id: u64, shard_count: u32) -> u32 {
    assert!(shard_count > 0, "shard_count must be > 0");
    let mut hasher = FxHasher::default();
    vector_id.hash(&mut hasher);
    (hasher.finish() % shard_count as u64) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn deterministic_for_same_id() {
        assert_eq!(shard_id(42, 4), shard_id(42, 4));
    }

    #[test]
    fn always_in_range() {
        for id in 0..10_000u64 {
            assert!(shard_id(id, 7) < 7);
        }
    }

    #[test]
    #[should_panic]
    fn zero_shards_panics() {
        shard_id(1, 0);
    }

    #[test]
    fn roughly_balanced_across_shards() {
        let shard_count = 4u32;
        let n = 100_000u64;
        let mut counts: HashMap<u32, u64> = HashMap::new();
        for id in 0..n {
            *counts.entry(shard_id(id, shard_count)).or_insert(0) += 1;
        }
        let expected = n / shard_count as u64;
        for shard in 0..shard_count {
            let count = *counts.get(&shard).unwrap_or(&0);
            // hash distribution should be within 10% of the ideal split
            let deviation = (count as f64 - expected as f64).abs() / expected as f64;
            assert!(deviation < 0.10, "shard {shard} skewed: {count} vs expected {expected}");
        }
    }
}
