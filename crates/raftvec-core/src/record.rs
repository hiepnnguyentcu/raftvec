use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct VectorRecord {
    pub id: u64,
    pub embedding: Vec<f32>,
    pub metadata: HashMap<String, String>,
}

impl VectorRecord {
    pub fn new(id: u64, embedding: Vec<f32>, metadata: HashMap<String, String>) -> Self {
        Self {
            id,
            embedding,
            metadata,
        }
    }
}

/// Ordered so that a higher score ranks "greater", and on a score tie the
/// lower id ranks "greater" — a total order needed for deterministic top-k
/// (score ties are not rare with synthetic/duplicate test vectors, and the
/// oracle-equality test requires both sides to break them the same way).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoredId {
    pub id: u64,
    pub score: f32,
}

impl Eq for ScoredId {}

impl Ord for ScoredId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.id.cmp(&self.id))
    }
}

impl PartialOrd for ScoredId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
