use raftvec_core::{cosine_similarity, VectorRecord};
use raftvec_node::store::Store;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;

fn random_vector(rng: &mut StdRng, dim: usize) -> Vec<f32> {
    (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect()
}

/// Independent reference implementation. Deliberately does NOT call
/// raftvec_core::brute_force_top_k or bounded_top_k — a naive sequential
/// sort-and-truncate, hand-written here, so the equality assertion below
/// tests the store's real path (parallel scan + bounded-heap top-k) against
/// ground truth rather than comparing the same code to itself.
fn oracle_search(records: &[(u64, Vec<f32>)], query: &[f32], k: usize) -> Vec<(u64, f32)> {
    let mut scored: Vec<(u64, f32)> = records
        .iter()
        .map(|(id, emb)| (*id, cosine_similarity(emb, query)))
        .collect();
    scored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    scored.truncate(k);
    scored
}

fn assert_matches_oracle(actual: &[raftvec_core::ScoredId], expected: &[(u64, f32)]) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "result count mismatch: got {}, expected {}",
        actual.len(),
        expected.len()
    );
    for (a, e) in actual.iter().zip(expected.iter()) {
        assert_eq!(a.id, e.0, "ranked id mismatch");
        assert!(
            (a.score - e.1).abs() < 1e-5,
            "score mismatch for id {}: got {}, expected {}",
            a.id,
            a.score,
            e.1
        );
    }
}

#[test]
fn store_search_matches_oracle_on_random_corpus() {
    let mut rng = StdRng::seed_from_u64(42);
    let dim = 32;
    let n = 5_000u64;
    let k = 10;

    let store = Store::new();
    store.create_collection("docs", dim, 1).unwrap();

    let mut reference: Vec<(u64, Vec<f32>)> = Vec::with_capacity(n as usize);
    let mut records = Vec::with_capacity(n as usize);
    for id in 0..n {
        let emb = random_vector(&mut rng, dim);
        reference.push((id, emb.clone()));
        records.push(VectorRecord::new(id, emb, HashMap::new()));
    }
    store.insert("docs", records).unwrap();

    for _ in 0..20 {
        let query = random_vector(&mut rng, dim);
        let expected = oracle_search(&reference, &query, k);
        let actual = store.search("docs", &query, k).unwrap();
        assert_matches_oracle(&actual, &expected);
    }
}

#[test]
fn store_search_matches_oracle_after_deletes() {
    let mut rng = StdRng::seed_from_u64(7);
    let dim = 16;
    let n = 2_000u64;
    let k = 5;

    let store = Store::new();
    store.create_collection("docs", dim, 1).unwrap();

    let mut reference: Vec<(u64, Vec<f32>)> = Vec::with_capacity(n as usize);
    let mut records = Vec::with_capacity(n as usize);
    for id in 0..n {
        let emb = random_vector(&mut rng, dim);
        reference.push((id, emb.clone()));
        records.push(VectorRecord::new(id, emb, HashMap::new()));
    }
    store.insert("docs", records).unwrap();

    // Delete every third id from both the store and the oracle's reference set.
    let deleted_ids: Vec<u64> = (0..n).step_by(3).collect();
    store.delete("docs", &deleted_ids).unwrap();
    reference.retain(|(id, _)| !deleted_ids.contains(id));

    for _ in 0..10 {
        let query = random_vector(&mut rng, dim);
        let expected = oracle_search(&reference, &query, k);
        let actual = store.search("docs", &query, k).unwrap();
        assert_matches_oracle(&actual, &expected);
    }
}

#[test]
fn store_search_matches_oracle_with_duplicate_scores() {
    // Identical embeddings force score ties, exercising the tie-break rule
    // (lower id ranks first) that both the store's heap and the oracle's
    // sort must agree on for exact equality to hold.
    let store = Store::new();
    store.create_collection("docs", 2, 1).unwrap();

    let records = vec![
        VectorRecord::new(3, vec![1.0, 0.0], HashMap::new()),
        VectorRecord::new(1, vec![1.0, 0.0], HashMap::new()),
        VectorRecord::new(2, vec![1.0, 0.0], HashMap::new()),
    ];
    let reference: Vec<(u64, Vec<f32>)> = records.iter().map(|r| (r.id, r.embedding.clone())).collect();
    store.insert("docs", records).unwrap();

    let query = vec![1.0, 0.0];
    let expected = oracle_search(&reference, &query, 3);
    let actual = store.search("docs", &query, 3).unwrap();
    assert_matches_oracle(&actual, &expected);
    assert_eq!(actual.iter().map(|s| s.id).collect::<Vec<_>>(), vec![1, 2, 3]);
}
