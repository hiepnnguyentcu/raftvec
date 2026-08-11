/// Cosine similarity between two equal-length vectors. Returns 0.0 for a
/// zero-magnitude vector rather than NaN, since "similarity to nothing" is
/// undefined and 0.0 keeps it out of any top-k without special-casing callers.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "cosine_similarity: dimension mismatch");
    cosine_from_norm_sq(a, b, norm_sq(a), norm_sq(b))
}

/// Sum of squares of `v` (the squared L2 norm). A vector's norm never
/// changes after insert, so the store computes this once per record and
/// the scan reuses it — cutting per-query work from 3 accumulations per
/// element to 1 (the dot product).
pub fn norm_sq(v: &[f32]) -> f32 {
    let mut acc = 0.0f32;
    for x in v {
        acc += x * x;
    }
    acc
}

/// Cosine similarity given precomputed squared norms.
///
/// Bit-identical to `cosine_similarity`: each accumulator (dot, |a|², |b|²)
/// sums the same terms in the same order whether the loops are fused or
/// separate, and the final expression is unchanged — which is what lets
/// the store cache norms without weakening the oracle's exact-equality
/// test. (Rust does not enable fast-math, so no reassociating
/// vectorization changes rounding behind our back.)
#[inline]
pub fn cosine_from_norm_sq(a: &[f32], b: &[f32], a_norm_sq: f32, b_norm_sq: f32) -> f32 {
    if a_norm_sq == 0.0 || b_norm_sq == 0.0 {
        return 0.0;
    }

    let mut dot = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
    }

    dot / (a_norm_sq.sqrt() * b_norm_sq.sqrt())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_vectors_score_one() {
        let v = vec![1.0, 2.0, 3.0];
        assert!((cosine_similarity(&v, &v) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn orthogonal_vectors_score_zero() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        assert!(cosine_similarity(&a, &b).abs() < 1e-6);
    }

    #[test]
    fn opposite_vectors_score_negative_one() {
        let a = vec![1.0, 0.0];
        let b = vec![-1.0, 0.0];
        assert!((cosine_similarity(&a, &b) - (-1.0)).abs() < 1e-6);
    }

    #[test]
    fn zero_vector_scores_zero() {
        let a = vec![0.0, 0.0];
        let b = vec![1.0, 1.0];
        assert_eq!(cosine_similarity(&a, &b), 0.0);
    }

    #[test]
    fn scale_invariant() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![2.0, 4.0, 6.0]; // same direction, different magnitude
        assert!((cosine_similarity(&a, &b) - 1.0).abs() < 1e-6);
    }
}
