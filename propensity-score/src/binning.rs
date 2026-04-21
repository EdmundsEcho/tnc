//! Binning strategies for propensity scores and other continuous values.
//!
//! A binning strategy assigns each value to a discrete group.
//!
//! Strategies:
//! - EqualRange: fixed-width bins across the value range
//! - Quantile: equal-count bins (each bin has ~same number of subjects)
//!
//! Note: Decile is quantile binning with count=10.

use nalgebra::DVector;

use crate::config::{BinConfig, BinStrategy};

/// Assign each value to a bin.
///
/// Returns 1-indexed bins. Values of exactly 0.0 get bin 0.
pub fn bin_scores(scores: &DVector<f64>, config: &BinConfig) -> Vec<u32> {
    match config.strategy {
        BinStrategy::EqualRange => bin_equal_range(scores, config.count),
        BinStrategy::Quantile => bin_quantile(scores, config.count),
    }
}

/// Equal-range binning: fixed-width bins across [0, 1].
///
/// Bin boundaries: [0, 1/n, 2/n, ..., 1]
/// Simple but can produce uneven bin sizes when values cluster.
fn bin_equal_range(scores: &DVector<f64>, count: u32) -> Vec<u32> {
    let step = 1.0 / count as f64;

    scores
        .iter()
        .map(|&s| {
            if s <= 0.0 {
                0
            } else if s >= 1.0 {
                count
            } else {
                let bin = (s / step).ceil() as u32;
                bin.clamp(1, count)
            }
        })
        .collect()
}

/// Quantile binning: each bin has approximately equal number of subjects.
///
/// Ranks all subjects by score, then divides into equal-count groups.
/// Subjects with score 0.0 are assigned bin 0 (excluded from ranking).
///
/// This is the recommended strategy for propensity score matching
/// because it produces balanced bins regardless of the score distribution.
/// No normalization of scores is needed — the ranking handles it.
fn bin_quantile(scores: &DVector<f64>, count: u32) -> Vec<u32> {
    let n = scores.len();
    if n == 0 {
        return vec![];
    }

    // Sort indices by score ascending
    let mut indexed: Vec<(usize, f64)> = scores
        .iter()
        .copied()
        .enumerate()
        .collect();
    indexed.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    let mut bins = vec![0u32; n];

    // Separate zeros from non-zeros
    let non_zero_start = indexed
        .iter()
        .position(|(_, v)| *v > 0.0)
        .unwrap_or(n);

    // Zeros get bin 0
    for i in 0..non_zero_start {
        bins[indexed[i].0] = 0;
    }

    // Divide non-zero subjects into equal-count bins by rank
    let non_zero_count = n - non_zero_start;
    if non_zero_count > 0 {
        let subjects_per_bin = non_zero_count as f64 / count as f64;

        for (rank, &(original_idx, _)) in indexed[non_zero_start..].iter().enumerate() {
            let bin = (rank as f64 / subjects_per_bin).floor() as u32 + 1;
            bins[original_idx] = bin.min(count);
        }
    }

    bins
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Equal-range tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_equal_range_basic() {
        let scores = DVector::from_vec(vec![0.0, 0.1, 0.3, 0.5, 0.7, 0.9, 1.0]);
        let config = BinConfig { count: 5, strategy: BinStrategy::EqualRange };
        let bins = bin_scores(&scores, &config);
        assert_eq!(bins[0], 0); // 0.0 → bin 0
        assert_eq!(bins[1], 1); // 0.1 → bin 1
        assert_eq!(bins[3], 3); // 0.5 → bin 3
        assert_eq!(bins[6], 5); // 1.0 → bin 5
    }

    // -----------------------------------------------------------------------
    // Quantile tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_quantile_equal_distribution() {
        // 10 subjects, 5 bins → 2 per bin
        let scores = DVector::from_vec(vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]);
        let config = BinConfig { count: 5, strategy: BinStrategy::Quantile };
        let bins = bin_scores(&scores, &config);

        let mut counts = vec![0u32; 6];
        for &b in &bins {
            counts[b as usize] += 1;
        }
        for bin in 1..=5u32 {
            assert_eq!(counts[bin as usize], 2, "bin {bin} should have 2 subjects");
        }
    }

    #[test]
    fn test_quantile_with_zeros() {
        // 3 zeros + 6 non-zeros, 3 bins → 2 non-zero per bin
        let scores = DVector::from_vec(vec![0.0, 0.0, 0.0, 0.1, 0.2, 0.5, 0.7, 0.8, 0.9]);
        let config = BinConfig { count: 3, strategy: BinStrategy::Quantile };
        let bins = bin_scores(&scores, &config);

        assert_eq!(bins[0], 0);
        assert_eq!(bins[1], 0);
        assert_eq!(bins[2], 0);

        let non_zero_bins: Vec<u32> = bins[3..].to_vec();
        let mut counts = vec![0u32; 4];
        for &b in &non_zero_bins {
            counts[b as usize] += 1;
        }
        assert_eq!(counts[1], 2);
        assert_eq!(counts[2], 2);
        assert_eq!(counts[3], 2);
    }

    #[test]
    fn test_quantile_clustered_scores() {
        // All scores in narrow range — equal-range would put them all in bin 1
        // Quantile spreads them evenly
        let scores = DVector::from_vec(vec![
            0.10, 0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19,
        ]);
        let config = BinConfig { count: 5, strategy: BinStrategy::Quantile };
        let bins = bin_scores(&scores, &config);

        let unique_bins: std::collections::HashSet<u32> = bins.iter().copied().collect();
        assert_eq!(unique_bins.len(), 5, "should use all 5 bins: {:?}", unique_bins);
    }

    #[test]
    fn test_quantile_as_decile() {
        // Quantile with count=10 is a decile
        let scores = DVector::from_vec((1..=100).map(|i| i as f64 / 100.0).collect());
        let config = BinConfig { count: 10, strategy: BinStrategy::Quantile };
        let bins = bin_scores(&scores, &config);

        let mut counts = vec![0u32; 11];
        for &b in &bins {
            counts[b as usize] += 1;
        }
        for bin in 1..=10u32 {
            assert_eq!(counts[bin as usize], 10, "decile bin {bin} should have 10 subjects");
        }
    }

    #[test]
    fn test_quantile_preserves_order() {
        // Higher score → higher bin
        let scores = DVector::from_vec(vec![0.9, 0.1, 0.5, 0.3, 0.7]);
        let config = BinConfig { count: 5, strategy: BinStrategy::Quantile };
        let bins = bin_scores(&scores, &config);

        assert!(bins[0] > bins[1], "0.9 should be in higher bin than 0.1");
        assert!(bins[2] > bins[3], "0.5 should be in higher bin than 0.3");
    }

    #[test]
    fn test_empty_scores() {
        let scores = DVector::from_vec(vec![]);
        let config = BinConfig { count: 5, strategy: BinStrategy::Quantile };
        let bins = bin_scores(&scores, &config);
        assert!(bins.is_empty());
    }
}
