//! Configuration types for propensity score estimation.

/// Configuration for the logistic regression fitting.
#[derive(Debug, Clone)]
pub struct Config {
    /// Maximum iterations for gradient descent
    pub max_iterations: u64,
    /// Convergence tolerance (cost delta between iterations)
    pub tolerance: f64,
    /// Learning rate for gradient descent
    pub learning_rate: f64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            max_iterations: 200,
            tolerance: 1e-6,
            learning_rate: 0.1,
        }
    }
}

/// Configuration for binning propensity scores.
#[derive(Debug, Clone)]
pub struct BinConfig {
    /// Number of bins
    pub count: u32,
    /// Binning strategy
    pub strategy: BinStrategy,
}

impl Default for BinConfig {
    fn default() -> Self {
        BinConfig {
            count: 5,
            strategy: BinStrategy::Quantile,
        }
    }
}

/// How to compute bin boundaries from propensity scores.
#[derive(Debug, Clone)]
pub enum BinStrategy {
    /// Equal-width bins: [0.0-0.2, 0.2-0.4, ...] for 5 bins
    EqualRange,
    /// Equal-count bins: each bin has ~same number of subjects
    Quantile,
}
