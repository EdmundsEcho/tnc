//! Logistic regression via gradient descent.
//!
//! Fits a binary logistic regression model to estimate propensity scores.
//! Uses iterative reweighted least squares (gradient descent with adaptive
//! learning rate) — simple, dependency-free, sufficient for propensity scoring.

use nalgebra::{DMatrix, DVector};

use crate::binning;
use crate::config::{BinConfig, Config};

/// Error type for propensity score estimation.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Optimization failed: {0}")]
    Optimization(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),
}

/// Results of fitting the logistic regression model.
#[derive(Debug, Clone)]
pub struct Findings {
    /// Feature weights (excluding intercept)
    pub weights: DVector<f64>,
    /// Intercept (bias term)
    pub intercept: f64,
    /// Number of iterations used
    pub iterations: u64,
    /// Final cost value
    pub cost: f64,
}

impl Findings {
    /// Predict propensity scores for new data.
    ///
    /// Returns a vector of probabilities in [0, 1].
    /// Features should NOT include a bias column.
    pub fn predict(&self, features: &DMatrix<f64>) -> DVector<f64> {
        let n = features.nrows();
        let mut scores = DVector::zeros(n);

        for i in 0..n {
            let z: f64 = row_dot(features, i, &self.weights) + self.intercept;
            scores[i] = sigmoid(z);
        }

        scores
    }

    /// Bin the propensity scores using the given configuration.
    pub fn bin(&self, scores: &DVector<f64>, config: &BinConfig) -> Vec<u32> {
        binning::bin_scores(scores, config)
    }

    /// Compute AUC (Area Under ROC Curve) for model evaluation.
    pub fn auc(&self, scores: &DVector<f64>, target: &DVector<f64>) -> f64 {
        compute_auc(scores, target)
    }
}

/// Fit a logistic regression model using gradient descent.
///
/// # Arguments
/// * `features` — n×p matrix of predictor values (no bias column)
/// * `target` — n×1 vector of binary outcomes (0.0 or 1.0)
/// * `config` — optimization configuration
///
/// # Returns
/// `Findings` with fitted weights, intercept, and diagnostics.
pub fn fit(
    features: &DMatrix<f64>,
    target: &DVector<f64>,
    config: &Config,
) -> Result<Findings, Error> {
    let n = features.nrows();
    let p = features.ncols();

    if n == 0 {
        return Err(Error::InvalidInput("No observations".to_string()));
    }
    if target.len() != n {
        return Err(Error::InvalidInput(format!(
            "Feature rows ({n}) != target length ({})",
            target.len()
        )));
    }

    tracing::info!(
        observations = n,
        features = p,
        "fitting logistic regression"
    );

    // Augment features with bias column
    let x = augment_with_bias(features);
    let dim = p + 1; // features + bias

    // Initialize weights to zero
    let mut w = DVector::zeros(dim);

    let learning_rate = config.learning_rate;
    let mut prev_cost = f64::INFINITY;

    for iter in 0..config.max_iterations {
        // Forward pass: compute predictions
        let mut predictions = DVector::zeros(n);
        for i in 0..n {
            let z: f64 = row_dot(&x, i, &w);
            predictions[i] = sigmoid(z);
        }

        // Cost: negative log-likelihood (averaged)
        let cost = neg_log_likelihood(&predictions, target, n);

        // Check convergence
        let cost_delta = (prev_cost - cost).abs();
        if cost_delta < config.tolerance && iter > 0 {
            tracing::info!(
                iterations = iter,
                cost = cost,
                "converged (cost delta < tolerance)"
            );
            return Ok(extract_findings(&w, p, iter, cost));
        }
        prev_cost = cost;

        // Gradient: (1/n) * X^T * (predictions - target)
        let errors = &predictions - target;
        let mut gradient = DVector::zeros(dim);
        for i in 0..n {
            let row = x.row(i);
            for j in 0..dim {
                gradient[j] += errors[i] * row[j];
            }
        }
        gradient /= n as f64;

        // Update weights
        w -= learning_rate * &gradient;

        if iter % 20 == 0 {
            tracing::debug!(
                iter = iter,
                cost = cost,
                gradient_norm = gradient.norm(),
                "gradient descent step"
            );
        }
    }

    // Did not converge within max_iterations — return best result
    let final_cost = neg_log_likelihood_from_weights(&x, target, &w, n);
    tracing::warn!(
        iterations = config.max_iterations,
        cost = final_cost,
        "did not converge within max iterations"
    );

    Ok(extract_findings(&w, p, config.max_iterations, final_cost))
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

/// Dot product of a matrix row with a column vector.
fn row_dot(mat: &DMatrix<f64>, row: usize, w: &DVector<f64>) -> f64 {
    let cols = mat.ncols();
    let mut sum = 0.0;
    for j in 0..cols {
        sum += mat[(row, j)] * w[j];
    }
    sum
}

/// Sigmoid activation: 1 / (1 + exp(-z))
/// Numerically stable version.
fn sigmoid(z: f64) -> f64 {
    if z >= 0.0 {
        1.0 / (1.0 + (-z).exp())
    } else {
        let ez = z.exp();
        ez / (1.0 + ez)
    }
}

/// Append a column of 1.0 (bias) to the feature matrix.
fn augment_with_bias(features: &DMatrix<f64>) -> DMatrix<f64> {
    let n = features.nrows();
    let p = features.ncols();
    let mut augmented = DMatrix::zeros(n, p + 1);

    for i in 0..n {
        for j in 0..p {
            augmented[(i, j)] = features[(i, j)];
        }
        augmented[(i, p)] = 1.0;
    }

    augmented
}

/// Negative log-likelihood from predictions.
fn neg_log_likelihood(predictions: &DVector<f64>, target: &DVector<f64>, n: usize) -> f64 {
    let mut cost = 0.0;
    for i in 0..n {
        let p = predictions[i].clamp(1e-15, 1.0 - 1e-15);
        let y = target[i];
        cost -= y * p.ln() + (1.0 - y) * (1.0 - p).ln();
    }
    cost / n as f64
}

/// Negative log-likelihood computed from weights (for final cost).
fn neg_log_likelihood_from_weights(
    x: &DMatrix<f64>,
    target: &DVector<f64>,
    w: &DVector<f64>,
    n: usize,
) -> f64 {
    let mut predictions = DVector::zeros(n);
    for i in 0..n {
        predictions[i] = sigmoid(row_dot(x, i, w));
    }
    neg_log_likelihood(&predictions, target, n)
}

/// Extract Findings from the weight vector.
fn extract_findings(w: &DVector<f64>, p: usize, iterations: u64, cost: f64) -> Findings {
    let weights = DVector::from_iterator(p, w.iter().take(p).copied());
    let intercept = w[p];

    Findings {
        weights,
        intercept,
        iterations,
        cost,
    }
}

/// Compute AUC (Area Under ROC Curve).
fn compute_auc(scores: &DVector<f64>, target: &DVector<f64>) -> f64 {
    let n = scores.len();
    if n == 0 {
        return 0.0;
    }

    let mut indexed: Vec<(f64, f64)> = scores
        .iter()
        .zip(target.iter())
        .map(|(&s, &t)| (s, t))
        .collect();
    indexed.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

    let total_pos = target.iter().filter(|&&t| t > 0.5).count() as f64;
    let total_neg = n as f64 - total_pos;

    if total_pos == 0.0 || total_neg == 0.0 {
        return 0.5;
    }

    let mut tp = 0.0;
    let mut fp = 0.0;
    let mut auc = 0.0;
    let mut prev_fpr = 0.0;
    let mut prev_tpr = 0.0;

    for (_, label) in &indexed {
        if *label > 0.5 {
            tp += 1.0;
        } else {
            fp += 1.0;
        }

        let tpr = tp / total_pos;
        let fpr = fp / total_neg;

        auc += (fpr - prev_fpr) * (tpr + prev_tpr) / 2.0;

        prev_tpr = tpr;
        prev_fpr = fpr;
    }

    auc
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    #[test]
    fn test_sigmoid() {
        assert_relative_eq!(sigmoid(0.0), 0.5, epsilon = 1e-10);
        assert!(sigmoid(10.0) > 0.99);
        assert!(sigmoid(-10.0) < 0.01);
    }

    #[test]
    fn test_fit_separable_data() {
        let features = DMatrix::from_row_slice(6, 1, &[
            3.0, 2.0, 1.0,  // class 1
            -1.0, -2.0, -3.0, // class 0
        ]);
        let target = DVector::from_vec(vec![1.0, 1.0, 1.0, 0.0, 0.0, 0.0]);

        let config = Config::default();
        let findings = fit(&features, &target, &config).unwrap();

        assert!(findings.weights[0] > 0.0, "weight should be positive");

        let scores = findings.predict(&features);
        assert!(scores[0] > 0.5, "class 1 should have score > 0.5");
        assert!(scores[3] < 0.5, "class 0 should have score < 0.5");

        let auc = findings.auc(&scores, &target);
        assert!(auc > 0.9, "AUC should be high for separable data: {auc}");
    }

    #[test]
    fn test_fit_two_features() {
        let features = DMatrix::from_row_slice(4, 2, &[
            1.0, 1.0,
            1.0, -1.0,
            -1.0, 1.0,
            -1.0, -1.0,
        ]);
        let target = DVector::from_vec(vec![1.0, 1.0, 0.0, 0.0]);

        let config = Config::default();
        let findings = fit(&features, &target, &config).unwrap();
        assert_eq!(findings.weights.len(), 2);

        let scores = findings.predict(&features);
        assert_eq!(scores.len(), 4);
    }

    #[test]
    fn test_predict_and_bin() {
        let features = DMatrix::from_row_slice(6, 1, &[
            3.0, 2.0, 1.0, -1.0, -2.0, -3.0,
        ]);
        let target = DVector::from_vec(vec![1.0, 1.0, 1.0, 0.0, 0.0, 0.0]);

        let config = Config::default();
        let findings = fit(&features, &target, &config).unwrap();

        let scores = findings.predict(&features);
        let bin_config = BinConfig::default();
        let bins = findings.bin(&scores, &bin_config);

        assert!(bins[0] >= bins[3], "class 1 should be in higher bin");
        assert_eq!(bins.len(), 6);
    }

    #[test]
    fn test_auc_perfect() {
        let scores = DVector::from_vec(vec![0.9, 0.8, 0.2, 0.1]);
        let target = DVector::from_vec(vec![1.0, 1.0, 0.0, 0.0]);
        let auc = compute_auc(&scores, &target);
        assert_relative_eq!(auc, 1.0, epsilon = 1e-10);
    }

    #[test]
    fn test_empty_input() {
        let features = DMatrix::zeros(0, 2);
        let target = DVector::zeros(0);
        let result = fit(&features, &target, &Config::default());
        assert!(result.is_err());
    }

    #[test]
    fn test_mismatched_dimensions() {
        let features = DMatrix::from_row_slice(3, 1, &[1.0, 2.0, 3.0]);
        let target = DVector::from_vec(vec![1.0, 0.0]); // wrong length
        let result = fit(&features, &target, &Config::default());
        assert!(result.is_err());
    }
}
