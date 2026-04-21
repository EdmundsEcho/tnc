//! Fit a logistic regression predicting "reached" from qualities.
//!
//! Produces a per-subject propensity score (probability of being reached
//! given the subject's qualities + baseline volumes). This is a diagnostic
//! that helps validate that the target list is consistent with observable
//! characteristics — and can be used later to propensity-match controls.

use std::collections::BTreeMap;

use nalgebra::{DMatrix, DVector};
use polars::prelude::*;

#[derive(Debug)]
pub struct PropensityOutput {
    pub scores: Vec<f64>,
    pub auc: f64,
    pub converged_iterations: u64,
    pub final_cost: f64,
    /// Names of the features the model trained on (for interpretation).
    pub feature_names: Vec<String>,
}

/// Fit propensity model.
///
/// Inputs (parallel, 1 row per subject):
///   * `subjects`       — DataFrame with the subject's qualities
///   * `windows`        — DataFrame with baseline windowed features (merged on subject)
///   * `subject_col`    — name of subject id column (for join key)
///   * `reached`        — 0/1 per subject (the TARGET for regression)
///   * `categorical`    — names of categorical qualities to one-hot encode
///   * `numeric`        — names of numeric baseline covariates to include as-is
///
/// Returns a propensity score per subject, in the same order as the subjects df.
pub fn fit_propensity(
    subjects: &DataFrame,
    windows: &DataFrame,
    subject_col: &str,
    reached: &[i64],
    categorical: &[&str],
    numeric: &[&str],
) -> Result<PropensityOutput, Box<dyn std::error::Error>> {
    // Build a subject-keyed row index in the windows df
    let w_subj = windows.column(subject_col)?.str()?.clone();
    let mut window_row_by_subject: BTreeMap<String, usize> = BTreeMap::new();
    for i in 0..windows.height() {
        if let Some(s) = w_subj.get(i) {
            window_row_by_subject.insert(s.to_string(), i);
        }
    }

    let n = subjects.height();

    // Gather categorical levels (drop-one baseline per column).
    // Accepts either string or integer (0/1) columns — integers are coerced
    // to their decimal string form so flags like `called_on` can be used
    // as categorical predictors.
    let mut cat_levels: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for c in categorical {
        let values = column_as_strings(subjects, c)?;
        let mut uniq: Vec<String> = values
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        // drop the first level as baseline
        if !uniq.is_empty() {
            uniq.remove(0);
        }
        cat_levels.insert(c.to_string(), uniq);
    }

    // Feature column assembly
    let mut feature_names: Vec<String> = Vec::new();
    let mut feature_cols: Vec<Vec<f64>> = Vec::new();

    // Categorical one-hot columns
    for c in categorical {
        let values = column_as_strings(subjects, c)?;
        let levels = cat_levels.get(*c).unwrap();
        for lvl in levels {
            let mut col_vec = vec![0.0; n];
            for i in 0..n {
                if values[i] == *lvl {
                    col_vec[i] = 1.0;
                }
            }
            feature_names.push(format!("{c}={lvl}"));
            feature_cols.push(col_vec);
        }
    }

    // Numeric windowed covariates (pulled from windows df) — z-scored to
    // keep gradient descent well-conditioned relative to 0/1 one-hot features.
    for name in numeric {
        let mut col_vec = vec![0.0; n];
        let nc = windows.column(*name)?.f64()?;
        let s_arr = subjects.column(subject_col)?.str()?;
        for i in 0..n {
            let s_id = s_arr.get(i).unwrap_or("");
            if let Some(&wi) = window_row_by_subject.get(s_id) {
                col_vec[i] = nc.get(wi).unwrap_or(0.0);
            }
        }
        // z-score
        let mean: f64 = col_vec.iter().sum::<f64>() / (n.max(1) as f64);
        let var: f64 = col_vec.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n.max(1) as f64);
        let std = var.sqrt().max(1e-9);
        for v in col_vec.iter_mut() {
            *v = (*v - mean) / std;
        }
        feature_names.push(name.to_string());
        feature_cols.push(col_vec);
    }

    if feature_cols.is_empty() {
        return Err("no features provided to propensity model".into());
    }

    // Build DMatrix<f64> (n rows, p features) and DVector<f64> target
    let p = feature_cols.len();
    let mut x = DMatrix::<f64>::zeros(n, p);
    for j in 0..p {
        for i in 0..n {
            x[(i, j)] = feature_cols[j][i];
        }
    }
    let y = DVector::<f64>::from_iterator(n, reached.iter().map(|&r| r as f64));

    // Fit
    let cfg = propensity_score::Config::default();
    let findings = propensity_score::fit(&x, &y, &cfg)
        .map_err(|e| format!("propensity fit failed: {e}"))?;

    // Predict
    let scores_dv = findings.predict(&x);
    let scores: Vec<f64> = scores_dv.iter().copied().collect();
    let auc = findings.auc(&scores_dv, &y);

    Ok(PropensityOutput {
        scores,
        auc,
        converged_iterations: findings.iterations,
        final_cost: findings.cost,
        feature_names,
    })
}

/// Extract a subject-quality column as a Vec<String>. Accepts either a
/// string column (common case) or an integer flag column (e.g. 0/1).
fn column_as_strings(df: &DataFrame, name: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let col = df.column(name)?;
    if let Ok(s) = col.str() {
        return Ok(s.into_iter().map(|v| v.unwrap_or("").to_string()).collect());
    }
    if let Ok(i) = col.i64() {
        return Ok(i.into_iter().map(|v| match v {
            Some(n) => n.to_string(),
            None => String::new(),
        }).collect());
    }
    Err(format!(
        "propensity: column `{name}` must be string or i64, got {:?}",
        col.dtype()
    )
    .into())
}

/// Add `propensity_score` (f64) and `propensity_decile` (1..10) columns to
/// the subjects DataFrame, using scores already computed.
pub fn attach_propensity(
    subjects: DataFrame,
    scores: &[f64],
    bin_count: usize,
) -> Result<DataFrame, PolarsError> {
    let score_col = Series::new(PlSmallStr::from("propensity_score"), scores).into_column();
    let mut out = subjects;
    out.with_column(score_col)?;

    // Propensity bucket: split scores into `bin_count` equal-count buckets (1..bin_count).
    let bins = bin_count.max(1);
    let n = scores.len();
    let mut indexed: Vec<(usize, f64)> = scores.iter().copied().enumerate().collect();
    indexed.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    let mut dec = vec![0i64; n];
    for (rank, (i, _)) in indexed.iter().enumerate() {
        let d = ((rank as f64 / n as f64) * bins as f64).floor() as i64 + 1;
        dec[*i] = d.min(bins as i64).max(1);
    }
    let dec_col = Series::new(PlSmallStr::from("propensity_decile"), &dec).into_column();
    out.with_column(dec_col)?;
    Ok(out)
}
