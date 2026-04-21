//! ANCOVA via ordinary least squares.
//!
//! Model: Y = β₀ + β_treatment · T + β_baseline · B + β_qual · OneHot(Q...) + β_window · W... + ε
//!
//! Reports the treatment coefficient (lift estimate) per outcome variable.

use std::collections::HashMap;

use nalgebra::{DMatrix, DVector};
use polars::prelude::*;

use crate::config::Config;
use crate::matching::MatchResult;

#[derive(Debug)]
pub struct AncovaResult {
    pub outcome_name: String,
    pub n_rows: usize,
    pub n_features: usize,
    /// Treatment coefficient (the lift estimate in the units of the outcome).
    pub beta_treatment: f64,
    /// Std. error of the treatment coefficient
    pub se_treatment: f64,
    /// t-statistic and p-value approximation (Gaussian tails as proxy)
    pub t_stat: f64,
    /// Adjusted control mean of the outcome (LS mean proxy)
    pub control_mean: f64,
    /// Adjusted test mean of the outcome
    pub test_mean: f64,
    /// Standard deviation of the outcome within the test group.
    pub test_sd: f64,
    /// Standard deviation of the outcome within the control group.
    pub control_sd: f64,
    /// Lift percent: (test - control) / control * 100
    pub lift_pct: f64,
}

/// Run ANCOVA on each outcome in the config.
pub fn run_ancova(
    cfg: &Config,
    subjects: &DataFrame,
    windows: &DataFrame,
    match_result: &MatchResult,
) -> Result<Vec<AncovaResult>, PolarsError> {
    let subject_col = &cfg.schema.subject;

    // Join subjects + windows into one wide frame
    let wide = subjects
        .clone()
        .lazy()
        .join(
            windows.clone().lazy(),
            [col(subject_col.as_str())],
            [col(subject_col.as_str())],
            JoinArgs::new(JoinType::Inner),
        )
        .collect()?;

    // Build subject → row index
    let npi_series = wide.column(subject_col.as_str())?.str()?.clone();
    let mut idx_by_npi: HashMap<String, usize> = HashMap::new();
    for i in 0..wide.height() {
        if let Some(s) = npi_series.get(i) {
            idx_by_npi.insert(s.to_string(), i);
        }
    }

    // Collect matched-pair row indices (test, control) and assign treatment labels
    let mut matched_rows: Vec<(usize, i64)> = Vec::new();
    for (t, c) in &match_result.pairs {
        if let (Some(&ti), Some(&ci)) = (idx_by_npi.get(t), idx_by_npi.get(c)) {
            matched_rows.push((ti, 1));
            matched_rows.push((ci, 0));
        }
    }

    if matched_rows.is_empty() {
        return Ok(Vec::new());
    }

    // Prepare covariates columns (numeric baseline + windowed covariates)
    let baseline_col = format!(
        "{}_{}",
        cfg.analysis.ancova.baseline.measurement, cfg.analysis.ancova.baseline.window
    );
    let mut numeric_cols: Vec<String> = Vec::new();
    numeric_cols.push(baseline_col.clone());
    for cw in &cfg.analysis.ancova.covariate_windows {
        numeric_cols.push(format!("{}_{}", cw.measurement, cw.window));
    }

    // Gather categorical covariate values (for one-hot encoding)
    let cat_cols = &cfg.analysis.ancova.covariate_qualities;
    let mut cat_values: HashMap<String, Vec<String>> = HashMap::new();
    for c in cat_cols {
        let arr = wide.column(c.as_str())?.str()?;
        let v: Vec<String> = arr
            .into_iter()
            .map(|s| s.unwrap_or("").to_string())
            .collect();
        cat_values.insert(c.clone(), v);
    }

    // Determine one-hot categories for each categorical col (drop-one encoding)
    let cat_levels: HashMap<String, Vec<String>> = cat_cols
        .iter()
        .map(|c| {
            let all = cat_values.get(c).unwrap();
            let mut uniq: Vec<String> =
                all.iter().cloned().collect::<std::collections::BTreeSet<_>>().into_iter().collect();
            // drop the first level as baseline
            if !uniq.is_empty() {
                uniq.remove(0);
            }
            (c.clone(), uniq)
        })
        .collect();

    let n = matched_rows.len();

    let mut results = Vec::new();
    for outcome in &cfg.analysis.ancova.outcomes {
        let outcome_name = format!("{}_{}", outcome.measurement, outcome.window);
        let y_vec: Vec<f64> = matched_rows
            .iter()
            .map(|(idx, _)| get_f64(&wide, *idx, &outcome_name))
            .collect();

        // Build X matrix
        //   [1 (intercept), treatment, baseline, covariate_windows..., one_hot(qualities)...]
        let mut x_cols: Vec<Vec<f64>> = Vec::new();
        x_cols.push(vec![1.0; n]); // intercept
        x_cols.push(matched_rows.iter().map(|(_, t)| *t as f64).collect()); // treatment
        for nc in &numeric_cols {
            x_cols.push(
                matched_rows
                    .iter()
                    .map(|(i, _)| get_f64(&wide, *i, nc))
                    .collect(),
            );
        }
        // One-hot
        for cat in cat_cols {
            let levels = cat_levels.get(cat).unwrap();
            let row_vals = cat_values.get(cat).unwrap();
            for lvl in levels {
                let col_vec: Vec<f64> = matched_rows
                    .iter()
                    .map(|(i, _)| if row_vals[*i] == *lvl { 1.0 } else { 0.0 })
                    .collect();
                x_cols.push(col_vec);
            }
        }

        let p = x_cols.len();
        let mut x = DMatrix::<f64>::zeros(n, p);
        for j in 0..p {
            for i in 0..n {
                x[(i, j)] = x_cols[j][i];
            }
        }
        let y = DVector::<f64>::from_vec(y_vec.clone());

        // OLS: β = (XᵀX)⁻¹ Xᵀy
        let xt = x.transpose();
        let xtx = &xt * &x;
        let Some(xtx_inv) = xtx.clone().try_inverse() else {
            continue;
        };
        let beta = &xtx_inv * &xt * &y;

        let beta_t = beta[1];

        // Residuals and SE
        let y_hat = &x * &beta;
        let residuals = &y - &y_hat;
        let rss = residuals.iter().map(|r| r * r).sum::<f64>();
        let dof = (n - p).max(1) as f64;
        let sigma2 = rss / dof;
        let var_beta_t = sigma2 * xtx_inv[(1, 1)];
        let se = var_beta_t.max(0.0).sqrt();
        let t_stat = if se > 1e-12 { beta_t / se } else { 0.0 };

        // Simple LS means: use the mean covariate values for each group,
        // then predict using beta. For MVP, compute unadjusted group means.
        let (mut control_sum, mut control_n) = (0.0, 0);
        let (mut test_sum, mut test_n) = (0.0, 0);
        for i in 0..n {
            let t_val = matched_rows[i].1;
            let yv = y[i];
            if t_val == 1 {
                test_sum += yv;
                test_n += 1;
            } else {
                control_sum += yv;
                control_n += 1;
            }
        }
        let control_mean = if control_n > 0 { control_sum / control_n as f64 } else { 0.0 };
        let test_mean = if test_n > 0 { test_sum / test_n as f64 } else { 0.0 };

        // Within-group SDs (sample std dev, ddof = 1).
        let (mut control_ss, mut test_ss) = (0.0, 0.0);
        for i in 0..n {
            let t_val = matched_rows[i].1;
            let yv = y[i];
            if t_val == 1 {
                test_ss += (yv - test_mean).powi(2);
            } else {
                control_ss += (yv - control_mean).powi(2);
            }
        }
        let test_sd = if test_n > 1 { (test_ss / (test_n - 1) as f64).sqrt() } else { 0.0 };
        let control_sd = if control_n > 1 { (control_ss / (control_n - 1) as f64).sqrt() } else { 0.0 };

        let lift_pct = if control_mean > 0.0 {
            (test_mean - control_mean) / control_mean * 100.0
        } else {
            0.0
        };

        results.push(AncovaResult {
            outcome_name,
            n_rows: n,
            n_features: p,
            beta_treatment: beta_t,
            se_treatment: se,
            t_stat,
            control_mean,
            test_mean,
            test_sd,
            control_sd,
            lift_pct,
        });
    }

    Ok(results)
}

fn get_f64(df: &DataFrame, row: usize, col: &str) -> f64 {
    df.column(col)
        .ok()
        .and_then(|c| c.f64().ok().and_then(|arr| arr.get(row)))
        .unwrap_or(0.0)
}
