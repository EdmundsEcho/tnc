//! Validation gates: pre-flight on inputs, post-match placebo test.

use std::collections::HashSet;

use polars::prelude::*;

use crate::config::Config;
use crate::matching::MatchResult;

#[derive(Debug)]
pub struct ValidationError(pub String);

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "validation failed: {}", self.0)
    }
}

impl std::error::Error for ValidationError {}

/// Pre-flight gate before matching. Enforces minimums on reached count,
/// eligible control pool, and test:control ratio in the universe.
pub fn validate_inputs(
    cfg: &Config,
    subjects: &DataFrame,
    universe: &HashSet<String>,
) -> Result<(), ValidationError> {
    let iv = &cfg.analysis.input_validation;
    let subject_col = cfg.schema.subject.as_str();

    let npi_ser = subjects
        .column(subject_col)
        .map_err(|e| ValidationError(e.to_string()))?
        .str()
        .map_err(|e| ValidationError(e.to_string()))?
        .clone();
    let reached_ser = subjects
        .column("reached")
        .map_err(|e| ValidationError(e.to_string()))?
        .i64()
        .map_err(|e| ValidationError(e.to_string()))?
        .clone();

    let mut reached_in_universe = 0usize;
    let mut control_in_universe = 0usize;
    for i in 0..subjects.height() {
        let Some(npi) = npi_ser.get(i) else { continue };
        if !universe.contains(npi) {
            continue;
        }
        if reached_ser.get(i).unwrap_or(0) == 1 {
            reached_in_universe += 1;
        } else {
            control_in_universe += 1;
        }
    }

    if reached_in_universe < iv.min_reached {
        return Err(ValidationError(format!(
            "reached-in-universe count {reached_in_universe} < min_reached {} — campaign didn't touch enough subjects",
            iv.min_reached
        )));
    }
    if control_in_universe < iv.min_eligible_control_pool {
        return Err(ValidationError(format!(
            "eligible control pool {control_in_universe} < min_eligible_control_pool {} — not enough controls in the universe",
            iv.min_eligible_control_pool
        )));
    }
    if iv.min_test_to_control_ratio > 0.0 && control_in_universe > 0 {
        let ratio = reached_in_universe as f64 / control_in_universe as f64;
        if ratio < iv.min_test_to_control_ratio {
            return Err(ValidationError(format!(
                "test:control ratio {ratio:.3} < min_test_to_control_ratio {:.3} \
                 ({reached_in_universe} reached vs. {control_in_universe} controls)",
                iv.min_test_to_control_ratio
            )));
        }
    }
    Ok(())
}

/// A single pre-period DiD measurement result — emitted to the dashboard
/// so users can see match-quality at a glance.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DidMeasurement {
    pub measurement: String,
    pub window: String,
    pub test_mean: f64,
    pub control_mean: f64,
    pub did: f64,
    pub max: f64,
    pub passed: bool,
}

/// Compute every configured pre-period DiD measurement without halting.
/// Returns all results (so the dashboard can render pass/fail per row).
pub fn compute_did_report(
    cfg: &Config,
    subjects: &DataFrame,
    windows: &DataFrame,
    matches: &MatchResult,
) -> Result<Vec<DidMeasurement>, PolarsError> {
    let subject_col = cfg.schema.subject.as_str();
    if cfg.analysis.match_validation.max_did_pre.is_empty() {
        return Ok(Vec::new());
    }

    let wide = subjects
        .clone()
        .lazy()
        .join(
            windows.clone().lazy(),
            [col(subject_col)],
            [col(subject_col)],
            JoinArgs::new(JoinType::Inner),
        )
        .collect()?;

    let npi_ser = wide.column(subject_col)?.str()?.clone();
    let mut idx: std::collections::HashMap<String, usize> =
        std::collections::HashMap::with_capacity(wide.height());
    for i in 0..wide.height() {
        if let Some(n) = npi_ser.get(i) {
            idx.insert(n.to_string(), i);
        }
    }

    let mut report = Vec::new();
    for tol in &cfg.analysis.match_validation.max_did_pre {
        let col_name = format!("{}_{}", tol.measurement, tol.window);
        let vals = wide.column(col_name.as_str())?.f64()?.clone();
        let (mut t_sum, mut t_n) = (0.0f64, 0usize);
        let (mut c_sum, mut c_n) = (0.0f64, 0usize);
        for (t_npi, c_npi) in &matches.pairs {
            if let Some(&ti) = idx.get(t_npi) {
                t_sum += vals.get(ti).unwrap_or(0.0);
                t_n += 1;
            }
            if let Some(&ci) = idx.get(c_npi) {
                c_sum += vals.get(ci).unwrap_or(0.0);
                c_n += 1;
            }
        }
        let t_mean = if t_n > 0 { t_sum / t_n as f64 } else { 0.0 };
        let c_mean = if c_n > 0 { c_sum / c_n as f64 } else { 0.0 };
        let did = (t_mean - c_mean).abs();
        report.push(DidMeasurement {
            measurement: tol.measurement.clone(),
            window: tol.window.clone(),
            test_mean: t_mean,
            control_mean: c_mean,
            did,
            max: tol.max,
            passed: did <= tol.max,
        });
    }
    Ok(report)
}

/// Post-match placebo test: for each configured (measurement, window) in the
/// PRE period, compare the group-mean delta between matched tests and matched
/// controls. If the pre-period DiD exceeds the per-measurement tolerance,
/// matching is presumed broken (tests and controls diverge before the
/// campaign even starts).
pub fn validate_match(
    cfg: &Config,
    subjects: &DataFrame,
    windows: &DataFrame,
    matches: &MatchResult,
) -> Result<(), ValidationError> {
    if cfg.analysis.match_validation.max_did_pre.is_empty() {
        return Ok(());
    }
    let subject_col = cfg.schema.subject.as_str();

    let wide = subjects
        .clone()
        .lazy()
        .join(
            windows.clone().lazy(),
            [col(subject_col)],
            [col(subject_col)],
            JoinArgs::new(JoinType::Inner),
        )
        .collect()
        .map_err(|e| ValidationError(e.to_string()))?;

    let npi_ser = wide
        .column(subject_col)
        .map_err(|e| ValidationError(e.to_string()))?
        .str()
        .map_err(|e| ValidationError(e.to_string()))?
        .clone();

    let mut idx: std::collections::HashMap<String, usize> =
        std::collections::HashMap::with_capacity(wide.height());
    for i in 0..wide.height() {
        if let Some(n) = npi_ser.get(i) {
            idx.insert(n.to_string(), i);
        }
    }

    for tol in &cfg.analysis.match_validation.max_did_pre {
        let col_name = format!("{}_{}", tol.measurement, tol.window);
        let vals = wide
            .column(col_name.as_str())
            .map_err(|e| ValidationError(e.to_string()))?
            .f64()
            .map_err(|e| ValidationError(e.to_string()))?
            .clone();

        let (mut t_sum, mut t_n) = (0.0, 0usize);
        let (mut c_sum, mut c_n) = (0.0, 0usize);
        for (t_npi, c_npi) in &matches.pairs {
            if let Some(&ti) = idx.get(t_npi) {
                t_sum += vals.get(ti).unwrap_or(0.0);
                t_n += 1;
            }
            if let Some(&ci) = idx.get(c_npi) {
                c_sum += vals.get(ci).unwrap_or(0.0);
                c_n += 1;
            }
        }
        if t_n == 0 || c_n == 0 {
            continue;
        }
        let t_mean = t_sum / t_n as f64;
        let c_mean = c_sum / c_n as f64;
        let did = (t_mean - c_mean).abs();
        if did > tol.max {
            return Err(ValidationError(format!(
                "pre-period DiD on {col_name} = {did:.3} (|test {t_mean:.3} − control {c_mean:.3}|) exceeds max {:.3} — matching is broken",
                tol.max
            )));
        }
    }
    Ok(())
}
