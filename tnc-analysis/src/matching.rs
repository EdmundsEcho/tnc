//! Test/control matching — categorical gates + volume gates + scoring.

use std::collections::{HashMap, HashSet};

use polars::prelude::*;

use crate::config::{Config, MatchingSpec};

/// Include a subject in matching only if their NPI is in the provided universe set.
fn is_in_universe(npi: &str, universe: &HashSet<String>) -> bool {
    universe.contains(npi)
}

#[derive(Debug)]
pub struct MatchResult {
    /// (test_npi, control_npi) pairs
    pub pairs: Vec<(String, String)>,
    /// Per-pair test-side brx_F12M decile (1..10) — parallel to `pairs`.
    pub pair_test_deciles: Vec<i64>,
    /// Per-test brx_F12M decile (0..10) for all attempted tests (matched or not).
    /// Index 0 = non-writers (D0); indices 1..10 are the equal-volume deciles.
    pub test_attempts_by_decile: [usize; 11],
    /// Per-test matched count indexed by brx_F12M decile (0..10).
    pub test_matches_by_decile: [usize; 11],
    /// Size of the control pool
    pub control_pool_size: usize,
    /// Sum of micro-pool sizes (controls-per-test, summed)
    pub total_micropool_size: usize,
    /// Number of tests that found no matches
    pub unmatched_tests: usize,
}

/// Run the full matching pipeline.
///
/// Inputs:
///   * `subjects` — contains subject + qualities + campaign_reach_date
///   * `windows` — wide DataFrame with columns like `brx_L12M`, `hrx_PRE03M`, etc.
///
/// Output: 1:1 test → control pairs (the "control group"), plus diagnostics.
/// Test = any eligible subject with a campaign_reach_date; control = any
/// eligible subject without one.
pub fn run_matching(
    cfg: &Config,
    subjects: &DataFrame,
    windows: &DataFrame,
    universe: &HashSet<String>,
) -> Result<MatchResult, PolarsError> {
    let subject_col = &cfg.schema.subject;

    // 1. Filter to eligible subjects AND restrict to the universe.
    let joined = subjects
        .clone()
        .lazy()
        .join(
            windows.clone().lazy(),
            [col(subject_col.as_str())],
            [col(subject_col.as_str())],
            JoinArgs::new(JoinType::Inner),
        )
        .filter(
            col("brx_L12M")
                .lt_eq(lit(cfg.analysis.eligibility.brx_l12m_max))
                .and(col("hrx_L12M").gt_eq(lit(cfg.analysis.eligibility.hrx_l12m_min))),
        )
        .collect()?;

    // Apply the universe filter (materialized as a boolean mask on the NPI col).
    let npi_series = joined.column(subject_col.as_str())?.str()?.clone();
    let mask_vec: Vec<bool> = (0..joined.height())
        .map(|i| {
            npi_series
                .get(i)
                .map(|n| is_in_universe(n, universe))
                .unwrap_or(false)
        })
        .collect();
    let mask = BooleanChunked::new(PlSmallStr::from("_uni"), mask_vec);
    let eligible = joined.filter(&mask)?;

    // 2. Tests = reached; controls = not-reached.
    let tests = eligible
        .clone()
        .lazy()
        .filter(col("campaign_reach_date").is_not_null())
        .collect()?;

    let controls = eligible
        .clone()
        .lazy()
        .filter(col("campaign_reach_date").is_null())
        .collect()?;

    if tests.height() == 0 {
        return Ok(MatchResult {
            pairs: vec![],
            pair_test_deciles: vec![],
            test_attempts_by_decile: [0; 11],
            test_matches_by_decile: [0; 11],
            control_pool_size: controls.height(),
            total_micropool_size: 0,
            unmatched_tests: 0,
        });
    }

    // 3. Build categorical key → list of control indices.
    //   Key = categorical_gates values + (optionally) propensity_decile.
    let cat_cols: Vec<&str> = cfg
        .analysis
        .matching
        .categorical_gates
        .iter()
        .map(|s| s.as_str())
        .collect();
    let use_prop_decile = cfg.analysis.matching.propensity_match;

    let mut controls_by_key: HashMap<String, Vec<usize>> = HashMap::new();
    for i in 0..controls.height() {
        let key = full_key(&controls, i, &cat_cols, use_prop_decile)?;
        controls_by_key.entry(key).or_default().push(i);
    }

    // 4. For each test, find micro-pool, then best-match control
    let mut pairs: Vec<(String, String)> = Vec::new();
    let mut pair_test_deciles: Vec<i64> = Vec::new();
    let mut test_attempts_by_decile = [0usize; 11];
    let mut test_matches_by_decile = [0usize; 11];
    let mut total_micropool = 0;
    let mut unmatched = 0;

    let test_npi = tests.column(subject_col.as_str())?.str()?.clone();
    let test_decile = tests.column("brx_L12M_decile")?.i64()?.clone();
    let ctrl_npi = controls.column(subject_col.as_str())?.str()?.clone();

    for t in 0..tests.height() {
        let t_dec = test_decile.get(t).unwrap_or(0);
        if (0..=10).contains(&t_dec) {
            test_attempts_by_decile[t_dec as usize] += 1;
        }

        let tkey = full_key(&tests, t, &cat_cols, use_prop_decile)?;
        let Some(candidates) = controls_by_key.get(&tkey) else {
            unmatched += 1;
            continue;
        };

        // Apply volume gates
        let filtered: Vec<usize> = candidates
            .iter()
            .copied()
            .filter(|&c| passes_volume_gates(cfg, &cfg.analysis.matching, &tests, t, &controls, c))
            .collect();

        total_micropool += filtered.len();

        if filtered.is_empty() {
            unmatched += 1;
            continue;
        }

        // Score each candidate: points per (rx_type, window) by minimum |difference|
        let best = best_match(&cfg.analysis.matching, &tests, t, &controls, &filtered)?;

        let t_npi = test_npi.get(t).unwrap_or_default().to_string();
        let c_npi = ctrl_npi.get(best).unwrap_or_default().to_string();
        pairs.push((t_npi, c_npi));
        pair_test_deciles.push(t_dec);
        if (0..=10).contains(&t_dec) {
            test_matches_by_decile[t_dec as usize] += 1;
        }
    }

    Ok(MatchResult {
        pairs,
        pair_test_deciles,
        test_attempts_by_decile,
        test_matches_by_decile,
        control_pool_size: controls.height(),
        total_micropool_size: total_micropool,
        unmatched_tests: unmatched,
    })
}

/// Key = categorical_gates values (+ propensity_decile if propensity_match).
fn full_key(
    df: &DataFrame,
    row: usize,
    cat_cols: &[&str],
    include_prop_decile: bool,
) -> Result<String, PolarsError> {
    let mut key = row_key(df, row, cat_cols)?;
    if include_prop_decile {
        let pd = df
            .column("propensity_decile")?
            .i64()?
            .get(row)
            .unwrap_or(0);
        key.push('|');
        key.push_str(&format!("pd{pd}"));
    }
    Ok(key)
}

fn row_key(df: &DataFrame, row: usize, cols: &[&str]) -> Result<String, PolarsError> {
    let mut parts: Vec<String> = Vec::with_capacity(cols.len());
    for c in cols {
        let s = df.column(c)?;
        let v = s.str()?.get(row).unwrap_or("").to_string();
        parts.push(v);
    }
    Ok(parts.join("|"))
}

fn get_f64(df: &DataFrame, row: usize, col: &str) -> f64 {
    df.column(col)
        .ok()
        .and_then(|c| c.f64().ok().and_then(|arr| arr.get(row)))
        .unwrap_or(0.0)
}

fn passes_volume_gates(
    cfg: &Config,
    spec: &MatchingSpec,
    tests: &DataFrame,
    t_idx: usize,
    controls: &DataFrame,
    c_idx: usize,
) -> bool {
    for gate in &spec.volume_gates {
        let col = format!(
            "{}_{}_decile",
            gate.source.measurement, gate.source.window,
        );
        let t_dec = get_i64(tests, t_idx, &col);
        let c_dec = get_i64(controls, c_idx, &col);
        // Compare *merged* bucket indices, not raw deciles, so decile_grouping
        // collapses D9+D10 (etc.) into a single comparable unit.
        let t_bucket = cfg.analysis.decile_grouping.bucket_of(
            &gate.source.measurement,
            &gate.source.window,
            t_dec,
        );
        let c_bucket = cfg.analysis.decile_grouping.bucket_of(
            &gate.source.measurement,
            &gate.source.window,
            c_dec,
        );
        if (t_bucket - c_bucket).abs() > gate.within_deciles {
            return false;
        }
    }
    true
}

fn get_i64(df: &DataFrame, row: usize, col: &str) -> i64 {
    df.column(col)
        .ok()
        .and_then(|c| c.i64().ok().and_then(|arr| arr.get(row)))
        .unwrap_or(0)
}

fn best_match(
    spec: &MatchingSpec,
    tests: &DataFrame,
    t_idx: usize,
    controls: &DataFrame,
    candidates: &[usize],
) -> Result<usize, PolarsError> {
    let mut points: HashMap<usize, i64> = candidates.iter().map(|&c| (c, 0)).collect();

    for rx in &spec.scoring.rx_types {
        for (window, pts) in &spec.scoring.window_points {
            let col_name = format!("{rx}_{window}");
            let tv = get_f64(tests, t_idx, &col_name);
            // Find candidate with minimum |difference|
            let mut best_diff = f64::INFINITY;
            let mut winners: Vec<usize> = Vec::new();
            for &c in candidates {
                let cv = get_f64(controls, c, &col_name);
                let d = (tv - cv).abs();
                if d < best_diff - 1e-9 {
                    best_diff = d;
                    winners.clear();
                    winners.push(c);
                } else if (d - best_diff).abs() < 1e-9 {
                    winners.push(c);
                }
            }
            for w in winners {
                *points.entry(w).or_insert(0) += *pts;
            }
        }
    }

    // Winner: max points, tiebreak by lowest index (deterministic)
    let mut best = candidates[0];
    let mut best_pts = *points.get(&best).unwrap_or(&0);
    for &c in candidates {
        let p = *points.get(&c).unwrap_or(&0);
        if p > best_pts || (p == best_pts && c < best) {
            best = c;
            best_pts = p;
        }
    }
    let _ = HashSet::<usize>::new();
    Ok(best)
}
