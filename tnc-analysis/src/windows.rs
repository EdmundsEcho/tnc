//! Windowed aggregate derivations — produces a wide DataFrame with one row per
//! subject and one column per (measurement, window) pair.
//!
//! Two families of windows are supported:
//!
//!   UNIVERSE-ANCHORED (`first_observed` / `last_observed`):
//!       Anchored on the study horizon — every subject has them regardless of
//!       reach status. Safe to use before matching.
//!
//!   PER-SUBJECT (`pre` / `test_period` / `post`):
//!       Anchored on each subject's `campaign_reach_date`. Controls have no
//!       real reach date, so for Phase-1 matching they use the earliest
//!       `campaign_reach_date` observed in the data as a virtual anchor.

use std::collections::HashMap;

use polars::prelude::*;

use crate::config::{Config, WindowAnchor, WindowDef};
use crate::treatment::Treatment;

/// Column name of the decile bucket for a given source column.
/// e.g., `decile_col("hrx_F12M")` → `"hrx_F12M_decile"`
pub fn decile_col(source: &str) -> String {
    format!("{source}_decile")
}

/// Equal-volume decile assignment.
///
///   D0       = subjects with value ≤ 0  (non-writers)
///   D1..D10  = subjects with value > 0, bucketed so each decile holds
///              ~1/10 of the TOTAL POSITIVE volume.
///
/// Consequence: D10 is a small group of high-volume prescribers (whales),
/// D1 is a long tail of low-volume writers. Subject count grows as you
/// move from D10 toward D1.
pub fn add_decile_column(
    df: DataFrame,
    source_col: &str,
    subject_col: &str,
) -> Result<DataFrame, PolarsError> {
    let n = df.height();
    if n == 0 {
        return Ok(df);
    }

    let npi = df.column(subject_col)?.str()?.clone();
    let val = df.column(source_col)?.f64()?.clone();

    let mut deciles = vec![0_i64; n];

    let mut positive: Vec<(usize, f64)> = (0..n)
        .filter_map(|i| {
            let v = val.get(i).unwrap_or(0.0);
            if v > 0.0 { Some((i, v)) } else { None }
        })
        .collect();

    if !positive.is_empty() {
        positive.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    let an = npi.get(a.0).unwrap_or("");
                    let bn = npi.get(b.0).unwrap_or("");
                    an.cmp(bn)
                })
        });

        let total: f64 = positive.iter().map(|(_, v)| *v).sum();
        let per_decile = total / 10.0;

        let mut cumsum = 0.0;
        for (orig_idx, v) in &positive {
            cumsum += *v;
            let d = (((cumsum - 1e-9) / per_decile).floor() as i64 + 1)
                .max(1)
                .min(10);
            deciles[*orig_idx] = d;
        }
    }

    let decile_name = decile_col(source_col);
    let col = Series::new(PlSmallStr::from(decile_name.as_str()), &deciles).into_column();
    let mut out = df;
    out.with_column(col)?;
    Ok(out)
}

/// Compute all derived windowed aggregates for every measurement.
/// Result is a DataFrame keyed by subject with columns like `brx_PRE12M`,
/// `hrx_F12M`, etc. — used by matching, eligibility, and ANCOVA.
pub fn compute_windows(
    cfg: &Config,
    subjects: &DataFrame,
    measurements: &HashMap<String, DataFrame>,
    treatment: &Treatment,
) -> Result<DataFrame, PolarsError> {
    let subject_col = cfg.schema.subject.as_str();
    let time_col = cfg.schema.time.as_str();
    let test_period = cfg.campaign.test_period_months;

    // Per-subject anchor month: reached subject → their reach date; else → earliest reach date.
    let earliest = treatment
        .earliest_reach_month()
        .unwrap_or_else(|| cfg.campaign.reach_window_first_month());
    let npis: Vec<String> = subjects
        .column(subject_col)?
        .str()?
        .into_iter()
        .map(|s| s.unwrap_or("").to_string())
        .collect();
    let anchors: Vec<i64> = treatment
        .campaign_reach_date
        .iter()
        .map(|v| v.unwrap_or(earliest))
        .collect();
    let anchor_df = DataFrame::new(vec![
        Series::new(PlSmallStr::from(subject_col), &npis).into_column(),
        Series::new(PlSmallStr::from("_anchor"), &anchors).into_column(),
    ])?;

    let mut result = subjects.select([subject_col])?;

    for (mname, mdf) in measurements {
        // Collapse component breakdown: sum value across components per (subject, time)
        let collapsed = mdf
            .clone()
            .lazy()
            .group_by([col(subject_col), col(time_col)])
            .agg([col("value").sum()])
            .collect()?;

        for (wname, wdef) in &cfg.analysis.derived_fields {
            let aggregated = match wdef.anchor {
                WindowAnchor::First | WindowAnchor::Last => {
                    let (start, end) = universe_range(wdef, cfg, earliest);
                    aggregate_universe(&collapsed, subject_col, time_col, start, end)?
                }
                WindowAnchor::Pre | WindowAnchor::TestPeriod | WindowAnchor::Post => {
                    let (offset_lo, offset_hi) = per_subject_offsets(wdef, test_period);
                    aggregate_per_subject(
                        &collapsed, &anchor_df, subject_col, time_col, offset_lo, offset_hi,
                    )?
                }
            };

            let new_col_name = format!("{mname}_{wname}");
            let aggregated = aggregated
                .lazy()
                .rename(["value"], [new_col_name.as_str()], true)
                .collect()?;

            result = result
                .lazy()
                .join(
                    aggregated.lazy(),
                    [col(subject_col)],
                    [col(subject_col)],
                    JoinArgs::new(JoinType::Left),
                )
                .with_column(col(new_col_name.as_str()).fill_null(lit(0.0)))
                .collect()?;
        }
    }

    // Attach the anchor itself as a convenience column for downstream diagnostics.
    let anchor_col = Series::new(PlSmallStr::from("reach_anchor_month"), &anchors).into_column();
    result.with_column(anchor_col)?;

    Ok(result)
}

/// Universe-anchored windows return an absolute [start, end] month range.
///
///   First: months from study_start, forward.       F12M = [1, 12].
///   Last:  months before the OBSERVED earliest campaign reach, back.
///          L12M = [earliest_reach - 12, earliest_reach - 1].
///
/// The Last anchor is computed from the data (subjects.campaign_reach_date),
/// not from the campaign config — the analysis must not share a source of
/// truth with the generator's planned reach schedule.
fn universe_range(def: &WindowDef, cfg: &Config, earliest_reach: i64) -> (i64, i64) {
    match def.anchor {
        WindowAnchor::First => {
            let first = cfg.campaign.study_first_month();
            (first, first + def.months - 1)
        }
        WindowAnchor::Last => {
            let end = earliest_reach - 1;
            let start = (end - def.months + 1).max(cfg.campaign.study_first_month());
            (start, end)
        }
        _ => unreachable!("universe_range called on non-universe anchor"),
    }
}

/// Per-subject windows return an (offset_lo, offset_hi) pair, where a month
/// `m` belongs to the window iff `offset_lo <= m − anchor <= offset_hi`.
fn per_subject_offsets(def: &WindowDef, test_period: i64) -> (i64, i64) {
    match def.anchor {
        WindowAnchor::Pre => (-def.months, -1),
        WindowAnchor::TestPeriod => (0, test_period - 1),
        WindowAnchor::Post => (test_period, test_period + def.months - 1),
        _ => unreachable!("per_subject_offsets called on universe anchor"),
    }
}

fn aggregate_universe(
    collapsed: &DataFrame,
    subject_col: &str,
    time_col: &str,
    start: i64,
    end: i64,
) -> Result<DataFrame, PolarsError> {
    collapsed
        .clone()
        .lazy()
        .filter(
            col(time_col)
                .gt_eq(lit(start))
                .and(col(time_col).lt_eq(lit(end))),
        )
        .group_by([col(subject_col)])
        .agg([col("value").sum().alias("value")])
        .collect()
}

fn aggregate_per_subject(
    collapsed: &DataFrame,
    anchor_df: &DataFrame,
    subject_col: &str,
    time_col: &str,
    offset_lo: i64,
    offset_hi: i64,
) -> Result<DataFrame, PolarsError> {
    collapsed
        .clone()
        .lazy()
        .join(
            anchor_df.clone().lazy(),
            [col(subject_col)],
            [col(subject_col)],
            JoinArgs::new(JoinType::Inner),
        )
        .with_column((col(time_col) - col("_anchor")).alias("_offset"))
        .filter(
            col("_offset")
                .gt_eq(lit(offset_lo))
                .and(col("_offset").lt_eq(lit(offset_hi))),
        )
        .group_by([col(subject_col)])
        .agg([col("value").sum().alias("value")])
        .collect()
}
