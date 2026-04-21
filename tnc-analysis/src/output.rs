//! Export intermediate + final data for the dashboard.

use std::{collections::HashMap, fs, path::Path};

use polars::prelude::*;
use serde::Serialize;

use crate::{ancova::AncovaResult, config::Config, matching::MatchResult, treatment::Treatment};

#[derive(Serialize)]
pub struct PipelineSummary {
    pub universe: usize,
    pub target_list: usize,
    pub reached: usize,
    pub eligible_controls: usize,
    pub matched_pairs: usize,
    pub unmatched_tests: usize,
    pub mean_micropool_size: f64,
    pub injected_lift_pct: f64,
    pub ramp_months: i64,
    /// Tests attempted per brx_F12M decile (index 0 = D1, index 9 = D10).
    pub test_attempts_by_decile: Vec<usize>,
    /// Tests successfully matched per brx_F12M decile.
    pub test_matches_by_decile: Vec<usize>,
    /// Four-stage subject count funnel for the dashboard waterfall chart.
    pub waterfall: WaterfallStages,
    /// Pre-period DiD placebo result per configured measurement — shown on
    /// the Matching view so users can see match quality at a glance.
    pub did_report: Vec<crate::validation::DidMeasurement>,
}

#[derive(Serialize)]
pub struct WaterfallStages {
    pub all_subjects: usize,
    pub universe: usize,
    pub eligible_for_matching: usize,
    pub matched: usize,
}

pub fn write_all(
    out_dir: &Path,
    cfg: &Config,
    subjects: &DataFrame,
    windows: &DataFrame,
    measurements: &HashMap<String, DataFrame>,
    treatment: &Treatment,
    match_result: &MatchResult,
    ancova_results: &[AncovaResult],
    universe_size: usize,
    etl_schema_path: &str,
    campaign_path: &str,
    analysis_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(out_dir)?;

    // Copy the three input configs into the output directory so the
    // dashboard can offer them as downloads alongside the data.
    fs::copy(etl_schema_path, out_dir.join("etl-schema.toml"))?;
    fs::copy(campaign_path, out_dir.join("campaign-cfg.toml"))?;
    fs::copy(analysis_path, out_dir.join("tnc-analysis-cfg.toml"))?;

    // 1. subjects.csv
    write_csv(&mut subjects.clone(), &out_dir.join("subjects.csv"))?;

    // 1b. target_list.csv — the campaign plan: one row per target-listed
    //     subject, with planned reach month (YYYY-MM). Mirrors a real-world
    //     input artifact. Reached is a subset: reached ⇔ campaign_reach_date
    //     non-null on subjects.csv.
    write_target_list(&out_dir.join("target_list.csv"), cfg, subjects, treatment)?;

    // 2. windows.csv
    write_csv(&mut windows.clone(), &out_dir.join("windows.csv"))?;

    // 3. matches.csv
    write_matches(&out_dir.join("matches.csv"), match_result)?;

    // 4. timeseries.csv — per-month × per-measurement × per-group (test/control/universe)
    let ts = build_timeseries(cfg, subjects, measurements, match_result)?;
    write_csv(&mut ts.clone(), &out_dir.join("timeseries.csv"))?;

    // 5. summary.json
    let n = subjects.height();
    let targets = treatment
        .planned_reach_date
        .iter()
        .filter(|f| f.is_some())
        .count();
    let reached = treatment.campaign_reach_date.iter().filter(|f| f.is_some()).count();
    let tests_seen = match_result.pairs.len() + match_result.unmatched_tests;
    let mean_mp = if tests_seen > 0 {
        match_result.total_micropool_size as f64 / tests_seen as f64
    } else {
        0.0
    };
    let tests_attempted = match_result.pairs.len() + match_result.unmatched_tests;
    let eligible_for_matching = match_result.control_pool_size + tests_attempted;
    let did_report =
        crate::validation::compute_did_report(cfg, subjects, windows, match_result)?;
    let summary = PipelineSummary {
        universe: n,
        target_list: targets,
        reached,
        eligible_controls: match_result.control_pool_size,
        matched_pairs: match_result.pairs.len(),
        unmatched_tests: match_result.unmatched_tests,
        mean_micropool_size: mean_mp,
        injected_lift_pct: cfg.lift.max_pct,
        ramp_months: cfg.lift.ramp_months,
        test_attempts_by_decile: match_result.test_attempts_by_decile.to_vec(),
        test_matches_by_decile: match_result.test_matches_by_decile.to_vec(),
        waterfall: WaterfallStages {
            all_subjects: n,
            universe: universe_size,
            eligible_for_matching,
            matched: 2 * match_result.pairs.len(),
        },
        did_report,
    };
    fs::write(
        out_dir.join("summary.json"),
        serde_json::to_string_pretty(&summary)?,
    )?;

    // 6. ancova.json
    let ancova_json: Vec<_> = ancova_results
        .iter()
        .map(|r| AncovaJson {
            outcome_name: r.outcome_name.clone(),
            n_rows: r.n_rows,
            beta_treatment: r.beta_treatment,
            se_treatment: r.se_treatment,
            t_stat: r.t_stat,
            test_mean: r.test_mean,
            control_mean: r.control_mean,
            test_sd: r.test_sd,
            control_sd: r.control_sd,
            lift_pct: r.lift_pct,
        })
        .collect();
    fs::write(
        out_dir.join("ancova.json"),
        serde_json::to_string_pretty(&ancova_json)?,
    )?;

    // 7. config.json — compact summary for the dashboard.
    //    All month fields are 1-based indices where month 1 == study_start_date.
    let rw_first = cfg.campaign.reach_window_first_month();
    let rw_last = cfg.campaign.reach_window_last_month();
    let earliest_reach_month = treatment
        .campaign_reach_date
        .iter()
        .filter_map(|m| *m)
        .min();
    let cfg_json = serde_json::json!({
        "injected_lift_pct": cfg.lift.max_pct,
        "ramp_months": cfg.lift.ramp_months,
        "study_start_date": cfg.campaign.study_start_date,
        "total_span_months": cfg.campaign.total_span_months,
        "min_pre_months": cfg.campaign.min_pre_months,
        "max_post_months": cfg.campaign.max_post_months,
        "test_period_months": cfg.campaign.test_period_months,
        "reach_window": {
            "start_date": cfg.campaign.reach_window.start_date,
            "duration_months": cfg.campaign.reach_window.duration_months,
            "first_month": rw_first,
            "last_month": rw_last,
        },
        "earliest_reach_month": earliest_reach_month,
        "subject_count": cfg.gen.subject_count,
        "time_range": {
            "first_month": cfg.gen.time_range.first_month,
            "last_month": cfg.gen.time_range.last_month,
        },
        "outcomes": cfg.analysis.ancova.outcomes,
    });
    fs::write(
        out_dir.join("config.json"),
        serde_json::to_string_pretty(&cfg_json)?,
    )?;

    // 8. cfg-parsed.json — the full, parsed contents of all three TOML files
    //    (schema, gen, campaign, lift, analysis) as a single JSON blob.
    //    The dashboard uses this to render structured configuration viewers
    //    without needing a client-side TOML parser.
    let cfg_parsed = serde_json::json!({
        "etl_schema": {
            "schema": &cfg.schema,
            "gen":    &cfg.gen,
        },
        "campaign": {
            "campaign": &cfg.campaign,
            "lift":     &cfg.lift,
        },
        "tnc_analysis": &cfg.analysis,
    });
    fs::write(
        out_dir.join("cfg-parsed.json"),
        serde_json::to_string_pretty(&cfg_parsed)?,
    )?;

    Ok(())
}

#[derive(Serialize)]
struct AncovaJson {
    outcome_name: String,
    n_rows: usize,
    beta_treatment: f64,
    se_treatment: f64,
    t_stat: f64,
    test_mean: f64,
    control_mean: f64,
    test_sd: f64,
    control_sd: f64,
    lift_pct: f64,
}

fn write_csv(df: &mut DataFrame, path: &Path) -> Result<(), PolarsError> {
    let mut file = std::fs::File::create(path).map_err(PolarsError::from)?;
    CsvWriter::new(&mut file).finish(df)?;
    Ok(())
}

fn write_matches(path: &Path, match_result: &MatchResult) -> std::io::Result<()> {
    let mut out = String::new();
    out.push_str("test_npi,control_npi\n");
    for (t, c) in &match_result.pairs {
        out.push_str(&format!("{t},{c}\n"));
    }
    fs::write(path, out)
}

fn write_target_list(
    path: &Path,
    cfg: &Config,
    subjects: &DataFrame,
    treatment: &Treatment,
) -> Result<(), Box<dyn std::error::Error>> {
    let subject_col = &cfg.schema.subject;
    let npis = subjects.column(subject_col.as_str())?.str()?.clone();
    let specs = subjects.column("specialty").ok().and_then(|c| c.str().ok().cloned());
    let states = subjects.column("state").ok().and_then(|c| c.str().ok().cloned());

    let mut out = String::from("npi,specialty,state,planned_reach_date\n");
    for i in 0..subjects.height() {
        if let Some(m) = treatment.planned_reach_date[i] {
            let npi = npis.get(i).unwrap_or("");
            let specialty = specs.as_ref().and_then(|s| s.get(i)).unwrap_or("");
            let state = states.as_ref().and_then(|s| s.get(i)).unwrap_or("");
            let date = crate::config::index_to_ym(&cfg.campaign.study_start_date, m);
            out.push_str(&format!("{npi},{specialty},{state},{date}\n"));
        }
    }
    fs::write(path, out)?;
    Ok(())
}

/// Build a per-month × per-measurement × per-group average time series.
///
/// Group assignments:
///   "test"      — subjects appearing as test_npi in any matched pair
///   "control"   — subjects appearing as control_npi in any matched pair
///   "universe"  — all subjects (for reference)
fn build_timeseries(
    cfg: &Config,
    subjects: &DataFrame,
    measurements: &HashMap<String, DataFrame>,
    match_result: &MatchResult,
) -> Result<DataFrame, PolarsError> {
    let subject_col = &cfg.schema.subject;
    let time_col = &cfg.schema.time;

    let test_ids: std::collections::HashSet<String> =
        match_result.pairs.iter().map(|(t, _)| t.clone()).collect();
    let control_ids: std::collections::HashSet<String> =
        match_result.pairs.iter().map(|(_, c)| c.clone()).collect();

    let npis: Vec<String> = subjects
        .column(subject_col.as_str())?
        .str()?
        .into_iter()
        .map(|s| s.unwrap_or("").to_string())
        .collect();
    let groups: Vec<String> = npis
        .iter()
        .map(|n| {
            if test_ids.contains(n) {
                "test".to_string()
            } else if control_ids.contains(n) {
                "control".to_string()
            } else {
                "other".to_string()
            }
        })
        .collect();
    let group_df = DataFrame::new(vec![
        Series::new(PlSmallStr::from(subject_col.as_str()), &npis).into_column(),
        Series::new(PlSmallStr::from("group"), &groups).into_column(),
    ])?;

    let mut rows_meas: Vec<String> = Vec::new();
    let mut rows_group: Vec<String> = Vec::new();
    let mut rows_month: Vec<i64> = Vec::new();
    let mut rows_mean: Vec<f64> = Vec::new();
    let mut rows_sd: Vec<f64> = Vec::new();
    let mut rows_n: Vec<i64> = Vec::new();

    for (mname, mdf) in measurements {
        let collapsed = mdf
            .clone()
            .lazy()
            .group_by([col(subject_col.as_str()), col(time_col.as_str())])
            .agg([col("value").sum()])
            .collect()?;

        let joined = collapsed
            .lazy()
            .join(
                group_df.clone().lazy(),
                [col(subject_col.as_str())],
                [col(subject_col.as_str())],
                JoinArgs::new(JoinType::Left),
            )
            .collect()?;

        for group_label in ["test", "control", "universe"] {
            let filtered = if group_label == "universe" {
                joined.clone()
            } else {
                joined
                    .clone()
                    .lazy()
                    .filter(col("group").eq(lit(group_label)))
                    .collect()?
            };
            if filtered.height() == 0 {
                continue;
            }
            let agg = filtered
                .lazy()
                .group_by([col(time_col.as_str())])
                .agg([
                    col("value").mean().alias("mean_value"),
                    col("value").std(1).alias("sd_value"),
                    col("value").count().alias("n_value"),
                ])
                .sort([time_col.as_str()], Default::default())
                .collect()?;

            let months = agg.column(time_col.as_str())?.i64()?;
            let means = agg.column("mean_value")?.f64()?;
            let sds = agg.column("sd_value")?.f64()?;
            let ns = agg.column("n_value")?.u32()?;

            for i in 0..agg.height() {
                rows_meas.push(mname.clone());
                rows_group.push(group_label.to_string());
                rows_month.push(months.get(i).unwrap_or(0));
                rows_mean.push(means.get(i).unwrap_or(0.0));
                // polars returns NaN for SD when n < 2 — emit 0.0 in that case.
                let sd = sds.get(i).unwrap_or(0.0);
                rows_sd.push(if sd.is_finite() { sd } else { 0.0 });
                rows_n.push(ns.get(i).unwrap_or(0) as i64);
            }
        }
    }

    Ok(DataFrame::new(vec![
        Series::new(PlSmallStr::from("measurement"), &rows_meas).into_column(),
        Series::new(PlSmallStr::from("group"), &rows_group).into_column(),
        Series::new(PlSmallStr::from("month"), &rows_month).into_column(),
        Series::new(PlSmallStr::from("mean_value"), &rows_mean).into_column(),
        Series::new(PlSmallStr::from("sd_value"), &rows_sd).into_column(),
        Series::new(PlSmallStr::from("n_value"), &rows_n).into_column(),
    ])?)
}
