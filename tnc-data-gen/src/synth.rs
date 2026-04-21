//! Synthetic-only pieces: sample a campaign plan + actual reach, inject lift
//! into the generated measurements, and augment the subjects frame with the
//! reach/called_on columns that a real-data input would already have.

use std::collections::HashMap;

use etl_unit_gen::GeneratedData;
use polars::prelude::*;
use rand::{Rng, SeedableRng};
use tnc_analysis::treatment::Treatment;
use tnc_analysis::Config;

/// Assign target-list membership (planned reach dates) and then the subset
/// that actually gets reached (campaign_reach_date).
///
/// Target-list selection is volume-weighted: reps focus on high-volume
/// prescribers, creating a correlation between observable qualities
/// (specialty, baseline Rx volumes) and target-list membership. Reach
/// execution (actual touch) happens to a random subset of the target list.
/// The propensity model learns the qualities-to-reach correlation.
pub fn inject_treatment(cfg: &Config, data: &GeneratedData) -> Treatment {
    let mut rng = rand::rngs::StdRng::seed_from_u64(cfg.gen.seed.wrapping_add(7777));
    let n = cfg.gen.subject_count;

    let mut planned_reach: Vec<Option<i64>> = vec![None; n];
    let mut actual_reach: Vec<Option<i64>> = vec![None; n];

    let target_count = (n as f64 * cfg.campaign.target_list_fraction) as usize;
    let reached_count =
        (target_count as f64 * cfg.campaign.reached_fraction_of_targets) as usize;

    let alpha = 1.5;
    let mut remaining: Vec<(usize, f64)> = (0..n)
        .map(|i| (i, data.volume_scales[i].powf(alpha)))
        .collect();
    let mut target_idxs: Vec<usize> = Vec::with_capacity(target_count);
    for _ in 0..target_count {
        if remaining.is_empty() {
            break;
        }
        let total_w: f64 = remaining.iter().map(|(_, w)| w).sum();
        let mut r = rng.gen::<f64>() * total_w;
        let mut pick = 0;
        for (j, (_, w)) in remaining.iter().enumerate() {
            r -= w;
            if r <= 0.0 {
                pick = j;
                break;
            }
        }
        target_idxs.push(remaining[pick].0);
        remaining.swap_remove(pick);
    }

    target_idxs.sort_by(|&a, &b| {
        data.volume_scales[b]
            .partial_cmp(&data.volume_scales[a])
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let rw_first = cfg.campaign.reach_window_first_month();
    let rw_last = cfg.campaign.reach_window_last_month();
    let rw_span = (rw_last - rw_first + 1).max(1);

    for (i, &idx) in target_idxs.iter().enumerate() {
        let offset = (i as i64) % rw_span;
        let m = rw_first + offset;
        planned_reach[idx] = Some(m);
        if i < reached_count {
            actual_reach[idx] = Some(m);
        }
    }

    let p_r = cfg.campaign.called_on.p_when_reached;
    let p_n = cfg.campaign.called_on.p_when_not_reached;
    let called_on: Vec<i64> = (0..n)
        .map(|i| {
            let p = if actual_reach[i].is_some() { p_r } else { p_n };
            if rng.gen::<f64>() < p { 1 } else { 0 }
        })
        .collect();

    Treatment {
        planned_reach_date: planned_reach,
        campaign_reach_date: actual_reach,
        called_on,
    }
}

/// Apply lift to the target measurement for reached subjects.
/// Modifies the measurement's `value` column (returns a new DataFrame).
pub fn apply_lift(
    data: GeneratedData,
    cfg: &Config,
    treatment: &Treatment,
) -> Result<GeneratedData, PolarsError> {
    let lift = &cfg.lift;
    let mut gd = data;

    if !gd.measurements.contains_key(&lift.applies_to) {
        return Ok(gd);
    }

    let subject_col_name = &cfg.schema.subject;
    let ids: Vec<String> = gd
        .subjects
        .column(subject_col_name)?
        .str()?
        .into_iter()
        .map(|s| s.unwrap_or_default().to_string())
        .collect();
    let mut reach_by_npi: HashMap<String, i64> = HashMap::new();
    for (i, id) in ids.iter().enumerate() {
        if let Some(m) = treatment.campaign_reach_date[i] {
            reach_by_npi.insert(id.clone(), m);
        }
    }

    let df = gd.measurements.remove(&lift.applies_to).unwrap();
    let npi_series = df.column(subject_col_name)?.str()?.clone();
    let month_series = df.column(&cfg.schema.time)?.i64()?.clone();
    let value_series = df.column("value")?.f64()?.clone();

    let max = lift.max_pct;
    let ramp = lift.ramp_months.max(1);

    let mut new_vals: Vec<f64> = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        let npi = npi_series.get(i).unwrap_or_default();
        let month = month_series.get(i).unwrap_or(0);
        let v = value_series.get(i).unwrap_or(0.0);

        let multiplier = if let Some(&reach) = reach_by_npi.get(npi) {
            let diff = month - reach;
            if diff < 0 {
                1.0
            } else if diff < ramp {
                let t = (diff as f64 + 1.0) / ramp as f64;
                1.0 + max * t
            } else {
                1.0 + max
            }
        } else {
            1.0
        };

        new_vals.push(v * multiplier);
    }

    let mut cols: Vec<Column> = Vec::new();
    for name in df.get_column_names() {
        if name.as_str() == "value" {
            cols.push(Series::new(PlSmallStr::from("value"), &new_vals).into_column());
        } else {
            cols.push(df.column(name.as_str())?.clone());
        }
    }
    let new_df = DataFrame::new(cols)?;
    gd.measurements.insert(lift.applies_to.clone(), new_df);

    Ok(gd)
}

/// Add campaign_reach_date + reached + called_on columns to the subjects frame.
/// In a real-data pipeline these columns already exist on the input; this is
/// only for the synthetic path.
pub fn augment_subjects(
    mut data: GeneratedData,
    treatment: &Treatment,
) -> Result<GeneratedData, PolarsError> {
    let reach_vec: Vec<Option<i64>> = treatment.campaign_reach_date.clone();
    let reach_col = Series::new(PlSmallStr::from("campaign_reach_date"), reach_vec).into_column();
    let reached_col =
        Series::new(PlSmallStr::from("reached"), &treatment.reached_flags()).into_column();
    let called_on_col =
        Series::new(PlSmallStr::from("called_on"), &treatment.called_on).into_column();
    data.subjects.with_column(reach_col)?;
    data.subjects.with_column(reached_col)?;
    data.subjects.with_column(called_on_col)?;
    Ok(data)
}
