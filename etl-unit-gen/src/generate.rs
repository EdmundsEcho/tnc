//! Universe generation — takes Schema + GenSpec, produces DataFrames.

use std::collections::HashMap;

use polars::prelude::*;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution as _, Normal};

use crate::{
    distribution::Distribution,
    schema::Schema,
    spec::GenSpec,
    GenError, Result,
};

fn s(name: &str) -> PlSmallStr {
    PlSmallStr::from(name)
}

/// The generated data: one subjects DataFrame + one DataFrame per measurement.
#[derive(Debug)]
pub struct GeneratedData {
    pub subjects: DataFrame,
    pub measurements: HashMap<String, DataFrame>,
    /// Optional per-subject volume scale (useful for downstream TnC logic)
    pub volume_scales: Vec<f64>,
}

pub fn generate(schema: &Schema, spec: &GenSpec) -> Result<GeneratedData> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(spec.seed);

    // ---- 1) Draw per-subject scale factors ----
    let n = spec.subject_count;
    let volume_scales: Vec<f64> = (0..n).map(|_| spec.subject_volume_scale.sample(&mut rng)).collect();

    // ---- 2) Build subjects DataFrame: one row per subject with qualities ----
    let ids: Vec<String> = (0..n).map(|i| format!("NPI-{:07}", 1_000_000 + i)).collect();
    let mut subject_columns: Vec<Column> = vec![Series::new(s(&schema.subject), &ids).into_column()];

    // Cache quality values so "derived_from" mappings can reference them.
    let mut quality_values: HashMap<String, Vec<String>> = HashMap::new();

    for q in &schema.qualities {
        let gen = spec
            .qualities
            .get(&q.name)
            .ok_or_else(|| GenError::Config(format!("Missing gen config for quality '{}'", q.name)))?;
        let vals: Vec<String> = if let Some(src) = &gen.derived_from {
            let parent = quality_values
                .get(src)
                .ok_or_else(|| GenError::Config(format!(
                    "Quality '{}' derives from '{}' which isn't generated yet", q.name, src
                )))?;
            parent
                .iter()
                .map(|v| gen.mapping.get(v).cloned().unwrap_or_else(|| v.clone()))
                .collect()
        } else if let Some(dist) = &gen.distribution {
            (0..n).map(|_| dist.sample_string(&mut rng)).collect()
        } else {
            return Err(GenError::Config(format!(
                "Quality '{}' has neither distribution nor derived_from", q.name
            )));
        };
        quality_values.insert(q.name.clone(), vals.clone());
        subject_columns.push(Series::new(s(&q.name), &vals).into_column());
    }

    let subjects = DataFrame::new(subject_columns)?;

    // ---- 3) Generate each measurement's long-form DataFrame ----
    let mut measurements = HashMap::new();
    let noise_dist = Normal::new(0.0, spec.noise.cv.max(1e-12)).unwrap();
    let first_month = spec.time_range.first_month;
    let last_month = spec.time_range.last_month;

    for m in &schema.measurements {
        let mgen = spec
            .measurements
            .get(&m.name)
            .ok_or_else(|| GenError::Config(format!("Missing gen config for measurement '{}'", m.name)))?;

        // Pre-sample component distributions once; we'll draw per observation.
        let component_dists: Vec<&Distribution> = m
            .components
            .iter()
            .map(|c| {
                mgen.components.get(c).map(|cg| &cg.distribution).ok_or_else(|| {
                    GenError::Config(format!(
                        "Missing component '{}' gen for measurement '{}'",
                        c, m.name
                    ))
                })
            })
            .collect::<Result<_>>()?;

        let mut subject_col: Vec<String> = Vec::new();
        let mut time_col: Vec<i64> = Vec::new();
        let mut component_cols: Vec<Vec<String>> = (0..m.components.len()).map(|_| Vec::new()).collect();
        let mut value_col: Vec<f64> = Vec::new();

        for (s_idx, s_id) in ids.iter().enumerate() {
            let scale = volume_scales[s_idx];
            for mth in first_month..=last_month {
                let t_factor = mgen.temporal.factor(mth - first_month);
                // Draw the base value for this subject × month
                let base = mgen.distribution.sample_numeric(&mut rng, scale * t_factor);
                let noise_mul = if spec.noise.cv > 0.0 {
                    let nz: f64 = noise_dist.sample(&mut rng);
                    (1.0 + nz).max(0.0)
                } else {
                    1.0
                };
                let total = (base * noise_mul).max(0.0);

                if m.components.is_empty() {
                    subject_col.push(s_id.clone());
                    time_col.push(mth);
                    value_col.push(total);
                } else if total <= 0.0 {
                    // emit a single zero row for this subject-month
                    subject_col.push(s_id.clone());
                    time_col.push(mth);
                    for (i, c) in m.components.iter().enumerate() {
                        let _ = c;
                        component_cols[i].push(component_dists[i].sample_string(&mut rng));
                    }
                    value_col.push(0.0);
                } else {
                    // Split total across components via categorical weights.
                    // For MVP we only handle one component per measurement (the common case).
                    // Multiple components would require a cartesian multinomial — out of scope.
                    if m.components.len() == 1 {
                        let values_weights = categorical_weights(component_dists[0]);
                        let splits = multinomial_split(&mut rng, total.round() as u64, &values_weights);
                        for (value_label, n_obs) in values_weights.iter().map(|(v, _)| v).zip(splits.iter()) {
                            if *n_obs > 0 {
                                subject_col.push(s_id.clone());
                                time_col.push(mth);
                                component_cols[0].push(value_label.clone());
                                value_col.push(*n_obs as f64);
                            }
                        }
                    } else {
                        // Fallback: sample one component combo per observation
                        subject_col.push(s_id.clone());
                        time_col.push(mth);
                        for (i, c) in m.components.iter().enumerate() {
                            let _ = c;
                            component_cols[i].push(component_dists[i].sample_string(&mut rng));
                        }
                        value_col.push(total);
                    }
                }
            }
        }

        let mut cols: Vec<Column> = vec![
            Series::new(s(&schema.subject), &subject_col).into_column(),
            Series::new(s(&schema.time), &time_col).into_column(),
        ];
        for (i, c) in m.components.iter().enumerate() {
            cols.push(Series::new(s(c), &component_cols[i]).into_column());
        }
        cols.push(Series::new(s("value"), &value_col).into_column());
        measurements.insert(m.name.clone(), DataFrame::new(cols)?);
    }

    Ok(GeneratedData {
        subjects,
        measurements,
        volume_scales,
    })
}

/// Extract (value, weight) pairs from a Categorical distribution.
fn categorical_weights(dist: &Distribution) -> Vec<(String, f64)> {
    match dist {
        Distribution::Categorical(values) => values
            .iter()
            .map(|e| (e.value.clone(), e.weight))
            .collect(),
        Distribution::UniformChoice(values) => values
            .iter()
            .map(|v| (v.clone(), 1.0))
            .collect(),
        _ => Vec::new(),
    }
}

/// Sample a multinomial split of `total` into bins with the given weights.
fn multinomial_split<R: Rng>(rng: &mut R, total: u64, weights: &[(String, f64)]) -> Vec<u64> {
    let sum: f64 = weights.iter().map(|(_, w)| w).sum();
    let mut remaining = total;
    let mut result = vec![0u64; weights.len()];
    let mut remaining_weight = sum;
    for (i, (_, w)) in weights.iter().enumerate() {
        if i == weights.len() - 1 {
            result[i] = remaining;
            break;
        }
        let p = (w / remaining_weight).clamp(0.0, 1.0);
        // binomial sample
        let mut count = 0;
        for _ in 0..remaining {
            if rng.gen::<f64>() < p {
                count += 1;
            }
        }
        result[i] = count;
        remaining = remaining.saturating_sub(count);
        remaining_weight -= w;
    }
    result
}
