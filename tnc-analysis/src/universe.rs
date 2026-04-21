//! Universe definition — restrict analysis to subjects whose
//! `(qualities × merged-writing-profile-buckets)` signature appears among
//! the reached cohort. If no reached subject is a cardiac surgeon, the
//! cardiac-surgery signature simply isn't in the universe.

use std::collections::HashSet;

use polars::prelude::*;

use crate::config::{Config, DecileGrouping};

/// Return the set of NPIs in the universe, and a per-signature diagnostic
/// count. The signature is a `|`-joined string of quality values followed
/// by `b{decile}` tokens for each configured writing profile.
pub struct UniverseFilter {
    pub npis: HashSet<String>,
    pub signature_counts: Vec<(String, usize)>, // (signature, count) — reached cohort
}

pub fn build_universe(
    cfg: &Config,
    subjects: &DataFrame,
    windows: &DataFrame,
) -> Result<UniverseFilter, PolarsError> {
    let subject_col = cfg.schema.subject.as_str();

    // Join subjects + windows for convenient column access
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
    let reached_ser = wide.column("reached")?.i64()?.clone();

    // Pre-materialize the columns needed to form a signature.
    let qual_values: Vec<Vec<String>> = cfg
        .analysis
        .universe
        .signature_qualities
        .iter()
        .map(|q| {
            Ok::<_, PolarsError>(
                wide.column(q.as_str())?
                    .str()?
                    .into_iter()
                    .map(|s| s.unwrap_or("").to_string())
                    .collect(),
            )
        })
        .collect::<Result<_, _>>()?;

    let wp_deciles: Vec<(String, String, Vec<i64>)> = cfg
        .analysis
        .universe
        .signature_writing_profiles
        .iter()
        .map(|wp| {
            let col_name = format!("{}_{}_decile", wp.measurement, wp.window);
            let vals: Vec<i64> = wide
                .column(col_name.as_str())?
                .i64()?
                .into_iter()
                .map(|v| v.unwrap_or(0))
                .collect();
            Ok::<_, PolarsError>((wp.measurement.clone(), wp.window.clone(), vals))
        })
        .collect::<Result<_, _>>()?;

    let grouping = &cfg.analysis.decile_grouping;
    let sig_at = |row: usize| -> String { signature(&qual_values, &wp_deciles, grouping, row) };

    // 1. Collect reached signatures.
    let mut reached_sigs: HashSet<String> = HashSet::new();
    let mut reached_counts: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    for i in 0..wide.height() {
        if reached_ser.get(i).unwrap_or(0) == 1 {
            let s = sig_at(i);
            reached_sigs.insert(s.clone());
            *reached_counts.entry(s).or_insert(0) += 1;
        }
    }

    // 2. Keep NPIs whose signature appears in the reached set.
    let mut npis = HashSet::with_capacity(wide.height());
    for i in 0..wide.height() {
        let s = sig_at(i);
        if reached_sigs.contains(&s) {
            if let Some(npi) = npi_ser.get(i) {
                npis.insert(npi.to_string());
            }
        }
    }

    let mut counts: Vec<(String, usize)> = reached_counts.into_iter().collect();
    counts.sort_by(|a, b| b.1.cmp(&a.1));

    Ok(UniverseFilter { npis, signature_counts: counts })
}

fn signature(
    qual_values: &[Vec<String>],
    wp_deciles: &[(String, String, Vec<i64>)],
    grouping: &DecileGrouping,
    row: usize,
) -> String {
    let mut parts: Vec<String> = Vec::with_capacity(qual_values.len() + wp_deciles.len());
    for q in qual_values {
        parts.push(q[row].clone());
    }
    for (m, w, decs) in wp_deciles {
        let bucket = grouping.bucket_of(m, w, decs[row]);
        parts.push(format!("{m}_{w}=b{bucket}"));
    }
    parts.join("|")
}
