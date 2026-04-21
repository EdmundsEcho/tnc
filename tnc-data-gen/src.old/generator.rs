//! Data generation logic.
//!
//! Builds subject pools with proper overlap, assigns qualities,
//! generates time-series measurement data with configurable lift.

use std::collections::BTreeMap;
use std::path::Path;

use chrono::{Datelike, NaiveDate};
use rand::prelude::*;
use rand_distr::{LogNormal, Normal};

use crate::config::Config;

// ---------------------------------------------------------------------------
// Generated data structures
// ---------------------------------------------------------------------------

pub struct GeneratedData {
    pub subjects: Vec<Subject>,
    pub target_list: Vec<TargetListEntry>,
    pub product_a_rx: Vec<RxRow>,
    pub comp_rx: Vec<RxRow>,
    pub other_market_rx: Vec<RxRow>,
    pub months: Vec<String>,
}

pub struct Subject {
    pub npi: String,
    pub specialty: String,
    pub state: String,
    pub zip: String,
    /// Base prescribing volume (determines decile)
    pub base_volume: f64,
    /// Decile (1=lowest, 10=highest)
    pub decile: u32,
    /// Is in product A pool
    pub in_product_a: bool,
    /// Is in comp pool
    pub in_comp: bool,
    /// Is in other market pool
    pub in_other_market: bool,
    /// Is on target list
    pub on_target_list: bool,
}

pub struct TargetListEntry {
    pub npi: String,
    pub specialty: String,
    pub state: String,
    pub zip: String,
    /// Reach date (Some = reached, None = on list but not reached)
    pub reach_date: Option<String>,
}

pub struct RxRow {
    pub npi: String,
    pub specialty: String,
    pub state: String,
    pub zip: String,
    pub payer: String,
    pub date: String,
    pub rx_count: f64,
}

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

pub fn generate(cfg: &Config) -> GeneratedData {
    let mut rng = StdRng::seed_from_u64(42); // reproducible

    // Compute counts
    let comp_count = cfg.subjects.comp_count;
    let product_a_count = (comp_count as f64 * cfg.subjects.product_a_ratio) as usize;
    let target_count = (product_a_count as f64 * cfg.subjects.target_list_ratio) as usize;
    let other_market_count = (comp_count as f64 * cfg.subjects.other_market_ratio) as usize;

    // Generate time range
    let months = generate_months(cfg);
    let campaign_start_idx = cfg.time.pre_campaign_months;

    // Generate all subjects (comp pool = full universe)
    let mut subjects: Vec<Subject> = (0..other_market_count)
        .map(|i| {
            let npi = format!("{:010}", 1000000000u64 + i as u64);
            let specialty = weighted_choice(&cfg.specialties.values, &cfg.specialties.population_weights, &mut rng);
            let state = cfg.states[rng.gen_range(0..cfg.states.len())].clone();
            let zip = format!("{:05}", rng.gen_range(10000..99999));
            let base_volume = generate_base_volume(cfg, &mut rng);

            Subject {
                npi,
                specialty,
                state,
                zip,
                base_volume,
                decile: 0, // assigned after sorting
                in_product_a: false,
                in_comp: false,
                in_other_market: true,
                on_target_list: false,
            }
        })
        .collect();

    // Sort by base_volume descending to assign deciles
    subjects.sort_by(|a, b| b.base_volume.partial_cmp(&a.base_volume).unwrap());

    // Assign deciles (1=lowest, 10=highest)
    let total = subjects.len() as f64;
    for (i, subject) in subjects.iter_mut().enumerate() {
        let pct = i as f64 / total;
        subject.decile = (10.0 - pct * 10.0).ceil() as u32;
        if subject.decile == 0 { subject.decile = 1; }
        if subject.decile > 10 { subject.decile = 10; }
    }

    // Reassign specialties to correlate with decile.
    // High-decile subjects are biased toward high-value specialties
    // (first in the config list). The correlation is noisy — not deterministic.
    let num_specs = cfg.specialties.values.len();
    for subject in subjects.iter_mut() {
        let spec_weights = build_decile_specialty_weights(
            subject.decile,
            num_specs,
            &mut rng,
        );
        subject.specialty = weighted_choice(
            &cfg.specialties.values,
            &spec_weights,
            &mut rng,
        );
    }

    // Assign comp pool (top comp_count subjects by volume)
    for subject in subjects.iter_mut().take(comp_count) {
        subject.in_comp = true;
    }

    // Assign product A pool (top product_a_count subjects by volume)
    for subject in subjects.iter_mut().take(product_a_count) {
        subject.in_product_a = true;
    }

    // Assign target list from product A subjects in configured decile range
    let pa_min_decile = 10 - cfg.subjects.target_list_product_a_max_decile as u32 + 1;
    let comp_min_decile = 10 - cfg.subjects.target_list_comp_max_decile as u32 + 1;

    {
        let mut tc: Vec<usize> = (0..subjects.len())
            .filter(|&i| {
                subjects[i].in_product_a
                    && subjects[i].decile >= pa_min_decile
                    && subjects[i].decile <= 10
            })
            .collect();
        tc.shuffle(&mut rng);

        let mut target_assigned = 0;
        for idx in tc {
            if target_assigned >= target_count { break; }
            if subjects[idx].decile >= comp_min_decile {
                subjects[idx].on_target_list = true;
                if rng.gen::<f64>() < 0.6 {
                    let new_spec = weighted_choice(
                        &cfg.specialties.values,
                        &cfg.specialties.campaign_weights,
                        &mut rng,
                    );
                    subjects[idx].specialty = new_spec;
                }
                target_assigned += 1;
            }
        }
        println!("  Assigned {target_assigned} subjects to target list");
    }

    // Generate target list with reach dates
    let campaign_start = parse_month(&cfg.time.campaign_start);
    let target_list: Vec<TargetListEntry> = subjects.iter()
        .filter(|s| s.on_target_list)
        .map(|s| {
            // Stagger reach dates over reach_span_months
            let offset_days = rng.gen_range(0..(cfg.time.reach_span_months as i64 * 30));
            let reach_date = campaign_start + chrono::Duration::days(offset_days);
            TargetListEntry {
                npi: s.npi.clone(),
                specialty: s.specialty.clone(),
                state: s.state.clone(),
                zip: s.zip.clone(),
                reach_date: Some(format!("{:02}-{}", reach_date.format("%y"), month_abbrev(reach_date.month()))),
            }
        })
        .collect();

    // Generate Rx data for each product
    let product_a_rx = generate_rx_data(
        &subjects,
        |s| s.in_product_a,
        cfg.values.comp_base_mean * cfg.values.product_a_ratio,
        &months,
        campaign_start_idx,
        &target_list,
        cfg,
        &mut rng,
    );

    let comp_rx = generate_rx_data(
        &subjects,
        |s| s.in_comp,
        cfg.values.comp_base_mean,
        &months,
        campaign_start_idx,
        &target_list,
        cfg,
        &mut rng,
    );

    let other_market_rx = generate_rx_data(
        &subjects,
        |s| s.in_other_market,
        cfg.values.comp_base_mean * cfg.values.other_market_ratio,
        &months,
        campaign_start_idx,
        &target_list,
        cfg,
        &mut rng,
    );

    GeneratedData {
        subjects,
        target_list,
        product_a_rx,
        comp_rx,
        other_market_rx,
        months,
    }
}

// ---------------------------------------------------------------------------
// Rx data generation
// ---------------------------------------------------------------------------

fn generate_rx_data(
    subjects: &[Subject],
    filter: impl Fn(&Subject) -> bool,
    base_mean: f64,
    months: &[String],
    campaign_start_idx: usize,
    target_list: &[TargetListEntry],
    cfg: &Config,
    rng: &mut StdRng,
) -> Vec<RxRow> {
    let noise_dist = Normal::new(0.0, cfg.values.noise_std_error * base_mean).unwrap();
    let campaign_start = parse_month(&cfg.time.campaign_start);

    // Build reach date lookup
    let reach_dates: BTreeMap<String, NaiveDate> = target_list.iter()
        .filter_map(|tl| {
            tl.reach_date.as_ref().map(|d| {
                (tl.npi.clone(), parse_reach_date(d))
            })
        })
        .collect();

    let mut rows = Vec::new();

    for subject in subjects.iter().filter(|s| filter(s)) {
        // Scale base volume by decile
        let decile_scale = subject.decile as f64 / 5.0;
        let subject_base = base_mean * decile_scale;

        let subject_reach_date = reach_dates.get(&subject.npi);

        for (month_idx, month_str) in months.iter().enumerate() {
            let month_date = parse_month(month_str);

            // Compute lift for reached subjects
            let lift_multiplier = if let Some(&reach_date) = subject_reach_date {
                let months_since_reach = months_between(reach_date, month_date);
                if months_since_reach < 0 {
                    1.0 // pre-reach
                } else if months_since_reach < cfg.lift.ramp_months as i32 {
                    // Linear ramp
                    let ramp_pct = (months_since_reach + 1) as f64 / cfg.lift.ramp_months as f64;
                    1.0 + cfg.lift.max_lift_pct * ramp_pct
                } else {
                    // Plateau
                    1.0 + cfg.lift.max_lift_pct
                }
            } else {
                1.0
            };

            // Generate one row per payer
            for (payer_idx, payer) in cfg.payer_types.iter().enumerate() {
                let payer_share = cfg.payer_weights[payer_idx];
                let base_rx = subject_base * payer_share * lift_multiplier;
                let noise: f64 = rng.sample(noise_dist);
                let rx_count = (base_rx + noise).max(0.0);

                if rx_count > 0.01 {
                    rows.push(RxRow {
                        npi: subject.npi.clone(),
                        specialty: subject.specialty.clone(),
                        state: subject.state.clone(),
                        zip: subject.zip.clone(),
                        payer: payer.clone(),
                        date: month_str.clone(),
                        rx_count: (rx_count * 100.0).round() / 100.0,
                    });
                }
            }
        }
    }

    println!("  Generated {} Rx rows", rows.len());
    rows
}

// ---------------------------------------------------------------------------
// Writers
// ---------------------------------------------------------------------------

pub fn write_target_list(data: &GeneratedData, output_dir: &Path) {
    let path = output_dir.join("target_list.csv");
    let mut wtr = csv::Writer::from_path(&path).unwrap();
    wtr.write_record(["npi", "specialty", "state", "zip", "MM-YYYY"]).unwrap();

    for entry in &data.target_list {
        wtr.write_record([
            &entry.npi,
            &entry.specialty,
            &entry.state,
            &entry.zip,
            entry.reach_date.as_deref().unwrap_or(""),
        ]).unwrap();
    }
    wtr.flush().unwrap();
    println!("  Wrote {} rows to {}", data.target_list.len(), path.display());
}

pub fn write_product_rx(data: &GeneratedData, output_dir: &Path, filename: &str, rows: &[RxRow]) {
    let path = output_dir.join(filename);
    let mut wtr = csv::Writer::from_path(&path).unwrap();
    wtr.write_record(["NPI Number", "Specialty", "State", "Zip", "Payment Type Group", "Year-Month", "Unit Count"]).unwrap();

    for row in rows {
        wtr.write_record([
            &row.npi,
            &row.specialty,
            &row.state,
            &row.zip,
            &row.payer,
            &row.date,
            &format!("{:.2}", row.rx_count),
        ]).unwrap();
    }
    wtr.flush().unwrap();
    println!("  Wrote {} rows to {}", rows.len(), path.display());
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn generate_months(cfg: &Config) -> Vec<String> {
    let start = parse_month(&cfg.time.campaign_start);
    let pre_start = start - chrono::Months::new(cfg.time.pre_campaign_months as u32);
    let end = start
        + chrono::Months::new((cfg.time.reach_span_months + cfg.time.post_last_reach_months) as u32);

    let mut months = Vec::new();
    let mut current = pre_start;
    while current <= end {
        months.push(format!("{}", current.format("%Y-%m")));
        current = current + chrono::Months::new(1);
    }
    months
}

fn parse_month(s: &str) -> NaiveDate {
    NaiveDate::parse_from_str(&format!("{s}-01"), "%Y-%m-%d")
        .unwrap_or_else(|_| panic!("Cannot parse month: {s}"))
}

fn parse_reach_date(s: &str) -> NaiveDate {
    // Format: "16-Jan" → 2016-01-01
    let parts: Vec<&str> = s.split('-').collect();
    let year = 2000 + parts[0].parse::<i32>().unwrap_or(16);
    let month = match parts.get(1).map(|s| s.to_lowercase()).as_deref() {
        Some("jan") => 1, Some("feb") => 2, Some("mar") => 3,
        Some("apr") => 4, Some("may") => 5, Some("jun") => 6,
        Some("jul") => 7, Some("aug") => 8, Some("sep") => 9,
        Some("oct") => 10, Some("nov") => 11, Some("dec") => 12,
        _ => 1,
    };
    NaiveDate::from_ymd_opt(year, month, 15).unwrap()
}

fn month_abbrev(m: u32) -> &'static str {
    match m {
        1 => "Jan", 2 => "Feb", 3 => "Mar", 4 => "Apr",
        5 => "May", 6 => "Jun", 7 => "Jul", 8 => "Aug",
        9 => "Sep", 10 => "Oct", 11 => "Nov", 12 => "Dec",
        _ => "Jan",
    }
}

fn months_between(from: NaiveDate, to: NaiveDate) -> i32 {
    (to.year() - from.year()) * 12 + (to.month() as i32 - from.month() as i32)
}

/// Build specialty probability weights biased by decile.
///
/// Specialties are ordered high-value to low-value in the config.
/// High deciles (10) get weights skewed toward early specialties (high-value).
/// Low deciles (1) get weights skewed toward late specialties (low-value).
/// Noise ensures overlap — some low-decile subjects still get high-value specialties.
fn build_decile_specialty_weights(
    decile: u32,
    num_specs: usize,
    rng: &mut StdRng,
) -> Vec<f64> {
    // decile 10 → center_idx near 0 (high-value specialties)
    // decile 1  → center_idx near num_specs-1 (low-value specialties)
    let center = (10.0 - decile as f64) / 9.0 * (num_specs as f64 - 1.0);

    // Add noise to the center so it's not perfectly deterministic
    let noise: f64 = rng.gen_range(-1.5..1.5);
    let noisy_center = (center + noise).clamp(0.0, (num_specs - 1) as f64);

    // Build gaussian-like weights centered on noisy_center
    let sigma = num_specs as f64 * 0.3; // spread
    let weights: Vec<f64> = (0..num_specs)
        .map(|i| {
            let dist = (i as f64 - noisy_center).abs();
            (-dist * dist / (2.0 * sigma * sigma)).exp()
        })
        .collect();

    weights
}

fn generate_base_volume(cfg: &Config, rng: &mut StdRng) -> f64 {
    // Exponential-like distribution: most subjects have low volume,
    // few have very high volume (matching decile distribution)
    let ln_dist = LogNormal::new(1.0, cfg.values.decile_base.ln()).unwrap();
    rng.sample(ln_dist)
}

fn weighted_choice(values: &[String], weights: &[f64], rng: &mut StdRng) -> String {
    let total: f64 = weights.iter().sum();
    let mut r = rng.gen::<f64>() * total;
    for (i, &w) in weights.iter().enumerate() {
        r -= w;
        if r <= 0.0 {
            return values[i].clone();
        }
    }
    values.last().cloned().unwrap_or_default()
}
