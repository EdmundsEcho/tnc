mod synth;

use std::path::PathBuf;

use etl_unit_gen::{generate, GeneratedData};
use tnc_analysis::config::{self, index_to_ym};
use tnc_analysis::{Analysis, Config};

fn etl_config_ym(cfg: &Config, month_idx: i64) -> String {
    index_to_ym(&cfg.campaign.study_start_date, month_idx)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // CLI:  tnc-data-gen [--seed <N|random>] <etl-schema.toml> <campaign-cfg.toml> <tnc-analysis-cfg.toml> [output-dir]
    let raw_args: Vec<String> = std::env::args().collect();
    let prog = raw_args.first().cloned().unwrap_or_else(|| "tnc-data-gen".to_string());

    let mut seed_override: Option<u64> = None;
    let mut positional: Vec<String> = Vec::with_capacity(raw_args.len());
    let mut it = raw_args.into_iter().skip(1);
    while let Some(a) = it.next() {
        if a == "--seed" {
            let v = it.next().ok_or("--seed requires a value")?;
            seed_override = Some(if v == "random" {
                rand::random::<u64>()
            } else {
                v.parse::<u64>()
                    .map_err(|e| format!("--seed: invalid number {v:?}: {e}"))?
            });
        } else if let Some(v) = a.strip_prefix("--seed=") {
            seed_override = Some(if v == "random" {
                rand::random::<u64>()
            } else {
                v.parse::<u64>()
                    .map_err(|e| format!("--seed=: invalid number {v:?}: {e}"))?
            });
        } else {
            positional.push(a);
        }
    }

    if positional.len() < 3 {
        eprintln!(
            "Usage: {} [--seed <N|random>] <etl-schema.toml> <campaign-cfg.toml> <tnc-analysis-cfg.toml> [output-dir]",
            prog
        );
        std::process::exit(2);
    }
    let etl_schema_path = &positional[0];
    let campaign_path = &positional[1];
    let analysis_path = &positional[2];
    let out_dir = positional
        .get(3)
        .cloned()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("output"));

    println!("Loading configs:");
    println!("  etl-schema:   {etl_schema_path}");
    println!("  campaign:     {campaign_path}");
    println!("  tnc-analysis: {analysis_path}");
    let mut cfg = config::load(etl_schema_path, campaign_path, analysis_path)?;
    if let Some(s) = seed_override {
        println!("  seed override: {s}  (was {} in etl-schema.toml)", cfg.gen.seed);
        cfg.gen.seed = s;
    } else {
        println!("  seed:         {}  (from etl-schema.toml; pass --seed <N|random> to override)", cfg.gen.seed);
    }

    println!("\n━━━ 1. GENERATING BASE DATA ━━━");
    println!("  subjects:       {}", cfg.gen.subject_count);
    println!(
        "  time range:     {}..={}",
        cfg.gen.time_range.first_month, cfg.gen.time_range.last_month
    );
    let base = generate(&cfg.schema, &cfg.gen)?;
    let subject_count = base.subjects.height();
    for (name, df) in &base.measurements {
        println!(
            "  {:>4} rows:      {:>7}  ({} cols)",
            name, df.height(), df.width()
        );
    }

    println!("\n━━━ 2. INJECTING TnC TREATMENT ━━━");
    let treatment = synth::inject_treatment(&cfg, &base);
    let n_target = treatment.planned_reach_date.iter().filter(|f| f.is_some()).count();
    let n_reached = treatment.campaign_reach_date.iter().filter(|f| f.is_some()).count();
    println!(
        "  target list:    {} ({:.1}%)",
        n_target,
        100.0 * n_target as f64 / subject_count as f64
    );
    println!(
        "  reached:        {} ({:.1}%  of universe;  {:.1}% of target list)",
        n_reached,
        100.0 * n_reached as f64 / subject_count as f64,
        if n_target > 0 { 100.0 * n_reached as f64 / n_target as f64 } else { 0.0 },
    );

    let data = synth::apply_lift(base, &cfg, &treatment)?;
    let data = synth::augment_subjects(data, &treatment)?;
    println!(
        "  lift applied:   +{:.0}% (max, plateaued at {} months)",
        cfg.lift.max_pct * 100.0,
        cfg.lift.ramp_months
    );
    let rw_first = cfg.campaign.reach_window_first_month();
    let rw_last = cfg.campaign.reach_window_last_month();
    println!(
        "  reach window:   {} .. {}   (month idx {}..{})",
        etl_config_ym(&cfg, rw_first),
        etl_config_ym(&cfg, rw_last),
        rw_first,
        rw_last,
    );
    println!(
        "  test period:    {} month(s) (excluded from both pre and post windows)",
        cfg.campaign.test_period_months
    );
    if let Some(earliest) = treatment.earliest_reach_month() {
        println!(
            "  earliest reach: {}  (L-anchor for micro-pool windows, derived from data)",
            etl_config_ym(&cfg, earliest),
        );
    }

    // ───────────────────────────────────────────────────────
    // ANALYSIS PIPELINE (type-state enforces phase order)
    // ───────────────────────────────────────────────────────
    let data_for_analysis = GeneratedData {
        subjects: data.subjects.clone(),
        measurements: data.measurements.clone(),
        volume_scales: data.volume_scales.clone(),
    };
    let analysis = Analysis::new(cfg.clone(), data_for_analysis, treatment);

    println!("\n━━━ 3. COMPUTING WINDOWED DERIVATIONS ━━━");
    let analysis = analysis.compute_windows()?;
    println!(
        "  wide frame:     {} rows × {} cols",
        analysis.windows().height(),
        analysis.windows().width()
    );

    println!("\n━━━ 3b. FITTING PROPENSITY MODEL (+ universe + input validation) ━━━");
    let y: Vec<i64> = match cfg.analysis.propensity.target.as_str() {
        "reached" => analysis.treatment().reached_flags(),
        "called_on" => analysis.treatment().called_on.clone(),
        other => return Err(format!("unknown propensity target: {other}").into()),
    };
    let cat_predictors: Vec<&str> = cfg
        .analysis
        .propensity
        .predictor_qualities
        .iter()
        .map(|s| s.as_str())
        .collect();
    let num_predictors: Vec<&str> = cfg
        .analysis
        .propensity
        .predictor_derived
        .iter()
        .map(|s| s.as_str())
        .collect();
    println!("  target:         {}", cfg.analysis.propensity.target);
    println!("  qual predictors:  {:?}", cat_predictors);
    println!("  num predictors:   {:?}", num_predictors);

    let analysis = analysis.fit_propensity(&y, &cat_predictors, &num_predictors)?;

    let prop = analysis.propensity();
    println!(
        "  features:       {}  (one-hot categorical + numeric)",
        prop.feature_names.len()
    );
    println!("  AUC:            {:.3}", prop.auc);
    println!(
        "  iterations:     {}  cost: {:.4}",
        prop.converged_iterations, prop.final_cost
    );

    let uni = analysis.universe();
    println!(
        "  universe:       {}  ({:.1}% of all subjects)",
        uni.npis.len(),
        100.0 * uni.npis.len() as f64 / analysis.subjects().height() as f64,
    );
    println!(
        "  signatures:     {}  (unique in reached cohort)",
        uni.signature_counts.len()
    );
    println!("  input validation: all gates passed");

    println!("\n━━━ 4. MATCHING (pool → micro-pool → control group) ━━━");
    let analysis = analysis.run_matching()?;
    let matches = analysis.matches();
    println!("  eligible controls (pool): {}", matches.control_pool_size);
    println!("  sum of micro-pool sizes:  {}", matches.total_micropool_size);
    println!("  matched test→control pairs: {}", matches.pairs.len());
    println!("  tests with no match:      {}", matches.unmatched_tests);

    if matches.pairs.is_empty() {
        println!("\n⚠  No matches found — cannot run ANCOVA.");
        return Ok(());
    }

    println!("\n━━━ 5. ANCOVA (+ match-quality placebo validation) ━━━");
    let analysis = analysis.run_ancova()?;
    let results = analysis.ancova();
    println!("  match validation: all DiD gates passed");
    println!(
        "  {:<18} {:>4}  {:>10}  {:>10}  {:>8}  {:>10}  {:>10}  {:>8}",
        "outcome", "N", "β_treat", "se", "t", "test_m", "control_m", "lift%"
    );
    println!("  {}", "─".repeat(94));
    for r in results {
        println!(
            "  {:<18} {:>4}  {:>10.3}  {:>10.3}  {:>8.2}  {:>10.3}  {:>10.3}  {:>+7.1}%",
            r.outcome_name,
            r.n_rows,
            r.beta_treatment,
            r.se_treatment,
            r.t_stat,
            r.test_mean,
            r.control_mean,
            r.lift_pct
        );
    }

    println!(
        "\n  INJECTED LIFT:  +{:.0}%   (plateau at {} months post-reach)",
        cfg.lift.max_pct * 100.0,
        cfg.lift.ramp_months
    );
    println!("  ↑ compare with the `lift%` column above. Expect the recovered");
    println!("    lift to approach the injected lift at longer horizons (POST06M).");

    println!("\n━━━ 6. EXPORTING DATA FOR DASHBOARD ━━━");
    analysis.export(&out_dir, etl_schema_path, campaign_path, analysis_path)?;
    println!(
        "  wrote to {}: subjects.csv, windows.csv, matches.csv,",
        out_dir.display()
    );
    println!("       timeseries.csv, summary.json, ancova.json, config.json");
    println!("       + etl-schema.toml, campaign-cfg.toml, tnc-analysis-cfg.toml (copies)");

    Ok(())
}
