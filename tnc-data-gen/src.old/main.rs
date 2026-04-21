//! TnC synthetic data generator.
//!
//! Reads config.json and produces CSV files matching the structure
//! expected by the Lucivia ETL pipeline:
//!   - target_list.csv     (campaign universe + reach dates)
//!   - productA.csv        (brand Rx — promoted product)
//!   - productComp.csv     (headroom Rx — competitive class)
//!   - productOtherMarket.csv (generics Rx)

mod config;
mod generator;

use std::path::Path;

fn main() {
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.json".to_string());

    println!("Loading config from: {config_path}");
    let cfg = config::load_config(Path::new(&config_path));

    println!("Generating synthetic TnC data...");
    println!("  Comp subjects:         {}", cfg.subjects.comp_count);
    println!("  Product A subjects:    {}", (cfg.subjects.comp_count as f64 * cfg.subjects.product_a_ratio) as usize);
    println!("  Target list subjects:  {}", (cfg.subjects.comp_count as f64 * cfg.subjects.product_a_ratio * cfg.subjects.target_list_ratio) as usize);
    println!("  Other market subjects: {}", (cfg.subjects.comp_count as f64 * cfg.subjects.other_market_ratio) as usize);
    println!("  Campaign start:        {}", cfg.time.campaign_start);
    println!("  Max lift:              {}%", cfg.lift.max_lift_pct * 100.0);

    let data = generator::generate(&cfg);

    let output_dir = Path::new(&cfg.output_dir);
    std::fs::create_dir_all(output_dir).expect("Failed to create output directory");

    generator::write_target_list(&data, output_dir);
    generator::write_product_rx(&data, output_dir, "productA.csv", &data.product_a_rx);
    generator::write_product_rx(&data, output_dir, "productComp.csv", &data.comp_rx);
    generator::write_product_rx(&data, output_dir, "productOtherMarket.csv", &data.other_market_rx);

    println!("Done! Files written to: {}", output_dir.display());
}
