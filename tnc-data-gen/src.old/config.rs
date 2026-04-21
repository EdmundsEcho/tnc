//! Configuration types — deserialized from config.json.

use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub subjects: SubjectConfig,
    pub time: TimeConfig,
    pub values: ValueConfig,
    pub lift: LiftConfig,
    pub payer_types: Vec<String>,
    pub payer_weights: Vec<f64>,
    pub specialties: SpecialtyConfig,
    pub states: Vec<String>,
    pub output_dir: String,
}

#[derive(Debug, Deserialize)]
pub struct SubjectConfig {
    pub comp_count: usize,
    pub product_a_ratio: f64,
    pub target_list_ratio: f64,
    pub other_market_ratio: f64,
    pub target_list_comp_max_decile: usize,
    pub target_list_product_a_max_decile: usize,
}

#[derive(Debug, Deserialize)]
pub struct TimeConfig {
    pub campaign_start: String,
    pub reach_span_months: usize,
    pub pre_campaign_months: usize,
    pub post_last_reach_months: usize,
    pub date_format: String,
}

#[derive(Debug, Deserialize)]
pub struct ValueConfig {
    pub comp_base_mean: f64,
    pub product_a_ratio: f64,
    pub other_market_ratio: f64,
    pub noise_std_error: f64,
    pub decile_distribution: String,
    pub decile_base: f64,
}

#[derive(Debug, Deserialize)]
pub struct LiftConfig {
    pub max_lift_pct: f64,
    pub ramp_months: usize,
    pub plateau_after_ramp: bool,
}

#[derive(Debug, Deserialize)]
pub struct SpecialtyConfig {
    pub values: Vec<String>,
    pub campaign_weights: Vec<f64>,
    pub population_weights: Vec<f64>,
}

pub fn load_config(path: &Path) -> Config {
    let content = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("Failed to read config: {e}"));
    serde_json::from_str(&content)
        .unwrap_or_else(|e| panic!("Failed to parse config: {e}"))
}
