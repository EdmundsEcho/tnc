//! Split-config loader — three TOML files compose one internal `Config`:
//!   1. etl-schema.toml      → [schema] + [gen]   (data-generation side)
//!   2. campaign-cfg.toml    → [campaign] + [lift]
//!   3. tnc-analysis-cfg.toml→ derived_fields + eligibility + propensity
//!                            + matching + ancova

use std::collections::HashMap;
use std::fs;

use etl_unit_gen::{GenSpec, Schema};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub schema: Schema,
    pub gen: GenSpec,
    pub campaign: CampaignConfig,
    pub lift: LiftSpec,
    pub analysis: AnalysisConfig,
}

// ─────────────────────────────────────────────────────────────
// Campaign (real-world mechanics of the TnC campaign)
// ─────────────────────────────────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CampaignConfig {
    /// Study start date (YYYY-MM-DD). Month index 1 maps to this month.
    pub study_start_date: String,
    /// Total observation horizon, in months.
    pub total_span_months: i64,
    /// Minimum pre-period (months) every reached subject must have before their reach date.
    pub min_pre_months: i64,
    /// Maximum post-period (months) after test_period we can observe for a reached subject.
    pub max_post_months: i64,
    /// Lag window between reach and post — excluded from analysis.
    pub test_period_months: i64,
    /// Fraction of the universe placed on the target list.
    pub target_list_fraction: f64,
    /// Fraction of the target list actually reached by the campaign
    /// (campaign_reach_date is non-null).
    pub reached_fraction_of_targets: f64,
    pub reach_window: ReachWindow,
    pub called_on: CalledOnSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReachWindow {
    /// Start of the reach window (YYYY-MM-DD).
    pub start_date: String,
    /// How many months the reach window spans (reach dates fall inside it).
    pub duration_months: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalledOnSpec {
    pub p_when_reached: f64,
    pub p_when_not_reached: f64,
}

// ─────────────────────────────────────────────────────────────
// Lift (synthetic-only signal injection)
// ─────────────────────────────────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiftSpec {
    pub applies_to: String,
    pub ramp_months: i64,
    pub max_pct: f64,
    pub plateau_after_ramp: bool,
}

// ─────────────────────────────────────────────────────────────
// Analysis (everything downstream of the data)
// ─────────────────────────────────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisConfig {
    pub derived_fields: HashMap<String, WindowDef>,
    pub eligibility: Eligibility,
    #[serde(default)]
    pub decile_grouping: DecileGrouping,
    pub universe: UniverseSpec,
    pub propensity: PropensitySpec,
    pub matching: MatchingSpec,
    pub ancova: AncovaSpec,
    #[serde(default)]
    pub input_validation: InputValidation,
    #[serde(default)]
    pub match_validation: MatchValidation,
}

/// Pre-flight gates on the input data. Failure halts the pipeline.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InputValidation {
    #[serde(default)]
    pub min_reached: usize,
    #[serde(default)]
    pub min_eligible_control_pool: usize,
    #[serde(default)]
    pub min_test_to_control_ratio: f64,
}

/// Post-match placebo test: pre-period DiD per measurement must be within
/// the configured tolerance. An empty vec disables the check entirely.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MatchValidation {
    #[serde(default)]
    pub max_did_pre: Vec<DidTolerance>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DidTolerance {
    pub measurement: String,
    pub window: String,
    pub max: f64,
}

/// Universe definition — who is eligible to be in the analysis at all.
/// A subject joins iff their (qualities × merged-writing-profile-buckets)
/// signature is observed among the reached cohort.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniverseSpec {
    /// Categorical qualities that must match (exact) between reached and
    /// candidate signatures.
    pub signature_qualities: Vec<String>,
    /// Writing-profile sources whose *merged* decile bucket must also match.
    pub signature_writing_profiles: Vec<VolumeSource>,
}

/// Per-(measurement, window) decile grouping. A *group* is an arbitrary
/// subset of `[0..=10]` that collapses into one merged bucket. Groups are
/// unordered and may include any deciles — typically contiguous (e.g.
/// merging whales `[9,10]` or low writers `[1,2]`). The merged bucket is
/// identified by the smallest decile in the group.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DecileGrouping {
    /// Default grouping applied to every (measurement, window) that has no
    /// matching override.
    #[serde(default)]
    pub default: DecileGroups,
    /// Per-source overrides. The first entry matching a source wins.
    #[serde(default)]
    pub overrides: Vec<DecileGroupOverride>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DecileGroups {
    #[serde(default)]
    pub groups: Vec<Vec<i64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecileGroupOverride {
    pub source: VolumeSource,
    pub groups: Vec<Vec<i64>>,
}

impl DecileGrouping {
    /// Groups active for this (measurement, window): override if present,
    /// else default.
    pub fn groups_for(&self, measurement: &str, window: &str) -> &[Vec<i64>] {
        self.overrides
            .iter()
            .find(|o| o.source.measurement == measurement && o.source.window == window)
            .map(|o| o.groups.as_slice())
            .unwrap_or(self.default.groups.as_slice())
    }

    /// Merged bucket index for a raw decile (0..=10). Deciles in the same
    /// group share the group's min value; deciles outside any group map to
    /// themselves. Out-of-range inputs pass through unchanged.
    pub fn bucket_of(&self, measurement: &str, window: &str, decile: i64) -> i64 {
        for g in self.groups_for(measurement, window) {
            if g.contains(&decile) {
                return *g.iter().min().unwrap_or(&decile);
            }
        }
        decile
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Eligibility {
    #[serde(rename = "brx_L12M_max")]
    pub brx_l12m_max: f64,
    #[serde(rename = "hrx_L12M_min")]
    pub hrx_l12m_min: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowDef {
    pub months: i64,
    pub anchor: WindowAnchor,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WindowAnchor {
    // Per-subject anchors — relative to each subject's campaign_reach_date
    // (controls use earliest_reach_month as a Phase-1 virtual anchor).
    /// Months ending at (and including) the month before campaign_reach_date,
    /// extending backward. PRE01M = the single month just before reach.
    Pre,
    /// The lag window [reach_date, reach_date + test_period_months − 1].
    /// Forbidden in matching / propensity / eligibility.
    TestPeriod,
    /// Months starting at reach_date + test_period_months.
    /// Forbidden in matching / propensity / eligibility.
    Post,

    // Universe anchors — same range for every subject.
    /// Months ending at `campaign.reach_window.first_month − 1`, extending
    /// BACKWARD. L01M = the single month just before the campaign starts,
    /// L12M = the 12 months immediately before. This is the Phase-1 /
    /// first-pass baseline — safe because it's entirely pre-campaign.
    /// "L" direction counts *back* in time.
    Last,
    /// Months from `study_start_date`, counting FORWARD.
    /// F01M = the first month of data, F12M = the first 12.
    /// Diagnostic; `Last` is preferred for pre-campaign baselines.
    First,
}

// ─────────────────────────────────────────────────────────────
// Propensity
// ─────────────────────────────────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropensitySpec {
    pub target: String,
    pub predictor_qualities: Vec<String>,
    pub predictor_derived: Vec<String>,
    pub bin_count: i64,
    pub binning: String,
}

// ─────────────────────────────────────────────────────────────
// Matching
// ─────────────────────────────────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchingSpec {
    pub categorical_gates: Vec<String>,
    /// If true, tests must pair with controls in the same propensity decile.
    #[serde(default)]
    pub propensity_match: bool,
    #[serde(default)]
    pub volume_gates: Vec<VolumeGate>,
    pub scoring: ScoringSpec,
    #[serde(default)]
    pub autotune: AutotuneSpec,
}

/// Autotuner config — reserved. When `enabled = true`, the pipeline would
/// start with the strictest gate values and progressively relax each entry
/// in `relaxation_schedule` by one decile at a time until
/// `matched_pairs / eligible_tests >= target_match_rate`. Not yet
/// implemented: the pipeline logs a TODO and proceeds with the explicit
/// volume-gate values instead.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AutotuneSpec {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub target_match_rate: f64,
    #[serde(default)]
    pub relaxation_schedule: Vec<AutotuneStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutotuneStep {
    pub source: VolumeSource,
    pub within_deciles_max: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeGate {
    pub source: VolumeSource,
    /// Test and control must share the same `{measurement}_{window}_decile`
    /// (D0..D10, where D0 = non-writers). `within_deciles = 0` means strict
    /// equality; 1 allows a one-decile neighbor, etc.
    #[serde(default)]
    pub within_deciles: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeSource {
    pub measurement: String,
    pub window: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoringSpec {
    pub rx_types: Vec<String>,
    pub window_points: HashMap<String, i64>,
}

// ─────────────────────────────────────────────────────────────
// ANCOVA
// ─────────────────────────────────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AncovaSpec {
    pub outcomes: Vec<MeasurementWindow>,
    pub baseline: MeasurementWindow,
    pub covariate_qualities: Vec<String>,
    pub covariate_windows: Vec<MeasurementWindow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasurementWindow {
    pub measurement: String,
    pub window: String,
}

// ─────────────────────────────────────────────────────────────
// Loader — three files in, one Config out.
// ─────────────────────────────────────────────────────────────
#[derive(Deserialize)]
struct SchemaFile {
    schema: Schema,
    #[serde(rename = "gen")]
    gen_: GenSpec,
}

#[derive(Deserialize)]
struct CampaignFile {
    campaign: CampaignConfig,
    lift: LiftSpec,
}

#[derive(Deserialize)]
struct AnalysisFile {
    derived_fields: HashMap<String, WindowDef>,
    eligibility: Eligibility,
    #[serde(default)]
    decile_grouping: DecileGrouping,
    universe: UniverseSpec,
    propensity: PropensitySpec,
    matching: MatchingSpec,
    ancova: AncovaSpec,
    #[serde(default)]
    input_validation: InputValidation,
    #[serde(default)]
    match_validation: MatchValidation,
}

// ─────────────────────────────────────────────────────────────
// Calendar helpers — the pipeline works internally on month indices
// (1 = study_start_date month). These helpers translate between
// YYYY-MM / YYYY-MM-DD strings and those indices.
// ─────────────────────────────────────────────────────────────

/// Parse "YYYY-MM-DD" or "YYYY-MM" into (year, month). Panics on malformed input.
pub fn parse_ym(s: &str) -> (i32, u32) {
    let parts: Vec<&str> = s.split('-').collect();
    let year: i32 = parts[0].parse().expect("invalid year");
    let month: u32 = parts[1].parse().expect("invalid month");
    assert!((1..=12).contains(&month), "month out of range: {month}");
    (year, month)
}

/// Count of months between two (year, month) pairs: b − a.
pub fn months_between((ay, am): (i32, u32), (by, bm): (i32, u32)) -> i64 {
    (by as i64 - ay as i64) * 12 + (bm as i64 - am as i64)
}

/// Month index (1-based) that a given calendar month corresponds to, given the study start.
pub fn month_index(study_start: &str, target: &str) -> i64 {
    months_between(parse_ym(study_start), parse_ym(target)) + 1
}

/// Convert a 1-based month index back to "YYYY-MM" given the study start.
pub fn index_to_ym(study_start: &str, index: i64) -> String {
    let (y0, m0) = parse_ym(study_start);
    let total = (y0 as i64) * 12 + (m0 as i64 - 1) + (index - 1);
    let year = (total / 12) as i32;
    let month = (total % 12) as u32 + 1;
    format!("{year:04}-{month:02}")
}

// Derived calendar accessors on CampaignConfig.
impl CampaignConfig {
    /// First month index of the reach window (1-based).
    pub fn reach_window_first_month(&self) -> i64 {
        month_index(&self.study_start_date, &self.reach_window.start_date)
    }
    pub fn reach_window_last_month(&self) -> i64 {
        self.reach_window_first_month() + self.reach_window.duration_months - 1
    }
    pub fn study_first_month(&self) -> i64 { 1 }
    pub fn study_last_month(&self) -> i64 { self.total_span_months }
}

pub fn load(
    etl_schema_path: &str,
    campaign_path: &str,
    analysis_path: &str,
) -> Result<Config, Box<dyn std::error::Error>> {
    let sf: SchemaFile = toml::from_str(&fs::read_to_string(etl_schema_path)?)
        .map_err(|e| format!("{etl_schema_path}: {e}"))?;
    let cf: CampaignFile = toml::from_str(&fs::read_to_string(campaign_path)?)
        .map_err(|e| format!("{campaign_path}: {e}"))?;
    let af: AnalysisFile = toml::from_str(&fs::read_to_string(analysis_path)?)
        .map_err(|e| format!("{analysis_path}: {e}"))?;

    let cfg = Config {
        schema: sf.schema,
        gen: sf.gen_,
        campaign: cf.campaign,
        lift: cf.lift,
        analysis: AnalysisConfig {
            derived_fields: af.derived_fields,
            eligibility: af.eligibility,
            decile_grouping: af.decile_grouping,
            universe: af.universe,
            propensity: af.propensity,
            matching: af.matching,
            ancova: af.ancova,
            input_validation: af.input_validation,
            match_validation: af.match_validation,
        },
    };

    validate_campaign_window(&cfg)?;
    validate_no_post_in_matching(&cfg)?;
    validate_decile_grouping(&cfg)?;
    Ok(cfg)
}

fn validate_decile_grouping(cfg: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let check_groups = |label: &str, groups: &[Vec<i64>]| -> Result<(), String> {
        let mut seen = [false; 11];
        for g in groups {
            for &d in g {
                if !(0..=10).contains(&d) {
                    return Err(format!("{label}: decile {d} out of range 0..=10"));
                }
                if seen[d as usize] {
                    return Err(format!("{label}: decile {d} appears in more than one group"));
                }
                seen[d as usize] = true;
            }
        }
        Ok(())
    };

    check_groups("decile_grouping.default", &cfg.analysis.decile_grouping.default.groups)?;
    for o in &cfg.analysis.decile_grouping.overrides {
        let label = format!(
            "decile_grouping.overrides[{}_{}]",
            o.source.measurement, o.source.window
        );
        check_groups(&label, &o.groups)?;
    }
    Ok(())
}

/// Verify the reach window respects min_pre and max_post constraints given
/// the study horizon.
fn validate_campaign_window(cfg: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let first = cfg.campaign.study_first_month();
    let last = cfg.campaign.study_last_month();
    let rw_first = cfg.campaign.reach_window_first_month();
    let rw_last = cfg.campaign.reach_window_last_month();

    let earliest_valid = first + cfg.campaign.min_pre_months;
    if rw_first < earliest_valid {
        return Err(format!(
            "campaign.reach_window.start_date={} is too early — must be on or after month \
             {} (study_start + min_pre_months = {})",
            cfg.campaign.reach_window.start_date,
            earliest_valid,
            index_to_ym(&cfg.campaign.study_start_date, earliest_valid),
        )
        .into());
    }

    let latest_valid = last - cfg.campaign.test_period_months - cfg.campaign.max_post_months + 1;
    if rw_last > latest_valid {
        return Err(format!(
            "campaign.reach_window extends too late — last reach month {} > {} (study_last − \
             test_period − max_post + 1 = {})",
            index_to_ym(&cfg.campaign.study_start_date, rw_last),
            index_to_ym(&cfg.campaign.study_start_date, latest_valid),
            latest_valid,
        )
        .into());
    }
    Ok(())
}

/// Enforce the INVARIANT: post / test_period windows may not appear in any
/// matching, propensity, or eligibility configuration. Only `pre`, `last`,
/// and `first` anchors are safe — all end before any subject's reach date.
fn validate_no_post_in_matching(cfg: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let forbidden = |wname: &str| match cfg.analysis.derived_fields.get(wname) {
        Some(d) => matches!(d.anchor, WindowAnchor::Post | WindowAnchor::TestPeriod),
        None => false,
    };

    // Propensity predictor derived fields
    for w in &cfg.analysis.propensity.predictor_derived {
        // predictor_derived looks like "brx_F12M" — split on first '_' past the stream
        // Actually derived names are "{measurement}_{window}" — extract window.
        if let Some(win) = w.rsplit_once('_').map(|(_, w)| w) {
            if forbidden(win) {
                return Err(format!(
                    "propensity.predictor_derived contains post-period window: {w}"
                )
                .into());
            }
        }
    }
    // Matching volume gates
    for g in &cfg.analysis.matching.volume_gates {
        if forbidden(&g.source.window) {
            return Err(format!(
                "matching.volume_gates uses post-period window: {}",
                g.source.window
            )
            .into());
        }
    }
    // Matching scoring windows
    for w in cfg.analysis.matching.scoring.window_points.keys() {
        if forbidden(w) {
            return Err(format!("matching.scoring uses post-period window: {w}").into());
        }
    }
    Ok(())
}
