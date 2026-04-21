//! Type-state pipeline for the TnC ANALYSIS side.
//!
//! Phases:
//!   Raw ──► Windowed ──► Scored ──► Matched ──► Estimated
//!
//! Each transition adds a new derived artifact (windows → propensity scores →
//! test/control pairs → ANCOVA results) or a narrowing filter (universe).
//! Validation gates sit at two boundaries: the input check runs during
//! `Windowed → Scored` (as soon as the universe is known), and the match
//! placebo check runs during `Matched → Estimated`.
//!
//! The compiler enforces phase order: you can't run ANCOVA without matching,
//! and you can't match without propensity scores.

use std::collections::HashSet;
use std::marker::PhantomData;
use std::path::Path;

use etl_unit_gen::GeneratedData;
use polars::prelude::*;

use crate::ancova::{self, AncovaResult};
use crate::config::Config;
use crate::matching::{self, MatchResult};
use crate::propensity::{self, PropensityOutput};
use crate::treatment::Treatment;
use crate::universe::{self, UniverseFilter};
use crate::validation;
use crate::windows as windows_mod;

// Phase marker types — carry no data, just tag the Analysis<S> state.
pub struct Raw;
pub struct Windowed;
pub struct Scored;
pub struct Matched;
pub struct Estimated;

/// Analysis handle parameterized by phase. Each transition returns a new
/// handle at the next phase; the predecessor is consumed.
pub struct Analysis<S> {
    cfg: Config,
    data: GeneratedData,
    treatment: Treatment,
    windows: Option<DataFrame>,
    universe: Option<UniverseFilter>,
    propensity: Option<PropensityOutput>,
    matches: Option<MatchResult>,
    ancova: Option<Vec<AncovaResult>>,
    _state: PhantomData<S>,
}

impl<S> Analysis<S> {
    pub fn cfg(&self) -> &Config { &self.cfg }
    pub fn subjects(&self) -> &DataFrame { &self.data.subjects }
    pub fn measurements(&self) -> &std::collections::HashMap<String, DataFrame> {
        &self.data.measurements
    }
    pub fn treatment(&self) -> &Treatment { &self.treatment }

    fn advance<T>(self) -> Analysis<T> {
        Analysis {
            cfg: self.cfg,
            data: self.data,
            treatment: self.treatment,
            windows: self.windows,
            universe: self.universe,
            propensity: self.propensity,
            matches: self.matches,
            ancova: self.ancova,
            _state: PhantomData,
        }
    }
}

// ─────────────────────────────────────────────────────────────
// Raw → Windowed
// ─────────────────────────────────────────────────────────────
impl Analysis<Raw> {
    pub fn new(cfg: Config, data: GeneratedData, treatment: Treatment) -> Self {
        Analysis {
            cfg,
            data,
            treatment,
            windows: None,
            universe: None,
            propensity: None,
            matches: None,
            ancova: None,
            _state: PhantomData,
        }
    }

    pub fn compute_windows(mut self) -> Result<Analysis<Windowed>, PolarsError> {
        let mut windows = windows_mod::compute_windows(
            &self.cfg,
            &self.data.subjects,
            &self.data.measurements,
            &self.treatment,
        )?;
        let subject_col = &self.cfg.schema.subject;
        for m in ["brx", "hrx", "grx"] {
            for w in ["L03M", "L06M", "L12M"] {
                let col = format!("{m}_{w}");
                if windows
                    .get_column_names()
                    .iter()
                    .any(|n| n.as_str() == col)
                {
                    windows = windows_mod::add_decile_column(windows, &col, subject_col)?;
                }
            }
        }
        self.windows = Some(windows);
        Ok(self.advance())
    }
}

// ─────────────────────────────────────────────────────────────
// Windowed → Scored  (+ universe build + input validation)
// ─────────────────────────────────────────────────────────────
impl Analysis<Windowed> {
    pub fn windows(&self) -> &DataFrame {
        self.windows.as_ref().expect("windows present in Windowed")
    }

    pub fn fit_propensity(
        mut self,
        y: &[i64],
        cat_predictors: &[&str],
        num_predictors: &[&str],
    ) -> Result<Analysis<Scored>, Box<dyn std::error::Error>> {
        // 1. Build the universe from the reached cohort's signatures.
        let windows_df = self.windows.as_ref().expect("windows present").clone();
        let uni = universe::build_universe(&self.cfg, &self.data.subjects, &windows_df)?;

        // 2. Pre-flight validation on inputs (reached/control counts, ratio).
        validation::validate_inputs(&self.cfg, &self.data.subjects, &uni.npis)?;

        // 3. Fit the propensity model and attach scores/deciles.
        let prop = propensity::fit_propensity(
            &self.data.subjects,
            &windows_df,
            &self.cfg.schema.subject,
            y,
            cat_predictors,
            num_predictors,
        )?;
        let subjects_with_prop = propensity::attach_propensity(
            self.data.subjects.clone(),
            &prop.scores,
            self.cfg.analysis.propensity.bin_count as usize,
        )?;
        // Attach in_universe as an i64 column (1/0) for dashboard use.
        let subject_col = &self.cfg.schema.subject;
        let npis_ser = subjects_with_prop.column(subject_col.as_str())?.str()?.clone();
        let in_universe: Vec<i64> = (0..subjects_with_prop.height())
            .map(|i| {
                npis_ser
                    .get(i)
                    .map(|n| if uni.npis.contains(n) { 1 } else { 0 })
                    .unwrap_or(0)
            })
            .collect();
        let mut subjects_with_prop = subjects_with_prop;
        subjects_with_prop.with_column(
            Series::new(PlSmallStr::from("in_universe"), &in_universe).into_column(),
        )?;
        self.data = GeneratedData {
            subjects: subjects_with_prop,
            measurements: self.data.measurements,
            volume_scales: self.data.volume_scales,
        };
        self.propensity = Some(prop);
        self.universe = Some(uni);
        Ok(self.advance())
    }
}

// ─────────────────────────────────────────────────────────────
// Scored → Matched
// ─────────────────────────────────────────────────────────────
impl Analysis<Scored> {
    pub fn universe(&self) -> &UniverseFilter {
        self.universe.as_ref().expect("universe present in Scored")
    }
    pub fn propensity(&self) -> &PropensityOutput {
        self.propensity.as_ref().expect("propensity present in Scored")
    }

    pub fn run_matching(mut self) -> Result<Analysis<Matched>, PolarsError> {
        let windows_df = self.windows.as_ref().expect("windows present").clone();
        let universe_npis: &HashSet<String> =
            &self.universe.as_ref().expect("universe present").npis;
        if self.cfg.analysis.matching.autotune.enabled {
            eprintln!(
                "  TODO: [matching.autotune].enabled = true (target_match_rate = {:.2}, \
                 {} relaxation steps) — autotune is not yet implemented; using the \
                 explicit within_deciles values from [matching.volume_gates].",
                self.cfg.analysis.matching.autotune.target_match_rate,
                self.cfg.analysis.matching.autotune.relaxation_schedule.len(),
            );
        }
        let result = matching::run_matching(&self.cfg, &self.data.subjects, &windows_df, universe_npis)?;
        self.matches = Some(result);
        Ok(self.advance())
    }
}

// ─────────────────────────────────────────────────────────────
// Matched → Estimated  (+ post-match DiD validation)
// ─────────────────────────────────────────────────────────────
impl Analysis<Matched> {
    pub fn matches(&self) -> &MatchResult {
        self.matches.as_ref().expect("matches present in Matched")
    }

    pub fn run_ancova(mut self) -> Result<Analysis<Estimated>, Box<dyn std::error::Error>> {
        let windows_df = self.windows.as_ref().expect("windows present").clone();
        let match_result = self.matches.as_ref().expect("matches present");

        // 1. Placebo test: pre-period DiD must be within tolerance.
        validation::validate_match(
            &self.cfg,
            &self.data.subjects,
            &windows_df,
            match_result,
        )?;

        // 2. Fit ANCOVA.
        let results = ancova::run_ancova(&self.cfg, &self.data.subjects, &windows_df, match_result)?;
        self.ancova = Some(results);
        Ok(self.advance())
    }
}

// ─────────────────────────────────────────────────────────────
// Estimated (terminal) → export artifacts
// ─────────────────────────────────────────────────────────────
impl Analysis<Estimated> {
    pub fn ancova(&self) -> &[AncovaResult] {
        self.ancova.as_deref().expect("ancova present in Estimated")
    }

    pub fn export(
        &self,
        out_dir: &Path,
        etl_schema_path: &str,
        campaign_path: &str,
        analysis_path: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let universe_size = self
            .universe
            .as_ref()
            .map(|u| u.npis.len())
            .unwrap_or(0);
        crate::output::write_all(
            out_dir,
            &self.cfg,
            &self.data.subjects,
            self.windows.as_ref().expect("windows present"),
            &self.data.measurements,
            &self.treatment,
            self.matches.as_ref().expect("matches present"),
            self.ancova.as_deref().expect("ancova present"),
            universe_size,
            etl_schema_path,
            campaign_path,
            analysis_path,
        )
    }
}
