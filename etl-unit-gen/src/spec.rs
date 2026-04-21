//! Generation spec — how to shape the random data.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::distribution::{Distribution, NoiseSpec};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenSpec {
    pub subject_count: usize,
    pub time_range: TimeRange,
    pub seed: u64,

    /// Per-subject scale factor distribution (the "decile effect").
    /// Drawn ONCE per subject and multiplies the measurement mean.
    pub subject_volume_scale: SubjectVolumeScale,

    #[serde(default)]
    pub qualities: HashMap<String, QualityGen>,

    #[serde(default)]
    pub measurements: HashMap<String, MeasurementGen>,

    #[serde(default)]
    pub noise: NoiseSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRange {
    pub first_month: i64,
    pub last_month: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "distribution", rename_all = "snake_case")]
pub enum SubjectVolumeScale {
    LogNormal { mu: f64, sigma: f64 },
    Constant { value: f64 },
}

impl SubjectVolumeScale {
    pub fn sample<R: rand::Rng>(&self, rng: &mut R) -> f64 {
        match self {
            SubjectVolumeScale::LogNormal { mu, sigma } => {
                let d = rand_distr::LogNormal::new(*mu, *sigma).unwrap();
                rand_distr::Distribution::sample(&d, rng)
            }
            SubjectVolumeScale::Constant { value } => *value,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityGen {
    /// Direct distribution (if present).
    #[serde(default)]
    pub distribution: Option<Distribution>,
    /// Derivation from another quality via fixed mapping.
    #[serde(default)]
    pub derived_from: Option<String>,
    #[serde(default)]
    pub mapping: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasurementGen {
    pub distribution: Distribution,
    #[serde(default)]
    pub temporal: TemporalShape,
    #[serde(default)]
    pub components: HashMap<String, ComponentGen>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentGen {
    pub distribution: Distribution,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TemporalShape {
    #[default]
    Constant,
    Linear {
        slope_per_month: f64,
    },
    Seasonal {
        period_months: i64,
        amplitude: f64,
        phase_month: i64,
    },
}

impl TemporalShape {
    /// Multiplicative factor at the given month offset (relative to time_range.first_month).
    pub fn factor(&self, month_idx: i64) -> f64 {
        match self {
            TemporalShape::Constant => 1.0,
            TemporalShape::Linear { slope_per_month } => 1.0 + slope_per_month * month_idx as f64,
            TemporalShape::Seasonal {
                period_months,
                amplitude,
                phase_month,
            } => {
                let phase = 2.0 * std::f64::consts::PI * (month_idx - phase_month) as f64
                    / (*period_months as f64);
                1.0 + amplitude * phase.sin()
            }
        }
    }
}
