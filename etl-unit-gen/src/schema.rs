//! Minimal schema types shaped for TnC-style analyses.
//!
//! This mirrors a subset of etl-unit's EtlSchema so this crate can be used
//! standalone. Integration with etl-unit proper is a later task.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub subject: String,
    pub time: String,
    #[serde(default)]
    pub qualities: Vec<QualityDef>,
    #[serde(default)]
    pub measurements: Vec<MeasurementDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityDef {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasurementDef {
    pub name: String,
    #[serde(default)]
    pub kind: MeasurementKind,
    #[serde(default)]
    pub components: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MeasurementKind {
    #[default]
    Count,
    Measure,
    Binary,
    Categorical,
}
