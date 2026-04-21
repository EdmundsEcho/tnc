//! Schema-driven synthetic data generator.
//!
//! Consumes a minimal schema (subject, time, qualities, measurements) plus a
//! generation spec (distributions, subject heterogeneity, noise) and produces
//! Polars DataFrames:
//!   - subjects_df: one row per subject (qualities)
//!   - measurement DataFrames: one per measurement, long-form
//!     (subject, time, [components...], value)

pub mod distribution;
pub mod generate;
pub mod schema;
pub mod spec;

pub use distribution::{Distribution, NoiseSpec};
pub use generate::{generate, GeneratedData};
pub use schema::{MeasurementDef, MeasurementKind, QualityDef, Schema};
pub use spec::{
    GenSpec, MeasurementGen, QualityGen, SubjectVolumeScale, TemporalShape,
};

#[derive(Debug, thiserror::Error)]
pub enum GenError {
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("Polars error: {0}")]
    Polars(#[from] polars::prelude::PolarsError),
}

pub type Result<T> = std::result::Result<T, GenError>;
