//! Propensity score estimation via logistic regression.
//!
//! Loosely coupled — depends only on nalgebra for matrix types.
//! No Polars, no DataFrame, no data infrastructure.
//!
//! Usage:
//! ```ignore
//! let findings = propensity_score::fit(&features, &target, &config)?;
//! let scores = findings.predict(&features);
//! let bins = findings.bin(&scores, &bin_config);
//! ```

pub mod binning;
pub mod config;
pub mod logistic;

pub use config::{Config, BinConfig, BinStrategy};
pub use logistic::{fit, Findings};
