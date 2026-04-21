//! TnC analysis pipeline — universe → micro-pool → match → ANCOVA.
//!
//! Driven by a type-state `Analysis<S>` (see [`pipeline`]) that enforces
//! phase order at compile time:
//!
//! ```text
//!   Raw ──► Windowed ──► Scored ──► Matched ──► Estimated
//! ```
//!
//! Consumers (services, CLIs) construct `Analysis::new(cfg, data, treatment)`
//! and walk the phases. Validation gates sit at the input boundary
//! (`Windowed → Scored`) and before ANCOVA (`Matched → Estimated`).

pub mod ancova;
pub mod config;
pub mod matching;
pub mod output;
pub mod pipeline;
pub mod propensity;
pub mod treatment;
pub mod universe;
pub mod validation;
pub mod windows;

pub use config::{load, Config};
pub use pipeline::{Analysis, Estimated, Matched, Raw, Scored, Windowed};
pub use treatment::Treatment;
