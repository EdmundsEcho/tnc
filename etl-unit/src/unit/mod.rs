//! ETL Units: The atomic building blocks of a Universe.
//!
//! ## Unit Types (Schema Definitions)
//!
//! - [`MeasurementUnit`]: Time-varying observations (subject × time × [components] → value)
//! - [`QualityUnit`]: Time-invariant attributes (subject → value)
//! - [`Derivation`]: Computed units from other units

mod derivation;
mod measurement;
mod null_value;
mod quality;

// Schema-level definitions (unchanged)
pub use derivation::{
	Computation, Derivation, IntoCanonicalVec, OverSubjectExpr, PointwiseExpr, TimeExpr, TimeUnit,
};
pub use measurement::{DataTemporality, MeasurementKind, MeasurementUnit, ResampleStrategy, TruthMapping};
pub use null_value::NullValue;
pub use quality::QualityUnit;
