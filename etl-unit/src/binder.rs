//! The [`SourceBinder`] trait: maps fetched DataFrames to [`BoundSource`]s.
//!
//! Data crates implement this alongside their `SourceFactory` implementations
//! (from `synapse-data-source`). The application layer calls
//! `factory.fetch()` → `factory.normalize()` → `binder.bind()` to produce
//! a `BoundSource` for the ETL universe.
//!
//! # Example
//!
//! ```rust,ignore
//! use synapse_etl_unit::{BoundSource, SourceBinder, ColumnNameExt};
//!
//! struct ScadaBinder;
//!
//! impl SourceBinder for ScadaBinder {
//!     fn bind(&self, df: polars::prelude::DataFrame) -> BoundSource {
//!         BoundSource::new("scada", df)
//!             .map("station_name".canonical(), "station_name".source())
//!             .map("timestamp".canonical(), "observation_time".source())
//!             .map("sump".canonical(), "sump".source())
//!             // ...
//!     }
//! }
//! ```

use polars::prelude::DataFrame;

use crate::source::BoundSource;

/// Maps a fetched + normalized DataFrame to a [`BoundSource`] for ETL processing.
///
/// Data crates implement this to specify how their physical columns map
/// to canonical ETL names, configure unpivots, and declare which ETL
/// units the source provides.
///
/// Each `SourceBinder` implementation is paired with a `SourceFactory`
/// (from `synapse-data-source`) and a [`SourceChain`]. The chain
/// fetches and concatenates data, then the binder maps it to the ETL
/// schema.
///
/// # Contract
///
/// The `bind()` method must:
/// 1. Create a `BoundSource` with a stable name
/// 2. Map all relevant columns (canonical ← source)
/// 3. Configure unpivots if needed (e.g., engine columns → engine_status)
/// 4. Declare which units it provides via `.provides()`
pub trait SourceBinder: Send + Sync {
	/// Bind a fetched + normalized DataFrame to a `BoundSource`.
	fn bind(&self, df: DataFrame) -> BoundSource;
}
