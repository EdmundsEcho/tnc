//! Time Range
//! NOTE: This is both a source and canonical level scope.
use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

use crate::{EtlResult, MeasurementUnit};

/// Represents the temporal bounds of a dataset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EtlTimeRange {
	/// Start timestamp (epoch milliseconds)
	pub start_ts:       i64,
	/// End timestamp (epoch milliseconds)
	pub end_ts:         i64,
	/// Duration in milliseconds
	pub duration_ms:    u64,
	/// Human-readable duration (e.g., "2h 5m")
	pub duration_human: String,
	/// The unit detected/used for the source timestamps
	pub source_unit:    String,
}

impl EtlTimeRange {
	/// Uses the signal policy to extract time range
	/// ... depends on accessing the time format string
	pub fn extract_time_range(
		df: &DataFrame,
		measurement: &MeasurementUnit,
	) -> EtlResult<EtlTimeRange> {
		crate::polars_fns::extract_time_range(df, measurement)
	}

	pub fn extract_time_range_from_parts(
		df: &DataFrame,
		time_col: &str,
		time_format: Option<&str>,
	) -> EtlResult<EtlTimeRange> {
		crate::polars_fns::extract_time_range_from_parts(df, time_col, time_format)
	}
}
