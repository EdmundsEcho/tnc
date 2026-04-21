//! Signal Policy Application Functions
//!
//! Functions for applying signal policies (Instant, Sliding, Tumbling windowing)
//! to measurement columns in a DataFrame.
//!
//! These functions are designed to work with `MeasurementUnit` which contains:
//! - `name: String` - the canonical measurement name (value column)
//! - `components: Vec<ComponentColumn>` - component dimensions (each has `canonical_name`)
//! - `signal_policy: Option<SignalPolicy>` - the policy to apply
//! - `signal_aggregation()` method - returns the aggregation (override or default from kind)

mod instant_policy_fn;
mod meta;
mod sliding_policy_fn;
mod time_range;
mod tumbling_policy_fn;
mod validation;

// re-export of static functions
pub(crate) use instant_policy_fn::apply_instant_policy;
pub(crate) use instant_policy_fn::apply_instant_policy_from_parts;
// other re-exports
pub(crate) use meta::SignalPolicyStats;
use polars::prelude::*;
pub(crate) use sliding_policy_fn::apply_sliding_policy;
pub(crate) use time_range::{extract_time_range, extract_time_range_from_parts};
use tracing::info;
use tracing::instrument;
pub(crate) use tumbling_policy_fn::apply_tumbling_policy;

use crate::{EtlResult, signal_policy::WindowStrategy, unit::MeasurementUnit};

/// Apply a signal policy to a single measurement column.
///
/// Dispatches to the appropriate windowing strategy (Instant, Sliding, or Tumbling).
/// Returns the transformed DataFrame and stats about the transformation.
#[instrument(skip(df, measurement), fields(measurement = %measurement.name))]
pub(crate) fn apply_signal_policy(
	df: DataFrame,
	measurement: &MeasurementUnit,
	source_name: &str,
) -> EtlResult<(DataFrame, Option<SignalPolicyStats>)> {
	info!("🟢 ROOT Applying signal policy to measurement '{}'", measurement.name);
	let Some(ref policy) = measurement.signal_policy else {
		info!("🟡 Empty policy; Return early");
		return Ok((df, None));
	};

	// 1. Calculate Input Stats
	let input_points = df.height();
	let policy_type = policy.windowing.name();
	let ttl_ms = policy.ttl().as_millis() as u64;

	// 2. Calculate Time Range (Input)
	// We do this before processing because processing might align/truncate the time grid
	let time_range = extract_time_range(&df, measurement)?;
	let time_span_ms = time_range.duration_ms;

	// 3. Dispatch Policy Execution
	// (Assuming apply_*_policy functions are imported or available in this module scope)
	let result_df = match &policy.windowing {
		WindowStrategy::Instant => {
			crate::polars_fns::signal_policy::apply_instant_policy(df, measurement)?
		}
		WindowStrategy::Sliding {
			duration,
			min_samples,
		} => apply_sliding_policy(df, measurement, *duration, *min_samples)?,
		WindowStrategy::Tumbling {
			duration,
			min_samples,
		} => apply_tumbling_policy(df, measurement, *duration, *min_samples)?,
	};

	// 4. Calculate Output Stats
	let grid_points = result_df.height();
	let value_col = result_df.column(measurement.name.as_str())?;
	let null_observations = value_col.null_count();

	let stats = SignalPolicyStats::new(
		measurement.name.clone(),
		source_name,
		policy_type,
		input_points,
		grid_points,
		null_observations,
		ttl_ms,
		time_span_ms,
	);

	Ok((result_df, Some(stats)))
}

// Note: If your codebase uses CanonicalColumnName as a newtype wrapper,
// adjust the component access accordingly. This implementation assumes
// ComponentColumn has a `canonical_name: String` field.

// =============================================================================
// Helper Functions
// =============================================================================

/// Get the value column name from a MeasurementUnit.
///
/// Adjust this function if your MeasurementUnit has a different accessor
/// (e.g., `value_column()` method or `value.canonical_name()`).
#[inline]
pub(crate) fn get_value_column(measurement: &MeasurementUnit) -> &str {
	// Option 1: Use measurement.name (common pattern where name == value column)
	&measurement.name

	// Option 2: If your MeasurementUnit has a value field with canonical_name():
	// measurement.value.canonical_name()

	// Option 3: If your MeasurementUnit has a value_column() method:
	// measurement.value_column()
}

/// Get component column names from a MeasurementUnit.
#[inline]
pub(crate) fn get_component_names(measurement: &MeasurementUnit) -> Vec<&str> {
	measurement.components.iter().map(|c| c.as_str()).collect()
}

