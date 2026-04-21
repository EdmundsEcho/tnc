use std::time::Duration as StdDuration;

use polars::prelude::*;
use tracing::{debug, instrument};

use crate::{EtlResult, aggregation::Aggregate, unit::MeasurementUnit};

// =============================================================================
// Sliding Policy
// =============================================================================

/// Apply Sliding window policy: rolling aggregation over duration.
///
/// Applies a rolling window aggregation partitioned by subject and components.
/// If fewer than `min_samples` values are in the window, the result is null.
#[instrument(skip(df, measurement), fields(measurement = %measurement.name))]
pub(crate) fn apply_sliding_policy(
	df: DataFrame,
	measurement: &MeasurementUnit,
	duration: StdDuration,
	min_samples: u32,
) -> EtlResult<DataFrame> {
	let subject_col = measurement.subject.as_str();
	let time_col = measurement.time.as_str();
	let value_col = super::get_value_column(measurement);
	let component_names = super::get_component_names(measurement);
	let aggregation = measurement.signal_aggregation();
	let window_size = duration.as_micros() as usize;

	// Capture input columns for verification
	let input_columns: Vec<String> = df
		.get_column_names()
		.iter()
		.map(|s| s.to_string())
		.collect();
	let input_width = df.width();

	debug!(
		 input_columns = ?input_columns,
		 window_us = window_size,
		 min_samples = min_samples,
		 "Applying sliding policy"
	);

	// Build partition columns: subject + components
	let mut partition_exprs: Vec<Expr> = vec![col(subject_col)];
	for comp_name in &component_names {
		partition_exprs.push(col(*comp_name));
	}

	// Sort by partition columns + time
	let mut sort_cols: Vec<&str> = vec![subject_col];
	sort_cols.extend(component_names.iter().copied());
	sort_cols.push(time_col);

	let sorted_df = df
		.clone()
		.lazy()
		.sort(sort_cols.clone(), SortMultipleOptions::default())
		.collect()?;

	// Build rolling options
	let rolling_opts = RollingOptionsFixedWindow {
		window_size,
		min_periods: min_samples as usize,
		..Default::default()
	};

	// Build aggregation expression based on type
	let agg_expr = match aggregation {
		Aggregate::Mean => col(value_col).rolling_mean(rolling_opts),
		Aggregate::Sum => col(value_col).rolling_sum(rolling_opts),
		Aggregate::Min => col(value_col).rolling_min(rolling_opts),
		Aggregate::Max => col(value_col).rolling_max(rolling_opts),
		Aggregate::Last => {
			// Polars doesn't have rolling_last - current value after sorting
			col(value_col)
		}
		Aggregate::First => col(value_col).shift(lit((window_size - 1) as i64)),
		Aggregate::Any => col(value_col).rolling_max(rolling_opts), // max of 0/1 = any true
		Aggregate::All => col(value_col).rolling_min(rolling_opts), // min of 0/1 = all true
		Aggregate::Count => {
			col(value_col)
				.is_not_null()
				.cast(DataType::UInt32)
				.rolling_sum(rolling_opts)
				.cast(DataType::Float64)
		}
		_ => col(value_col).rolling_mean(rolling_opts), // Fallback
	}
	.over(&partition_exprs)
	.alias("_signal_value");

	// Apply rolling aggregation
	let result = sorted_df
		.lazy()
		.with_column(agg_expr)
		.with_column(col("_signal_value").alias(value_col))
		.drop(cols(["_signal_value"]))
		.collect()?;

	let output_columns: Vec<String> = result
		.get_column_names()
		.iter()
		.map(|s| s.to_string())
		.collect();

	debug!(
		 rows = result.height(),
		 output_columns = ?output_columns,
		 "Sliding policy applied"
	);

	// Verify column preservation
	assert_eq!(
		input_width,
		result.width(),
		"Sliding policy changed column count! Input: {:?}, Output: {:?}",
		input_columns,
		output_columns
	);

	Ok(result)
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use super::*;
	use crate::{MeasurementKind, MeasurementUnit, signal_policy::SignalPolicy};

	#[test]
	fn test_sliding_policy_preserves_columns() {
		let measurement =
			MeasurementUnit::new("station", "timestamp", "value", MeasurementKind::Measure)
				.with_signal_policy(SignalPolicy::sliding(Duration::from_secs(30), 3));

		let timestamps: Vec<i64> = (0..10).map(|i| 1000000i64 + i * 6_000_000).collect();

		let df = df! {
			 "station" => vec!["A"; 10],
			 "timestamp" => timestamps,
			 "value" => vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
		}
		.unwrap()
		.lazy()
		.with_column(col("timestamp").cast(DataType::Datetime(TimeUnit::Microseconds, None)))
		.collect()
		.unwrap();

		let input_width = df.width();
		let result = apply_sliding_policy(df, &measurement, Duration::from_secs(30), 3).unwrap();

		assert_eq!(input_width, result.width());
		assert!(result.column("station").is_ok());
		assert!(result.column("timestamp").is_ok());
		assert!(result.column("value").is_ok());
	}
}
