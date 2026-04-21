use std::time::Duration as StdDuration;

use polars::prelude::*;
use tracing::{debug, instrument};

use crate::{EtlResult, aggregation::Aggregate, unit::MeasurementUnit};

// =============================================================================
// Tumbling Policy
// =============================================================================

/// Apply Tumbling window policy: fixed time buckets with aggregation.
///
/// Groups data into non-overlapping time buckets and aggregates within each.
/// If fewer than `min_samples` values are in a bucket, the result is null.
#[instrument(skip(df, measurement), fields(measurement = %measurement.name))]
pub(crate) fn apply_tumbling_policy(
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
	let bucket_us = duration.as_micros() as i64;

	// Capture input columns for verification
	let input_columns: Vec<String> = df
		.get_column_names()
		.iter()
		.map(|s| s.to_string())
		.collect();

	debug!(
		 input_columns = ?input_columns,
		 bucket_us = bucket_us,
		 min_samples = min_samples,
		 "Applying tumbling policy"
	);

	// Build group-by columns: subject + time_bucket + components
	let mut group_cols: Vec<Expr> = vec![
		col(subject_col),
		// Create time bucket: floor(time / bucket_size) * bucket_size
		(col(time_col).dt().timestamp(TimeUnit::Microseconds) / lit(bucket_us) * lit(bucket_us))
			.cast(DataType::Datetime(TimeUnit::Microseconds, None))
			.alias("_time_bucket"),
	];

	for comp_name in &component_names {
		group_cols.push(col(*comp_name));
	}

	// Build aggregation expression
	let agg_expr = match aggregation {
		Aggregate::Mean => col(value_col).mean(),
		Aggregate::Sum => col(value_col).sum(),
		Aggregate::Min => col(value_col).min(),
		Aggregate::Max => col(value_col).max(),
		Aggregate::Last => col(value_col).last(),
		Aggregate::First => col(value_col).first(),
		Aggregate::Count => col(value_col).count().cast(DataType::Float64),
		Aggregate::Any => col(value_col).max(), // max of 0/1 = any true
		Aggregate::All => col(value_col).min(), // min of 0/1 = all true
		_ => col(value_col).mean(),             // Fallback
	}
	.alias(value_col);

	let count_expr = col(value_col).count().alias("_sample_count");

	// Get other columns to preserve (first value in each bucket)
	// These are columns that are NOT: subject, time, value, or components
	let preserved_cols: Vec<Expr> = df
		.get_column_names()
		.iter()
		.filter(|c| {
			let c_str = c.as_str();
			c_str != subject_col &&
				c_str != time_col &&
				c_str != value_col &&
				!component_names.contains(&c_str)
		})
		.map(|c| col(c.as_str()).first().alias(c.as_str()))
		.collect();

	debug!(preserved_cols_count = preserved_cols.len(), "Preserving additional columns");

	// Build full aggregation list
	let mut agg_list = vec![agg_expr, count_expr];
	agg_list.extend(preserved_cols);

	let original_rows = df.height();

	// Apply grouping and aggregation
	let aggregated = df
        .lazy()
        .group_by(group_cols)
        .agg(agg_list)
        // Null out values where sample count < min_samples
        .with_column(
            when(col("_sample_count").lt(lit(min_samples as i64)))
                .then(lit(NULL))
                .otherwise(col(value_col))
                .alias(value_col),
        )
        // Rename time bucket back to time column
        .with_column(col("_time_bucket").alias(time_col))
        .drop(cols(["_time_bucket", "_sample_count"]))
        .sort([subject_col, time_col], SortMultipleOptions::default())
        .collect()?;

	let output_columns: Vec<String> = aggregated
		.get_column_names()
		.iter()
		.map(|s| s.to_string())
		.collect();

	debug!(
		 original_rows,
		 result_rows = aggregated.height(),
		 output_columns = ?output_columns,
		 "Tumbling policy applied"
	);

	// Verify column preservation (tumbling reduces rows but should keep columns)
	// Note: Column order may differ due to group_by, but all columns should be present
	let input_set: std::collections::HashSet<_> = input_columns.iter().collect();
	let output_set: std::collections::HashSet<_> = output_columns.iter().collect();

	assert_eq!(
		input_set, output_set,
		"Tumbling policy changed columns! Input: {:?}, Output: {:?}",
		input_columns, output_columns
	);

	Ok(aggregated)
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use super::*;
	use crate::{MeasurementKind, MeasurementUnit, signal_policy::SignalPolicy};

	#[test]
	fn test_tumbling_policy_preserves_columns() {
		let measurement =
			MeasurementUnit::new("station", "timestamp", "value", MeasurementKind::Measure)
				.with_signal_policy(SignalPolicy::tumbling(Duration::from_secs(30), 2));

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

		let input_cols: Vec<String> = df
			.get_column_names()
			.iter()
			.map(|s| s.to_string())
			.collect();
		let result = apply_tumbling_policy(df, &measurement, Duration::from_secs(30), 2).unwrap();
		let output_cols: Vec<String> = result
			.get_column_names()
			.iter()
			.map(|s| s.to_string())
			.collect();

		// Same columns (order may differ)
		let input_set: std::collections::HashSet<_> = input_cols.iter().collect();
		let output_set: std::collections::HashSet<_> = output_cols.iter().collect();
		assert_eq!(input_set, output_set);

		// Verify specific columns
		assert!(result.column("station").is_ok());
		assert!(result.column("timestamp").is_ok());
		assert!(result.column("value").is_ok());

		// Tumbling should reduce row count (10 rows -> fewer buckets)
		assert!(result.height() < 10);
	}

	#[test]
	fn test_tumbling_policy_with_extra_columns() {
		let measurement =
			MeasurementUnit::new("station", "timestamp", "value", MeasurementKind::Measure)
				.with_signal_policy(SignalPolicy::tumbling(Duration::from_secs(30), 2));

		let timestamps: Vec<i64> = (0..6).map(|i| 1000000i64 + i * 6_000_000).collect();

		let df = df! {
			 "station" => vec!["A"; 6],
			 "timestamp" => timestamps,
			 "value" => vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
			 "extra_col" => vec!["x", "x", "x", "x", "x", "x"]
		}
		.unwrap()
		.lazy()
		.with_column(col("timestamp").cast(DataType::Datetime(TimeUnit::Microseconds, None)))
		.collect()
		.unwrap();

		let result = apply_tumbling_policy(df, &measurement, Duration::from_secs(30), 2).unwrap();

		// Extra column should be preserved
		assert!(result.column("extra_col").is_ok());
	}
}
