//! Time Range
//! NOTE: This is both a source and canonical level scope.
use polars::prelude::*;
use tracing::debug;

use crate::{EtlError, EtlResult, EtlTimeRange, MeasurementUnit};

/// Extract time range from a DataFrame using a MeasurementUnit.
///
/// This is the high-level API that extracts time column and format from the measurement.
pub fn extract_time_range(
	df: &DataFrame,
	measurement: &MeasurementUnit,
) -> EtlResult<EtlTimeRange> {
	let time_col = measurement.time.as_str();
	let time_format = measurement
		.signal_policy
		.as_ref()
		.and_then(|p| p.time_format.clone());

	extract_time_range_from_parts(df, time_col, time_format.as_deref())
}

/// Extract time range from a DataFrame using primitive arguments.
///
/// This is the low-level API useful for tests and callers without a MeasurementUnit.
///
/// # Arguments
/// * `df` - The DataFrame to analyze
/// * `time_col` - The name of the time column
/// * `time_format` - Optional chrono format string (e.g., "%Y-%m-%d %H:%M:%S")
///
/// # Logic
/// * If `time_format` is provided AND column is String: Parses to Datetime using pattern.
/// * If `time_format` is provided AND column is already Datetime: Ignores format, uses column directly.
/// * If `time_format` is None: Expects Datetime or numeric (i64) column.
pub fn extract_time_range_from_parts(
	df: &DataFrame,
	time_col: &str,
	time_format: Option<&str>,
) -> EtlResult<EtlTimeRange> {
	// 1. Get the Series
	let series = df
		.column(time_col)
		.map_err(|e| EtlError::DataProcessing(format!("Time column '{}' missing: {}", time_col, e)))?
		.as_materialized_series();

	let dtype = series.dtype();

	// 2. Branch Logic based on configuration vs reality
	let (min, max, unit_label) = match time_format {
		// --- PATH A: time_format provided ---
		Some(fmt) => {
			match dtype {
				// String column: parse using the provided format
				DataType::String => {
					debug!(
						time_col = time_col,
						time_format = fmt,
						"Parsing string time column to datetime"
					);

					// Use Lazy API to safely access 'to_datetime' expression
					let options = StrptimeOptions {
						format: Some(PlSmallStr::from_str(fmt)),
						strict: false, // Allow some flexibility
						..Default::default()
					};

					let parsed_df = df
						.clone()
						.lazy()
						.select([col(time_col)
							.str()
							.to_datetime(
								Some(TimeUnit::Milliseconds), // Force standardizing to ms
								None,                         // No timezone
								options,
								lit("raise"), // Fail on bad format
							)
							.alias("parsed_time")])
						.collect()
						.map_err(|e| {
							EtlError::DataProcessing(format!("Failed to parse time string: {}", e))
						})?;

					let parsed_series = parsed_df
						.column("parsed_time")
						.map_err(|_| EtlError::DataProcessing("Parsing produced no column".into()))?
						.as_materialized_series();

					// Extract Min/Max from the parsed datetime
					let ca = parsed_series
						.datetime()
						.map_err(|_| EtlError::DataProcessing("Parsed column is not datetime".into()))?;
					let phys = ca.physical();

					let min = phys
						.min()
						.ok_or_else(|| EtlError::DataProcessing("Time column is empty".into()))?;
					let max = phys
						.max()
						.ok_or_else(|| EtlError::DataProcessing("Time column is empty".into()))?;

					// Since we forced TimeUnit::Milliseconds above, these are already ms values
					(min, max, "String (Parsed)".to_string())
				}

				// Datetime column: ignore time_format, use directly
				DataType::Datetime(tu, _) => {
					debug!(
						time_col = time_col,
						dtype = ?dtype,
						time_format = fmt,
						"Time column is already Datetime, ignoring time_format"
					);

					let conversion = match tu {
						TimeUnit::Nanoseconds => 1_000_000,
						TimeUnit::Microseconds => 1_000,
						TimeUnit::Milliseconds => 1,
					};

					let ca = series.datetime().unwrap();
					let phys = ca.physical(); // Get i64 representation

					let min = phys
						.min()
						.ok_or_else(|| EtlError::DataProcessing("Time column is empty".into()))?;
					let max = phys
						.max()
						.ok_or_else(|| EtlError::DataProcessing("Time column is empty".into()))?;

					(min / conversion, max / conversion, format!("{:?}", tu))
				}

				// Other types with time_format: error
				_ => {
					return Err(EtlError::DataProcessing(format!(
						"Time format '{}' provided, but column '{}' is type {:?} (expected String or Datetime)",
						fmt, time_col, dtype
					)));
				}
			}
		}

		// --- PATH B: No parsing (Numeric/Datetime input) ---
		None => {
			match dtype {
				DataType::Datetime(tu, _) => {
					let conversion = match tu {
						TimeUnit::Nanoseconds => 1_000_000,
						TimeUnit::Microseconds => 1_000,
						TimeUnit::Milliseconds => 1,
					};

					let ca = series.datetime().unwrap();
					let phys = ca.physical(); // Get i64 representation

					let min = phys
						.min()
						.ok_or_else(|| EtlError::DataProcessing("Time column is empty".into()))?;
					let max = phys
						.max()
						.ok_or_else(|| EtlError::DataProcessing("Time column is empty".into()))?;

					(min / conversion, max / conversion, format!("{:?}", tu))
				}

				DataType::Int64 => {
					let ca = series.i64().unwrap();
					let min = ca
						.min()
						.ok_or_else(|| EtlError::DataProcessing("Time column is empty".into()))?;
					let max = ca
						.max()
						.ok_or_else(|| EtlError::DataProcessing("Time column is empty".into()))?;

					// Assume Milliseconds for raw integers if not specified
					(min, max, "i64 (assumed ms)".into())
				}

				// Reject Strings if no format provided
				DataType::String => {
					return Err(EtlError::DataProcessing(format!(
						"Column '{}' is String but no time_format was provided. Cannot infer time.",
						time_col
					)));
				}

				_ => {
					return Err(EtlError::DataProcessing(format!(
						"Unsupported time column type: {:?}",
						dtype
					)));
				}
			}
		}
	};

	let duration_ms = (max - min).max(0) as u64;

	Ok(EtlTimeRange {
		start_ts: min,
		end_ts: max,
		duration_ms,
		duration_human: format_duration_human(duration_ms),
		source_unit: unit_label,
	})
}

/// Helper to format duration logic
fn format_duration_human(millis: u64) -> String {
	let secs = millis / 1000;
	if secs == 0 {
		return format!("{}ms", millis);
	}

	let units = [(86400, "d"), (3600, "h"), (60, "m")];

	for (divisor, label) in units {
		if secs >= divisor {
			let val = secs / divisor;
			let remainder = secs % divisor;
			if remainder == 0 {
				return format!("{}{}", val, label);
			} else {
				let next_unit_secs = if divisor == 86400 {
					3600
				} else {
					60
				};
				if divisor == 60 {
					return format!("{}m {}s", val, remainder);
				}
				let rem_val = remainder / next_unit_secs;
				return format!(
					"{}{}{}",
					val,
					label,
					if rem_val > 0 {
						format!(
							" {}{}",
							rem_val,
							if divisor == 86400 {
								"h"
							} else {
								"m"
							}
						)
					} else {
						"".into()
					}
				);
			}
		}
	}

	format!("{}s", secs)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
	use super::*;

	fn make_datetime_df() -> DataFrame {
		let timestamps: Vec<i64> = vec![0, 30_000, 60_000];
		df! {
			"ts" => &timestamps
		}
		.unwrap()
		.lazy()
		.with_column(col("ts").cast(DataType::Datetime(TimeUnit::Milliseconds, None)))
		.collect()
		.unwrap()
	}

	fn make_string_df() -> DataFrame {
		df! {
			"ts" => &["2025-01-01T00:00:00Z", "2025-01-01T00:01:00Z", "2025-01-01T00:02:00Z"]
		}
		.unwrap()
	}

	#[test]
	fn test_datetime_column_no_format() {
		let df = make_datetime_df();
		let result = extract_time_range_from_parts(&df, "ts", None);
		assert!(result.is_ok());
		let range = result.unwrap();
		assert_eq!(range.start_ts, 0);
		assert_eq!(range.end_ts, 60_000);
	}

	#[test]
	fn test_datetime_column_with_format_ignores_format() {
		let df = make_datetime_df();
		// Provide a format even though column is already Datetime
		// Should succeed by ignoring the format
		let result = extract_time_range_from_parts(&df, "ts", Some("%Y-%m-%dT%H:%M:%SZ"));
		assert!(result.is_ok(), "Should gracefully ignore time_format when column is Datetime");
		let range = result.unwrap();
		assert_eq!(range.start_ts, 0);
		assert_eq!(range.end_ts, 60_000);
	}

	#[test]
	fn test_string_column_with_format_parses() {
		let df = make_string_df();
		let result = extract_time_range_from_parts(&df, "ts", Some("%Y-%m-%dT%H:%M:%SZ"));
		assert!(result.is_ok(), "Should parse string column with format");
		let range = result.unwrap();
		assert!(range.duration_ms > 0);
	}

	#[test]
	fn test_string_column_without_format_errors() {
		let df = make_string_df();
		let result = extract_time_range_from_parts(&df, "ts", None);
		assert!(result.is_err(), "Should error when string column has no format");
		assert!(result.unwrap_err().to_string().contains("no time_format"));
	}
}
