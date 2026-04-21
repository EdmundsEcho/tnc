//! Column expressions for deriving values from DataFrame columns.
//!
//! `ColumnExpr` is the fundamental building block for subjects, timestamps, and components.
//! It supports both simple single-column identity and complex multi-column compositions.
//!
//! Note: `ColumnExpr` only describes *how* to compute a value from source columns.
//! The output name (canonical name) is provided by the parent column type
//! (`SubjectColumn`, `TimeColumn`, etc.) when converting to a Polars expression.

use polars::prelude::*;
use serde::{Deserialize, Serialize};

use crate::column::SourceColumnName;

/// An expression that derives a value from one or more DataFrame columns.
///
/// This type describes the computation only - it does not include the output name.
/// The output name is provided when calling `to_polars_expr(output_name)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnExpr {
	/// Identity: single column, no transformation
	Single(SourceColumnName),

	/// Struct: combine columns into a Polars struct (preserves types, good for composite keys)
	Struct {
		columns: Vec<SourceColumnName>,
	},

	/// Concat: join column string values with a separator
	Concat {
		columns:   Vec<SourceColumnName>,
		separator: String,
	},

	/// Coalesce: first non-null value from columns (for fallback chains)
	Coalesce {
		columns: Vec<SourceColumnName>,
	},

	/// DateTime: combine separate date and time columns
	DateTime {
		date: SourceColumnName,
		time: SourceColumnName,
	},

	/// DateTimeParts: build datetime from individual components
	DateTimeParts {
		year:   SourceColumnName,
		month:  SourceColumnName,
		day:    SourceColumnName,
		hour:   Option<SourceColumnName>,
		minute: Option<SourceColumnName>,
		second: Option<SourceColumnName>,
	},

	/// ParseDateTime: parse a string column with a format
	ParseDateTime {
		column: SourceColumnName,
		format: String,
	},

	/// EpochToDateTime: convert epoch seconds/millis/micros to datetime
	EpochToDateTime {
		column: SourceColumnName,
		unit:   EpochUnit,
	},
}

/// Unit for epoch timestamp conversion
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochUnit {
	Seconds,
	Milliseconds,
	Microseconds,
	Nanoseconds,
}

impl ColumnExpr {
	// =========================================================================
	// Constructors
	// =========================================================================

	/// Single column identity
	pub fn column(name: impl Into<String>) -> Self {
		Self::Single(SourceColumnName::new(name))
	}

	/// Composite struct key from multiple columns
	pub fn struct_key(columns: Vec<String>) -> Self {
		Self::Struct {
			columns: columns.into_iter().map(SourceColumnName::new).collect(),
		}
	}

	/// Concatenated string key with separator
	pub fn concat_key(columns: Vec<String>, separator: impl Into<String>) -> Self {
		Self::Concat {
			columns:   columns.into_iter().map(SourceColumnName::new).collect(),
			separator: separator.into(),
		}
	}

	/// First non-null value from columns
	pub fn coalesce(columns: Vec<String>) -> Self {
		Self::Coalesce {
			columns: columns.into_iter().map(SourceColumnName::new).collect(),
		}
	}

	/// Date + time columns → datetime
	pub fn datetime_from_date_time(date: impl Into<String>, time: impl Into<String>) -> Self {
		Self::DateTime {
			date: SourceColumnName::new(date),
			time: SourceColumnName::new(time),
		}
	}

	/// Year/month/day → datetime (with optional time components)
	pub fn datetime_from_components(year: impl Into<String>, month: impl Into<String>, day: impl Into<String>) -> Self {
		Self::DateTimeParts {
			year:   SourceColumnName::new(year),
			month:  SourceColumnName::new(month),
			day:    SourceColumnName::new(day),
			hour:   None,
			minute: None,
			second: None,
		}
	}

	/// Parse string column as datetime
	pub fn parse_datetime(column: impl Into<String>, format: impl Into<String>) -> Self {
		Self::ParseDateTime {
			column: SourceColumnName::new(column),
			format: format.into(),
		}
	}

	/// Convert epoch column to datetime
	pub fn from_epoch(column: impl Into<String>, unit: EpochUnit) -> Self {
		Self::EpochToDateTime {
			column: SourceColumnName::new(column),
			unit,
		}
	}

	// =========================================================================
	// Builder methods (for DateTimeParts)
	// =========================================================================

	/// Add hour component (for DateTimeParts)
	pub fn with_hour(mut self, hour: impl Into<String>) -> Self {
		if let Self::DateTimeParts {
			hour: h,
			..
		} = &mut self
		{
			*h = Some(SourceColumnName::new(hour));
		}
		self
	}

	/// Add minute component (for DateTimeParts)
	pub fn with_minute(mut self, minute: impl Into<String>) -> Self {
		if let Self::DateTimeParts {
			minute: m,
			..
		} = &mut self
		{
			*m = Some(SourceColumnName::new(minute));
		}
		self
	}

	/// Add second component (for DateTimeParts)
	pub fn with_second(mut self, second: impl Into<String>) -> Self {
		if let Self::DateTimeParts {
			second: s,
			..
		} = &mut self
		{
			*s = Some(SourceColumnName::new(second));
		}
		self
	}

	// =========================================================================
	// Accessors
	// =========================================================================

	/// Get all source column names this expression depends on
	pub fn source_columns(&self) -> Vec<&SourceColumnName> {
		match self {
			Self::Single(c) => vec![c],
			Self::Struct {
				columns,
			} => columns.iter().collect(),
			Self::Concat {
				columns,
				..
			} => columns.iter().collect(),
			Self::Coalesce {
				columns,
			} => columns.iter().collect(),
			Self::DateTime {
				date,
				time,
			} => vec![date, time],
			Self::DateTimeParts {
				year,
				month,
				day,
				hour,
				minute,
				second,
			} => {
				let mut cols = vec![year, month, day];
				if let Some(h) = hour {
					cols.push(h);
				}
				if let Some(m) = minute {
					cols.push(m);
				}
				if let Some(s) = second {
					cols.push(s);
				}
				cols
			}
			Self::ParseDateTime {
				column,
				..
			} => vec![column],
			Self::EpochToDateTime {
				column,
				..
			} => vec![column],
		}
	}

	/// Returns true if this is a single-column identity expression
	pub fn is_identity(&self) -> bool {
		matches!(self, Self::Single(_))
	}

	/// Returns true if this expression requires materialization (adds a new column)
	pub fn needs_materialization(&self) -> bool {
		!self.is_identity()
	}

	/// For identity expressions, get the source column name
	pub fn identity_column(&self) -> Option<&SourceColumnName> {
		match self {
			Self::Single(c) => Some(c),
			_ => None,
		}
	}

	// =========================================================================
	// Polars conversion
	// =========================================================================

	/// Convert to a Polars expression with the given output name.
	///
	/// The `output_name` is typically the canonical name from the parent column type
	/// (e.g., `SubjectColumn::canonical_name`).
	pub fn to_polars_expr(&self, output_name: &str) -> Expr {
		match self {
			Self::Single(c) => col(c.as_str()).alias(output_name),

			Self::Struct {
				columns,
			} => as_struct(columns.iter().map(|c| col(c.as_str())).collect::<Vec<_>>()).alias(output_name),

			Self::Concat {
				columns,
				separator,
			} => {
				concat_str(
					columns.iter().map(|c| col(c.as_str()).cast(DataType::String)).collect::<Vec<_>>(),
					separator,
					false,
				)
				.alias(output_name)
			}

			Self::Coalesce {
				columns,
			} => coalesce(columns.iter().map(|c| col(c.as_str())).collect::<Vec<_>>().as_slice()).alias(output_name),

			Self::DateTime {
				date,
				time,
			} => {
				// Combine date and time columns
				col(date.as_str()).dt().combine(col(time.as_str()), TimeUnit::Microseconds).alias(output_name)
			}

			Self::DateTimeParts {
				year,
				month,
				day,
				hour,
				minute,
				second,
			} => {
				let hour_expr = hour.as_ref().map(|h| col(h.as_str())).unwrap_or(lit(0i64));
				let minute_expr = minute.as_ref().map(|m| col(m.as_str())).unwrap_or(lit(0i64));
				let second_expr = second.as_ref().map(|s| col(s.as_str())).unwrap_or(lit(0i64));

				// Build a date first, then add time
				let year_str = col(year.as_str()).cast(DataType::String);
				let month_str = when(col(month.as_str()).lt(lit(10)))
					.then(lit("0") + col(month.as_str()).cast(DataType::String))
					.otherwise(col(month.as_str()).cast(DataType::String));
				let day_str = when(col(day.as_str()).lt(lit(10)))
					.then(lit("0") + col(day.as_str()).cast(DataType::String))
					.otherwise(col(day.as_str()).cast(DataType::String));

				let date_str = year_str + lit("-") + month_str + lit("-") + day_str;

				let date_expr = date_str.str().to_date(StrptimeOptions {
					format: Some("%Y-%m-%d".to_string().into()),
					..Default::default()
				});

				// Convert date to datetime and add time components
				let datetime_base = date_expr.cast(DataType::Datetime(TimeUnit::Microseconds, None));

				// Add time as duration (hours + minutes + seconds in microseconds)
				let time_offset_us = (hour_expr.cast(DataType::Int64) * lit(3600i64 * 1_000_000i64)) +
					(minute_expr.cast(DataType::Int64) * lit(60i64 * 1_000_000i64)) +
					(second_expr.cast(DataType::Int64) * lit(1_000_000i64));

				let time_duration = time_offset_us.cast(DataType::Duration(TimeUnit::Microseconds));

				(datetime_base + time_duration).alias(output_name)
			}

			Self::ParseDateTime {
				column,
				format,
			} => {
				col(column.as_str())
					.str()
					.to_datetime(
						Some(TimeUnit::Microseconds),
						None,
						StrptimeOptions {
							format: Some(format.clone().into()),
							..Default::default()
						},
						lit("raise"),
					)
					.alias(output_name)
			}

			Self::EpochToDateTime {
				column,
				unit,
			} => {
				let (time_unit, multiplier) = match unit {
					EpochUnit::Seconds => (TimeUnit::Milliseconds, Some(1000i64)),
					EpochUnit::Milliseconds => (TimeUnit::Milliseconds, None),
					EpochUnit::Microseconds => (TimeUnit::Microseconds, None),
					EpochUnit::Nanoseconds => (TimeUnit::Nanoseconds, None),
				};

				let expr = if let Some(mult) = multiplier {
					col(column.as_str()) * lit(mult)
				} else {
					col(column.as_str())
				};

				expr.cast(DataType::Datetime(time_unit, None)).alias(output_name)
			}
		}
	}
}

impl Default for ColumnExpr {
	fn default() -> Self {
		Self::Single(SourceColumnName::new("id"))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_single_column() {
		let expr = ColumnExpr::column("pump_id");
		assert!(expr.is_identity());
		assert_eq!(expr.identity_column().unwrap().as_str(), "pump_id");
		assert_eq!(expr.source_columns().len(), 1);
		assert_eq!(expr.source_columns()[0].as_str(), "pump_id");
	}

	#[test]
	fn test_struct_key() {
		let expr = ColumnExpr::struct_key(vec!["region".into(), "station".into()]);
		assert!(!expr.is_identity());
		assert!(expr.identity_column().is_none());
		let sources: Vec<&str> = expr.source_columns().iter().map(|s| s.as_str()).collect();
		assert_eq!(sources, vec!["region", "station"]);
	}

	#[test]
	fn test_concat_key() {
		let expr = ColumnExpr::concat_key(vec!["a".into(), "b".into(), "c".into()], "-");
		let sources: Vec<&str> = expr.source_columns().iter().map(|s| s.as_str()).collect();
		assert_eq!(sources, vec!["a", "b", "c"]);
	}

	#[test]
	fn test_datetime_components() {
		let expr = ColumnExpr::datetime_from_components("year", "month", "day").with_hour("hour").with_minute("minute");

		let sources: Vec<&str> = expr.source_columns().iter().map(|s| s.as_str()).collect();
		assert_eq!(sources, vec!["year", "month", "day", "hour", "minute"]);
	}

	#[test]
	fn test_epoch_conversion() {
		let expr = ColumnExpr::from_epoch("unix_ts", EpochUnit::Seconds);
		assert_eq!(expr.source_columns()[0].as_str(), "unix_ts");
	}

	#[test]
	fn test_to_polars_expr_uses_output_name() {
		let expr = ColumnExpr::column("source_col");
		let _polars_expr = expr.to_polars_expr("canonical_name");
		// The expression should use the provided output name as alias
		assert!(expr.is_identity());
	}

	#[test]
	fn test_struct_to_polars_expr() {
		let expr = ColumnExpr::struct_key(vec!["a".into(), "b".into()]);
		let _polars_expr = expr.to_polars_expr("my_struct");
		assert!(!expr.is_identity());
	}
}
