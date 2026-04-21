//! Validation and instrumentation for signal policy operations.
//!
//! NOTE: Not yet activated. This module defines a validation framework
//! for asserting DataFrame shape and signal policy expectations before
//! and after processing. The signal policy currently works without
//! these checks. Revisit when adding automated signal policy testing
//! or when debugging policy behavior in production.
//!
//! Provides pre/post validation, shape assertions, and detailed tracing
//! to catch issues early and aid debugging.

use polars::{datatypes::TimeUnit as PolarsTimeUnit, prelude::*};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{EtlError, EtlResult};

// =============================================================================
// DataFrame Shape Snapshot
// =============================================================================

/// Captures the shape and schema of a DataFrame for validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFrameShape {
	pub rows:          usize,
	pub cols:          usize,
	pub column_names:  Vec<String>,
	#[serde(skip)]
	pub column_dtypes: Vec<DataType>,
}

impl DataFrameShape {
	pub fn from_df(df: &DataFrame) -> Self {
		Self {
			rows:          df.height(),
			cols:          df.width(),
			column_names:  df
				.get_column_names()
				.iter()
				.map(|s| s.to_string())
				.collect(),
			column_dtypes: df.dtypes().to_vec(),
		}
	}

	/// Check if another shape has the same columns (names and types)
	pub fn columns_match(&self, other: &DataFrameShape) -> bool {
		self.column_names == other.column_names && self.column_dtypes == other.column_dtypes
	}

	/// Get dtype for a column by name
	pub fn get_dtype(&self, col_name: &str) -> Option<&DataType> {
		self
			.column_names
			.iter()
			.position(|n| n == col_name)
			.map(|i| &self.column_dtypes[i])
	}

	/// Assert a column exists with expected dtype
	pub fn assert_column_dtype(&self, col_name: &str, expected: &DataType) -> EtlResult<()> {
		match self.get_dtype(col_name) {
			Some(actual) if actual == expected => Ok(()),
			Some(actual) => {
				Err(EtlError::Config(format!(
					"Column '{}' has wrong type: expected {:?}, got {:?}",
					col_name, expected, actual
				)))
			}
			None => Err(EtlError::MissingColumn(col_name.to_string())),
		}
	}

	/// Assert column is a Datetime type (any unit), return the unit
	pub fn assert_datetime_column(&self, col_name: &str) -> EtlResult<PolarsTimeUnit> {
		match self.get_dtype(col_name) {
			Some(DataType::Datetime(tu, _)) => Ok(*tu),
			Some(actual) => {
				Err(EtlError::Config(format!(
					"Column '{}' must be Datetime type, got {:?}",
					col_name, actual
				)))
			}
			None => Err(EtlError::MissingColumn(col_name.to_string())),
		}
	}

	/// Get dtype as string for serialization
	pub fn dtype_strings(&self) -> Vec<String> {
		self
			.column_dtypes
			.iter()
			.map(|dt| format!("{:?}", dt))
			.collect()
	}
}

impl std::fmt::Display for DataFrameShape {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "({} rows × {} cols)", self.rows, self.cols)
	}
}

// =============================================================================
// Signal Policy Expectations
// =============================================================================

/// Expected characteristics of signal policy output
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyExpectation {
	/// Name of the policy for logging
	pub policy_name:     String,
	/// Input time span in the source time unit
	pub input_time_span: i64,
	/// Time unit being used (as string for serialization)
	pub time_unit_str:   String,
	/// Number of partitions (subjects × components)
	pub partition_count: usize,

	#[serde(skip, default = "default_time_unit")]
	pub time_unit: PolarsTimeUnit,
}

impl PolicyExpectation {
	pub fn new(policy_name: impl Into<String>) -> Self {
		Self {
			policy_name:     policy_name.into(),
			input_time_span: 0,
			time_unit:       PolarsTimeUnit::Milliseconds,
			time_unit_str:   "Milliseconds".into(),
			partition_count: 1,
		}
	}

	pub fn with_time_span(mut self, span: i64) -> Self {
		self.input_time_span = span;
		self
	}

	pub fn with_time_unit(mut self, tu: PolarsTimeUnit) -> Self {
		self.time_unit = tu;
		self.time_unit_str = format!("{:?}", tu);
		self
	}

	pub fn with_partition_count(mut self, count: usize) -> Self {
		self.partition_count = count;
		self
	}
}

impl std::fmt::Display for PolicyExpectation {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			f,
			"{}: span={}, partitions={}",
			self.policy_name,
			self.input_time_span,
			self.partition_count,
		)
	}
}

// =============================================================================
// Validation Results
// =============================================================================

/// Result of a single validation check
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValidationResult {
	Ok,
	Warning(String),
	Error(String),
}

impl ValidationResult {
	pub fn is_ok(&self) -> bool {
		matches!(self, ValidationResult::Ok)
	}

	pub fn is_warning(&self) -> bool {
		matches!(self, ValidationResult::Warning(_))
	}

	pub fn is_error(&self) -> bool {
		matches!(self, ValidationResult::Error(_))
	}

	/// Get the message if this is a warning or error
	pub fn message(&self) -> Option<&str> {
		match self {
			ValidationResult::Ok => None,
			ValidationResult::Warning(msg) => Some(msg),
			ValidationResult::Error(msg) => Some(msg),
		}
	}

	/// Convert to a string suitable for metadata
	pub fn to_issue_string(&self) -> Option<String> {
		match self {
			ValidationResult::Ok => None,
			ValidationResult::Warning(msg) => Some(format!("[WARN] {}", msg)),
			ValidationResult::Error(msg) => Some(format!("[ERROR] {}", msg)),
		}
	}

	/// Log the result at appropriate level
	pub fn log(&self, context: &str) {
		match self {
			ValidationResult::Ok => {
				debug!(context = context, "Validation passed");
			}
			ValidationResult::Warning(msg) => {
				warn!(context = context, warning = %msg, "Validation warning");
			}
			ValidationResult::Error(msg) => {
				tracing::error!(context = context, error = %msg, "Validation failed");
			}
		}
	}
}

// =============================================================================
// Validation Events (for MetaCollector)
// =============================================================================

/// A validation event that can be collected during processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationEvent {
	/// Context where validation occurred (e.g., "instant_policy", "time_bucket")
	pub context: String,
	/// The validation result
	pub result:  ValidationResult,
	/// Optional details about the validation
	pub details: Option<ValidationDetails>,
}

impl ValidationEvent {
	pub fn ok(context: impl Into<String>) -> Self {
		Self {
			context: context.into(),
			result:  ValidationResult::Ok,
			details: None,
		}
	}

	pub fn warning(context: impl Into<String>, message: impl Into<String>) -> Self {
		Self {
			context: context.into(),
			result:  ValidationResult::Warning(message.into()),
			details: None,
		}
	}

	pub fn error(context: impl Into<String>, message: impl Into<String>) -> Self {
		Self {
			context: context.into(),
			result:  ValidationResult::Error(message.into()),
			details: None,
		}
	}

	pub fn with_details(mut self, details: ValidationDetails) -> Self {
		self.details = Some(details);
		self
	}

	/// Convert to a human-readable issue string
	pub fn to_issue_string(&self) -> Option<String> {
		self
			.result
			.to_issue_string()
			.map(|msg| format!("{}: {}", self.context, msg))
	}
}

/// Additional details about a validation event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationDetails {
	/// Input row count
	#[serde(skip_serializing_if = "Option::is_none")]
	pub input_rows:    Option<usize>,
	/// Output row count
	#[serde(skip_serializing_if = "Option::is_none")]
	pub output_rows:   Option<usize>,
	/// Expected row count
	#[serde(skip_serializing_if = "Option::is_none")]
	pub expected_rows: Option<usize>,
	/// Columns involved
	#[serde(skip_serializing_if = "Vec::is_empty")]
	pub columns:       Vec<String>,
	/// Time unit used
	#[serde(skip_serializing_if = "Option::is_none")]
	pub time_unit:     Option<String>,
}

impl ValidationDetails {
	pub fn new() -> Self {
		Self {
			input_rows:    None,
			output_rows:   None,
			expected_rows: None,
			columns:       Vec::new(),
			time_unit:     None,
		}
	}

	pub fn with_input_rows(mut self, rows: usize) -> Self {
		self.input_rows = Some(rows);
		self
	}

	pub fn with_output_rows(mut self, rows: usize) -> Self {
		self.output_rows = Some(rows);
		self
	}

	pub fn with_expected_rows(mut self, rows: usize) -> Self {
		self.expected_rows = Some(rows);
		self
	}

	pub fn with_columns(mut self, columns: Vec<String>) -> Self {
		self.columns = columns;
		self
	}

	pub fn with_time_unit(mut self, tu: PolarsTimeUnit) -> Self {
		self.time_unit = Some(format!("{:?}", tu));
		self
	}
}

impl Default for ValidationDetails {
	fn default() -> Self {
		Self::new()
	}
}

// =============================================================================
// Policy Validator
// =============================================================================

/// Validates signal policy operations pre and post execution
#[derive(Debug)]
pub struct PolicyValidator {
	pub policy_name: String,
	pub input_shape: DataFrameShape,
	pub time_col:    String,
	pub time_unit:   PolarsTimeUnit,
	pub expectation: Option<PolicyExpectation>,
	/// Collected validation events during processing
	events:          Vec<ValidationEvent>,
}

impl PolicyValidator {
	/// Create validator from input DataFrame
	pub fn new(
		policy_name: impl Into<String>,
		df: &DataFrame,
		time_col: impl Into<String>,
	) -> EtlResult<Self> {
		let policy_name = policy_name.into();
		let time_col = time_col.into();
		let input_shape = DataFrameShape::from_df(df);

		// Validate and extract time unit
		let time_unit = input_shape.assert_datetime_column(&time_col)?;

		debug!(
			 policy = %policy_name,
			 input_shape = %input_shape,
			 time_col = %time_col,
			 time_unit = ?time_unit,
			 columns = ?input_shape.column_names,
			 "PolicyValidator created"
		);

		Ok(Self {
			policy_name,
			input_shape,
			time_col,
			time_unit,
			expectation: None,
			events: Vec::new(),
		})
	}

	/// Set expected output characteristics
	pub fn with_expectation(mut self, expectation: PolicyExpectation) -> Self {
		debug!(
			 policy = %self.policy_name,
			 expectation = %expectation,
			 "Set policy expectation"
		);
		self.expectation = Some(expectation);
		self
	}

	/// Record a validation event
	pub fn record_event(&mut self, event: ValidationEvent) {
		event.result.log(&event.context);
		self.events.push(event);
	}

	/// Get all collected events
	pub fn events(&self) -> &[ValidationEvent] {
		&self.events
	}

	/// Get all events as issue strings (filtering out Ok results)
	pub fn issue_strings(&self) -> Vec<String> {
		self
			.events
			.iter()
			.filter_map(|e| e.to_issue_string())
			.collect()
	}

	/// Validate that two DataFrames can be joined on specified columns
	pub fn validate_join_compatibility(
		&mut self,
		left: &DataFrame,
		right: &DataFrame,
		join_cols: &[&str],
	) -> EtlResult<()> {
		let left_shape = DataFrameShape::from_df(left);
		let right_shape = DataFrameShape::from_df(right);

		for col_name in join_cols {
			let left_dtype = left_shape.get_dtype(col_name).ok_or_else(|| {
				EtlError::MissingColumn(format!(
					"Left DataFrame missing join column '{}'. Available: {:?}",
					col_name, left_shape.column_names
				))
			})?;

			let right_dtype = right_shape.get_dtype(col_name).ok_or_else(|| {
				EtlError::MissingColumn(format!(
					"Right DataFrame missing join column '{}'. Available: {:?}",
					col_name, right_shape.column_names
				))
			})?;

			if left_dtype != right_dtype {
				let event = ValidationEvent::error(
					"join_compatibility",
					format!(
						"Join column '{}' type mismatch: left={:?}, right={:?}",
						col_name, left_dtype, right_dtype
					),
				);
				self.record_event(event);

				return Err(EtlError::Config(format!(
					"Join column '{}' type mismatch: left={:?}, right={:?}",
					col_name, left_dtype, right_dtype
				)));
			}

			debug!(
				 column = %col_name,
				 dtype = ?left_dtype,
				 "Join column types match"
			);
		}

		self.record_event(ValidationEvent::ok("join_compatibility").with_details(
			ValidationDetails::new().with_columns(join_cols.iter().map(|s| s.to_string()).collect()),
		));

		debug!(
			 left_shape = %left_shape,
			 right_shape = %right_shape,
			 join_cols = ?join_cols,
			 "Join compatibility validated"
		);

		Ok(())
	}

	/// Validate output DataFrame against expectations
	pub fn validate_output(&mut self, output_df: &DataFrame) -> Vec<ValidationResult> {
		let output_shape = DataFrameShape::from_df(output_df);
		let mut results = Vec::new();

		// Check column preservation
		if !self.input_shape.columns_match(&output_shape) {
			let missing: Vec<_> = self
				.input_shape
				.column_names
				.iter()
				.filter(|c| !output_shape.column_names.contains(c))
				.cloned()
				.collect();
			let extra: Vec<_> = output_shape
				.column_names
				.iter()
				.filter(|c| !self.input_shape.column_names.contains(c))
				.cloned()
				.collect();

			if !missing.is_empty() || !extra.is_empty() {
				let msg = format!("Column mismatch. Missing: {:?}, Extra: {:?}", missing, extra);
				results.push(ValidationResult::Error(msg.clone()));
				self.record_event(ValidationEvent::error("column_preservation", msg));
			}

			// Check for dtype changes
			// ... to avoid mutating while reading...
			// 1. Create a temporary buffer to hold the events
			let mut events_to_record = Vec::new();

			// 2. READ PHASE: Iterate immutably
			for (i, col_name) in self.input_shape.column_names.iter().enumerate() {
				if let Some(output_dtype) = output_shape.get_dtype(col_name) {
					let input_dtype = &self.input_shape.column_dtypes[i];

					if input_dtype != output_dtype {
						let msg = format!(
							"Column '{}' dtype changed: {:?} -> {:?}",
							col_name, input_dtype, output_dtype
						);

						// Push to local results (assuming 'results' is a local Vec, not on self)
						results.push(ValidationResult::Warning(msg.clone()));

						// STAGE the event locally instead of calling self.record_event() immediately
						events_to_record.push(ValidationEvent::warning("dtype_change", msg));
					}
				}
			}

			// 3. WRITE PHASE: Apply the changes
			// The immutable borrow of 'self' inside the loop has explicitly ended here.
			// Now we are free to borrow 'self' mutably.
			for event in events_to_record {
				self.record_event(event);
			}
		}

		// Log summary
		debug!(
			 input_shape = %self.input_shape,
			 output_shape = %output_shape,
			 validation_issues = results.len(),
			 "Output validation complete"
		);

		for result in &results {
			result.log(&self.policy_name);
		}

		if results.is_empty() {
			self.record_event(
				ValidationEvent::ok("output_validation").with_details(
					ValidationDetails::new()
						.with_input_rows(self.input_shape.rows)
						.with_output_rows(output_shape.rows)
						.with_time_unit(self.time_unit),
				),
			);
			results.push(ValidationResult::Ok);
		}

		results
	}

	/// Create a validation summary for tracing
	pub fn summary(&self, output_df: &DataFrame) -> ValidationSummary {
		let output_shape = DataFrameShape::from_df(output_df);
		ValidationSummary {
			policy_name:   self.policy_name.clone(),
			input_rows:    self.input_shape.rows,
			output_rows:   output_shape.rows,
			input_cols:    self.input_shape.cols,
			output_cols:   output_shape.cols,
			time_unit:     self.time_unit,
			expected_rows: None,
			issues:        self.issue_strings(),
		}
	}
}

/// Summary for structured logging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationSummary {
	pub policy_name:   String,
	pub input_rows:    usize,
	pub output_rows:   usize,
	pub input_cols:    usize,
	pub output_cols:   usize,
	#[serde(skip, default = "default_time_unit")]
	pub time_unit:     PolarsTimeUnit,
	pub expected_rows: Option<usize>,
	/// Issues encountered during validation
	#[serde(skip_serializing_if = "Vec::is_empty")]
	pub issues:        Vec<String>,
}

impl ValidationSummary {
	/// Get the time unit as a string
	pub fn time_unit_str(&self) -> String {
		format!("{:?}", self.time_unit)
	}

	/// Check if there were any issues
	pub fn has_issues(&self) -> bool {
		!self.issues.is_empty()
	}
}

impl Default for ValidationSummary {
	fn default() -> Self {
		Self {
			policy_name:   String::new(),
			input_rows:    0,
			output_rows:   0,
			input_cols:    0,
			output_cols:   0,
			time_unit:     PolarsTimeUnit::Milliseconds,
			expected_rows: None,
			issues:        Vec::new(),
		}
	}
}

fn default_time_unit() -> PolarsTimeUnit {
	PolarsTimeUnit::Milliseconds
}

impl std::fmt::Display for ValidationSummary {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			f,
			"{}: {} rows -> {} rows ({} cols), time_unit={:?}",
			self.policy_name, self.input_rows, self.output_rows, self.output_cols, self.time_unit
		)?;
		if let Some(expected) = self.expected_rows {
			write!(f, ", expected={}", expected)?;
		}
		if !self.issues.is_empty() {
			write!(f, ", issues={}", self.issues.len())?;
		}
		Ok(())
	}
}

// =============================================================================
// Utility Functions
// =============================================================================

/// Extract time bounds from a DataFrame column with validation
pub fn extract_time_bounds_validated(
	df: &DataFrame,
	time_col: &str,
	time_unit: PolarsTimeUnit,
) -> EtlResult<(i64, i64)> {
	let shape = DataFrameShape::from_df(df);
	let actual_unit = shape.assert_datetime_column(time_col)?;

	if actual_unit != time_unit {
		return Err(EtlError::Config(format!(
			"Time column '{}' unit mismatch: expected {:?}, got {:?}",
			time_col, time_unit, actual_unit
		)));
	}

	let bounds = df
		.clone()
		.lazy()
		.select([
			col(time_col).dt().timestamp(time_unit).min().alias("min_t"),
			col(time_col).dt().timestamp(time_unit).max().alias("max_t"),
		])
		.collect()?;

	let min_time = bounds
		.column("min_t")?
		.i64()?
		.get(0)
		.ok_or_else(|| EtlError::SignalPolicy("No valid timestamps for min".into()))?;

	let max_time = bounds
		.column("max_t")?
		.i64()?
		.get(0)
		.ok_or_else(|| EtlError::SignalPolicy("No valid timestamps for max".into()))?;

	debug!(
		 min_time = min_time,
		 max_time = max_time,
		 span = max_time - min_time,
		 time_unit = ?time_unit,
		 "Extracted time bounds"
	);

	Ok((min_time, max_time))
}

/// Convert a collection of ValidationResults to issue strings
pub fn results_to_issues(results: &[ValidationResult]) -> Vec<String> {
	results.iter().filter_map(|r| r.to_issue_string()).collect()
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_dataframe_shape() {
		let df = df! {
			 "a" => [1, 2, 3],
			 "b" => ["x", "y", "z"]
		}
		.unwrap();

		let shape = DataFrameShape::from_df(&df);
		assert_eq!(shape.rows, 3);
		assert_eq!(shape.cols, 2);
		assert_eq!(shape.column_names, vec!["a", "b"]);
	}

	#[test]
	fn test_policy_expectation() {
		let exp = PolicyExpectation::new("test")
			.with_time_span(100_000)
			.with_partition_count(2);

		assert_eq!(exp.input_time_span, 100_000);
		assert_eq!(exp.partition_count, 2);
	}

	#[test]
	fn test_validation_result_to_issue_string() {
		assert!(ValidationResult::Ok.to_issue_string().is_none());

		let warning = ValidationResult::Warning("test warning".into());
		assert_eq!(warning.to_issue_string(), Some("[WARN] test warning".into()));

		let error = ValidationResult::Error("test error".into());
		assert_eq!(error.to_issue_string(), Some("[ERROR] test error".into()));
	}

	#[test]
	fn test_validation_event() {
		let event = ValidationEvent::warning("test_context", "something happened").with_details(
			ValidationDetails::new()
				.with_input_rows(100)
				.with_output_rows(50),
		);

		assert_eq!(event.to_issue_string(), Some("test_context: [WARN] something happened".into()));
		assert_eq!(event.details.as_ref().unwrap().input_rows, Some(100));
	}

	#[test]
	fn test_assert_datetime_column() {
		let df = df! {
			 "time" => [1i64, 2, 3]
		}
		.unwrap()
		.lazy()
		.with_column(col("time").cast(DataType::Datetime(PolarsTimeUnit::Milliseconds, None)))
		.collect()
		.unwrap();

		let shape = DataFrameShape::from_df(&df);
		let tu = shape.assert_datetime_column("time").unwrap();
		assert_eq!(tu, PolarsTimeUnit::Milliseconds);
	}

	#[test]
	fn test_assert_datetime_column_wrong_type() {
		let df = df! {
			 "time" => [1i64, 2, 3]
		}
		.unwrap();

		let shape = DataFrameShape::from_df(&df);
		let result = shape.assert_datetime_column("time");
		assert!(result.is_err());
	}

	#[test]
	fn test_validation_summary_display() {
		let summary = ValidationSummary {
			policy_name:   "instant".into(),
			input_rows:    100,
			output_rows:   50,
			input_cols:    5,
			output_cols:   5,
			time_unit:     PolarsTimeUnit::Milliseconds,
			expected_rows: Some(50),
			issues:        vec!["[WARN] test issue".into()],
		};

		let display = format!("{}", summary);
		assert!(display.contains("instant"));
		assert!(display.contains("100 rows -> 50 rows"));
		assert!(display.contains("issues=1"));
	}

	#[test]
	fn test_results_to_issues() {
		let results = vec![
			ValidationResult::Ok,
			ValidationResult::Warning("warn 1".into()),
			ValidationResult::Ok,
			ValidationResult::Error("error 1".into()),
		];

		let issues = results_to_issues(&results);
		assert_eq!(issues.len(), 2);
		assert!(issues[0].contains("WARN"));
		assert!(issues[1].contains("ERROR"));
	}
}
