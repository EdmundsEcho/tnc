//! Error types for the etl-unit crate.
use thiserror::Error;

/// Errors that can occur in ETL unit operations
#[derive(Error, Debug)]
pub enum EtlError {
	/// A required column is missing from the DataFrame
	#[error("Missing column: {0}")]
	MissingColumn(String),

	/// Column has unexpected type
	#[error("Invalid column type for '{column}': expected {expected}, got {actual}")]
	InvalidColumnType {
		column: String,
		expected: String,
		actual: String,
	},

	/// Referenced EtlUnit not found in schema
	#[error("EtlUnit not found: {0}")]
	UnitNotFound(String),

	/// Circular dependency detected in derived measurements
	#[error("Circular dependency detected in derived measurements: {0}")]
	CircularDependency(String),

	/// Validation failed (General)
	#[error("Validation error: {0}")]
	Validation(String),

	/// Schema configuration error (IO/JSON/YAML parsing)
	#[error("Schema configuration error: {0}")]
	Config(String),

	/// Polars operation failed (simple, for #[from] conversion)
	#[error("Polars error: {0}")]
	Polars(#[from] polars::prelude::PolarsError),

	/// Polars operation failed with context
	#[error("{context}")]
	PolarsWithContext {
		context: String,
		#[source]
		source: polars::prelude::PolarsError,
	},

	/// Null values found where not allowed
	#[error("Unexpected null values in column '{column}': {count} nulls found")]
	UnexpectedNulls {
		column: String,
		count: usize,
	},

	/// Subset request error
	#[error("Subset request error: {0}")]
	SubsetRequest(String),

	/// Signal policy error
	#[error("Signal policy error: {0}")]
	SignalPolicy(String),

	/// Data proocessing error
	#[error("Data processing error: {0}")]
	DataProcessing(String),

	/// Cannot combine schema error: String reason
	#[error("Cannot combine with other schema error: {0}")]
	CannotCombineSchema(String),

	/// Component was crushed during universe building
	///
	/// This occurs when a component column is defined in the schema for a measurement,
	/// but not all sources providing that measurement have the component. To maintain
	/// consistency, the component is aggregated out of all sources.
	#[error(
		"Component '{component}' on measurement '{measurement}' was crushed during universe build. \
		 Sources missing this component: {sources_missing:?}. Cannot filter or group by this \
		 component. {reason}"
	)]
	ComponentCrushed {
		/// The component that was crushed
		component: String,
		/// The measurement this component belonged to
		measurement: String,
		/// Human-readable explanation
		reason: String,
		/// Sources that were missing the component
		sources_missing: Vec<String>,
	},

}

impl EtlError {
	/// Create a Polars error with context describing what operation failed.
	///
	/// # Example
	/// ```rust,ignore
	/// df.lazy()
	///     .group_by([col("station")])
	///     .agg([col("value").mean()])
	///     .collect()
	///     .map_err(|e| EtlError::polars_context(
	///         e,
	///         format!("Time bucketing failed for interval '{}' with groups {:?}",
	///             interval, group_cols)
	///     ))?;
	/// ```
	pub fn polars_context(source: polars::prelude::PolarsError, context: impl Into<String>) -> Self {
		EtlError::PolarsWithContext {
			context: context.into(),
			source,
		}
	}
}

/// Extension trait for adding context to Polars Results
pub trait PolarsResultExt<T> {
	/// Add context to a Polars error result.
	///
	/// # Example
	/// ```rust,ignore
	/// use crate::error::PolarsResultExt;
	///
	/// df.lazy()
	///     .collect()
	///     .with_context(|| format!(
	///         "Time bucketing failed. Interval: {}, Groups: {:?}",
	///         interval, groups
	///     ))?;
	/// ```
	fn with_context<F, S>(self, f: F) -> EtlResult<T>
	where
		F: FnOnce() -> S,
		S: Into<String>;
}

impl<T> PolarsResultExt<T> for Result<T, polars::prelude::PolarsError> {
	fn with_context<F, S>(self, f: F) -> EtlResult<T>
	where
		F: FnOnce() -> S,
		S: Into<String>,
	{
		self.map_err(|e| EtlError::PolarsWithContext {
			context: f().into(),
			source: e,
		})
	}
}
/// Result type alias for ETL operations
pub type EtlResult<T> = Result<T, EtlError>;
