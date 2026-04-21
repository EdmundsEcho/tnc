//! Unpivot: Transform wide source data into long canonical measurements.
//!
//! An unpivot converts multiple source columns into a single canonical measurement
//! with component columns that identify the source of each value.
//!
//! ## Conceptual Model
//!
//! ```text
//! SOURCE (wide format)                      CANONICAL (long format / Fragment)
//! ┌───────────┬───────────┬───────────┐     ┌───────────┬───────────────┐
//! │ engine_1  │ engine_2  │ engine_3  │     │ engine_id │ engine_status │
//! ├───────────┼───────────┼───────────┤ ──► ├───────────┼───────────────┤
//! │     1     │     0     │     1     │     │    "1"    │       1       │
//! └───────────┴───────────┴───────────┘     │    "2"    │       0       │
//!                                           │    "3"    │       1       │
//!        SourceColumnName                   └───────────┴───────────────┘
//!                                                  CanonicalColumnName
//! ```
//!
//! ## Namespace Bridge
//!
//! `UnpivotConfig` explicitly bridges the source and canonical namespaces:
//!
//! - **Inputs** (`from_source`): `SourceColumnName` — columns in the raw DataFrame
//! - **Outputs** (`creates`, `component`): `CanonicalColumnName` — columns in the fragment
//! - **Context** (`subject`, `time`): Source columns that provide subject/time context
//!
//! ## Usage
//!
//! ```ignore
//! let unpivot = UnpivotConfig::creates("engine_status", MeasurementKind::Categorical)
//!     .subject("station_name".source(), "station_name".canonical())
//!     .time("observation_time".source(), "timestamp".canonical())
//!     .component("engine_id")
//!     .from_source("engine_1", [("engine_id", "1")])
//!     .from_source("engine_2", [("engine_id", "2")])
//!     .from_source("engine_3", [("engine_id", "3")])
//!     .build();
//!
//! let fragment = unpivot.execute(&source_df, "scada_source")?;
//! ```

use std::collections::HashMap;

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use crate::{
	MeasurementKind,
	column::{CanonicalColumnName, SourceColumnName},
	error::{EtlError, EtlResult},
	universe::MeasurementFragment,
};

// =============================================================================
// Core Types
// =============================================================================

/// Configuration for unpivoting wide source data into a canonical measurement.
///
/// This configuration bridges the source namespace (raw DataFrame columns) and
/// the canonical namespace (schema-level column names).
///
/// The unpivot is self-contained: it knows which source columns provide subject
/// and time context, so `execute()` only needs the DataFrame and source name.
///
/// Use [`UnpivotConfig::creates`] to start building.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnpivotConfig {
	/// What the unpivot creates (canonical level)
	pub output: UnpivotOutput,
	/// The source columns and their component tag values
	pub inputs: Vec<UnpivotInput>,
	/// Subject column mapping (source → canonical)
	pub subject: ColumnMapping,
	/// Time column mapping (source → canonical)
	pub time: ColumnMapping,
}

/// Mapping from source column to canonical column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMapping {
	/// The source column name (in the DataFrame)
	pub source: SourceColumnName,
	/// The canonical column name (in the schema)
	pub canonical: CanonicalColumnName,
}

impl UnpivotConfig {
	pub(crate) fn name(&self) -> &CanonicalColumnName {
		&self.output.measurement
	}
}

/// The canonical output of an unpivot operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnpivotOutput {
	/// Name of the measurement being created (canonical)
	pub measurement: CanonicalColumnName,
	/// Kind of measurement (affects downstream aggregation)
	pub kind: MeasurementKind,
	/// Component columns created by the unpivot (canonical)
	pub components: Vec<CanonicalColumnName>,
}

/// A single source column and its component tag values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnpivotInput {
	/// The source column containing values (source namespace)
	pub source_column: SourceColumnName,
	/// Component values for this source column: canonical_component_name → value
	pub tags: HashMap<CanonicalColumnName, String>,
}

// =============================================================================
// Builder API
// =============================================================================

impl UnpivotConfig {
	/// Start building an unpivot that creates a canonical measurement.
	///
	/// # Arguments
	/// * `measurement` - The canonical name of the measurement being created
	/// * `kind` - The measurement kind (Categorical, Measure, etc.)
	///
	/// # Example
	/// ```ignore
	/// let unpivot = UnpivotConfig::creates("engine_status", MeasurementKind::Categorical)
	///     .subject("station_name".source(), "station_name".canonical())
	///     .time("observation_time".source(), "timestamp".canonical())
	///     .component("engine_id")
	///     .from_source("engine_1", [("engine_id", "1")])
	///     .from_source("engine_2", [("engine_id", "2")])
	///     .build();
	/// ```
	pub fn creates(
		measurement: impl Into<CanonicalColumnName>,
		kind: MeasurementKind,
	) -> UnpivotBuilder {
		UnpivotBuilder {
			measurement: measurement.into(),
			kind,
			components: Vec::new(),
			inputs: Vec::new(),
			subject: None,
			time: None,
		}
	}
}

/// Builder for [`UnpivotConfig`].
#[derive(Debug, Clone)]
pub struct UnpivotBuilder {
	measurement: CanonicalColumnName,
	kind: MeasurementKind,
	components: Vec<CanonicalColumnName>,
	inputs: Vec<UnpivotInput>,
	subject: Option<ColumnMapping>,
	time: Option<ColumnMapping>,
}

impl UnpivotBuilder {
	/// Specify the subject column mapping (source → canonical).
	///
	/// # Arguments
	/// * `source` - The source column name in the DataFrame
	/// * `canonical` - The canonical column name in the schema
	///
	/// # Example
	/// ```ignore
	/// .subject("station_name".source(), "station_name".canonical())
	/// ```
	pub fn subject(
		mut self,
		source: impl Into<SourceColumnName>,
		canonical: impl Into<CanonicalColumnName>,
	) -> Self {
		self.subject = Some(ColumnMapping {
			source: source.into(),
			canonical: canonical.into(),
		});
		self
	}

	/// Specify the time column mapping (source → canonical).
	///
	/// # Arguments
	/// * `source` - The source column name in the DataFrame
	/// * `canonical` - The canonical column name in the schema
	///
	/// # Example
	/// ```ignore
	/// .time("observation_time".source(), "timestamp".canonical())
	/// ```
	pub fn time(
		mut self,
		source: impl Into<SourceColumnName>,
		canonical: impl Into<CanonicalColumnName>,
	) -> Self {
		self.time = Some(ColumnMapping {
			source: source.into(),
			canonical: canonical.into(),
		});
		self
	}

	/// Declare a component column that will be created (canonical namespace).
	///
	/// Each input source column must provide a value for this component via tags.
	///
	/// # Example
	/// ```ignore
	/// .component("engine_id")   // canonical name
	/// .component("bank")        // can have multiple components
	/// ```
	pub fn component(mut self, name: impl Into<CanonicalColumnName>) -> Self {
		self.components.push(name.into());
		self
	}

	/// Map a source column to this measurement with component tag values.
	///
	/// # Arguments
	/// * `source_column` - The name of the column in the source DataFrame (source namespace)
	/// * `tags` - Component values identifying this source column: (canonical_component, value)
	///
	/// # Example
	/// ```ignore
	/// // Single component
	/// .from_source("engine_1_running", [("engine_id", "1")])
	///
	/// // Multiple components
	/// .from_source("pump_2_bank_a", [("pump_id", "2"), ("bank", "A")])
	/// ```
	pub fn from_source<S, I, K, V>(mut self, source_column: S, tags: I) -> Self
	where
		S: Into<SourceColumnName>,
		I: IntoIterator<Item = (K, V)>,
		K: Into<CanonicalColumnName>,
		V: Into<String>,
	{
		let tags_map: HashMap<CanonicalColumnName, String> = tags
			.into_iter()
			.map(|(k, v)| (k.into(), v.into()))
			.collect();

		self.inputs.push(UnpivotInput {
			source_column: source_column.into(),
			tags: tags_map,
		});
		self
	}

	/// Map multiple source columns at once.
	///
	/// # Arguments
	/// * `sources` - Iterator of (source_column, tags) pairs
	///
	/// # Example
	/// ```ignore
	/// // Single component - array of (source_col, tag_value)
	/// .from_sources([
	///     ("engine_1", [("engine_id", "1")]),
	///     ("engine_2", [("engine_id", "2")]),
	///     ("engine_3", [("engine_id", "3")]),
	/// ])
	///
	/// // Multiple components
	/// .from_sources([
	///     ("eng1_bank_a", [("engine_id", "1"), ("bank", "A")]),
	///     ("eng1_bank_b", [("engine_id", "1"), ("bank", "B")]),
	///     ("eng2_bank_a", [("engine_id", "2"), ("bank", "A")]),
	///     ("eng2_bank_b", [("engine_id", "2"), ("bank", "B")]),
	/// ])
	/// ```
	pub fn from_sources<I, S, T, K, V>(mut self, sources: I) -> Self
	where
		I: IntoIterator<Item = (S, T)>,
		S: Into<SourceColumnName>,
		T: IntoIterator<Item = (K, V)>,
		K: Into<CanonicalColumnName>,
		V: Into<String>,
	{
		for (source_column, tags) in sources {
			let tags_map: HashMap<CanonicalColumnName, String> = tags
				.into_iter()
				.map(|(k, v)| (k.into(), v.into()))
				.collect();

			self.inputs.push(UnpivotInput {
				source_column: source_column.into(),
				tags: tags_map,
			});
		}
		self
	}

	/// Build the unpivot configuration.
	///
	/// # Panics
	/// Panics if validation fails. Use [`build_checked`] for fallible construction.
	pub fn build(self) -> UnpivotConfig {
		self
			.build_checked()
			.expect("UnpivotConfig validation failed")
	}

	/// Build the unpivot configuration with validation.
	pub fn build_checked(self) -> EtlResult<UnpivotConfig> {
		let subject = self.subject.ok_or_else(|| {
			EtlError::Config(format!(
				"Unpivot '{}': subject column mapping not specified. Use .subject(source, canonical)",
				self.measurement
			))
		})?;

		let time = self.time.ok_or_else(|| {
			EtlError::Config(format!(
				"Unpivot '{}': time column mapping not specified. Use .time(source, canonical)",
				self.measurement
			))
		})?;

		let config = UnpivotConfig {
			output: UnpivotOutput {
				measurement: self.measurement,
				kind: self.kind,
				components: self.components,
			},
			inputs: self.inputs,
			subject,
			time,
		};
		config.validate()?;
		Ok(config)
	}
}

impl From<UnpivotBuilder> for UnpivotConfig {
	fn from(builder: UnpivotBuilder) -> Self {
		builder.build()
	}
}

// =============================================================================
// Accessors
// =============================================================================

impl UnpivotConfig {
	/// The canonical measurement name this unpivot creates.
	pub fn measurement_name(&self) -> &CanonicalColumnName {
		&self.output.measurement
	}

	/// The measurement kind.
	pub fn kind(&self) -> MeasurementKind {
		self.output.kind
	}

	/// The source subject column name.
	pub fn source_subject(&self) -> &SourceColumnName {
		&self.subject.source
	}

	/// The canonical subject column name.
	pub fn canonical_subject(&self) -> &CanonicalColumnName {
		&self.subject.canonical
	}

	/// The source time column name.
	pub fn source_time(&self) -> &SourceColumnName {
		&self.time.source
	}

	/// The canonical time column name.
	pub fn canonical_time(&self) -> &CanonicalColumnName {
		&self.time.canonical
	}

	/// The source columns being unpivoted (value columns, not subject/time).
	pub fn source_columns(&self) -> impl Iterator<Item = &SourceColumnName> {
		self.inputs.iter().map(|i| &i.source_column)
	}

	/// All source columns required by this unpivot (subject, time, and value columns).
	pub fn all_source_columns(&self) -> Vec<&SourceColumnName> {
		let mut cols = vec![&self.subject.source, &self.time.source];
		cols.extend(self.inputs.iter().map(|i| &i.source_column));
		cols
	}

	/// The canonical component columns being created.
	pub fn component_columns(&self) -> &[CanonicalColumnName] {
		&self.output.components
	}

	/// Number of source columns being unpivoted.
	pub fn input_count(&self) -> usize {
		self.inputs.len()
	}

	/// Get the tag value for a source column and component.
	pub fn tag_value(
		&self,
		source: &SourceColumnName,
		component: &CanonicalColumnName,
	) -> Option<&str> {
		self
			.inputs
			.iter()
			.find(|i| &i.source_column == source)
			.and_then(|i| i.tags.get(component))
			.map(|s| s.as_str())
	}

	/// Validate the configuration.
	pub fn validate(&self) -> EtlResult<()> {
		if self.inputs.is_empty() {
			return Err(EtlError::Config(format!(
				"Unpivot '{}' has no source columns",
				self.output.measurement
			)));
		}

		// Check that all inputs have values for all declared components
		for input in &self.inputs {
			for component in &self.output.components {
				if !input.tags.contains_key(component) {
					return Err(EtlError::Config(format!(
						"Unpivot '{}': source column '{}' missing tag for component '{}'",
						self.output.measurement, input.source_column, component
					)));
				}
			}

			// Warn about extra tags not in declared components
			for tag_component in input.tags.keys() {
				if !self.output.components.contains(tag_component) {
					return Err(EtlError::Config(format!(
						"Unpivot '{}': source column '{}' has tag '{}' but component not declared",
						self.output.measurement, input.source_column, tag_component
					)));
				}
			}
		}

		Ok(())
	}
}

// =============================================================================
// Execution
// =============================================================================

impl UnpivotConfig {
	/// Execute the unpivot transformation on source data.
	///
	/// This is the bridge between source and canonical namespaces:
	/// - Input: DataFrame with `SourceColumnName` columns
	/// - Output: `MeasurementFragment` with `CanonicalColumnName` columns
	///
	/// The unpivot uses its internal subject and time mappings, so no column
	/// parameters are needed.
	///
	/// # Arguments
	/// * `source_df` - The source DataFrame containing the columns to unpivot
	/// * `source_name` - The name of the source (for fragment metadata)
	///
	/// # Returns
	/// A [`MeasurementFragment`] with canonical column names, ready for accumulation.
	///
	/// # Example
	/// ```ignore
	/// let fragment = unpivot_config.execute(&source_df, "scada_source")?;
	/// // fragment.data has columns: station_name, timestamp, engine_id, engine_status (all canonical)
	/// ```
	#[instrument(skip(self, source_df), fields(measurement = %self.output.measurement, inputs = self.inputs.len()))]
	pub fn execute(
		&self,
		source_df: &DataFrame,
		source_name: &str,
	) -> EtlResult<MeasurementFragment> {
		self.validate()?;

		debug!(
			 source_columns = ?self.inputs.iter().map(|i| i.source_column.as_str()).collect::<Vec<_>>(),
			 components = ?self.output.components.iter().map(|c| c.as_str()).collect::<Vec<_>>(),
			 subject_source = %self.subject.source,
			 subject_canonical = %self.subject.canonical,
			 time_source = %self.time.source,
			 time_canonical = %self.time.canonical,
			 "Executing unpivot"
		);

		// For each source column, create a mini-DataFrame with:
		// - subject, time (renamed to canonical)
		// - component columns (from tags, as literals)
		// - measurement value (from source column, renamed to canonical)
		let mut fragments: Vec<DataFrame> = Vec::with_capacity(self.inputs.len());

		for input in &self.inputs {
			let mini_df = self.build_input_fragment(source_df, input)?;
			fragments.push(mini_df);
		}

		// Stack all mini-fragments
		let stacked = if fragments.len() == 1 {
			fragments.remove(0)
		} else {
			concat(
				fragments
					.iter()
					.map(|df| df.clone().lazy())
					.collect::<Vec<_>>(),
				UnionArgs::default(),
			)?
			.collect()?
		};

		let rows_before = stacked.height();
		let stacked_height = stacked.height();

		// Filter out rows where the measurement value is null.
		// Re-enabled: null engine observations are treated as absence for
		// raw-path display; processed-path TTL / null_value handle the
		// remaining sparseness.
		let filtered = stacked
			.lazy()
			.filter(col(self.output.measurement.as_str()).is_not_null())
			.collect()?;

		debug!(
			rows_before = rows_before,
			rows_after = filtered.height(),
			rows_removed = stacked_height - filtered.height(),
			"🦀 ✅ Unpivot Filtered null measurement values"
		);

		// Row-count invariant (disabled): with the null filter above, the
		// output row count is `source_rows × input_columns` minus any null
		// measurement values, so it intentionally violates the strict
		// "source_rows × input_columns" relationship. Keeping the check as
		// documentation of the stricter invariant that would hold without
		// the filter.
		//
		// let expected_rows = source_df.height() * self.inputs.len();
		// if filtered.height() != expected_rows {
		// 	return Err(EtlError::DataProcessing(format!(
		// 		"Unpivot '{}': row count invariant violated. \
		// 		 Source has {} rows and {} input columns, so unpivot output must have \
		// 		 {} rows (source_rows × input_columns), but got {}.",
		// 		self.output.measurement.as_str(),
		// 		source_df.height(),
		// 		self.inputs.len(),
		// 		expected_rows,
		// 		filtered.height(),
		// 	)));
		// }

		Ok(MeasurementFragment::new(
			self.output.measurement.clone(),
			source_name,
			self.output.kind,
			self.output.components.clone(),
			filtered,
		))
	}

	/// Build a fragment DataFrame for a single source column.
	fn build_input_fragment(
		&self,
		source_df: &DataFrame,
		input: &UnpivotInput,
	) -> EtlResult<DataFrame> {
		let measurement_name = self.output.measurement.as_str();
		let source_col_name = input.source_column.as_str();

		// Verify source column exists
		if source_df.column(source_col_name).is_err() {
			return Err(EtlError::MissingColumn(format!(
				"Unpivot '{}': source column '{}' not found in DataFrame. Available: {:?}",
				measurement_name,
				source_col_name,
				source_df
					.get_column_names()
					.iter()
					.map(|s| s.as_str())
					.collect::<Vec<_>>()
			)));
		}

		// Verify subject column exists
		if source_df.column(self.subject.source.as_str()).is_err() {
			return Err(EtlError::MissingColumn(format!(
				"Unpivot '{}': subject column '{}' not found in DataFrame. Available: {:?}",
				measurement_name,
				self.subject.source,
				source_df
					.get_column_names()
					.iter()
					.map(|s| s.as_str())
					.collect::<Vec<_>>()
			)));
		}

		// Verify time column exists
		if source_df.column(self.time.source.as_str()).is_err() {
			return Err(EtlError::MissingColumn(format!(
				"Unpivot '{}': time column '{}' not found in DataFrame. Available: {:?}",
				measurement_name,
				self.time.source,
				source_df
					.get_column_names()
					.iter()
					.map(|s| s.as_str())
					.collect::<Vec<_>>()
			)));
		}

		// Build select expressions
		let mut select_exprs: Vec<Expr> = Vec::new();

		// Subject and time: rename from source to canonical
		select_exprs.push(col(self.subject.source.as_str()).alias(self.subject.canonical.as_str()));
		select_exprs.push(col(self.time.source.as_str()).alias(self.time.canonical.as_str()));

		// Component columns: add as literals (canonical names)
		for component in &self.output.components {
			let tag_value = input.tags.get(component).ok_or_else(|| {
				EtlError::Config(format!(
					"Unpivot '{}': missing tag '{}' for source column '{}'",
					measurement_name, component, source_col_name
				))
			})?;

			select_exprs.push(lit(tag_value.clone()).alias(component.as_str()));
		}

		// Measurement value: rename source column to canonical measurement name
		select_exprs.push(col(source_col_name).alias(measurement_name));

		// Execute selection
		let fragment = source_df.clone().lazy().select(select_exprs).collect()?;

		Ok(fragment)
	}

	/// Execute unpivot and return just the DataFrame (convenience method).
	///
	/// Use [`execute`] if you need the full [`MeasurementFragment`] with metadata.
	pub fn execute_to_df(&self, source_df: &DataFrame) -> EtlResult<DataFrame> {
		self.execute(source_df, "unknown")?.materialize()
	}
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_unpivot_config_builder() {
		let config = UnpivotConfig::creates("engine_status", MeasurementKind::Categorical)
			.subject("station_name", "station_name")
			.time("observation_time", "timestamp")
			.component("engine")
			.from_sources([
				("engine_1", [("engine", "1")]),
				("engine_2", [("engine", "2")]),
				("engine_3", [("engine", "3")]),
			])
			.build();

		assert_eq!(config.measurement_name().as_str(), "engine_status");
		assert_eq!(config.source_subject().as_str(), "station_name");
		assert_eq!(config.canonical_subject().as_str(), "station_name");
		assert_eq!(config.source_time().as_str(), "observation_time");
		assert_eq!(config.canonical_time().as_str(), "timestamp");
		assert_eq!(config.input_count(), 3);
		assert_eq!(config.component_columns().len(), 1);
	}

	#[test]
	fn test_unpivot_config_missing_subject() {
		let result = UnpivotConfig::creates("engine_status", MeasurementKind::Categorical)
			.time("observation_time", "timestamp")
			.component("engine")
			.from_source("engine_1", [("engine", "1")])
			.build_checked();

		assert!(result.is_err());
		assert!(result.unwrap_err().to_string().contains("subject"));
	}

	#[test]
	fn test_unpivot_config_missing_time() {
		let result = UnpivotConfig::creates("engine_status", MeasurementKind::Categorical)
			.subject("station_name", "station_name")
			.component("engine")
			.from_source("engine_1", [("engine", "1")])
			.build_checked();

		assert!(result.is_err());
		assert!(result.unwrap_err().to_string().contains("time"));
	}

	#[test]
	fn test_all_source_columns() {
		let config = UnpivotConfig::creates("engine_status", MeasurementKind::Categorical)
			.subject("station_name", "station_name")
			.time("observation_time", "timestamp")
			.component("engine")
			.from_sources([("engine_1", [("engine", "1")]), ("engine_2", [("engine", "2")])])
			.build();

		let all_cols = config.all_source_columns();
		assert_eq!(all_cols.len(), 4); // subject, time, engine_1, engine_2
		assert!(all_cols.iter().any(|c| c.as_str() == "station_name"));
		assert!(all_cols.iter().any(|c| c.as_str() == "observation_time"));
		assert!(all_cols.iter().any(|c| c.as_str() == "engine_1"));
		assert!(all_cols.iter().any(|c| c.as_str() == "engine_2"));
	}
}
