//! BoundSource and registry for managing data sources.
//!
//! A `BoundSource` binds a DataFrame to a schema by mapping source columns
//! to canonical names. This separation allows the same schema to work with
//! different source column layouts.
//!
//! Composition is determined by codomain (unit name = value column):
//! - Same codomain from multiple sources → Stack (union rows)
//! - Different codomains → Join on subject

// TODO: Review the component crush logic when trying to stack etl-units

use std::collections::HashMap;

use polars::prelude::DataFrame;

use crate::{
	UnpivotConfig,
	column::{CanonicalColumnName, DomainSignature, SourceColumnName},
	composition::{ComponentReduction, CompositionPlan, CompositionStrategy},
	error::{EtlError, EtlResult},
	expr::ColumnExpr,
	request::AggregationType,
	schema::EtlSchema,
	unit::MeasurementKind,
	unit_ref::EtlUnitRef,
};

// ============================================================================
// Binding Rule
// ============================================================================

/// A rule for deriving a canonical column from a source DataFrame.
///
/// Each canonical column in a [`BoundSource`] is mapped to one of these
/// rules: either a direct reference to a physical column, or a computed
/// expression over physical columns.
///
/// `BindingRule` is the *recipe*; the [`crate::plan::ColumnBinding`] /
/// [`crate::plan::CodomainBinding`] types in the plan layer are the
/// *resolved* physical/canonical pairs that downstream stages consume.
#[derive(Debug, Clone)]
pub enum BindingRule {
	/// Direct column reference: canonical ← source column (with optional rename)
	Direct(SourceColumnName),
	/// Computed column: canonical ← expression over source columns
	Computed(ColumnExpr),
}

impl BindingRule {
	/// Create a direct mapping from a source column
	pub fn direct(source: impl Into<String>) -> Self {
		Self::Direct(SourceColumnName::new(source))
	}

	/// Create a computed mapping from an expression
	pub fn computed(expr: ColumnExpr) -> Self {
		Self::Computed(expr)
	}

	/// Get all source columns this mapping depends on
	pub fn source_columns(&self) -> Vec<&SourceColumnName> {
		match self {
			Self::Direct(col) => vec![col],
			Self::Computed(expr) => expr.source_columns(),
		}
	}

	/// Check if this is a simple identity mapping (source name = canonical name)
	pub fn is_identity(&self, canonical: &CanonicalColumnName) -> bool {
		match self {
			Self::Direct(source) => source.as_str() == canonical.as_str(),
			Self::Computed(_) => false,
		}
	}
}

// ============================================================================
// BoundSource
// ============================================================================

/// A DataFrame bound to a schema with column mappings.
///
/// `BoundSource` defines how to extract canonical columns from a specific
/// DataFrame. Multiple sources can be bound to the same schema, each with
/// different column names or computed expressions.
///
/// # Example
///
/// ```rust,ignore
/// let source = BoundSource::new("scada", df)
///     .map("station", "site_id")              // canonical ← source
///     .map("timestamp", "obs_time")
///     .map_expr("pump_id", ColumnExpr::struct_key(vec!["region", "pump_num"]))
///     .map("water_level", "reading")
///     .provides(vec![EtlUnitRef::measurement("water_level")]);
/// ```
#[derive(Debug, Clone)]
pub struct BoundSource {
	/// Identifier for this source (used for provenance, logging)
	pub name: String,

	/// The DataFrame containing the data.
	/// Wrapped in Arc so cloning a BoundSource (e.g., for the build plan)
	/// is cheap, and multiple measurements can share a reference to the
	/// same source data without deep copies.
	pub data: std::sync::Arc<DataFrame>,

	/// Canonical name → how to get it from this DataFrame
	pub columns: HashMap<CanonicalColumnName, BindingRule>,

	/// Unpivot transformations to apply before extraction
	pub unpivots: Vec<UnpivotConfig>,

	/// Which EtlUnits this source provides data for.
	/// None means "infer from available columns".
	pub etl_units: Option<Vec<EtlUnitRef>>,

	/// Priority for stacking (lower = higher priority for dedup)
	pub priority: u32,
}

impl BoundSource {
	/// Create a new bound source
	pub fn new(name: impl Into<String>, data: DataFrame) -> Self {
		Self {
			name: name.into(),
			data: std::sync::Arc::new(data),
			columns: HashMap::new(),
			unpivots: Vec::new(),
			etl_units: None,
			priority: 0,
		}
	}

	/// Create a bound source with identity mappings for all schema columns.
	///
	/// Use when source column names match canonical names exactly.
	pub fn identity(name: impl Into<String>, data: DataFrame, schema: &EtlSchema) -> Self {
		let mut source = Self::new(name, data);

		// Map all canonical names to themselves
		for canonical in schema.all_canonical_names() {
			source.columns.insert(
				canonical.clone(),
				BindingRule::Direct(SourceColumnName::new(canonical.as_str())),
			);
		}

		source
	}

	// =========================================================================
	// Column Mapping Builder Methods
	// =========================================================================

	/// Map a canonical name to a source column (direct mapping)
	///
	/// # Arguments
	/// * `canonical` - The canonical column name (from schema)
	/// * `source` - The source column name (in DataFrame)
	///
	/// # Example
	/// ```rust,ignore
	/// use synapse_etl_unit::column::ColumnNameExt;
	///
	/// let source = BoundSource::new("scada", df)
	///     .map("station".canonical(), "site_id".source())
	///     .map("timestamp".canonical(), "obs_time".source())
	///     .map("water_level".canonical(), "reading".source());
	/// ```
	pub fn map(mut self, canonical: CanonicalColumnName, source: SourceColumnName) -> Self {
		self
			.columns
			.insert(canonical, BindingRule::Direct(source));
		self
	}

	/// Map a canonical name to a computed expression
	///
	/// # Arguments
	/// * `canonical` - The canonical column name (from schema)
	/// * `expr` - The expression to compute the value
	pub fn map_expr(mut self, canonical: impl Into<String>, expr: ColumnExpr) -> Self {
		self
			.columns
			.insert(CanonicalColumnName::new(canonical), BindingRule::Computed(expr));
		self
	}

	/// Add multiple direct mappings at once
	///
	/// # Example
	/// ```rust,ignore
	/// use synapse_etl_unit::column::ColumnNameExt;
	///
	/// let source = BoundSource::new("scada", df)
	///     .map_all([
	///         ("station".canonical(), "site_id".source()),
	///         ("timestamp".canonical(), "obs_time".source()),
	///     ]);
	/// ```
	pub fn map_all(
		mut self,
		mappings: impl IntoIterator<Item = (CanonicalColumnName, SourceColumnName)>,
	) -> Self {
		for (canonical, source) in mappings {
			self
				.columns
				.insert(canonical, BindingRule::Direct(source));
		}
		self
	}

	// =========================================================================
	// Unpivot Configuration
	// =========================================================================

	/// Add an unpivot transformation
	///
	/// Unpivots are applied during source binding, before stacking/joining.
	pub fn unpivot(mut self, unpivot: UnpivotConfig) -> Self {
		self.unpivots.push(unpivot);
		self
	}

	// =========================================================================
	// Unit Configuration
	// =========================================================================

	/// Specify which units (codomains) this source hydrates
	pub fn provides(mut self, units: Vec<EtlUnitRef>) -> Self {
		self.etl_units = Some(units);
		self
	}

	/// Set the priority for deduplication (lower = higher priority)
	pub fn with_priority(mut self, priority: u32) -> Self {
		self.priority = priority;
		self
	}

	// =========================================================================
	// Column Resolution
	// =========================================================================

	/// Get the mapping for a canonical column
	pub fn get_mapping(&self, canonical: &CanonicalColumnName) -> Option<&BindingRule> {
		self.columns.get(canonical)
	}

	/// Get the source column name for a canonical name (for direct mappings only)
	pub fn get_source_column(&self, canonical: &CanonicalColumnName) -> Option<&SourceColumnName> {
		match self.columns.get(canonical) {
			Some(BindingRule::Direct(source)) => Some(source),
			_ => None,
		}
	}

	/// Check if this source has a mapping for a canonical column
	pub fn has_mapping(&self, canonical: &CanonicalColumnName) -> bool {
		self.columns.contains_key(canonical)
	}

	/// Check if this source can provide a canonical column.
	///
	/// Returns true if:
	/// - There's an explicit mapping, OR
	/// - No mapping exists but the DataFrame has a column with the canonical name
	pub fn can_provide(&self, canonical: &CanonicalColumnName) -> bool {
		if self.columns.contains_key(canonical) {
			// Has explicit mapping - verify source columns exist
			if let Some(mapping) = self.columns.get(canonical) {
				return mapping
					.source_columns()
					.iter()
					.all(|src| self.data.column(src.as_str()).is_ok());
			}
		}

		// Fall back to identity: check if DataFrame has a column with canonical name
		self.data.column(canonical.as_str()).is_ok()
	}

	/// Get all source columns required by all mappings
	pub fn required_source_columns(&self) -> Vec<&SourceColumnName> {
		self
			.columns
			.values()
			.flat_map(|mapping| mapping.source_columns())
			.collect()
	}

	/// Get all column names in the source DataFrame
	pub fn dataframe_columns(&self) -> Vec<&str> {
		self
			.data
			.get_column_names()
			.into_iter()
			.map(|s| s.as_str())
			.collect()
	}

	// =========================================================================
	// Validation
	// =========================================================================

	/// Validate that all mapped source columns exist in the DataFrame
	pub fn validate(&self) -> EtlResult<()> {
		let df_columns: std::collections::HashSet<&str> = self
			.data
			.get_column_names()
			.into_iter()
			.map(|s| s.as_str())
			.collect();

		for (canonical, mapping) in &self.columns {
			for source_col in mapping.source_columns() {
				if !df_columns.contains(source_col.as_str()) {
					return Err(EtlError::MissingColumn(format!(
						"Source '{}': mapping for canonical '{}' references missing column '{}'",
						self.name,
						canonical.as_str(),
						source_col.as_str()
					)));
				}
			}
		}

		// Validate unpivots
		for unpivot in &self.unpivots {
			unpivot.validate()?;
			// Check that unpivot source columns exist
			for source_col in unpivot.source_columns() {
				if !df_columns.contains(source_col.as_str()) {
					return Err(EtlError::MissingColumn(format!(
						"Source '{}': unpivot '{}' references missing column '{}'",
						self.name,
						unpivot.name(),
						source_col
					)));
				}
			}
		}

		Ok(())
	}

	/// Validate that this source can provide all canonical columns required by a schema
	pub fn validate_against_schema(&self, schema: &EtlSchema) -> EtlResult<()> {
		// First validate internal consistency
		self.validate()?;

		// Check that we can provide subject and time
		if !self.can_provide(&schema.subject) {
			return Err(EtlError::MissingColumn(format!(
				"Source '{}' cannot provide subject column '{}'",
				self.name,
				schema.subject.as_str()
			)));
		}

		if !self.can_provide(&schema.time) {
			return Err(EtlError::MissingColumn(format!(
				"Source '{}' cannot provide time column '{}'",
				self.name,
				schema.time.as_str()
			)));
		}

		// Check units this source claims to provide
		if let Some(ref units) = self.etl_units {
			for unit_ref in units {
				let name = unit_ref.as_str();
				match unit_ref {
					EtlUnitRef::Measurement(n) => {
						if let Some(measurement) = schema.get_measurement(n.as_str()) {
							if !self.can_provide(&measurement.value) {
								return Err(EtlError::MissingColumn(format!(
									"Source '{}' claims to provide '{}' but cannot provide value column '{}'",
									self.name, name, measurement.value.as_str()
								)));
							}
						}
					}
					EtlUnitRef::Quality(n) => {
						if let Some(quality) = schema.get_quality(n.as_str()) {
							if !self.can_provide(&quality.value) {
								return Err(EtlError::MissingColumn(format!(
									"Source '{}' claims to provide '{}' but cannot provide value column '{}'",
									self.name, name, quality.value.as_str()
								)));
							}
						}
					}
					EtlUnitRef::Derivation(_) => {
						// Derivations are computed, not directly provided by sources.
						// Validation of base dependencies happens elsewhere.
					}
				}
			}
		}

		Ok(())
	}
}

// ============================================================================
// Stack Configuration
// ============================================================================

/// Configuration for stacking sources with the same codomain.
#[derive(Debug, Clone, Default)]
pub struct StackConfig {
	/// Column name to add for source provenance.
	/// If Some, a column with this name is added containing the source name.
	pub source_column: Option<String>,

	/// Strategy for handling duplicates (by subject, time, components)
	pub dedup: DedupStrategy,
}

impl StackConfig {
	pub fn new() -> Self {
		Self::default()
	}

	/// Add a source provenance column with the given name
	pub fn with_source_column(mut self, name: impl Into<String>) -> Self {
		self.source_column = Some(name.into());
		self
	}

	/// Set the deduplication strategy
	pub fn with_dedup(mut self, strategy: DedupStrategy) -> Self {
		self.dedup = strategy;
		self
	}
}

/// Strategy for handling duplicate (subject, time, [components]) combinations
/// when stacking sources.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DedupStrategy {
	/// Keep all rows (no dedup) - source column differentiates
	#[default]
	KeepAll,

	/// Keep first occurrence by source priority
	FirstWins,

	/// Keep last occurrence by source priority
	LastWins,

	/// Error if duplicates found
	Error,
}

// ============================================================================
// Source Registry
// ============================================================================

/// Registry of bound sources for a schema.
///
/// The `BoundSource` is where we marry the schema with an actual data source.
/// Manages multiple sources and determines how to compose them based on
/// codomain (unit name = value column):
/// - Same codomain → Stack (union rows)
/// - Different codomains → Join on subject
#[derive(Debug)]
pub struct EtlUniverseBuildPlan {
	/// The schema defining all EtlUnits
	pub schema: EtlSchema,

	/// All bound sources
	pub sources: Vec<BoundSource>,

	/// Configuration for stacking sources with same codomain
	/// Note: The domain also needs to match prior to stacking.  Measurements
	/// with a different number of components need to be processed and aligned
	/// if possible. Otherwise, the stacking will fail.
	pub stack_config: StackConfig,
}

/// Controls whether signal policies are applied when reading measurement data.
///
/// Used by `Universe::subset_with_mode()` and `MeasurementData::data_for()`.
/// NOT used during build — extraction always produces raw data. Signal policy
/// is applied lazily on first `Apply` access.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SignalPolicyMode {
	/// Apply signal policies (grid-aligned output). Requires `ensure_processed()`.
	#[default]
	Apply,
	/// Skip signal policies — raw extracted data, no grid alignment.
	Skip,
}

impl EtlUniverseBuildPlan {
	/// Create a build plan with a schema.
	pub fn new(schema: EtlSchema) -> Self {
		Self {
			schema,
			sources: Vec::new(),
			stack_config: StackConfig::default(),
		}
	}

	/// Create with a single source using identity mappings.
	pub fn single(schema: EtlSchema, data: DataFrame) -> Self {
		let source = BoundSource::identity("default", data, &schema);
		Self {
			schema,
			sources: vec![source],
			stack_config: StackConfig::default(),
		}
	}

	/// Create with a single named source using identity mappings.
	pub fn single_named(schema: EtlSchema, name: impl Into<String>, data: DataFrame) -> Self {
		let source = BoundSource::identity(name, data, &schema);
		Self {
			schema,
			sources: vec![source],
			stack_config: StackConfig::default(),
		}
	}

	/// Add a bound source
	pub fn source(mut self, source: BoundSource) -> Self {
		self.sources.push(source);
		self
	}

	/// Set the stack configuration
	pub fn with_stack_config(mut self, config: StackConfig) -> Self {
		self.stack_config = config;
		self
	}

	/// Set stack source column name
	pub fn with_source_column(mut self, name: impl Into<String>) -> Self {
		self.stack_config.source_column = Some(name.into());
		self
	}

	/// Set stack dedup strategy
	pub fn with_dedup(mut self, strategy: DedupStrategy) -> Self {
		self.stack_config.dedup = strategy;
		self
	}

	// =========================================================================
	// Lookups
	// =========================================================================

	/// Get a source by name
	pub fn get_source(&self, name: &str) -> Option<&BoundSource> {
		self.sources.iter().find(|s| s.name == name)
	}

	/// Get all source names
	pub fn source_names(&self) -> Vec<&str> {
		self.sources.iter().map(|s| s.name.as_str()).collect()
	}

	/// Check if this is a single-source registry
	pub fn is_single_source(&self) -> bool {
		self.sources.len() == 1
	}

	/// Find which sources provide a given unit (by codomain / unit name)
	pub fn sources_for_unit(&self, unit_name: &CanonicalColumnName) -> Vec<&BoundSource> {
		self
			.sources
			.iter()
			.filter(|s| {
				match &s.etl_units {
					Some(units) => units.iter().any(|u| u.name() == unit_name),
					None => self.source_can_provide_unit(s, unit_name),
				}
			})
			.collect()
	}

	/// Check if a source can provide a unit (has required columns)
	fn source_can_provide_unit(&self, source: &BoundSource, unit_name: &str) -> bool {
		// Check measurements
		if let Some(m) = self.schema.get_measurement(unit_name) {
			return source.can_provide(&m.value);
		}

		// Check qualities
		if let Some(q) = self.schema.get_quality(unit_name) {
			return source.can_provide(&q.value);
		}

		// Check derived (sources provide base measurements, derived are computed)
		if self.schema.get_derivation(unit_name).is_some() {
			return self.source_provides_derived_dependencies(source, unit_name);
		}

		false
	}

	/// Check if a source provides all dependencies for a derived measurement
	fn source_provides_derived_dependencies(
		&self,
		source: &BoundSource,
		derived_name: &str,
	) -> bool {
		let Some(derived) = self.schema.get_derivation(derived_name) else {
			return false;
		};

		derived
			.input_columns()
			.iter()
			.all(|dep_name| self.source_can_provide_unit(source, dep_name))
	}

	// =========================================================================
	// Composition Planning
	// =========================================================================

	/// Plan how to compose sources for the requested units.
	pub fn plan_composition(&self, unit_names: &[CanonicalColumnName]) -> CompositionPlan {
		let mut unit_strategies = HashMap::new();

		for unit_name in unit_names {
			let strategy = self.plan_unit_composition(unit_name);
			unit_strategies.insert(unit_name.clone(), strategy);
		}

		let join_units = unit_strategies
			.iter()
			.filter(|(_, s)| !s.is_incompatible())
			.map(|(name, _)| name.clone())
			.collect();

		CompositionPlan {
			unit_strategies,
			join_units,
		}
	}

	/// Plan composition for a single unit (codomain)
	fn plan_unit_composition(&self, unit_name: &CanonicalColumnName) -> CompositionStrategy {
		let sources = self.sources_for_unit(unit_name);

		if sources.is_empty() {
			return CompositionStrategy::Incompatible {
				unit:   unit_name.to_string(),
				reason: format!("No source provides unit '{}'", unit_name),
			};
		}

		if sources.len() == 1 {
			return CompositionStrategy::Direct {
				source: sources[0].name.clone(),
			};
		}

		// Multiple sources for the same codomain - validate domain compatibility
		let canonical_signature = match self.schema.get_domain_signature(unit_name) {
			Some(sig) => sig,
			None => {
				return CompositionStrategy::Incompatible {
					unit:   unit_name.to_string(),
					reason: format!("Unit '{}' not found in schema", unit_name),
				};
			}
		};

		let mut reductions = Vec::new();
		let mut source_names = Vec::new();

		for source in &sources {
			match self.validate_source_domain(source, unit_name, &canonical_signature) {
				Ok(extra_components) => {
					// crush extra components
					if !extra_components.is_empty() {
						let agg = self.get_unit_aggregation(unit_name);
						reductions.push(ComponentReduction {
							source:            source.name.clone(),
							reduce_components: extra_components,
							aggregation:       agg,
						});
					}
					source_names.push(source.name.clone());
				}
				// cannot stack because we have a missing component
				Err(reason) => {
					return CompositionStrategy::Incompatible {
						unit: unit_name.to_string(),
						reason,
					};
				}
			}
		}

		CompositionStrategy::Stack {
			sources: source_names,
			reductions,
		}
	}

	/// Validate that a source's domain is compatible with the canonical signature.
	fn validate_source_domain(
		&self,
		source: &BoundSource,
		unit_name: &str,
		canonical: &DomainSignature,
	) -> Result<Vec<SourceColumnName>, String> {
		let source_sig = self
			.source_domain_signature(source, unit_name)
			.ok_or_else(|| {
				format!(
					"Cannot determine domain signature for unit '{}' in source '{}'",
					unit_name, source.name
				)
			})?;

		// Check subject compatibility
		if source_sig.subject != canonical.subject {
			return Err(format!(
				"Source '{}' has incompatible subject '{}' (expected '{}')",
				source.name,
				source_sig.subject.as_str(),
				canonical.subject.as_str()
			));
		}

		// Check time compatibility
		if source_sig.time != canonical.time {
			return Err(format!(
				"Source '{}' has incompatible time column '{:?}' (expected '{:?}')",
				source.name, source_sig.time, canonical.time
			));
		}

		// WARN: This is where the EtlUnit Specification is used to assess the compatiblity
		// with the source data. SourceColumnName interacts with CanonicalColumnName.
		// Check components
		// Extra components need to be crushed before stacking
		let extra_components: Vec<SourceColumnName> = source_sig
			.components
			.iter()
			.filter(|c| !canonical.components.contains(c))
			.map(|c| c.as_str().into())
			.collect();

		let missing_components: Vec<&CanonicalColumnName> = canonical
			.components
			.iter()
			.filter(|c| !source_sig.components.contains(c))
			.collect();

		// we cannot create new components
		if !missing_components.is_empty() {
			return Err(format!(
				"Source '{}' is missing required components: {:?}",
				source.name,
				missing_components
					.iter()
					.map(|c| c.as_str())
					.collect::<Vec<_>>()
			));
		}

		// to be crushed
		Ok(extra_components)
	}

	/// Get the domain signature for a unit as provided by a specific source
	fn source_domain_signature(
		&self,
		_source: &BoundSource,
		unit_name: &str,
	) -> Option<DomainSignature> {
		// Get canonical signature from schema
		// Full implementation would verify actual columns exist and apply transformations
		self.schema.get_domain_signature(unit_name)
	}

	/// Get the default aggregation for a unit based on its measurement kind
	fn get_unit_aggregation(&self, unit_name: &CanonicalColumnName) -> AggregationType {
		if let Some(m) = self.schema.get_measurement(unit_name) {
			match m.kind {
				MeasurementKind::Count => AggregationType::Sum,
				MeasurementKind::Measure => AggregationType::Mean,
				MeasurementKind::Average => AggregationType::Mean,
				MeasurementKind::Categorical => AggregationType::Last,
				MeasurementKind::Binary => AggregationType::Max,
			}
		} else {
			AggregationType::First
		}
	}

	// =========================================================================
	// Validation
	// =========================================================================

	/// Validate all sources against the schema
	pub fn validate(&self) -> EtlResult<()> {
		for source in &self.sources {
			source.validate_against_schema(&self.schema)?;
		}

		// Check that all units can be provided
		for m in &self.schema.measurements {
			if self.sources_for_unit(&m.name).is_empty() {
				return Err(EtlError::Config(format!("No source provides measurement '{}'", m.name)));
			}
		}

		for q in &self.schema.qualities {
			if self.sources_for_unit(&q.name).is_empty() {
				return Err(EtlError::Config(format!("No source provides quality '{}'", q.name)));
			}
		}

		Ok(())
	}
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
	use polars::{df, prelude::DataFrame};

	use super::*;
	use crate::{ColumnNameExt, MeasurementKind};

	fn make_test_schema() -> EtlSchema {
		EtlSchema::new("test")
			.subject("subject_id")
			.time("time")
			.quality("name")
			.measurement_with_defaults("value_a", MeasurementKind::Measure)
			.measurement_with_defaults("value_b", MeasurementKind::Measure)
			.build()
			.unwrap()
	}

	fn make_test_df_a() -> DataFrame {
		df! {
			 "station_id" => [1, 2],
			 "timestamp" => [100i64, 200],
			 "station_name" => ["A", "B"],
			 "reading_a" => [1.0, 2.0]
		}
		.unwrap()
	}

	fn make_test_df_b() -> DataFrame {
		df! {
			 "site_id" => [1, 2],
			 "obs_time" => [100i64, 200],
			 "reading_b" => [10.0, 20.0]
		}
		.unwrap()
	}

	#[test]
	fn test_column_mapping_direct() {
		let mapping = BindingRule::direct("station_id");
		assert_eq!(mapping.source_columns().len(), 1);
		assert_eq!(mapping.source_columns()[0].as_str(), "station_id");
	}

	#[test]
	fn test_column_mapping_computed() {
		let mapping =
			BindingRule::computed(ColumnExpr::struct_key(vec!["region".into(), "pump_num".into()]));
		assert_eq!(mapping.source_columns().len(), 2);
	}

	#[test]
	fn test_bound_source_basic() {
		let df = make_test_df_a();
		let source = BoundSource::new("scada", df)
			.map("subject_id".canonical(), "station_id".source())
			.map("time".canonical(), "timestamp".source())
			.map("name".canonical(), "station_name".source())
			.map("value_a".canonical(), "reading_a".source())
			.provides(vec![EtlUnitRef::quality("name"), EtlUnitRef::measurement("value_a")]);

		let subject = CanonicalColumnName::new("subject_id");
		assert!(source.can_provide(&subject));

		let mapping = source.get_mapping(&subject).unwrap();
		if let BindingRule::Direct(src) = mapping {
			assert_eq!(src.as_str(), "station_id");
		} else {
			panic!("Expected direct mapping");
		}
	}

	#[test]
	fn test_bound_source_identity() {
		let schema = EtlSchema::new("test")
			.subject("station_id")
			.time("timestamp")
			.measurement_with_defaults("reading", MeasurementKind::Measure)
			.build()
			.unwrap();

		let df = df! {
			 "station_id" => [1, 2],
			 "timestamp" => [100i64, 200],
			 "reading" => [1.0, 2.0]
		}
		.unwrap();

		let source = BoundSource::identity("default", df, &schema);

		// Should have mappings for all canonical names
		assert!(source.has_mapping(&CanonicalColumnName::new("station_id")));
		assert!(source.has_mapping(&CanonicalColumnName::new("timestamp")));
		assert!(source.has_mapping(&CanonicalColumnName::new("reading")));
	}

	#[test]
	fn test_bound_source_validation() {
		let df = make_test_df_a();
		let source = BoundSource::new("scada", df)
			.map("subject_id".canonical(), "station_id".source())
			.map("time".canonical(), "timestamp".source())
			.map("value_a".canonical(), "nonexistent_column".source()); // This column doesn't exist

		let result = source.validate();
		assert!(result.is_err());
		assert!(
			result
				.unwrap_err()
				.to_string()
				.contains("nonexistent_column")
		);
	}

	#[test]
	fn test_single_source_registry() {
		let schema = make_test_schema();
		let df = df! {
			 "subject_id" => [1, 2],
			 "time" => [100i64, 200],
			 "name" => ["A", "B"],
			 "value_a" => [1.0, 2.0],
			 "value_b" => [10.0, 20.0]
		}
		.unwrap();

		let registry = EtlUniverseBuildPlan::single(schema, df);

		assert!(registry.is_single_source());
		assert_eq!(registry.source_names(), vec!["default"]);
	}

	#[test]
	fn test_multi_source_registry() {
		let schema = make_test_schema();

		let source_a = BoundSource::new("source_a", make_test_df_a())
			.map("subject_id".canonical(), "station_id".source())
			.map("time".canonical(), "timestamp".source())
			.map("name".canonical(), "station_name".source())
			.map("value_a".canonical(), "reading_a".source())
			.provides(vec![EtlUnitRef::quality("name"), EtlUnitRef::measurement("value_a")]);

		let source_b = BoundSource::new("source_b", make_test_df_b())
			.map("subject_id".canonical(), "site_id".source())
			.map("time".canonical(), "obs_time".source())
			.map("value_b".canonical(), "reading_b".source())
			.provides(vec![EtlUnitRef::measurement("value_b")]);

		let registry = EtlUniverseBuildPlan::new(schema)
			.source(source_a)
			.source(source_b);

		assert!(!registry.is_single_source());
		assert_eq!(registry.source_names().len(), 2);
	}

	#[test]
	fn test_sources_for_unit() {
		let schema = EtlSchema::new("test")
			.subject("subject_id")
			.time("time")
			.measurement_with_defaults("value_a", MeasurementKind::Measure)
			.measurement_with_defaults("value_b", MeasurementKind::Measure)
			.build()
			.unwrap();

		let source_a =
			BoundSource::new("source_a", make_test_df_a()).provides(vec![EtlUnitRef::measurement("value_a")]);

		let source_b =
			BoundSource::new("source_b", make_test_df_b()).provides(vec![EtlUnitRef::measurement("value_b")]);

		let registry = EtlUniverseBuildPlan::new(schema)
			.source(source_a)
			.source(source_b);

		let sources_a = registry.sources_for_unit(&("value_a").into());
		assert_eq!(sources_a.len(), 1);
		assert_eq!(sources_a[0].name, "source_a");

		let sources_b = registry.sources_for_unit(&("value_b").into());
		assert_eq!(sources_b.len(), 1);
		assert_eq!(sources_b[0].name, "source_b");
	}

	#[test]
	fn test_composition_plan_stack() {
		let schema = EtlSchema::new("test")
			.subject("subject_id")
			.time("time")
			.measurement_with_defaults("value_a", MeasurementKind::Measure)
			.build()
			.unwrap();

		let source_north =
			BoundSource::new("region_north", make_test_df_a()).provides(vec![EtlUnitRef::measurement("value_a")]);

		let source_south =
			BoundSource::new("region_south", make_test_df_a()).provides(vec![EtlUnitRef::measurement("value_a")]);

		let registry = EtlUniverseBuildPlan::new(schema)
			.source(source_north)
			.source(source_south);

		let plan = registry.plan_composition(&["value_a".into()]);

		assert!(plan.requires_stacking());
		assert!(!plan.requires_joining());

		let strategy = plan
			.get_strategy(&CanonicalColumnName::new("value_a"))
			.unwrap();
		assert!(strategy.is_stack());
		assert_eq!(strategy.source_names().len(), 2);
	}

	#[test]
	fn test_composition_plan_join() {
		let schema = EtlSchema::new("test")
			.subject("subject_id")
			.time("time")
			.measurement_with_defaults("value_a", MeasurementKind::Measure)
			.measurement_with_defaults("value_b", MeasurementKind::Measure)
			.build()
			.unwrap();

		let source_a =
			BoundSource::new("source_a", make_test_df_a()).provides(vec![EtlUnitRef::measurement("value_a")]);

		let source_b =
			BoundSource::new("source_b", make_test_df_b()).provides(vec![EtlUnitRef::measurement("value_b")]);

		let registry = EtlUniverseBuildPlan::new(schema)
			.source(source_a)
			.source(source_b);

		let plan = registry.plan_composition(&["value_a".into(), "value_b".into()]);

		assert!(plan.requires_joining());
		assert!(!plan.requires_stacking());
		assert_eq!(plan.join_units.len(), 2);
	}

	#[test]
	fn test_stack_config() {
		let config = StackConfig::new()
			.with_source_column("data_source")
			.with_dedup(DedupStrategy::FirstWins);

		assert_eq!(config.source_column, Some("data_source".into()));
		assert_eq!(config.dedup, DedupStrategy::FirstWins);
	}
}
