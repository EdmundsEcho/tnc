//! EtlUnitFragment: Intermediate representation during universe building.
//!
//! Fragments are extracted from source DataFrames using BoundSource mappings.
//! They contain data with **canonical** column names, ready for stacking.
//!
//! ## Data Flow
//!
//! ```text
//! Source DataFrame (SourceColumnName)
//!         │
//!         ├── BoundSource extracts measurement
//!         │   └── MeasurementFragment (CanonicalColumnName)
//!         │
//!         ├── BoundSource extracts quality
//!         │   └── QualityFragment (CanonicalColumnName)
//!         │
//!         └── UnpivotConfig.execute() (bridges Source → Canonical)
//!             └── MeasurementFragment (CanonicalColumnName)
//! ```

use std::collections::HashMap;

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{
	MeasurementKind,
	aggregation::Aggregate,
	column::CanonicalColumnName,
	error::{EtlError, EtlResult},
	polars_fns::SignalPolicyStats,
	universe::measurement_storage::FragmentRef,
};

// =============================================================================
// Fragment Types
// =============================================================================

/// A fragment of an ETL unit extracted from a single source.
///
/// Fragments contain data with **canonical** column names (the extraction
/// process via `BoundSource` handles the Source → Canonical mapping).
#[derive(Debug, Clone)]
pub enum EtlUnitFragment {
	Measurement(MeasurementFragment),
	Quality(QualityFragment),
}

impl EtlUnitFragment {
	/// Get the unit name.
	pub fn name(&self) -> &CanonicalColumnName {
		match self {
			EtlUnitFragment::Measurement(m) => &m.unit_name,
			EtlUnitFragment::Quality(q) => &q.unit_name,
		}
	}

	/// Get the source name.
	pub fn source_name(&self) -> &str {
		match self {
			EtlUnitFragment::Measurement(m) => &m.source_name,
			EtlUnitFragment::Quality(q) => &q.source_name,
		}
	}

	/// Get row count.
	pub fn height(&self) -> usize {
		match self {
			EtlUnitFragment::Measurement(m) => m.fragment.height(),
			EtlUnitFragment::Quality(q) => q.data.height(),
		}
	}

	/// Check if this is a measurement fragment.
	pub fn is_measurement(&self) -> bool {
		matches!(self, EtlUnitFragment::Measurement(_))
	}

	/// Check if this is a quality fragment.
	pub fn is_quality(&self) -> bool {
		matches!(self, EtlUnitFragment::Quality(_))
	}

	/// Get as measurement fragment, if applicable.
	pub fn as_measurement(&self) -> Option<&MeasurementFragment> {
		match self {
			EtlUnitFragment::Measurement(m) => Some(m),
			_ => None,
		}
	}

	/// Get as quality fragment, if applicable.
	pub fn as_quality(&self) -> Option<&QualityFragment> {
		match self {
			EtlUnitFragment::Quality(q) => Some(q),
			_ => None,
		}
	}
}

// =============================================================================
// Measurement Fragment
// =============================================================================

/// A measurement fragment extracted from a single source.
///
/// Contains data with **canonical** column names: subject, time, [components], value.
/// The value column is named using the unit's canonical name.
///
/// The `fragment` field holds a `FragmentRef` — typically a `ColumnRef` pointing
/// into the shared `Arc<DataFrame>` from the `BoundSource`. This avoids copying
/// data during extraction. The data is materialized only when needed (crush,
/// signal policy, stacking).
#[derive(Debug, Clone)]
pub struct MeasurementFragment {
	/// The canonical name of the measurement
	pub unit_name: CanonicalColumnName,

	/// The source this fragment was extracted from
	pub source_name: String,

	/// The measurement kind (affects aggregation during crushing)
	pub kind: MeasurementKind,

	/// Component columns present in this fragment (canonical names)
	pub components: Vec<CanonicalColumnName>,

	/// How this fragment references its data — a ColumnRef into the shared
	/// source DataFrame, or a Materialized DataFrame for transformed data.
	pub fragment: FragmentRef,

	/// Statistics from signal policy application, if any
	pub signal_policy_stats: Option<SignalPolicyStats>,
}

impl MeasurementFragment {
	/// Create a new measurement fragment with an owned DataFrame.
	///
	/// Use for fragments that have already been materialized (unpivot,
	/// truth mapping, etc.). For zero-copy extraction, construct with
	/// `FragmentRef::ColumnRef` directly.
	pub fn new(
		unit_name: impl Into<CanonicalColumnName>,
		source_name: impl Into<String>,
		kind: MeasurementKind,
		components: Vec<CanonicalColumnName>,
		data: DataFrame,
	) -> Self {
		Self {
			unit_name: unit_name.into(),
			source_name: source_name.into(),
			kind,
			components,
			fragment: FragmentRef::Materialized(data),
			signal_policy_stats: None,
		}
	}

	/// Create a fragment with a FragmentRef directly.
	pub fn with_ref(
		unit_name: impl Into<CanonicalColumnName>,
		source_name: impl Into<String>,
		kind: MeasurementKind,
		components: Vec<CanonicalColumnName>,
		fragment: FragmentRef,
	) -> Self {
		Self {
			unit_name: unit_name.into(),
			source_name: source_name.into(),
			kind,
			components,
			fragment,
			signal_policy_stats: None,
		}
	}

	/// Materialize this fragment's data as a DataFrame.
	///
	/// For `ColumnRef`, this selects and renames columns from the shared source.
	/// For `Materialized`, this clones the owned DataFrame.
	pub fn materialize(&self) -> EtlResult<DataFrame> {
		self.fragment.as_dataframe().map_err(Into::into)
	}

	/// Add signal policy stats to this fragment.
	pub fn with_signal_policy_stats(mut self, stats: Option<SignalPolicyStats>) -> Self {
		self.signal_policy_stats = stats;
		self
	}

	/// Check if this fragment has a specific component.
	pub fn has_component(&self, component: &CanonicalColumnName) -> bool {
		self.components.contains(component)
	}

	/// Get the columns in this fragment's data.
	///
	/// For ColumnRef, materializes to inspect columns. For Materialized,
	/// reads directly.
	pub fn columns(&self) -> Vec<String> {
		match &self.fragment {
			FragmentRef::Materialized(df) => df
				.get_column_names()
				.iter()
				.map(|s| s.to_string())
				.collect(),
			_ => {
				// Materialize to get column names
				match self.materialize() {
					Ok(df) => df.get_column_names().iter().map(|s| s.to_string()).collect(),
					Err(_) => Vec::new(),
				}
			}
		}
	}

	/// Validate that required columns exist.
	///
	/// # Arguments
	/// * `subject_col` - The canonical subject column name
	/// * `time_col` - The canonical time column name
	pub fn validate(
		&self,
		subject_col: &CanonicalColumnName,
		time_col: &CanonicalColumnName,
	) -> EtlResult<()> {
		let cols = self.columns();

		if !cols.iter().any(|c| c == subject_col.as_str()) {
			return Err(EtlError::MissingColumn(format!(
				"MeasurementFragment '{}' from '{}' missing subject column '{}'",
				self.unit_name, self.source_name, subject_col
			)));
		}

		if !cols.iter().any(|c| c == time_col.as_str()) {
			return Err(EtlError::MissingColumn(format!(
				"MeasurementFragment '{}' from '{}' missing time column '{}'",
				self.unit_name, self.source_name, time_col
			)));
		}

		if !cols.iter().any(|c| c == self.unit_name.as_str()) {
			return Err(EtlError::MissingColumn(format!(
				"MeasurementFragment '{}' from '{}' missing value column",
				self.unit_name, self.source_name
			)));
		}

		for comp in &self.components {
			if !cols.iter().any(|c| c == comp.as_str()) {
				return Err(EtlError::MissingColumn(format!(
					"MeasurementFragment '{}' from '{}' missing component column '{}'",
					self.unit_name, self.source_name, comp
				)));
			}
		}

		Ok(())
	}
}

impl From<MeasurementFragment> for EtlUnitFragment {
	fn from(m: MeasurementFragment) -> Self {
		EtlUnitFragment::Measurement(m)
	}
}

// =============================================================================
// Quality Fragment
// =============================================================================

/// A quality fragment extracted from a single source.
///
/// Contains data with **canonical** column names: subject, value.
/// The value column is named using the unit's canonical name.
#[derive(Debug, Clone)]
pub struct QualityFragment {
	/// The canonical name of the quality
	pub unit_name: CanonicalColumnName,

	/// The source this fragment was extracted from
	pub source_name: String,

	/// The data with canonical column names
	pub data: DataFrame,
}

impl QualityFragment {
	/// Create a new quality fragment.
	///
	/// # Arguments
	/// * `unit_name` - The canonical quality name
	/// * `source_name` - The source this fragment came from
	/// * `data` - DataFrame with canonical column names
	pub fn new(
		unit_name: impl Into<CanonicalColumnName>,
		source_name: impl Into<String>,
		data: DataFrame,
	) -> Self {
		Self {
			unit_name: unit_name.into(),
			source_name: source_name.into(),
			data,
		}
	}

	/// Get the columns in this fragment's DataFrame.
	pub fn columns(&self) -> Vec<String> {
		self
			.data
			.get_column_names()
			.iter()
			.map(|s| s.to_string())
			.collect()
	}

	/// Validate that required columns exist.
	///
	/// # Arguments
	/// * `subject_col` - The canonical subject column name
	pub fn validate(&self, subject_col: &CanonicalColumnName) -> EtlResult<()> {
		let cols = self.columns();

		if !cols.iter().any(|c| c == subject_col.as_str()) {
			return Err(EtlError::MissingColumn(format!(
				"QualityFragment '{}' from '{}' missing subject column '{}'",
				self.unit_name, self.source_name, subject_col
			)));
		}

		if !cols.iter().any(|c| c == self.unit_name.as_str()) {
			return Err(EtlError::MissingColumn(format!(
				"QualityFragment '{}' from '{}' missing value column",
				self.unit_name, self.source_name
			)));
		}

		Ok(())
	}
}

impl From<QualityFragment> for EtlUnitFragment {
	fn from(q: QualityFragment) -> Self {
		EtlUnitFragment::Quality(q)
	}
}

// =============================================================================
// Fragment Accumulator
// =============================================================================

/// Accumulator for collecting fragments during extraction.
///
/// Separates measurement and quality fragments for appropriate stacking behavior.
#[derive(Debug, Default)]
pub struct FragmentAccumulator {
	pub measurements: HashMap<CanonicalColumnName, Vec<MeasurementFragment>>,
	pub qualities: HashMap<CanonicalColumnName, Vec<QualityFragment>>,
}

impl FragmentAccumulator {
	/// Create a new empty accumulator.
	pub fn new() -> Self {
		Self::default()
	}

	/// Add a fragment (dispatches based on type).
	pub fn add(&mut self, fragment: EtlUnitFragment) {
		match fragment {
			EtlUnitFragment::Measurement(m) => self.add_measurement(m),
			EtlUnitFragment::Quality(q) => self.add_quality(q),
		}
	}

	/// Add a measurement fragment directly.
	pub fn add_measurement(&mut self, fragment: MeasurementFragment) {
		self
			.measurements
			.entry(fragment.unit_name.clone())
			.or_default()
			.push(fragment);
	}

	/// Add a quality fragment directly.
	pub fn add_quality(&mut self, fragment: QualityFragment) {
		self
			.qualities
			.entry(fragment.unit_name.clone())
			.or_default()
			.push(fragment);
	}

	/// Add multiple fragments.
	pub fn add_all(&mut self, fragments: impl IntoIterator<Item = EtlUnitFragment>) {
		for fragment in fragments {
			self.add(fragment);
		}
	}

	/// Get measurement names.
	pub fn measurement_names(&self) -> impl Iterator<Item = &CanonicalColumnName> {
		self.measurements.keys()
	}

	/// Get quality names.
	pub fn quality_names(&self) -> impl Iterator<Item = &CanonicalColumnName> {
		self.qualities.keys()
	}

	/// Get measurement fragments for a specific unit.
	pub fn get_measurement(&self, name: &CanonicalColumnName) -> Option<&Vec<MeasurementFragment>> {
		self.measurements.get(name)
	}

	/// Get quality fragments for a specific unit.
	pub fn get_quality(&self, name: &CanonicalColumnName) -> Option<&Vec<QualityFragment>> {
		self.qualities.get(name)
	}

	/// Number of distinct measurements.
	pub fn measurement_count(&self) -> usize {
		self.measurements.len()
	}

	/// Number of distinct qualities.
	pub fn quality_count(&self) -> usize {
		self.qualities.len()
	}

	/// Total number of fragments.
	pub fn total_fragments(&self) -> usize {
		self.measurements.values().map(|v| v.len()).sum::<usize>()
			+ self.qualities.values().map(|v| v.len()).sum::<usize>()
	}

	/// Check if empty.
	pub fn is_empty(&self) -> bool {
		self.measurements.is_empty() && self.qualities.is_empty()
	}

	/// Consume and return separated maps.
	pub fn into_parts(
		self,
	) -> (
		HashMap<CanonicalColumnName, Vec<MeasurementFragment>>,
		HashMap<CanonicalColumnName, Vec<QualityFragment>>,
	) {
		(self.measurements, self.qualities)
	}
}

// =============================================================================
// Composed Units (After Stacking)
// =============================================================================

/// Record of a component crushed during stacking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrushedComponent {
	/// The measurement this component belongs to
	pub measurement_name: CanonicalColumnName,
	/// The component that was crushed
	pub component_name: CanonicalColumnName,
	/// The inputs that have this component
	pub input_units_with: Vec<String>,
	/// The inputs that don't have this component
	pub input_units_without: Vec<String>,
	/// The reducer required to melt extra components
	pub aggregation: Aggregate,
}

/// A measurement after stacking fragments from multiple sources.
#[derive(Debug, Clone)]
pub struct ComposedMeasurement {
	/// The canonical name
	pub name: CanonicalColumnName,

	/// The measurement kind
	pub kind: MeasurementKind,

	/// Component columns (after any crushing)
	pub components: Vec<CanonicalColumnName>,

	/// Sources that contributed
	pub sources: Vec<String>,

	/// Components that were crushed during stacking
	pub crushed_components: Vec<CrushedComponent>,

	/// The composed data — materialized after stacking/crushing.
	pub fragment: FragmentRef,

	/// Signal policy statistics collected from fragments
	pub signal_policy_stats: Vec<SignalPolicyStats>,
}

impl ComposedMeasurement {
	/// Stack multiple measurement fragments into a composed measurement.
	///
	/// Handles:
	/// - Schema compatibility validation
	/// - Component crushing (when fragments have different components)
	/// - DataFrame concatenation
	/// - Signal policy stats collection
	pub fn from_fragments(
		fragments: Vec<MeasurementFragment>,
		subject_col: &CanonicalColumnName,
		time_col: &CanonicalColumnName,
	) -> EtlResult<Self> {
		if fragments.is_empty() {
			return Err(EtlError::Config(
				"Cannot create ComposedMeasurement from empty fragments".into(),
			));
		}

		let unit_name = fragments[0].unit_name.clone();
		let kind = fragments[0].kind;

		// Validate all fragments
		for frag in &fragments {
			if frag.unit_name != unit_name {
				return Err(EtlError::Config(format!(
					"Fragment unit mismatch: expected '{}', got '{}'",
					unit_name, frag.unit_name
				)));
			}
			frag.validate(subject_col, time_col)?;
		}

		let sources: Vec<String> = fragments.iter().map(|f| f.source_name.clone()).collect();

		// Collect signal policy stats from all fragments
		let signal_policy_stats: Vec<SignalPolicyStats> = fragments
			.iter()
			.filter_map(|f| f.signal_policy_stats.clone())
			.collect();

		// Single fragment, no stacking needed — pass FragmentRef through.
		if fragments.len() == 1 {
			let frag = fragments.into_iter().next().unwrap();
			debug!(
				 unit = %unit_name,
				 source = ?sources,
				 storage = ?frag.fragment.storage_description().kind,
				 "Single fragment — passing through (no stacking)"
			);
			return Ok(Self {
				name: unit_name,
				kind,
				components: frag.components,
				sources,
				crushed_components: Vec::new(),
				fragment: frag.fragment,
				signal_policy_stats,
			});
		}

		// Multiple fragments — analyze components and stack
		let (final_components, crushed_components, processed_dfs) =
			Self::analyze_components(&fragments, kind)?;

		debug!(
			 unit = %unit_name,
			 sources = ?sources,
			 components = final_components.len(),
			 crushed = crushed_components.len(),
			 signal_policy_stats = signal_policy_stats.len(),
			 "Stacking measurement fragments"
		);

		// Stack DataFrames
		let stacked = Self::stack_dataframes(
			processed_dfs,
			subject_col,
			time_col,
			&final_components,
			&unit_name,
		)?;

		Ok(Self {
			name: unit_name,
			kind,
			components: final_components,
			sources,
			crushed_components,
			fragment: FragmentRef::Materialized(stacked),
			signal_policy_stats,
		})
	}

	/// Analyze component availability across fragments and determine crushing.
	fn analyze_components(
		fragments: &[MeasurementFragment],
		kind: MeasurementKind,
	) -> EtlResult<(Vec<CanonicalColumnName>, Vec<CrushedComponent>, Vec<DataFrame>)> {
		use std::collections::HashSet;

		// Single fragment: no crushing needed
		if fragments.len() == 1 {
			return Ok((fragments[0].components.clone(), Vec::new(), vec![fragments[0].materialize()?]));
		}

		// Collect component availability across sources
		let mut component_sources: HashMap<CanonicalColumnName, HashSet<String>> = HashMap::new();
		for frag in fragments {
			for comp in &frag.components {
				component_sources
					.entry(comp.clone())
					.or_default()
					.insert(frag.source_name.clone());
			}
		}

		let all_sources: HashSet<String> = fragments.iter().map(|f| f.source_name.clone()).collect();

		let mut final_components = Vec::new();
		let mut crushed = Vec::new();

		for (comp, sources_with) in &component_sources {
			if sources_with.len() == fragments.len() {
				// All fragments have this component
				final_components.push(comp.clone());
			} else {
				// Some fragments missing: must crush
				let sources_without: Vec<String> = all_sources
					.iter()
					.filter(|s| !sources_with.contains(*s))
					.cloned()
					.collect();

				let aggregation = kind.default_aggregation();

				warn!(
					 component = %comp,
					 sources_with = ?sources_with.iter().collect::<Vec<_>>(),
					 sources_without = ?sources_without,
					 aggregation = ?aggregation,
					 "Crushing component"
				);

				crushed.push(CrushedComponent {
					measurement_name: fragments[0].unit_name.clone(),
					component_name: comp.clone(),
					input_units_with: sources_with.iter().cloned().collect(),
					input_units_without: sources_without,
					aggregation,
				});
			}
		}

		// Sort for deterministic ordering
		final_components.sort_by(|a, b| a.as_str().cmp(b.as_str()));

		// Process fragments: crush extra components where needed
		let processed: EtlResult<Vec<DataFrame>> = fragments
			.iter()
			.map(|frag| {
				let needs_crushing = frag
					.components
					.iter()
					.any(|c| crushed.iter().any(|cc| &cc.component_name == c));

				if needs_crushing {
					Self::crush_fragment(frag, &final_components, kind)
				} else {
					frag.materialize()
				}
			})
			.collect();

		Ok((final_components, crushed, processed?))
	}

	/// Crush extra components from a fragment by aggregating.
	fn crush_fragment(
		frag: &MeasurementFragment,
		keep_components: &[CanonicalColumnName],
		kind: MeasurementKind,
	) -> EtlResult<DataFrame> {
		let data = frag.materialize()?;
		let col_names: Vec<String> = data
			.get_column_names()
			.iter()
			.map(|s| s.to_string())
			.collect();

		// First two columns are subject and time by convention
		let subject_col = &col_names[0];
		let time_col = &col_names[1];

		let mut group_cols: Vec<Expr> = vec![col(subject_col), col(time_col)];
		for comp in keep_components {
			if col_names.contains(&comp.as_str().to_string()) {
				group_cols.push(col(comp.as_str()));
			}
		}

		let agg = kind.default_aggregation();

		// MostRecent/LeastRecent: sort by the crushed component column,
		// then deduplicate on (subject, time) keeping last/first.
		// This selects the value from the row with the max/min component.
		if matches!(agg, Aggregate::MostRecent | Aggregate::LeastRecent) {
			// Identify the component being crushed (in fragment but not in keep_components)
			let crushed_comp = frag.components.iter()
				.find(|c| !keep_components.contains(c));

			if let Some(comp_col) = crushed_comp {
				let descending = matches!(agg, Aggregate::MostRecent);
				// Sort by group keys + component, then deduplicate on group keys
				// keeping the first row (which is the max/min component after sort)
				let mut sort_cols = group_cols.iter()
					.map(|e| e.clone())
					.collect::<Vec<_>>();
				sort_cols.push(col(comp_col.as_str()));

				let sort_descending: Vec<bool> = sort_cols.iter()
					.enumerate()
					.map(|(i, _)| i == sort_cols.len() - 1 && descending)
					.collect();

				// Sort by group keys + component (desc for MostRecent),
				// then group_by the group keys and take first() of the value.
				// This picks the value from the row with max/min component.
				let value_agg = col(frag.unit_name.as_str()).first()
					.alias(frag.unit_name.as_str());

				return data.clone().lazy()
					.sort_by_exprs(
						sort_cols,
						SortMultipleOptions::new().with_order_descending_multi(sort_descending),
					)
					.group_by(group_cols)
					.agg([value_agg])
					.collect()
					.map_err(Into::into);
			}
			// Fallback if no crushed component found — use mean
			tracing::warn!(
				measurement = frag.unit_name.as_str(),
				"MostRecent/LeastRecent: no component to sort by, falling back to mean"
			);
		}

		let agg_expr = match agg {
			Aggregate::Mean => col(frag.unit_name.as_str()).mean(),
			Aggregate::Sum => col(frag.unit_name.as_str()).sum(),
			Aggregate::Min => col(frag.unit_name.as_str()).min(),
			Aggregate::Max => col(frag.unit_name.as_str()).max(),
			Aggregate::Last => col(frag.unit_name.as_str()).last(),
			Aggregate::First => col(frag.unit_name.as_str()).first(),
			Aggregate::Count => col(frag.unit_name.as_str()).count(),
			_ => col(frag.unit_name.as_str()).mean(),
		}
		.alias(frag.unit_name.as_str());

		data
			.clone()
			.lazy()
			.group_by(group_cols)
			.agg([agg_expr])
			.collect()
			.map_err(Into::into)
	}

	/// Stack DataFrames with consistent column selection.
	fn stack_dataframes(
		dfs: Vec<DataFrame>,
		subject_col: &CanonicalColumnName,
		time_col: &CanonicalColumnName,
		components: &[CanonicalColumnName],
		value_col: &CanonicalColumnName,
	) -> EtlResult<DataFrame> {
		let mut select_cols: Vec<Expr> = vec![col(subject_col.as_str()), col(time_col.as_str())];

		for comp in components {
			select_cols.push(col(comp.as_str()));
		}
		select_cols.push(col(value_col.as_str()));

		let normalized: Vec<LazyFrame> = dfs
			.into_iter()
			.map(|df| df.lazy().select(select_cols.clone()))
			.collect();

		concat(normalized, UnionArgs::default())?
			.collect()
			.map_err(Into::into)
	}

	/// Check if a component was crushed.
	pub fn was_crushed(&self, component: &CanonicalColumnName) -> bool {
		self
			.crushed_components
			.iter()
			.any(|c| &c.component_name == component)
	}

	/// Get row count.
	pub fn height(&self) -> usize {
		self.fragment.height()
	}
}

/// A quality after stacking/deduplicating fragments from multiple sources.
#[derive(Debug, Clone)]
pub struct ComposedQuality {
	/// The canonical name
	pub name: CanonicalColumnName,

	/// Sources that contributed
	pub sources: Vec<String>,

	/// The composed data (deduplicated by subject)
	pub data: DataFrame,
}

impl ComposedQuality {
	/// Compose quality fragments.
	///
	/// Qualities are deduplicated by subject, taking the first value encountered.
	pub fn from_fragments(
		fragments: Vec<QualityFragment>,
		subject_col: &CanonicalColumnName,
	) -> EtlResult<Self> {
		if fragments.is_empty() {
			return Err(EtlError::Config("Cannot create ComposedQuality from empty fragments".into()));
		}

		let unit_name = fragments[0].unit_name.clone();

		// Validate all fragments
		for frag in &fragments {
			if frag.unit_name != unit_name {
				return Err(EtlError::Config(format!(
					"Fragment unit mismatch: expected '{}', got '{}'",
					unit_name, frag.unit_name
				)));
			}
			frag.validate(subject_col)?;
		}

		let sources: Vec<String> = fragments.iter().map(|f| f.source_name.clone()).collect();

		debug!(
			 unit = %unit_name,
			 sources = ?sources,
			 "Stacking quality fragments"
		);

		// Stack all fragments with consistent column selection
		let dfs: Vec<LazyFrame> = fragments
			.into_iter()
			.map(|f| {
				f.data
					.lazy()
					.select([col(subject_col.as_str()), col(unit_name.as_str())])
			})
			.collect();

		// Concatenate and dedupe (first value wins)
		let stacked = concat(dfs, UnionArgs::default())?
			.unique(Some(subject_col.into()), UniqueKeepStrategy::First)
			.collect()?;

		Ok(Self {
			name: unit_name,
			sources,
			data: stacked,
		})
	}

	/// Get row count.
	pub fn height(&self) -> usize {
		self.data.height()
	}
}

// =============================================================================
// Stacking Operation
// =============================================================================

/// Stack all fragments into composed units.
///
/// # Arguments
/// * `accumulator` - The collected fragments
/// * `subject_col` - The canonical subject column name
/// * `time_col` - The canonical time column name
///
/// # Returns
/// Tuple of (composed measurements, composed qualities)
pub fn stack_all_fragments(
	accumulator: FragmentAccumulator,
	subject_col: &CanonicalColumnName,
	time_col: &CanonicalColumnName,
) -> EtlResult<(Vec<ComposedMeasurement>, Vec<ComposedQuality>)> {
	let (measurements, qualities) = accumulator.into_parts();

	let composed_measurements: EtlResult<Vec<ComposedMeasurement>> = measurements
		.into_values()
		.map(|frags| ComposedMeasurement::from_fragments(frags, subject_col, time_col))
		.collect();

	let composed_qualities: EtlResult<Vec<ComposedQuality>> = qualities
		.into_values()
		.map(|frags| ComposedQuality::from_fragments(frags, subject_col))
		.collect();

	Ok((composed_measurements?, composed_qualities?))
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
	use super::*;

	fn make_measurement_fragment(
		unit_name: &str,
		source_name: &str,
		components: Vec<&str>,
	) -> MeasurementFragment {
		let mut df = df! {
			 "subject" => ["A", "B"],
			 "time" => [100i64, 200],
			 unit_name => [1.0, 2.0]
		}
		.unwrap();

		for comp in &components {
			df = df
				.lazy()
				.with_column(lit("val").alias(*comp))
				.collect()
				.unwrap();
		}

		MeasurementFragment::new(
			unit_name,
			source_name,
			MeasurementKind::Measure,
			components
				.into_iter()
				.map(CanonicalColumnName::from)
				.collect(),
			df,
		)
	}

	fn make_quality_fragment(unit_name: &str, source_name: &str) -> QualityFragment {
		QualityFragment::new(
			unit_name,
			source_name,
			df! {
				 "subject" => ["A", "B"],
				 unit_name => ["val_a", "val_b"]
			}
			.unwrap(),
		)
	}

	#[test]
	fn test_measurement_fragment_validation() {
		let frag = make_measurement_fragment("temp", "source_a", vec!["sensor"]);

		assert!(frag.validate(&"subject".into(), &"time".into()).is_ok());
		assert!(frag.validate(&"wrong".into(), &"time".into()).is_err());
	}

	#[test]
	fn test_quality_fragment_validation() {
		let frag = make_quality_fragment("name", "source_a");

		assert!(frag.validate(&"subject".into()).is_ok());
		assert!(frag.validate(&"wrong".into()).is_err());
	}

	#[test]
	fn test_accumulator_separates_types() {
		let mut acc = FragmentAccumulator::new();

		acc.add(make_measurement_fragment("temp", "source_a", vec![]).into());
		acc.add(make_quality_fragment("name", "source_a").into());

		assert_eq!(acc.measurement_count(), 1);
		assert_eq!(acc.quality_count(), 1);
		assert_eq!(acc.total_fragments(), 2);
	}

	#[test]
	fn test_composed_measurement_single_fragment() {
		let frag = make_measurement_fragment("temp", "source_a", vec!["sensor"]);

		let composed =
			ComposedMeasurement::from_fragments(vec![frag], &"subject".into(), &"time".into())
				.unwrap();

		assert_eq!(composed.name.as_str(), "temp");
		assert_eq!(composed.sources, vec!["source_a"]);
		assert_eq!(composed.components.len(), 1);
		assert!(composed.crushed_components.is_empty());
		assert!(composed.signal_policy_stats.is_empty());
	}

	#[test]
	fn test_composed_measurement_stacks_compatible() {
		let frag_a = make_measurement_fragment("temp", "source_a", vec!["sensor"]);
		let frag_b = make_measurement_fragment("temp", "source_b", vec!["sensor"]);

		let composed = ComposedMeasurement::from_fragments(
			vec![frag_a, frag_b],
			&"subject".into(),
			&"time".into(),
		)
		.unwrap();

		assert_eq!(composed.sources.len(), 2);
		assert_eq!(composed.height(), 4); // 2 + 2 rows
		assert!(composed.crushed_components.is_empty());
	}

	#[test]
	fn test_composed_measurement_crushes_incompatible() {
		let frag_a = make_measurement_fragment("temp", "source_a", vec!["color"]);
		let frag_b = make_measurement_fragment("temp", "source_b", vec![]); // missing color

		let composed = ComposedMeasurement::from_fragments(
			vec![frag_a, frag_b],
			&"subject".into(),
			&"time".into(),
		)
		.unwrap();

		assert_eq!(composed.crushed_components.len(), 1);
		assert_eq!(composed.crushed_components[0].component_name.as_str(), "color");
		assert!(composed.components.is_empty());
	}

	#[test]
	fn test_composed_quality_deduplicates() {
		let frag_a = QualityFragment::new(
			"name",
			"source_a",
			df! { "subject" => ["A", "B"], "name" => ["First A", "First B"] }.unwrap(),
		);
		let frag_b = QualityFragment::new(
			"name",
			"source_b",
			df! { "subject" => ["A", "C"], "name" => ["Second A", "First C"] }.unwrap(),
		);

		let composed =
			ComposedQuality::from_fragments(vec![frag_a, frag_b], &"subject".into()).unwrap();

		// 3 unique subjects: A, B, C
		assert_eq!(composed.height(), 3);
	}

	#[test]
	fn test_stack_all_fragments() {
		let mut acc = FragmentAccumulator::new();

		acc.add_measurement(make_measurement_fragment("temp", "source_a", vec![]));
		acc.add_measurement(make_measurement_fragment("temp", "source_b", vec![]));
		acc.add_quality(make_quality_fragment("name", "source_a"));

		let (measurements, qualities) =
			stack_all_fragments(acc, &"subject".into(), &"time".into()).unwrap();

		assert_eq!(measurements.len(), 1);
		assert_eq!(qualities.len(), 1);
		assert_eq!(measurements[0].height(), 4); // stacked
		assert_eq!(qualities[0].height(), 2); // deduplicated
	}
}
