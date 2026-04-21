//! SubsetUniverse: the result of executing a subset request.
//!
//! Contains clean data plus metadata needed for presentation.
//! At this level, there's no distinction between source and derived measurements—
//! they're all just columns with metadata.

pub mod meta;
pub mod stages;
pub mod subset_executor;
use std::marker::PhantomData;

use chrono::{DateTime, Utc};
pub use meta::*;
use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};
pub use subset_executor::SubsetExecutor;

use crate::{CanonicalColumnName, MeasurementKind, chart_hints::ChartHints};

// ============================================================================
// Subset mode markers
// ============================================================================
//
// `SubsetUniverse` is parameterized by a mode marker so the compiler can
// distinguish "raw observations (original timestamps, nulls preserved)" from
// "processed data (grid-aligned, signal-policy + null-fill applied)". The
// marker rides along in a `PhantomData`; at runtime the struct is identical
// in both cases. At compile time, consumers constrain their inputs (e.g.,
// `decimate_subset_processed` takes `SubsetUniverse<Processed>`, so passing
// raw data is a type error rather than a runtime panic).

/// Marker for raw subsets: original observations at their original timestamps,
/// no grid alignment, no signal policy, nulls preserved.
#[derive(Debug, Clone, Copy)]
pub struct Raw;

/// Marker for processed subsets: grid-aligned, signal-policy applied,
/// `null_value` and `null_value_extension` filled. Invariant: no nulls in
/// requested measurement columns.
#[derive(Debug, Clone, Copy)]
pub struct Processed;

mod sealed {
	pub trait Sealed {}
	impl Sealed for super::Raw {}
	impl Sealed for super::Processed {}
}

/// Sealed trait: only `Raw` and `Processed` are valid subset modes.
pub trait SubsetMode: sealed::Sealed {}
impl SubsetMode for Raw {}
impl SubsetMode for Processed {}

/// Universe, SubsetRequest -> SubsetUniverse -> Chart
/// The result of executing a subset request.
/// Contains clean data plus metadata needed for presentation.
///
/// The `M` type parameter records whether this subset is [`Raw`] or
/// [`Processed`]. Defaults to `Processed` so existing code that names
/// `SubsetUniverse` bare keeps working for the common case.
#[derive(Debug, Clone)]
pub struct SubsetUniverse<M: SubsetMode = Processed> {
	/// The clean DataFrame (no nulls in requested columns when `M = Processed`)
	pub data: DataFrame,

	/// Metadata for each measurement column
	pub measurements: Vec<MeasurementMeta>,

	/// Metadata for each quality column
	pub qualities: Vec<QualityMeta>,

	/// General information about the subset
	pub info: SubsetInfo,

	/// Compile-time mode marker (zero-sized).
	pub(crate) _mode: PhantomData<M>,
}

impl<M: SubsetMode> SubsetUniverse<M> {
	/// Create a new SubsetUniverse in the caller's chosen mode. Use via
	/// [`SubsetUniverse::new_processed`] or [`SubsetUniverse::new_raw`] at
	/// call sites so the mode is spelled out.
	pub fn new_in_mode(
		data: DataFrame,
		measurements: Vec<MeasurementMeta>,
		qualities: Vec<QualityMeta>,
		info: SubsetInfo,
	) -> Self {
		Self {
			data,
			measurements,
			qualities,
			info,
			_mode: PhantomData,
		}
	}

	/// Get the DataFrame
	pub fn dataframe(&self) -> &DataFrame {
		&self.data
	}

	/// Consume and return the DataFrame
	pub fn into_dataframe(self) -> DataFrame {
		self.data
	}

	/// Replace the DataFrame (e.g., after transforms). Mode is preserved.
	pub fn with_dataframe(mut self, data: DataFrame) -> Self {
		self.info.row_count = data.height();
		self.data = data;
		self
	}

	/// Get measurement metadata by column name
	pub fn get_measurement(&self, column: &str) -> Option<&MeasurementMeta> {
		self.measurements.iter().find(|m| m.column == column.into())
	}

	/// Get quality metadata by column name
	pub fn get_quality(&self, column: &str) -> Option<&QualityMeta> {
		self.qualities.iter().find(|q| q.column == column.into())
	}

	/// Get all measurement column names
	pub fn measurement_columns(&self) -> Vec<&str> {
		self
			.measurements
			.iter()
			.map(|m| m.column.as_str())
			.collect()
	}

	/// Get all quality column names
	pub fn quality_columns(&self) -> Vec<&str> {
		self.qualities.iter().map(|q| q.column.as_str()).collect()
	}

	/// Check if this subset has any measurements
	pub fn has_measurements(&self) -> bool {
		!self.measurements.is_empty()
	}

	/// Check if this subset has any qualities
	pub fn has_qualities(&self) -> bool {
		!self.qualities.is_empty()
	}

	/// Get the time column name
	pub fn time_column(&self) -> Option<&str> {
		self.info.time_column.as_deref()
	}

	/// Get the subject column name
	pub fn subject_column(&self) -> &str {
		&self.info.subject_column
	}
}

impl SubsetUniverse<Processed> {
	/// Construct a processed subset. Caller asserts that downstream
	/// contracts (no nulls in requested columns, grid alignment, etc.)
	/// have been honored.
	pub fn new_processed(
		data: DataFrame,
		measurements: Vec<MeasurementMeta>,
		qualities: Vec<QualityMeta>,
		info: SubsetInfo,
	) -> Self {
		Self::new_in_mode(data, measurements, qualities, info)
	}

	/// Back-compat constructor kept to reduce churn in existing code paths
	/// that predate the phantom marker. Prefer [`new_processed`] for new
	/// callers — it spells the mode out.
	pub fn new(
		data: DataFrame,
		measurements: Vec<MeasurementMeta>,
		qualities: Vec<QualityMeta>,
		info: SubsetInfo,
	) -> Self {
		Self::new_processed(data, measurements, qualities, info)
	}
}

impl SubsetUniverse<Raw> {
	/// Construct a raw subset — original observations, nulls preserved,
	/// no grid alignment.
	pub fn new_raw(
		data: DataFrame,
		measurements: Vec<MeasurementMeta>,
		qualities: Vec<QualityMeta>,
		info: SubsetInfo,
	) -> Self {
		Self::new_in_mode(data, measurements, qualities, info)
	}
}

/// Metadata for a measurement column in the subset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasurementMeta {
	/// Column name in the DataFrame (= unit name = codomain)
	pub column: CanonicalColumnName,

	/// Measurement kind (for aggregation semantics)
	pub kind: MeasurementKind,

	/// Chart presentation hints
	pub chart_hints: ChartHints,

	/// Whether the measurement declared a `null_value` in its config.
	///
	/// Downstream strictness depends on this flag: if the author configured
	/// a `null_value`, they expect every grid cell to be filled — nulls are
	/// a pipeline bug. If they didn't configure one, nulls are accepted as
	/// legitimate "no observation" markers and are dropped per-column before
	/// decimation rather than raising an error.
	#[serde(default)]
	pub has_null_value: bool,
}

impl MeasurementMeta {
	pub fn new<T: Into<CanonicalColumnName>>(column: T, kind: MeasurementKind) -> Self {
		let kind_hints = match kind {
			MeasurementKind::Categorical => ChartHints::categorical(),
			_ => ChartHints::measure(),
		};
		Self {
			column: column.into(),
			kind,
			chart_hints: kind_hints,
			has_null_value: false,
		}
	}

	pub fn with_chart_hints(mut self, hints: ChartHints) -> Self {
		self.chart_hints = hints;
		self
	}

	/// Mark this measurement as having a configured `null_value`.
	/// Downstream decimators will enforce the no-nulls invariant on it.
	pub fn with_null_value_configured(mut self, has_null_value: bool) -> Self {
		self.has_null_value = has_null_value;
		self
	}
}

/// Metadata for a quality column in the subset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityMeta {
	/// Column name in the DataFrame
	pub column: CanonicalColumnName,

	/// Chart presentation hints
	pub chart_hints: ChartHints,
}

impl QualityMeta {
	pub fn new(column: CanonicalColumnName) -> Self {
		Self {
			column,
			chart_hints: ChartHints::quality(),
		}
	}

	pub fn with_chart_hints(mut self, hints: ChartHints) -> Self {
		self.chart_hints = hints;
		self
	}
}

/// General information about the subset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubsetInfo {
	/// Schema name
	pub schema_name: String,

	/// Subject column name in the DataFrame
	pub subject_column: String,

	/// Time column name (None for quality-only subsets)
	#[serde(skip_serializing_if = "Option::is_none")]
	pub time_column: Option<String>,

	/// Row count
	pub row_count: usize,

	/// Distinct subject count
	pub subject_count: usize,

	/// Time range of the data
	#[serde(skip_serializing_if = "Option::is_none")]
	pub time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,

	/// Sources that contributed data (for provenance)
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub sources: Vec<String>,

	/// Subset pipeline stage trace — what transformations were applied.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub stage_trace: Vec<stages::StageDiag>,

	/// When the request carried a `report_interval`, the per-cell stats
	/// for every `(subject, bucket, measurement)` produced by
	/// [`crate::interval::apply_interval`]. Empty otherwise.
	///
	/// Each row carries N, null_count, value, stderr, min, max, and the
	/// resampling path the planner chose — enough for the UI to show
	/// "monthly mean sump = 3.42 (N=43,180; stderr=0.02; min=-0.9, max=8.1)"
	/// and for analytics to weight or filter by N.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub interval_stats: Vec<crate::interval::IntervalStats>,
}

impl SubsetInfo {
	pub fn new(schema_name: impl Into<String>, subject_column: impl Into<String>) -> Self {
		Self {
			schema_name:     schema_name.into(),
			subject_column:  subject_column.into(),
			time_column:     None,
			row_count:       0,
			subject_count:   0,
			time_range:      None,
			sources:         Vec::new(),
			stage_trace:     Vec::new(),
			interval_stats:  Vec::new(),
		}
	}

	pub fn with_time_column(mut self, time_column: impl Into<String>) -> Self {
		self.time_column = Some(time_column.into());
		self
	}

	pub fn with_row_count(mut self, count: usize) -> Self {
		self.row_count = count;
		self
	}

	pub fn with_subject_count(mut self, count: usize) -> Self {
		self.subject_count = count;
		self
	}

	pub fn with_time_range(mut self, start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
		self.time_range = Some((start, end));
		self
	}

	pub fn with_sources(mut self, sources: Vec<String>) -> Self {
		self.sources = sources;
		self
	}

	pub fn add_source(mut self, source: impl Into<String>) -> Self {
		self.sources.push(source.into());
		self
	}
}

#[cfg(test)]
mod tests {
	use polars::prelude::*;

	use super::*;
	use crate::chart_hints::AxisId;

	#[test]
	fn test_measurement_meta() {
		let meta = MeasurementMeta::new("sump_ft", MeasurementKind::Measure);
		assert_eq!(meta.column, "sump_ft".into());
		assert_eq!(meta.kind, MeasurementKind::Measure);
		assert!(!meta.chart_hints.stepped);
	}

	#[test]
	fn test_measurement_meta_categorical() {
		let meta = MeasurementMeta::new("engine_1", MeasurementKind::Categorical);
		assert!(meta.chart_hints.stepped);
		assert_eq!(meta.chart_hints.axis, AxisId::Y2);
	}

	#[test]
	fn test_measurement_meta_custom_hints() {
		let meta = MeasurementMeta::new("fuel_pct", MeasurementKind::Measure)
			.with_chart_hints(ChartHints::new().axis(AxisId::Y1).label("Fuel Level"));

		assert_eq!(meta.chart_hints.axis, AxisId::Y1);
		assert_eq!(meta.chart_hints.label, Some("Fuel Level".into()));
	}

	#[test]
	fn test_quality_meta() {
		let meta = QualityMeta::new("region".into());
		assert_eq!(meta.column, "region".into());
		// Quality defaults to bar chart with subject index
		assert_eq!(meta.chart_hints.chart_type, crate::chart_hints::ChartType::Bar);
		assert_eq!(meta.chart_hints.index, crate::chart_hints::Index::Subject);
	}

	#[test]
	fn test_subset_info() {
		let info = SubsetInfo::new("pump_telemetry", "station_id")
			.with_time_column("timestamp")
			.with_row_count(100)
			.with_subject_count(5)
			.with_sources(vec!["scada".into()]);

		assert_eq!(info.schema_name, "pump_telemetry");
		assert_eq!(info.subject_column, "station_id");
		assert_eq!(info.time_column, Some("timestamp".into()));
		assert_eq!(info.row_count, 100);
		assert_eq!(info.subject_count, 5);
		assert_eq!(info.sources, vec!["scada"]);
	}

	#[test]
	fn test_subset_universe() {
		let df = df! {
			 "station_id" => [1, 1, 2, 2],
			 "timestamp" => [100i64, 200, 100, 200],
			 "sump_ft" => [1.0, 2.0, 3.0, 4.0],
			 "engine_1" => [0, 1, 1, 0]
		}
		.unwrap();

		let measurements = vec![
			MeasurementMeta::new("sump_ft", MeasurementKind::Measure),
			MeasurementMeta::new("engine_1", MeasurementKind::Categorical),
		];

		let info = SubsetInfo::new("test", "station_id")
			.with_time_column("timestamp")
			.with_row_count(4)
			.with_subject_count(2);

		let universe = SubsetUniverse::new(df, measurements, vec![], info);

		assert_eq!(universe.measurement_columns(), vec!["sump_ft", "engine_1"]);
		assert!(universe.has_measurements());
		assert!(!universe.has_qualities());
		assert_eq!(universe.time_column(), Some("timestamp"));
		assert_eq!(universe.subject_column(), "station_id");

		let sump = universe.get_measurement("sump_ft").unwrap();
		assert_eq!(sump.kind, MeasurementKind::Measure);

		let engine = universe.get_measurement("engine_1").unwrap();
		assert!(engine.chart_hints.stepped);
	}
}
