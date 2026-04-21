//! Universe of ETL Units
//!
//! The Universe stores measurements and qualities separately in HashMaps,
//! deferring composition until a SubsetRequest is made. This avoids null-introduction
//! problems that arise from joining measurements with different component structures.
//!
//! ## Storage Model
//!
//! ```text
//! Universe
//! ├── measurements: HashMap<Name, MeasurementData>
//! │   ├── "sump"          → (MeasurementUnit, DataFrame[subject, time, sump])
//! │   ├── "fuel"          → (MeasurementUnit, DataFrame[subject, time, fuel])
//! │   └── "engine_status" → (MeasurementUnit, DataFrame[subject, time, engine, engine_status])
//! │                         ^ components preserved until subset time
//! └── qualities: HashMap<Name, QualityData>
//!     └── "region"        → (QualityUnit, DataFrame[subject, region])
//! ```
//!
//! ## Subset Composition
//!
//! When `Universe::subset()` is called:
//! 1. Determine output interval (request interval or longest TTL)
//! 2. Build master time grid (subject × time at interval)
//! 3. For each measurement: crush components → resample → join
//!    - Downsample (TTL < interval): aggregate, then equi-join
//!    - Upsample (TTL > interval): truncate time, then asof join with TTL tolerance
//! 4. Apply `null_value_extension` after each join
//! 5. Left join qualities
//! 6. Compute derivations on composed data
//!
//! Components are always crushed during subset - the request's component filters are ignored.

use std::{collections::HashMap, time::Duration};

use chrono::Utc;
use polars::prelude::*;
use tracing::{debug, info, warn};

use super::{ComposedMeasurement, ComposedQuality, FragmentRef, UniverseBuildInfo, derivation};
use crate::{
	Aggregate, BoundSource, CanonicalColumnName, DataTemporality, EtlError, EtlResult, EtlSchema,
	MeasurementKind, MeasurementUnit, NullValue, QualityUnit, SignalPolicyMode,
	polars_fns::SignalPolicyStats,
	request::EtlUnitSubsetRequest,
	subset::{MeasurementMeta, QualityMeta, SubsetInfo, SubsetUniverse},
};

// =============================================================================
// Constants
// =============================================================================

/// Default TTL applied to measurements that declare no `signal_policy`.
///
/// One source of truth for the staleness window fallback. Used by
/// [`MeasurementData::ttl`] when no policy is declared, and by the
/// wide-join executor's per-source TTL resolution when a `SourceJoin`'s
/// `signal_config.ttl_ms` is `None`.
// DEFAULT_TTL removed — every measurement must have a signal policy
// with an explicit TTL configured. Schema validation enforces this.
// If you need a TTL value, read it from the measurement's signal policy.

// =============================================================================
// Data Wrappers
// =============================================================================

/// The lifecycle state of a measurement's data in the Universe.
///
/// ```text
///  Created ──ensure_aligned()──► Aligned
///     ▲                             │
///     └──invalidate_aligned()───────┘
/// ```
///
/// - **Raw**: data is a `FragmentRef` (ColumnRef or Materialized).
///   No signal policy or resampling applied. This is the initial state.
/// - **Aligned**: signal policy + upsample + downsample applied.
///   The `FragmentRef` is preserved for memory diagnostics.
///   The `DataFrame` is the processed result, cached for subset reuse.
#[derive(Debug, Clone)]
pub enum MeasurementState {
	/// Raw data — no signal policy or resampling applied.
	Raw {
		fragment: FragmentRef,
	},
	/// Aligned data — signal policy + upsample + downsample cached.
	Aligned {
		/// Original raw data (preserved for diagnostics and re-alignment).
		fragment: FragmentRef,
		/// The processed result (signal → upsample → downsample).
		data: DataFrame,
		/// Signal policy statistics from the alignment pass.
		stats: Option<SignalPolicyStats>,
	},
}

/// Data for a single measurement in the Universe.
///
/// The `state` field encodes the lifecycle — raw or aligned.
/// Transitions are explicit via `ensure_aligned()` and `invalidate_aligned()`.
///
/// The DataFrame contains columns: (subject, time, [components...], value)
/// Components are preserved here and crushed only at subset time.
#[derive(Debug, Clone)]
pub struct MeasurementData {
	/// The measurement unit definition (kind, signal policy, null values, etc.)
	pub unit: MeasurementUnit,

	/// Lifecycle state — raw or aligned.
	pub state: MeasurementState,
}

impl MeasurementData {
	/// Create new measurement data (raw, no signal policy applied).
	pub fn new(unit: MeasurementUnit, data: DataFrame) -> Self {
		Self {
			unit,
			state: MeasurementState::Raw {
				fragment: FragmentRef::Materialized(data),
			},
		}
	}

	/// Access the raw fragment (available in both states).
	pub fn fragment(&self) -> &FragmentRef {
		match &self.state {
			MeasurementState::Raw { fragment } => fragment,
			MeasurementState::Aligned { fragment, .. } => fragment,
		}
	}

	/// Access the aligned DataFrame (None if in Raw state).
	pub fn aligned(&self) -> Option<&DataFrame> {
		match &self.state {
			MeasurementState::Raw { .. } => None,
			MeasurementState::Aligned { data, .. } => Some(data),
		}
	}

	/// Access signal policy statistics (None if in Raw state or no stats).
	pub fn signal_policy_stats(&self) -> Option<&SignalPolicyStats> {
		match &self.state {
			MeasurementState::Raw { .. } => None,
			MeasurementState::Aligned { stats, .. } => stats.as_ref(),
		}
	}

	/// Whether the measurement is in the Aligned state.
	pub fn is_aligned(&self) -> bool {
		matches!(self.state, MeasurementState::Aligned { .. })
	}

	/// Compute the aligned DataFrame: signal → upsample → downsample.
	/// Cached in `self.state` as `MeasurementState::Aligned` for reuse across subset requests.
	fn compute_aligned(
		&mut self,
		source_name: &str,
		unified_rate_ms: Option<i64>,
		action: Option<&super::alignment::AlignAction>,
	) -> EtlResult<()> {
		// OPTIMIZATION OPPORTUNITY: This materializes the ColumnRef into a full DataFrame
		// for signal policy input. For 5 SCADA measurements sharing one Arc<DataFrame>,
		// this creates 5 temporary copies of the selected columns (~23 MB each).
		// A lazy pipeline (as_lazy() → signal policy as lazy exprs → collect once)
		// would eliminate these intermediate DataFrames.
		let raw_df = self.fragment().as_dataframe().map_err(EtlError::Polars)?;

		let native_rate_ms = self.unit.sample_rate_ms;

		// --- Step 1: Signal policy at native rate ---
		let (signal_df, self_stats) = if self.unit.signal_policy.is_some() {
			let (result, stats) = crate::polars_fns::apply_signal_policy(
				raw_df,
				&self.unit,
				source_name,
			)?;

			if let Some(ref stats) = stats {
				tracing::info!(
					measurement = self.unit.name.as_str(),
					native_rate_ms = ?native_rate_ms,
					input_points = stats.input_points,
					grid_points = stats.grid_points,
					fill_rate = format!("{:.1}%", stats.fill_rate * 100.0).as_str(),
					"Step 1: Signal policy applied"
				);
			}
			// Fill nulls introduced by the grid join
			let filled = if let Some(ref nv) = self.unit.null_value {
				let fill_expr: Expr = nv.clone().into();
				let value_col = self.unit.name.as_str();
				let filled = result.lazy()
					.with_column(col(value_col).fill_null(fill_expr).alias(value_col))
					.collect()?;
				tracing::debug!(
					measurement = self.unit.name.as_str(),
					null_value_extension = ?nv,
					"Step 1b: Filled grid nulls with null_value_extension"
				);
				filled
			} else {
				result
			};
			(filled, stats)
		} else {
			tracing::debug!(
				measurement = self.unit.name.as_str(),
				"Step 1: No signal policy — using raw data"
			);
			(raw_df, None)
		};

		// Steps 2 & 3: Upsample or Downsample based on the AlignmentSpec action.
		// The action is pre-computed — no rate comparison logic here.
		let time_col = self.unit.time.as_str();
		let value_col = self.unit.name.as_str();
		let subject_col = self.unit.subject.as_str();

		use super::alignment::AlignAction;
		let aligned_df = match action {
			Some(AlignAction::Upsample { strategy }) => {
				let target = unified_rate_ms.unwrap_or(native_rate_ms.unwrap_or(60_000));
				tracing::info!(
					measurement = self.unit.name.as_str(),
					from_ms = ?native_rate_ms,
					to_ms = target,
					strategy = ?strategy,
					rows_before = signal_df.height(),
					"Step 2: Upsampling (from AlignmentSpec)"
				);
				resample_df(
					&signal_df, time_col, value_col, subject_col,
					&self.unit.components, target, *strategy,
				)?
			}
			Some(AlignAction::Downsample { strategy }) => {
				let target = unified_rate_ms.unwrap_or(native_rate_ms.unwrap_or(60_000));
				tracing::info!(
					measurement = self.unit.name.as_str(),
					from_ms = ?native_rate_ms,
					to_ms = target,
					strategy = ?strategy,
					rows_before = signal_df.height(),
					"Step 3: Downsampling (from AlignmentSpec)"
				);
				resample_df(
					&signal_df, time_col, value_col, subject_col,
					&self.unit.components, target, *strategy,
				)?
			}
			Some(AlignAction::SignalOnly) | Some(AlignAction::PassThrough) | None => {
				tracing::debug!(
					measurement = self.unit.name.as_str(),
					action = ?action,
					"No resampling needed"
				);
				signal_df
			}
		};

		// Store the aligned DataFrame in the Aligned state.
		// This is the signal policy + upsample/downsample result, cached for
		// subset reuse. The 49.7 MB "Signal policy" in diagnostics is the sum
		// of all aligned DataFrames across measurements.
		//
		// OPTIMIZATION OPPORTUNITY: The aligned DataFrame duplicates data that's
		// already in the fragment's shared Arc<DataFrame> (just resampled). For
		// measurements where signal policy is a no-op at native rate (e.g., SCADA
		// at 60s with 60s grid), the aligned DataFrame is nearly identical to the
		// raw data. These could share the fragment instead of storing a copy.
		// Only measurements that actually resample (upsample/downsample) need a
		// separate aligned DataFrame.
		let fragment = match std::mem::replace(&mut self.state, MeasurementState::Raw { fragment: FragmentRef::Materialized(DataFrame::empty()) }) {
			MeasurementState::Raw { fragment } => fragment,
			MeasurementState::Aligned { fragment, .. } => fragment,
		};
		self.state = MeasurementState::Aligned { fragment, data: aligned_df, stats: self_stats };
		Ok(())
	}

	/// Invalidate the cached aligned data (e.g., after workbench extension).
	pub fn invalidate_aligned(&mut self) {
		let fragment = match std::mem::replace(&mut self.state, MeasurementState::Raw { fragment: FragmentRef::Materialized(DataFrame::empty()) }) {
			MeasurementState::Raw { fragment } => fragment,
			MeasurementState::Aligned { fragment, .. } => fragment,
		};
		self.state = MeasurementState::Raw { fragment };
	}

	/// Get the temporality of this measurement
	pub fn temporality(&self) -> DataTemporality {
		self.unit.temporality
	}

	/// Check if this is forecast data
	pub fn is_forecast(&self) -> bool {
		self.unit.is_forecast()
	}

	/// Check if this is historical data
	pub fn is_historical(&self) -> bool {
		self.unit.is_historical()
	}

	/// Create from a ComposedMeasurement (after stacking phase)
	pub fn from_composed(composed: ComposedMeasurement) -> Self {
		Self {
			unit: MeasurementUnit::new(
				composed.name.clone(),
				composed.name.clone(),
				composed.name.clone(),
				composed.kind,
			)
			.with_components(
				composed
					.components
					.iter()
					.map(|c| c.as_str().to_string())
					.collect(),
			),
			state: MeasurementState::Raw { fragment: composed.fragment },
		}
	}

	/// Create with full unit definition.
	///
	/// If `stats` is `Some`, the data is treated as already aligned
	/// (signal policy was applied externally). Otherwise starts in Raw state.
	pub fn with_unit(
		unit: MeasurementUnit,
		data: DataFrame,
		stats: Option<SignalPolicyStats>,
	) -> Self {
		let fragment = FragmentRef::Materialized(data.clone());
		let state = if stats.is_some() {
			MeasurementState::Aligned { fragment, data, stats }
		} else {
			MeasurementState::Raw { fragment }
		};
		Self { unit, state }
	}

	/// Get the TTL from the signal policy.
	///
	/// Panics if no signal policy is set — schema validation
	/// (`EtlSchemaBuilder::build()`) guarantees every measurement
	/// has a signal policy. If you hit this panic, the measurement
	/// was constructed without going through the schema builder.
	pub fn ttl(&self) -> Duration {
		self
			.unit
			.signal_policy
			.as_ref()
			.expect(
				"MeasurementData::ttl() called on a measurement without a signal policy. \
				 Every measurement must have a signal policy configured. \
				 This is validated at schema build time — if you see this panic, \
				 the measurement was constructed without schema validation."
			)
			.ttl()
	}

	/// Get row count of raw data
	pub fn height(&self) -> usize {
		self.fragment().height()
	}

	/// Check if this measurement has components
	pub fn has_components(&self) -> bool {
		!self.unit.components.is_empty()
	}

	/// Get component column names
	pub fn components(&self) -> &[CanonicalColumnName] {
		&self.unit.components
	}

	/// Compute the processed (signal-policy-applied) version.
	/// Called by `Universe::ensure_processed()`.
	pub(crate) fn compute_processed(&mut self, source_name: &str) -> EtlResult<()> {
		if self.is_aligned() {
			return Ok(());
		}

		let raw_df = self.fragment().as_dataframe().map_err(EtlError::Polars)?;

		let (aligned_data, stats) = if self.unit.signal_policy.is_some() {
			let (result, stats) = crate::polars_fns::apply_signal_policy(
				raw_df,
				&self.unit,
				source_name,
			)?;

			if let Some(ref stats) = stats {
				tracing::info!(
					measurement = self.unit.name.as_str(),
					input_points = stats.input_points,
					grid_points = stats.grid_points,
					fill_rate = format!("{:.1}%", stats.fill_rate * 100.0).as_str(),
					"Signal policy applied (lazy)"
				);
			}
			(result, stats)
		} else {
			(raw_df, None)
		};

		// Extract fragment from old state and transition to Aligned
		let placeholder = MeasurementState::Raw { fragment: FragmentRef::Materialized(DataFrame::empty()) };
		let fragment = match std::mem::replace(&mut self.state, placeholder) {
			MeasurementState::Raw { fragment } => fragment,
			MeasurementState::Aligned { fragment, .. } => fragment,
		};
		self.state = MeasurementState::Aligned { fragment, data: aligned_data, stats };

		Ok(())
	}
}

/// Data for a single quality (unit definition + DataFrame)
#[derive(Debug, Clone)]
pub struct QualityData {
	/// The quality unit definition
	pub unit: QualityUnit,
	/// The data with columns: (subject, value)
	pub data: DataFrame,
}

impl QualityData {
	/// Create new quality data
	pub fn new(unit: QualityUnit, data: DataFrame) -> Self {
		Self {
			unit,
			data,
		}
	}

	/// Create from a ComposedQuality (after stacking phase)
	pub fn from_composed(composed: ComposedQuality, unit: QualityUnit) -> Self {
		Self {
			unit,
			data: composed.data,
		}
	}

	/// Get row count
	pub fn height(&self) -> usize {
		self.data.height()
	}
}

// =============================================================================
// Diagnostics
// =============================================================================

/// Per-measurement diagnostic info for the Universe.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MeasurementDiag {
	pub name: String,
	pub kind: String,
	pub raw_rows: usize,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub processed_rows: Option<usize>,
	pub has_signal_policy: bool,
	pub signal_policy_computed: bool,
	pub components: Vec<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub signal_policy_stats: Option<MeasurementPolicyDiag>,
	/// Estimated source sample rate in milliseconds (median interval between observations).
	/// None if insufficient data to estimate.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub source_sample_rate_ms: Option<i64>,
	/// Signal policy TTL in seconds — determines the master grid resolution.
	/// When multiple measurements are combined, the longest TTL sets the grid.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub ttl_secs: Option<u64>,
	/// How the measurement data is stored: "column_ref", "stacked", or "materialized".
	pub storage_kind: String,
	/// Bytes owned by this measurement (0 for ColumnRef — data is shared).
	pub owned_bytes: usize,
	/// Bytes in the shared source DataFrame (for ColumnRef — shared with other measurements).
	pub shared_source_bytes: usize,
}

/// Memory usage summary for the Universe.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MemorySummary {
	/// Bytes owned by this Universe (Materialized fragments + processed + qualities).
	/// For ColumnRef measurements, this is near zero — data is shared.
	pub owned_bytes: usize,
	/// Bytes in shared source DataFrames (ColumnRef/Stacked fragments).
	/// This memory is shared across multiple measurements from the same source.
	pub shared_source_bytes: usize,
	/// Bytes in signal-policy-processed DataFrames (always materialized).
	pub processed_bytes: usize,
	/// Bytes in quality DataFrames.
	pub quality_bytes: usize,
	/// Number of measurements stored as ColumnRef (zero-copy).
	pub column_ref_count: usize,
	/// Number of measurements stored as Materialized (owned copy).
	pub materialized_count: usize,
	/// Per-measurement breakdown: name → (storage_kind, owned_bytes, shared_bytes).
	pub per_measurement: Vec<MeasurementMemory>,
}

/// Per-measurement memory info.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MeasurementMemory {
	pub name: String,
	pub storage_kind: String,
	/// Bytes owned by this measurement's fragment.
	pub fragment_bytes: usize,
	/// Bytes in the shared source (for ColumnRef — shared with other measurements).
	pub shared_source_bytes: usize,
	/// Bytes in the processed (signal policy) DataFrame, if computed.
	pub processed_bytes: usize,
}

/// Estimate the median sample rate of a DataFrame's time column.
///
/// Looks at the first datetime column, computes intervals between
/// consecutive rows (for one subject), and returns the median in ms.
/// Resample a DataFrame to a target rate using the given strategy.
///
/// Handles both upsampling (forward_fill, interpolate) and downsampling
/// (mean, max, sum, last). The operation is per-subject (grouped by
/// subject + components).
///
/// The DataFrame must have a Datetime time column sorted within each
/// subject group.
fn resample_df(
	df: &DataFrame,
	time_col: &str,
	value_col: &str,
	subject_col: &str,
	components: &[CanonicalColumnName],
	target_rate_ms: i64,
	strategy: crate::ResampleStrategy,
) -> EtlResult<DataFrame> {
	use crate::ResampleStrategy;

	let duration_str = format!("{}ms", target_rate_ms);
	let every = polars::prelude::Duration::parse(&duration_str);

	// Build group-by columns for subject + components
	let mut group_col_names: Vec<PlSmallStr> = vec![subject_col.into()];
	for comp in components {
		group_col_names.push(comp.as_str().into());
	}

	let is_upsample = matches!(
		strategy,
		ResampleStrategy::ForwardFill | ResampleStrategy::Interpolate | ResampleStrategy::Null
	);

	let result = if is_upsample {
		// UPSAMPLE: create missing time slots then fill.
		// DataFrame::upsample generates rows at every `target_rate_ms` interval,
		// grouped by subject + components. New rows have null values.
		let sorted = df.clone().lazy()
			.sort([subject_col, time_col], SortMultipleOptions::default())
			.collect()?;

		let upsampled = sorted.upsample(
			group_col_names.clone(),
			time_col,
			every,
		)?;

		// Apply fill strategy to the null values created by upsampling
		match strategy {
			ResampleStrategy::ForwardFill => {
				upsampled.fill_null(FillNullStrategy::Forward(None))?
			}
			ResampleStrategy::Interpolate => {
				// Collect to lazy, interpolate per subject, collect back
				upsampled.lazy()
					.with_column(
						col(value_col)
							.interpolate(InterpolationMethod::Linear)
							.over([col(subject_col)])
							.alias(value_col),
					)
					.collect()?
			}
			ResampleStrategy::Null => {
				// Leave nulls as-is
				upsampled
			}
			_ => unreachable!("is_upsample guard"),
		}
	} else {
		// DOWNSAMPLE: group into coarser time buckets and aggregate.
		let mut group_exprs: Vec<Expr> = vec![col(subject_col)];
		for comp in components {
			group_exprs.push(col(comp.as_str()));
		}

		let agg_expr = match strategy {
			ResampleStrategy::Mean => col(value_col).mean().alias(value_col),
			ResampleStrategy::Max => col(value_col).max().alias(value_col),
			ResampleStrategy::Min => col(value_col).min().alias(value_col),
			ResampleStrategy::Sum => col(value_col).sum().alias(value_col),
			ResampleStrategy::Last => col(value_col).last().alias(value_col),
			_ => unreachable!("downsample strategies only"),
		};

		df.clone().lazy()
			.sort([time_col], SortMultipleOptions::default())
			.group_by_dynamic(
				col(time_col),
				group_exprs,
				DynamicGroupOptions {
					every,
					period: every,
					offset: polars::prelude::Duration::parse("0ms"),
					..Default::default()
				},
			)
			.agg([agg_expr])
			.collect()?
	};

	tracing::info!(
		rows_before = df.height(),
		rows_after = result.height(),
		strategy = ?strategy,
		target_rate_ms = target_rate_ms,
		is_upsample = is_upsample,
		"Resample complete"
	);

	Ok(result)
}

fn estimate_sample_rate(df: &polars::prelude::DataFrame) -> Option<i64> {
	use polars::prelude::*;

	if df.height() < 2 { return None; }

	// Find first datetime column
	let time_col = df.get_columns().iter()
		.find(|c| matches!(c.dtype(), DataType::Datetime(_, _)))?;

	let series = time_col.as_materialized_series();
	let ca = series.datetime().ok()?;

	// Sample first 1000 rows for performance
	let limit = std::cmp::min(df.height(), 1000);
	let mut intervals: Vec<i64> = Vec::new();

	let unit_factor: i64 = match ca.time_unit() {
		TimeUnit::Milliseconds => 1,
		TimeUnit::Microseconds => 1000,
		TimeUnit::Nanoseconds => 1_000_000,
	};

	let phys = &ca.phys;
	for i in 1..limit {
		if let (Some(prev), Some(curr)) = (phys.get(i - 1), phys.get(i)) {
			let diff = (curr - prev) / unit_factor;
			if diff > 0 { intervals.push(diff); }
		}
	}

	if intervals.is_empty() { return None; }
	intervals.sort_unstable();
	Some(intervals[intervals.len() / 2]) // median
}

/// Signal policy stats for diagnostics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MeasurementPolicyDiag {
	pub input_points: usize,
	pub grid_points: usize,
	pub fill_rate: f64,
	pub ttl_ms: u64,
}

// =============================================================================
// Universe
// =============================================================================

/// Collection of ETL units, stored separately for flexible composition.
///
/// Measurements and qualities are stored in HashMaps with components preserved.
/// Composition into a single DataFrame happens at request time via [`Universe::subset`].
#[derive(Debug, Clone)]
pub struct Universe {
	/// Schema defining the universe structure
	pub schema: EtlSchema,

	/// Measurements by canonical name (components preserved)
	pub measurements: HashMap<CanonicalColumnName, MeasurementData>,

	/// Qualities by canonical name
	pub qualities: HashMap<CanonicalColumnName, QualityData>,

	/// Build metadata
	pub build_info: UniverseBuildInfo,

	/// Alignment specification — the single source of truth for how
	/// measurements are aligned to a common sample rate.
	/// Set by `ensure_aligned()`. Used by subset for the grid interval
	/// and serialized directly for diagnostics.
	pub alignment: Option<super::alignment::AlignmentSpec>,
}

impl Universe {
	/// Create a new Universe
	pub fn new(
		measurements: HashMap<CanonicalColumnName, MeasurementData>,
		qualities: HashMap<CanonicalColumnName, QualityData>,
		schema: EtlSchema,
		build_info: UniverseBuildInfo,
	) -> Self {
		Self {
			schema,
			measurements,
			qualities,
			build_info,
			alignment: None,
		}
	}

	/// Create an empty Universe from a schema (for fluent building)
	pub fn from_schema(schema: EtlSchema) -> Self {
		let build_info = UniverseBuildInfo::builder(&schema.name).build();
		Self {
			schema,
			measurements: HashMap::new(),
			qualities: HashMap::new(),
			build_info,
			alignment: None,
		}
	}

	/// Add a measurement (fluent builder)
	///
	/// # Example
	/// ```ignore
	/// let universe = Universe::from_schema(schema)
	///     .with_measurement(measurement_unit, df)
	///     .with_measurement(other_unit, other_df);
	/// ```
	pub fn with_measurement(mut self, unit: MeasurementUnit, data: DataFrame) -> Self {
		let name = unit.name.clone();
		self
			.measurements
			.insert(name, MeasurementData::new(unit, data));
		self
	}

	/// Add a measurement with signal policy stats (fluent builder)
	pub fn with_measurement_and_stats(
		mut self,
		unit: MeasurementUnit,
		data: DataFrame,
		stats: SignalPolicyStats,
	) -> Self {
		let name = unit.name.clone();
		self
			.measurements
			.insert(name, MeasurementData::with_unit(unit, data, Some(stats)));
		self
	}

	/// Add a quality (fluent builder)
	///
	/// # Example
	/// ```ignore
	/// let universe = Universe::from_schema(schema)
	///     .with_quality(quality_unit, df);
	/// ```
	pub fn with_quality(mut self, unit: QualityUnit, data: DataFrame) -> Self {
		let name = unit.name.clone();
		self.qualities.insert(name, QualityData::new(unit, data));
		self
	}

	/// Set build info (fluent builder)
	pub fn with_build_info(mut self, build_info: UniverseBuildInfo) -> Self {
		self.build_info = build_info;
		self
	}

	// =========================================================================
	// Accessors
	// =========================================================================

	/// Get the schema
	pub fn schema(&self) -> &EtlSchema {
		&self.schema
	}

	/// Get build info
	pub fn build_info(&self) -> &UniverseBuildInfo {
		&self.build_info
	}

	/// Get a measurement by name
	pub fn get_measurement(&self, name: &CanonicalColumnName) -> Option<&MeasurementData> {
		self.measurements.get(name)
	}

	/// Get a quality by name
	pub fn get_quality(&self, name: &CanonicalColumnName) -> Option<&QualityData> {
		self.qualities.get(name)
	}

	/// Get all measurement names
	pub fn measurement_names(&self) -> impl Iterator<Item = &CanonicalColumnName> {
		self.measurements.keys()
	}

	/// Get all quality names
	pub fn quality_names(&self) -> impl Iterator<Item = &CanonicalColumnName> {
		self.qualities.keys()
	}

	/// Get measurement count
	pub fn measurement_count(&self) -> usize {
		self.measurements.len()
	}

	/// Get quality count
	pub fn quality_count(&self) -> usize {
		self.qualities.len()
	}

	/// Check if a measurement exists
	pub fn has_measurement(&self, name: &str) -> bool {
		self
			.measurements
			.contains_key(&CanonicalColumnName::new(name))
	}

	/// Diagnostic summary of the Universe's measurement contents.
	pub fn measurement_diagnostics(&self) -> Vec<MeasurementDiag> {
		self.measurements.iter().map(|(name, md)| {
			// OPTIMIZATION OPPORTUNITY: as_dataframe() materializes the ColumnRef just for
			// sample rate estimation. For 5 ColumnRef measurements this creates 5 temporary
			// DataFrames (~23 MB each) that are immediately dropped. The sample rate could
			// instead be computed from the shared Arc<DataFrame>'s time column directly,
			// or cached on the MeasurementData after first computation.
			let raw_df = md.fragment().as_dataframe().ok();
			let source_sample_rate_ms = raw_df.as_ref().and_then(|df| estimate_sample_rate(df));
			let ttl_secs = md.unit.signal_policy.as_ref()
				.map(|p| p.ttl().as_secs());

			let storage_desc = md.fragment().storage_description();

			MeasurementDiag {
				name: name.as_str().to_string(),
				kind: format!("{:?}", md.unit.kind),
				raw_rows: md.fragment().height(),
				processed_rows: md.aligned().map(|df| df.height()),
				has_signal_policy: md.unit.signal_policy.is_some(),
				signal_policy_computed: md.is_aligned(),
				components: md.unit.components.iter()
					.map(|c| c.as_str().to_string()).collect(),
				signal_policy_stats: md.signal_policy_stats().map(|s| {
					MeasurementPolicyDiag {
						input_points: s.input_points,
						grid_points: s.grid_points,
						fill_rate: s.fill_rate,
						ttl_ms: s.ttl_ms,
					}
				}),
				source_sample_rate_ms,
				ttl_secs,
				storage_kind: storage_desc.kind,
				owned_bytes: md.fragment().owned_bytes(),
				shared_source_bytes: md.fragment().shared_source_bytes(),
			}
		}).collect()
	}

	/// Memory usage summary for the entire Universe.
	pub fn memory_summary(&self) -> MemorySummary {
		use memuse::DynamicUsage;
		use std::collections::HashSet;

		let mut total_owned = 0usize;
		let mut total_processed = 0usize;
		let mut column_ref_count = 0usize;
		let mut materialized_count = 0usize;
		let mut per_measurement = Vec::new();

		// Track unique Arc pointers to avoid double-counting shared sources.
		// Multiple ColumnRefs from the same BoundSource share one Arc<DataFrame>.
		let mut seen_arcs: HashSet<usize> = HashSet::new();
		let mut deduplicated_shared = 0usize;

		for (name, md) in &self.measurements {
			let frag = md.fragment();
			let fragment_bytes = frag.dynamic_usage();
			let shared_bytes = frag.shared_source_bytes();
			let proc_bytes = md.aligned()
				.map(|df| df.estimated_size())
				.unwrap_or(0);
			let storage_desc = frag.storage_description();

			total_owned += fragment_bytes;
			total_processed += proc_bytes;

			// Deduplicate shared source bytes by Arc pointer identity
			for ptr in frag.source_arc_ptrs() {
				if seen_arcs.insert(ptr) {
					// First time seeing this Arc — count its size
					deduplicated_shared += frag.shared_source_bytes_for_ptr(ptr);
				}
			}

			if frag.is_materialized() {
				materialized_count += 1;
			} else {
				column_ref_count += 1;
			}

			per_measurement.push(MeasurementMemory {
				name: name.as_str().to_string(),
				storage_kind: storage_desc.kind,
				fragment_bytes,
				shared_source_bytes: shared_bytes,
				processed_bytes: proc_bytes,
			});
		}

		let mut quality_bytes = 0usize;
		for qd in self.qualities.values() {
			quality_bytes += qd.data.estimated_size();
		}

		MemorySummary {
			owned_bytes: total_owned + total_processed + quality_bytes,
			shared_source_bytes: deduplicated_shared,
			processed_bytes: total_processed,
			quality_bytes,
			column_ref_count,
			materialized_count,
			per_measurement,
		}
	}

	/// Check if a derivation exists in the schema
	pub fn has_derivation(&self, name: &str) -> bool {
		self.schema.get_derivation(name).is_some()
	}

	/// Get info about a crushed component, if it was crushed during build
	pub fn get_crushed_component_info(
		&self,
		component: &str,
	) -> Option<&super::CrushedComponentInfo> {
		self
			.build_info
			.components_crushed
			.iter()
			.find(|c| c.component == component)
	}

	// =========================================================================
	// Incremental Extension
	// =========================================================================

	/// Extend the Universe with data from new bound sources (time extension).
	///
	/// Extracts fragments from the new sources, then vertical-stacks them
	/// onto existing measurement DataFrames. Invalidates any cached
	/// `processed` data (signal policy must be recomputed over the full range).
	/// Extend the Universe with data from new time-range partitions.
	///
	/// # Current implementation (interim)
	///
	/// Returns `Err(EtlError::NeedsRebuild)` to signal the caller to
	/// do a full workbench rebuild. The previous implementation
	/// materialized ColumnRef fragments via `vstack`, which destroyed
	/// source-Arc identity and broke the plan layer's SourceJoin
	/// grouping (the "discharge not found in sump" bug).
	///
	/// # Target implementation (Ref-based extension)
	///
	/// Lengthen without materializing:
	///
	/// 1. Each new partition coordinate produces a new `Arc<DataFrame>`
	///    via fetch+bind. The new partition's ColumnRef points into
	///    this new Arc.
	///
	/// 2. The existing measurement's fragment transitions from
	///    `ColumnRef(arc_a)` to `Stacked([ColumnRef(arc_a), ColumnRef(arc_b)])`.
	///    Each sub-ref retains its own source-Arc identity.
	///
	/// 3. `as_dataframe()` on a Stacked fragment concatenates lazily
	///    (vertical stack of lazy selects, one per partition). The plan
	///    layer groups by the FIRST Arc pointer — which is stable across
	///    extensions — so SourceJoin grouping is preserved.
	///
	/// No materialization, no DataFrame cloning. Just composing Refs.
	pub fn extend_with_sources(&mut self, _new_sources: &[BoundSource]) -> EtlResult<()> {
		Err(EtlError::Config(
			"TIME_EXTEND_NEEDS_REBUILD: extend_with_sources requires a full \
			 workbench rebuild until Ref-based extension is implemented. \
			 The caller should catch this and rebuild.".into()
		))
	}

	/// Extend the Universe with additional measurements extracted from
	/// existing bound sources.
	///
	/// Used when the user requests a measurement that wasn't in the
	/// original workbench scope. The bound sources (retained on the
	/// Workbench) already contain all columns — we just need to extract
	/// the new measurement fragments.
	// =====================================================================
	// XXX ANALYTICS DESIGN DEBT — DO NOT REMOVE UNTIL ADDRESSED
	// =====================================================================
	// This method materializes the new measurement into a `Materialized`
	// fragment via `frag.materialize()`. That breaks the source-Arc
	// identity invariant the plan layer (synapse-etl-unit/src/plan/)
	// relies on for `SourceJoin` grouping:
	//
	//   - `Materialized` fragments report empty `source_arc_ptrs()`,
	//     so they get `source_key = 0` in `build_runtime_plan`.
	//   - The original `ColumnRef` siblings keep their real Arc pointer.
	//   - Plan grouping puts the new measurement in its own SourceJoin,
	//     which fails `is_wide_join_eligible` (`columns.len() <= 1`).
	//   - The per-measurement loop's `filtered_cache.get(&source_key)`
	//     misses on key=0, falls through to materializing the whole
	//     fragment again, and the column silently drops out of
	//     downstream joins in some edge cases.
	//
	// The CORRECT fix is to keep the new measurement as a `ColumnRef`
	// against the same `Arc<DataFrame>` the existing measurements
	// already share — i.e., insert `frag` (or its inner `ColumnRefData`)
	// directly into `self.measurements` instead of calling
	// `frag.materialize()`. `extract_source_fragments` already produces
	// ColumnRef fragments, so the data is right there.
	//
	// Risks to validate before flipping this (see
	// `project_analytics_extend_workbench` memory entry):
	//   1. `MeasurementData::compute_aligned` must work on a ColumnRef
	//      whose Arc is shared with already-aligned siblings (verify it
	//      doesn't mutate the shared Arc).
	//   2. Existing siblings' `aligned` cache may need invalidation if
	//      the new `AlignmentSpec` changes `unified_rate_ms`.
	//   3. Add a regression test that mirrors:
	//      visualize [sump] → analyze [sump, discharge] → expect both
	//      columns in the analytics subset.
	//
	// Until this is fixed, `Pipeline::subset_for_analysis` (in the
	// data-pipeline crate) bypasses extension entirely and composes a
	// fresh universe per call. See the matching XXX block there.
	// =====================================================================
	/// Extend the Universe with additional measurements from existing
	/// bound sources.
	///
	/// # Current implementation (interim)
	///
	/// Returns `Err(EtlError::NeedsRebuild)` to signal the caller to
	/// do a full workbench rebuild. The previous implementation
	/// materialized the new measurement's ColumnRef, breaking
	/// source-Arc identity and plan grouping.
	///
	/// # Target implementation (Ref-based extension)
	///
	/// Widen without materializing:
	///
	/// 1. The new measurement's source Arc is the SAME Arc that its
	///    siblings already reference (they come from the same partition).
	///    The extraction step produces a `ColumnRef(same_arc)` naturally.
	///
	/// 2. Just insert the new `MeasurementData` with its ColumnRef
	///    intact. No materialization, no cloning. The plan layer groups
	///    it with its siblings because they share the same Arc pointer.
	///
	/// 3. `ensure_aligned()` runs `compute_aligned()` on the new
	///    measurement, which materializes into its own aligned
	///    DataFrame. The original ColumnRef is preserved in the
	///    fragment for plan grouping.
	///
	/// The key insight: widening is just "add a MeasurementData with
	/// a ColumnRef to the existing Arc." The Arc already has the column.
	pub fn extend_with_measurements(
		&mut self,
		_new_measurement_names: &[CanonicalColumnName],
		_bound_sources: &[BoundSource],
	) -> EtlResult<()> {
		Err(EtlError::Config(
			"MEAS_EXTEND_NEEDS_REBUILD: extend_with_measurements requires a \
			 full workbench rebuild until Ref-based extension is implemented. \
			 The caller should catch this and rebuild.".into()
		))
	}

	// =========================================================================
	// TTL Helpers
	// =========================================================================

	/// Get the longest TTL among specified measurements
	pub fn longest_ttl(&self, measurement_names: &[CanonicalColumnName]) -> Duration {
		measurement_names
			.iter()
			.filter_map(|name| self.measurements.get(name))
			.map(|m| m.ttl())
			.max()
			.unwrap_or(Duration::from_secs(60))
	}

	/// Get the shortest TTL among specified measurements
	pub fn shortest_ttl(&self, measurement_names: &[CanonicalColumnName]) -> Duration {
		measurement_names
			.iter()
			.filter_map(|name| self.measurements.get(name))
			.map(|m| m.ttl())
			.min()
			.unwrap_or(Duration::from_secs(60))
	}

	// =========================================================================
	// Subset Execution
	// =========================================================================

	/// Pre-compute signal-policy-processed data for all measurements.
	///
	/// Call this once before issuing `subset()` calls with `SignalPolicyMode::Apply`.
	/// After this, `subset()` can read processed data immutably.
	pub fn ensure_processed(&mut self) -> EtlResult<()> {
		let start = std::time::Instant::now();
		let count = self.measurements.len();

		for (_name, measurement) in &mut self.measurements {
			measurement.compute_processed("universe")?;
		}

		let elapsed = start.elapsed();
		let stats: Vec<serde_json::Value> = self.measurements.values()
			.filter_map(|m| {
				m.signal_policy_stats().map(|s| {
					serde_json::json!({
						"measurement": m.unit.name.as_str(),
						"input_points": s.input_points,
						"grid_points": s.grid_points,
						"fill_rate": format!("{:.1}%", s.fill_rate * 100.0),
						"ttl_ms": s.ttl_ms,
					})
				})
			})
			.collect();

		info!(
			measurements = count,
			elapsed_ms = elapsed.as_millis() as u64,
			signal_policy = %serde_json::to_string(&stats).unwrap_or_default(),
			"Signal policy applied to all measurements"
		);

		Ok(())
	}

	/// Pre-compute aligned data for all measurements (signal → upsample → downsample).
	/// Call this before subsetting to populate the cache.
	/// Align all measurements using the provided spec.
	///
	/// The spec determines what action each measurement takes
	/// (signal only, upsample, downsample, pass through).
	/// Results are cached in `aligned` on each MeasurementData.
	pub fn ensure_aligned(&mut self, spec: Option<super::alignment::AlignmentSpec>) -> EtlResult<()> {
		let unified_rate_ms = spec.as_ref().map(|s| s.unified_rate_ms);
		self.alignment = spec;

		for md in self.measurements.values_mut() {
			if !md.is_aligned() {
				// Look up this measurement's action from the spec
				let action = self.alignment.as_ref()
					.and_then(|s| s.action_for(&md.unit.name))
					.cloned();

				md.compute_aligned("universe", unified_rate_ms, action.as_ref())?;
			}
		}
		Ok(())
	}

	/// Produce a **processed** subset: grid-aligned, signal-policy applied,
	/// `null_value` and `null_value_extension` filled. Invariant: no nulls
	/// in requested measurement columns.
	pub fn subset(&self, request: &EtlUnitSubsetRequest)
		-> EtlResult<SubsetUniverse<crate::subset::Processed>>
	{
		self.subset_with_mode(request, SignalPolicyMode::Apply)
	}

	/// Produce a **raw** subset: observations at their original timestamps,
	/// no grid alignment, no signal policy, nulls preserved. Use for
	/// displaying unprocessed source data.
	pub fn subset_raw(&self, request: &EtlUnitSubsetRequest)
		-> EtlResult<SubsetUniverse<crate::subset::Raw>>
	{
		let subject_col = self.schema.subject.as_str();
		let time_col = self.schema.time.as_str();

		let raw_measurement_names: Vec<CanonicalColumnName> = if request.measurements.is_empty() {
			self.measurements.keys().cloned().collect()
		} else {
			request.measurements.clone()
		};

		self.subset_raw_no_grid(
			&raw_measurement_names, request, subject_col, time_col,
		)
	}

	/// Private: the Apply (processed) pipeline. The `mode` parameter is
	/// retained internally because the per-measurement plan layer still
	/// threads it through, but the public entry points ([`subset`],
	/// [`subset_raw`]) decide the mode — callers no longer pick.
	pub(crate) fn subset_with_mode(
		&self,
		request: &EtlUnitSubsetRequest,
		mode: SignalPolicyMode,
	) -> EtlResult<SubsetUniverse<crate::subset::Processed>> {
		let subject_col = self.schema.subject.as_str();
		let time_col = self.schema.time.as_str();

		// Determine which measurements to include
		let raw_measurement_names: Vec<CanonicalColumnName> = if request.measurements.is_empty() {
			// When qualities are requested with no measurements, treat empty as "no measurements"
			// rather than "all measurements" — this enables the qualities-only path.
			if !request.qualities.is_empty() {
				Vec::new()
			} else {
				// "All measurements" means base measurements *and* every
				// derivation declared on the schema. The dependency
				// expansion below will pull in any base columns the
				// derivations need.
				let mut all: Vec<CanonicalColumnName> = self.measurements.keys().cloned().collect();
				for deriv in &self.schema.derivations {
					all.push(deriv.name.clone());
				}
				all
			}
		} else {
			// Validate requested measurements exist
			for name in &request.measurements {
				if !self.measurements.contains_key(name) && self.schema.get_derivation(name).is_none() {
					return Err(EtlError::UnitNotFound(format!(
						"Measurement '{}' not found. Available: {:?}",
						name,
						self.measurements.keys().collect::<Vec<_>>()
					)));
				}
			}
			request.measurements.clone()
		};

		// Expand derivation dependencies. If the request includes a
		// derivation, the base measurements (and other derivations) it
		// depends on must also be in the pipeline so the post-join
		// derivation evaluator finds the columns it needs. Walks the
		// schema's derivation table transitively, depth-first, with
		// dedup so dependencies appear before their dependents.
		let measurement_names = expand_derivation_dependencies(&raw_measurement_names, &self.schema);

		// Validate requested qualities exist
		for name in &request.qualities {
			if !self.qualities.contains_key(name) {
				return Err(EtlError::UnitNotFound(format!(
					"Quality '{}' not found. Available: {:?}",
					name,
					self.qualities.keys().collect::<Vec<_>>()
				)));
			}
		}

		// Qualities-only path: no time grid, just subject × quality
		if measurement_names.is_empty() && !request.qualities.is_empty() {
			return self.subset_qualities_only(request);
		}

		info!(
			 measurements = ?measurement_names,
			 request = ?request,
			 mode = ?mode,
			 "🦀 Subset Request"
		);

		// Raw mode is no longer reachable from this entry point — the
		// public API splits the two modes by function name. We keep the
		// internal `SignalPolicyMode` parameter because the per-measurement
		// plan layer and the join-strategy selector still consume it.
		debug_assert!(
			!matches!(mode, SignalPolicyMode::Skip),
			"subset_with_mode must not be called with Skip — use `Universe::subset_raw` for raw data",
		);

		// 1. Determine output interval.
		// Read from the AlignmentSpec — the single source of truth.
		let interval = if let Some(ref spec) = self.alignment {
			std::time::Duration::from_millis(spec.unified_rate_ms as u64)
		} else {
			request
				.interval
				.as_ref()
				.map(|i| i.duration())
				.unwrap_or_else(|| self.longest_ttl(&measurement_names))
		};

		debug!(interval_ms = interval.as_millis(), "Determined output interval");

		let mut stage_trace: Vec<crate::subset::stages::StageDiag> = Vec::new();

		// 2. Build master time grid
		let master_grid_start = std::time::Instant::now();
		let master_grid = self.build_master_grid(&measurement_names, request, interval)?;
		let master_grid_elapsed = master_grid_start.elapsed().as_micros() as u64;

		debug!(rows = master_grid.height(), "Built master grid");

		// Emit a BuildMasterGrid diagnostic capturing the realized grid
		// parameters. This is the single place downstream measurements
		// must land after joining; if measurement output doesn't appear
		// at every minute, compare its time values to this grid.
		{
			let interval_ms = interval.as_millis() as i64;
			let n_subjects = master_grid
				.column(subject_col)
				.ok()
				.and_then(|c| c.n_unique().ok())
				.unwrap_or(0);
			let (grid_min_ms, grid_max_ms, n_time_points) = master_grid
				.column(time_col)
				.ok()
				.map(|c| {
					let phys = c.to_physical_repr();
					let ca = phys.i64().cloned();
					let (mn, mx) = ca
						.as_ref()
						.map(|a| (a.min().unwrap_or(0), a.max().unwrap_or(0)))
						.unwrap_or((0, 0));
					let unique = c.n_unique().unwrap_or(0);
					(mn, mx, unique)
				})
				.unwrap_or((0, 0, 0));

			let has_historical = measurement_names.iter()
				.filter_map(|n| self.measurements.get(n))
				.any(|m| m.is_historical());
			let has_forecast = measurement_names.iter()
				.filter_map(|n| self.measurements.get(n))
				.any(|m| m.is_forecast());
			let temporality = match (has_historical, has_forecast) {
				(true, true) => "combined",
				(true, false) => "historical",
				(false, true) => "forecast",
				(false, false) => "unknown",
			};

			let mut notes: Vec<String> = Vec::new();
			notes.push(format!(
				"expected_rows = n_subjects * n_time_points = {} * {} = {}",
				n_subjects,
				n_time_points,
				n_subjects * n_time_points,
			));
			notes.push(format!("actual_rows = {}", master_grid.height()));
			if n_time_points > 0 {
				let span_ms = grid_max_ms.saturating_sub(grid_min_ms);
				let measured_step = span_ms / (n_time_points.saturating_sub(1).max(1) as i64);
				notes.push(format!(
					"measured_step_ms = {} (configured interval_ms = {})",
					measured_step, interval_ms,
				));
			}

			stage_trace.push(crate::subset::stages::StageDiag {
				stage: crate::subset::stages::SubsetStage::BuildMasterGrid {
					interval_ms,
					grid_min_ms,
					grid_max_ms,
					n_time_points,
					n_subjects,
					temporality: temporality.to_string(),
				},
				rows_after: master_grid.height(),
				elapsed_us: master_grid_elapsed,
				notes,
			});
		}

		// 3. Extract time range and subject filter from pre-filtering.
		//    This narrows measurement data BEFORE crush/resample/join,
		//    avoiding processing rows outside the subset scope.
		//
		//    When the request doesn't specify a bounded time range, fall
		//    back to the master grid's actual time extent. This is critical
		//    in the cached-Universe architecture where `aligned()` data
		//    covers the FULL data range (potentially weeks), but the subset
		//    should only cover the visible window.
		let time_filter: Option<(i64, i64)> = request.time_range.as_ref().and_then(|tr| {
			match (tr.start, tr.end) {
				(Some(s), Some(e)) => Some((s.timestamp_millis(), e.timestamp_millis())),
				_ => None,
			}
		}).or_else(|| {
			// Derive from the master grid's time column.
			// The grid defines the subset's visible window — aligned()
			// data outside this range must be excluded.
			let tc = master_grid.column(time_col).ok()?;
			let min_scalar = tc.min_reduce().ok()?;
			let max_scalar = tc.max_reduce().ok()?;
			let min_ms = match min_scalar.value() {
				AnyValue::Datetime(ms, TimeUnit::Milliseconds, _) => *ms,
				_ => return None,
			};
			let max_ms = match max_scalar.value() {
				AnyValue::Datetime(ms, TimeUnit::Milliseconds, _) => *ms,
				_ => return None,
			};
			Some((min_ms, max_ms))
		});
		let subject_filter_values: Option<Vec<String>> = request.subject_filter.as_ref().and_then(|sf| {
			match sf {
				crate::request::SubjectFilter::Include(values) => {
					let strings: Vec<String> = values.iter()
						.filter_map(|v| v.as_str().map(|s| s.to_string()))
						.collect();
					if strings.is_empty() { None } else { Some(strings) }
				}
				_ => None,
			}
		});

		// 4. Build the runtime plan and execute the filter stage.
		//
		// The plan layer (synapse-etl-unit::plan) produces a RuntimePlan
		// from the universe + request. Its FilterPlan contains one
		// SourceFilter per unique Arc<DataFrame>, exploiting the
		// shared-source invariant: every measurement extracted from a
		// given Arc shares its subject column, time column, and
		// component columns by construction.
		//
		// The filter step iterates `plan.core.filter.filters` and
		// produces one filtered DataFrame per source, cached by raw Arc
		// pointer for the per-measurement loop downstream.
		use std::collections::HashMap as StdHashMap;

		let runtime_plan = crate::plan::build_runtime_plan(self, request)?;
		let mut filtered_cache: StdHashMap<usize, DataFrame> = StdHashMap::new();

		debug!(
			sources = runtime_plan.source_count(),
			crushes = runtime_plan.core.crush.op_count(),
			joins = runtime_plan.join.op_count(),
			derivations = runtime_plan.derivations.len(),
			"Runtime plan built"
		);

		for src_filter in &runtime_plan.core.filter.filters {
			let source_key_raw = src_filter.source_key().as_raw();
			if filtered_cache.contains_key(&source_key_raw) {
				continue;
			}

			// Pull the underlying Arc<DataFrame> from the first
			// measurement consumer. All members of a source share the
			// same Arc, so any consumer's fragment yields the same
			// underlying frame.
			let first_member = src_filter.consumers.iter()
				.find(|c| c.is_measurement())
				.ok_or_else(|| EtlError::Config(
					"SourceFilter has no measurement consumers".into()
				))?;
			let first_md = self.measurements.get(first_member.name())
				.ok_or_else(|| EtlError::Config(format!(
					"Measurement '{}' missing during filter stage",
					first_member.name()
				)))?;

			let source_data = first_md.fragment().raw_source_dataframe()
				.ok_or_else(|| EtlError::Config("Cannot get raw source for filter".into()))?
				.clone();

			let physical_time = src_filter.source.time.physical.as_str();
			let physical_subject = src_filter.source.subject.physical.as_str();

			let filter_start = std::time::Instant::now();
			let filtered = {
				let mut lf = source_data.lazy();
				if let Some((start_ms, end_ms)) = time_filter {
					let tz = Some(polars::prelude::TimeZone::UTC);
					lf = lf.filter(
						col(physical_time).gt_eq(lit(start_ms).cast(DataType::Datetime(TimeUnit::Milliseconds, tz.clone())))
						.and(col(physical_time).lt_eq(lit(end_ms).cast(DataType::Datetime(TimeUnit::Milliseconds, tz))))
					);
				}
				if let Some(ref subjects) = subject_filter_values {
					let series = polars::prelude::Series::new("_sf".into(), subjects);
					lf = lf.filter(col(physical_subject).is_in(lit(series).implode(), false));
				}
				lf.collect()?
			};

			let member_names: Vec<String> = src_filter.consumers.iter()
				.map(|c| c.name().as_str().to_string())
				.collect();

			stage_trace.push(crate::subset::stages::StageDiag {
				stage: crate::subset::stages::SubsetStage::Filter {
					measurement: member_names.join(", "),
					has_time_filter: time_filter.is_some(),
					has_subject_filter: subject_filter_values.is_some(),
				},
				rows_after: filtered.height(),
				elapsed_us: filter_start.elapsed().as_micros() as u64,
				notes: Vec::new(),
			});

			filtered_cache.insert(source_key_raw, filtered);
		}

		let mut result = master_grid;

		// 5. Wide-join pass.
		//
		// For each `SourceJoin` in the plan that's eligible for the
		// wide path, batch the signal-policy + resample + join into one
		// pass. Eligibility (see `Universe::is_wide_join_eligible`):
		// multi-column join, no component dimensions, ttl <= interval.
		// Members handled here are added to `handled_by_wide` so the
		// per-measurement loop below skips them.
		let mut handled_by_wide: std::collections::HashSet<CanonicalColumnName> =
			std::collections::HashSet::new();
		for src_join in &runtime_plan.join.joins {
			if !self.is_wide_join_eligible(src_join, interval) {
				continue;
			}
			let (new_result, mut diags) = self.execute_wide_source_join(
				src_join,
				&filtered_cache,
				result,
				mode,
				interval,
			)?;
			result = new_result;
			stage_trace.append(&mut diags);
			for jc in &src_join.columns {
				handled_by_wide.insert(jc.unit.name().clone());
			}
		}

		// 6. Per-measurement pipeline — handles measurements not covered
		// by the wide-join pass. Uses the typed pipeline module which
		// enforces: filter → signal_policy → crush/expand → join → null_fill.
		// The compiler prevents reordering these phases.
		{
			let time_bounds = time_filter.unwrap_or((0, i64::MAX));
			let subjects_vec: Option<Vec<String>> = subject_filter_values.clone();

			let plans = crate::pipeline::plan::build_measurement_plans(
				self, request, mode, interval, &handled_by_wide,
			)?;

			for plan in &plans {
				let (new_result, phase_diags) = crate::pipeline::execute_measurement(
					plan,
					result,
					time_bounds,
					subjects_vec.as_deref(),
				)?;
				result = new_result;

				// Convert pipeline PhaseDiags to stage_trace StageDiags
				for pd in &phase_diags {
					stage_trace.push(crate::subset::stages::StageDiag {
						stage: crate::subset::stages::SubsetStage::Pipeline {
							measurement: plan.name.as_str().to_string(),
							phase: pd.phase.to_string(),
						},
						rows_after: pd.output_rows,
						elapsed_us: pd.elapsed_us,
						notes: pd.notes.clone(),
					});
				}
			}
		}

		// 4. Join qualities
		for quality_name in &request.qualities {
			let qual_start = std::time::Instant::now();
			let quality_data = self.qualities.get(quality_name).ok_or_else(|| {
				EtlError::UnitNotFound(format!("Quality '{}' not found", quality_name))
			})?;

			result = self.join_quality_df(result, quality_data.data.clone(), subject_col)?;
			stage_trace.push(crate::subset::stages::StageDiag {
				stage: crate::subset::stages::SubsetStage::JoinQuality {
					quality: quality_name.as_str().to_string(),
				},
				rows_after: result.height(),
				elapsed_us: qual_start.elapsed().as_micros() as u64,
				notes: Vec::new(),
			});

			if let Some(ref null_val) = quality_data.unit.null_value_extension {
				result = apply_null_fill(&result, quality_name.as_str(), null_val)?;
			}
		}

		// 4b. Apply quality filter (filter subjects by quality value)
		if let Some(ref qf) = request.quality_filter {
			result = self.apply_quality_filter(result, &qf.quality, &qf.values)?;
		}

		// 5. Compute derivations
		if !self.schema.derivations.is_empty() {
			let requested_derivations: Vec<_> = measurement_names
				.iter()
				.filter(|n| self.schema.get_derivation(n).is_some())
				.collect();

			if !requested_derivations.is_empty() {
				debug!(count = requested_derivations.len(), "Computing derivations");
				result = derivation::compute_all_derivations(result, &self.schema)?;
			}
		}
		let has_historical = measurement_names
			.iter()
			.filter_map(|name| self.measurements.get(name))
			.any(|m| m.is_historical());

		debug!(rows = result.height(), columns = result.width(), "Before time range filter");

		// 6. Apply time range filter only for historical-only requests
		// (Combined requests already have correct grid bounds)
		let has_forecast = measurement_names
			.iter()
			.filter_map(|name| self.measurements.get(name))
			.any(|m| m.is_forecast());

		if has_historical &&
			!has_forecast &&
			let Some(ref time_range) = request.time_range
		{
			result = self.apply_time_filter(result, time_col, time_range)?;
		}
		debug!(rows = result.height(), columns = result.width(), "After time range filter");

		// 7. Apply subject filter if specified
		if let Some(ref subject_filter) = request.subject_filter {
			result = self.apply_subject_filter(result, subject_col, subject_filter)?;
		}

		// 8. Project the result frame down to the columns the user
		// actually requested.
		//
		// Dependency expansion (see `expand_derivation_dependencies`)
		// pulls base columns into the pipeline so derivations can find
		// their inputs, but the caller didn't ask for those — drop
		// them before returning so the API contract is "what you ask
		// for is what you get, plus the subject and time keys."
		//
		// Skipped when the request was "all measurements" (empty
		// list), since the user has implicitly asked for everything
		// the universe provides.
		if !request.measurements.is_empty() {
			use std::collections::HashSet;
			let actual: HashSet<String> = result
				.get_column_names()
				.iter()
				.map(|s| s.to_string())
				.collect();
			let mut keep: Vec<String> = Vec::new();
			let push_unique = |c: &str, keep: &mut Vec<String>| {
				if actual.contains(c) && !keep.iter().any(|k| k == c) {
					keep.push(c.to_string());
				}
			};
			push_unique(subject_col, &mut keep);
			push_unique(time_col, &mut keep);
			for name in &raw_measurement_names {
				push_unique(name.as_str(), &mut keep);
			}
			for q in &request.qualities {
				push_unique(q.as_str(), &mut keep);
			}
			let select_exprs: Vec<Expr> = keep.iter().map(|c| col(c.as_str())).collect();
			result = result.lazy().select(select_exprs).collect()?;
			debug!(
				kept = keep.len(),
				dropped = actual.len() - keep.len(),
				"Projected result down to requested columns"
			);
		}

		debug!(rows = result.height(), columns = result.width(), "Subset composition complete");

		// Build metadata. We pass `raw_measurement_names` (the original
		// request) rather than the dependency-expanded list so the
		// metadata matches the projected result frame.
		let measurement_metas = self.build_measurement_metas(&raw_measurement_names);
		let quality_metas = self.build_quality_metas(&request.qualities);

		// Apply report interval (bucketed aggregation) if the request
		// asked for one. Must run AFTER all measurement columns are in
		// `result` so each bucket aggregates all requested measurements
		// in a single group_by pass.
		let mut interval_stats: Vec<crate::interval::IntervalStats> = Vec::new();
		if let Some(ref report) = request.report_interval {
			let interval_stage_start = std::time::Instant::now();
			let plans: Vec<crate::interval::ResamplingPlan> = raw_measurement_names
				.iter()
				.filter_map(|name| {
					// Skip derivations — they're computed per-row and
					// aren't directly aggregable into buckets.
					if self.schema.get_derivation(name).is_some() {
						return None;
					}
					let md = self.measurements.get(name)?;
					Some(
						crate::interval::ResamplingPlanner::new(&md.unit, report).plan(),
					)
				})
				.collect();

			if !plans.is_empty() {
				let out = crate::interval::apply_interval(
					&result, &plans, &report.bucket, subject_col, time_col,
				)?;
				result = out.data;
				interval_stats = out.stats;

				// Count unique buckets emitted (distinct bucket_start_ms
				// across all subjects).
				let mut seen: std::collections::HashSet<i64> = std::collections::HashSet::new();
				for s in &interval_stats {
					seen.insert(s.bucket_start_ms);
				}

				stage_trace.push(crate::subset::stages::StageDiag {
					stage: crate::subset::stages::SubsetStage::ReportInterval {
						bucket: report.bucket.truncate_spec(),
						strategy: format!("{:?}", report.strategy),
						n_buckets: seen.len(),
						n_measurements: plans.len(),
					},
					rows_after: result.height(),
					elapsed_us: interval_stage_start.elapsed().as_micros() as u64,
					notes: {
						let mut notes = Vec::new();
						notes.push(format!(
							"strategy = {:?}", report.strategy,
						));
						notes.push(format!(
							"per-plan paths: {}",
							plans
								.iter()
								.map(|p| format!("{}={:?}", p.measurement.as_str(), p.path))
								.collect::<Vec<_>>()
								.join(", "),
						));
						notes
					},
				});
			}
		}

		let subject_count = result
			.column(subject_col)
			.map(|c| c.n_unique().unwrap_or(0))
			.unwrap_or(0);

		let mut info = SubsetInfo::new(&self.schema.name, subject_col)
			.with_time_column(time_col)
			.with_row_count(result.height())
			.with_subject_count(subject_count);
		info.stage_trace = stage_trace;
		info.interval_stats = interval_stats;

		Ok(SubsetUniverse::new(result, measurement_metas, quality_metas, info))
	}

	// =========================================================================
	// Internal: Qualities-Only Subset
	// =========================================================================

	/// Produce a subject × quality DataFrame with no time grid.
	///
	/// Used when the request contains qualities but no measurements.
	/// Collects unique subjects from quality DataFrames, left-joins each
	/// requested quality, applies quality_filter and subject_filter.
	/// Raw subset: no grid, no resample, no signal policy.
	///
	/// Each measurement is filtered by time + subject, crushed (if components),
	/// and joined on (subject, time). Data stays at its original observation
	/// timestamps — SCADA at 60s, MRMS at 3600s, etc.
	fn subset_raw_no_grid(
		&self,
		measurement_names: &[CanonicalColumnName],
		request: &EtlUnitSubsetRequest,
		subject_col: &str,
		time_col: &str,
	) -> EtlResult<SubsetUniverse<crate::subset::Raw>> {
		let time_filter = request.time_range.as_ref().and_then(|tr| {
			match (tr.start, tr.end) {
				(Some(s), Some(e)) => Some((s.timestamp_millis(), e.timestamp_millis())),
				_ => None,
			}
		});

		let subject_filter_values: Option<Vec<String>> = request.subject_filter.as_ref().and_then(|sf| {
			match sf {
				crate::request::SubjectFilter::Include(values) => {
					let strings: Vec<String> = values.iter()
						.filter_map(|v| v.as_str().map(|s| s.to_string()))
						.collect();
					if strings.is_empty() { None } else { Some(strings) }
				}
				_ => None,
			}
		});

		let mut result: Option<DataFrame> = None;
		let mut measurement_metas = Vec::new();

		for measurement_name in measurement_names {
			if self.schema.get_derivation(measurement_name).is_some() {
				continue;
			}

			let measurement_data = match self.measurements.get(measurement_name) {
				Some(md) => md,
				None => continue,
			};

			// Get raw data
			let data = measurement_data.fragment().as_dataframe()
				.map_err(EtlError::Polars)?;

			// Filter by time + subject
			let mut lf = data.clone().lazy();

			if let Some((start_ms, end_ms)) = time_filter {
				let tz = Some(polars::prelude::TimeZone::UTC);
				lf = lf.filter(
					col(time_col).gt_eq(lit(start_ms).cast(DataType::Datetime(TimeUnit::Milliseconds, tz.clone())))
					.and(col(time_col).lt_eq(lit(end_ms).cast(DataType::Datetime(TimeUnit::Milliseconds, tz))))
				);
			}

			if let Some(ref subjects) = subject_filter_values {
				let series = polars::prelude::Series::new("_sf".into(), subjects);
				lf = lf.filter(col(subject_col).is_in(lit(series).implode(), false));
			}

			let filtered = lf.collect()?;

			// Crush components if needed
			let crushed = if measurement_data.has_components() {
				let value_col = measurement_data.unit.name.as_str();
				let agg = measurement_data.unit.signal_aggregation();
				let agg_expr = build_agg_expr(value_col, agg);
				filtered.lazy()
					.group_by([col(subject_col), col(time_col)])
					.agg([agg_expr])
					.collect()?
			} else {
				filtered
			};

			// Select only (subject, time, value)
			let value_col = measurement_data.unit.name.as_str();
			let selected = crushed.lazy()
				.select([
					col(subject_col),
					col(time_col),
					col(value_col),
				])
				.collect()?;

			debug!(
				measurement = %measurement_name,
				rows = selected.height(),
				"Raw subset (no grid)"
			);

			let meta = MeasurementMeta::new(measurement_name.clone(), measurement_data.unit.kind)
				.with_null_value_configured(measurement_data.unit.null_value.is_some());
			let meta = if let Some(ref hints) = measurement_data.unit.chart_hints {
				meta.with_chart_hints(hints.clone())
			} else {
				meta
			};
			measurement_metas.push(meta);

			// Join with result
			result = Some(match result {
				None => selected,
				Some(existing) => {
					existing.join(
						&selected,
						[subject_col, time_col],
						[subject_col, time_col],
						JoinArgs::new(JoinType::Full)
							.with_coalesce(JoinCoalesce::CoalesceColumns),
						None,
					)?
				}
			});
		}

		// Sort by (subject, time) so LTTB and chart rendering work correctly
		let data = match result {
			Some(df) => df.lazy()
				.sort([subject_col, time_col], SortMultipleOptions::default())
				.collect()?,
			None => DataFrame::empty(),
		};

		// Count subjects
		let subject_count = data.column(subject_col)
			.map(|c| c.n_unique().unwrap_or(0))
			.unwrap_or(0);

		let info = SubsetInfo::new(&self.schema.name, subject_col)
			.with_time_column(time_col)
			.with_row_count(data.height())
			.with_subject_count(subject_count);

		debug!(
			rows = data.height(),
			subjects = subject_count,
			measurements = measurement_metas.len(),
			"Raw subset complete (no grid alignment)"
		);

		Ok(SubsetUniverse::new_raw(
			data,
			measurement_metas,
			Vec::new(),
			info,
		))
	}

	fn subset_qualities_only(
		&self,
		request: &EtlUnitSubsetRequest,
	) -> EtlResult<SubsetUniverse> {
		let subject_col = self.schema.subject.as_str();

		// Collect unique subjects from the quality DataFrames
		let mut all_subjects: Vec<String> = Vec::new();
		for quality_name in &request.qualities {
			if let Some(quality_data) = self.qualities.get(quality_name) {
				if let Ok(subjects) = quality_data.data.column(subject_col) {
					if let Ok(unique) = subjects.unique() {
						for i in 0..unique.len() {
							if let Ok(AnyValue::String(s)) = unique.get(i) {
								let s = s.to_string();
								if !all_subjects.contains(&s) {
									all_subjects.push(s);
								}
							} else if let Ok(AnyValue::StringOwned(s)) = unique.get(i) {
								let s = s.to_string();
								if !all_subjects.contains(&s) {
									all_subjects.push(s);
								}
							}
						}
					}
				}
			}
		}
		all_subjects.sort();

		if all_subjects.is_empty() {
			return Err(EtlError::Config("No subjects found in quality data".into()));
		}

		// Build single-column subject DataFrame
		let subject_series = Series::new(subject_col.into(), &all_subjects);
		let mut result = DataFrame::new(vec![subject_series.into()])?;

		// Left-join each requested quality
		for quality_name in &request.qualities {
			let quality_data = self.qualities.get(quality_name).ok_or_else(|| {
				EtlError::UnitNotFound(format!("Quality '{}' not found", quality_name))
			})?;

			result = self.join_quality_df(result, quality_data.data.clone(), subject_col)?;

			// Apply null_value_extension if specified
			if let Some(ref null_val) = quality_data.unit.null_value_extension {
				result = apply_null_fill(&result, quality_name.as_str(), null_val)?;
			}
		}

		// Apply quality filter
		if let Some(ref qf) = request.quality_filter {
			result = self.apply_quality_filter(result, &qf.quality, &qf.values)?;
		}

		// Apply subject filter
		if let Some(ref subject_filter) = request.subject_filter {
			result = self.apply_subject_filter(result, subject_col, subject_filter)?;
		}

		let subject_count = result
			.column(subject_col)
			.map(|c| c.n_unique().unwrap_or(0))
			.unwrap_or(0);

		let quality_metas = self.build_quality_metas(&request.qualities);

		let info = SubsetInfo::new(&self.schema.name, subject_col)
			.with_row_count(result.height())
			.with_subject_count(subject_count);

		debug!(
			rows = result.height(),
			subjects = subject_count,
			qualities = request.qualities.len(),
			"Qualities-only subset complete"
		);

		Ok(SubsetUniverse::new(result, vec![], quality_metas, info))
	}

	// =========================================================================
	// Internal: Quality Filter
	// =========================================================================

	/// Filter rows where a quality column's value is in the provided list.
	fn apply_quality_filter(
		&self,
		df: DataFrame,
		quality: &CanonicalColumnName,
		values: &[String],
	) -> EtlResult<DataFrame> {
		let filter_series = Series::new("_qf".into(), values);
		df.lazy()
			.filter(col(quality.as_str()).is_in(lit(filter_series), true))
			.collect()
			.map_err(Into::into)
	}

	// =========================================================================
	// Internal: Grid Building
	// =========================================================================
	/// Build the master time grid for composition
	///
	/// This is temporality-aware:
	/// - Historical measurements: use request time range (backward from now/end)
	/// - Forecast measurements: use data's natural range (forward from now)
	/// - Combined: grid spans from historical min to forecast max
	fn build_master_grid(
		&self,
		measurement_names: &[CanonicalColumnName],
		request: &EtlUnitSubsetRequest,
		interval: Duration,
	) -> EtlResult<DataFrame> {
		let subject_col = self.schema.subject.as_str();
		let time_col = self.schema.time.as_str();
		let now_ms = Utc::now().timestamp_millis();

		// Partition measurements by temporality
		let (historical_names, forecast_names) = self.partition_by_temporality(measurement_names);
		let has_historical = !historical_names.is_empty();
		let has_forecast = !forecast_names.is_empty();

		info!(
			 has_historical = has_historical,
			 has_forecast = has_forecast,
			 historical_names = ?historical_names,
			 forecast_names = ?forecast_names,
			 "Partitioned measurements by temporality"
		);

		// Collect all unique subjects from all measurements
		let all_subjects = self.collect_all_subjects(measurement_names, subject_col)?;
		if all_subjects.is_empty() {
			return Err(EtlError::Config("No subjects found in measurements".into()));
		}

		// Compute time bounds for each temporality
		let historical_bounds = if has_historical {
			let bounds =
				self.compute_historical_bounds(&historical_names, request, time_col, now_ms)?;
			if let Some((min, max)) = bounds {
				let hours = (max - min) as f64 / 3_600_000.0;
				info!(
					historical_min_ms = min,
					historical_max_ms = max,
					historical_hours = format!("{:.1}", hours),
					"Historical bounds computed"
				);
			} else {
				warn!("Historical bounds returned None - no historical data found");
			}
			bounds
		} else {
			None
		};

		let forecast_bounds = if has_forecast {
			// Debug: inspect forecast measurement data
			for name in &forecast_names {
				if let Some(measurement_data) = self.measurements.get(name) {
					if let Ok(md_df) = measurement_data.fragment().as_dataframe() {
						debug!(
							 measurement = %name,
							 temporality = ?measurement_data.temporality(),
							 columns = ?md_df.get_column_names(),
							 rows = md_df.height(),
							 "Forecast measurement info"
						);

						// Check if time column exists and get its range
						if let Ok(time_series) = md_df.column(time_col) {
							if let Ok(ca) = time_series.datetime() {
								let phys = ca.physical();
								debug!(
									 time_col_min = ?phys.min(),
									 time_col_max = ?phys.max(),
									 "Forecast time column range"
								);
							} else {
								warn!(
									 time_col = time_col,
									 dtype = ?time_series.dtype(),
									 "Forecast time column is not datetime type"
								);
							}
						} else {
							warn!(
								 time_col = time_col,
								 available_cols = ?md_df.get_column_names(),
								 "Forecast time column not found"
							);
						}
					}
				}
			}

			let bounds = self.compute_forecast_bounds(&forecast_names, request, time_col, now_ms)?;
			if let Some((min, max)) = bounds {
				let hours = (max - min) as f64 / 3_600_000.0;
				let hours_from_now = (max - now_ms) as f64 / 3_600_000.0;
				info!(
					forecast_min_ms = min,
					forecast_max_ms = max,
					forecast_hours = format!("{:.1}", hours),
					forecast_hours_from_now = format!("{:.1}", hours_from_now),
					"Forecast bounds computed"
				);
			} else {
				warn!("Forecast bounds returned None - no forecast data found");
			}
			bounds
		} else {
			None
		};

		// Combine bounds into final grid range
		let (grid_min, grid_max) =
			self.combine_time_bounds(historical_bounds, forecast_bounds, now_ms)?;

		// Calculate and log the total hours in the grid
		let total_hours = (grid_max - grid_min) as f64 / 3_600_000.0;
		let historical_hours = historical_bounds
			.map(|(min, max)| (max - min) as f64 / 3_600_000.0)
			.unwrap_or(0.0);
		let forecast_hours = forecast_bounds
			.map(|(min, max)| (max - min) as f64 / 3_600_000.0)
			.unwrap_or(0.0);

		info!(
			total_grid_hours = format!("{:.1}", total_hours),
			historical_hours = format!("{:.1}", historical_hours),
			forecast_hours = format!("{:.1}", forecast_hours),
			grid_min_ms = grid_min,
			grid_max_ms = grid_max,
			now_ms = now_ms,
			"⏰ Combined time bounds for master grid"
		);

		// Apply subject filter
		let filtered_subjects =
			self.apply_subject_filter_to_list(all_subjects, request.subject_filter.as_ref());
		if filtered_subjects.is_empty() {
			return Err(EtlError::Config("No subjects remain after filtering".into()));
		}

		// Align time range to interval boundaries
		let interval_ms = interval.as_millis() as i64;
		let aligned_min = (grid_min / interval_ms) * interval_ms;
		let aligned_max = (grid_max / interval_ms) * interval_ms;

		// Calculate aligned hours
		let aligned_total_hours = (aligned_max - aligned_min) as f64 / 3_600_000.0;

		info!(
			aligned_min_ms = aligned_min,
			aligned_max_ms = aligned_max,
			aligned_total_hours = format!("{:.1}", aligned_total_hours),
			interval_ms = interval_ms,
			"Aligned master grid time range to interval boundaries"
		);

		// Generate time grid
		let time_points: Vec<i64> = (aligned_min..=aligned_max)
			.step_by(interval_ms as usize)
			.collect();

		// Cross join subjects × times
		let n_subjects = filtered_subjects.len();
		let n_times = time_points.len();

		let subjects_repeated: Vec<String> = filtered_subjects
			.iter()
			.flat_map(|s| std::iter::repeat_n(s.clone(), n_times))
			.collect();

		let times_repeated: Vec<i64> = (0..n_subjects)
			.flat_map(|_| time_points.iter().cloned())
			.collect();

		// Detect timezone from measurement data so the grid matches
		let grid_tz = measurement_names
			.iter()
			.filter_map(|name| self.measurements.get(name))
			.find_map(|md| {
				md.fragment().as_dataframe()
					.ok()
					.and_then(|df| df.column(time_col)
						.ok()
						.and_then(|c| match c.dtype() {
							DataType::Datetime(_, tz) => tz.clone(),
							_ => None,
						}))
			});

		let subject_series = Series::new(subject_col.into(), subjects_repeated);
		// Use ChunkedArray to preserve timezone (polars 0.51 .cast() drops tz)
		let time_series = Int64Chunked::new(time_col.into(), &times_repeated)
			.into_datetime(TimeUnit::Milliseconds, grid_tz)
			.into_series();

		info!(
			n_subjects = n_subjects,
			n_time_points = n_times,
			total_grid_rows = n_subjects * n_times,
			"👉 >>>> Built master grid"
		);

		DataFrame::new(vec![subject_series.into(), time_series.into()]).map_err(Into::into)
	}

	/// Partition measurement names by temporality
	fn partition_by_temporality(
		&self,
		measurement_names: &[CanonicalColumnName],
	) -> (Vec<CanonicalColumnName>, Vec<CanonicalColumnName>) {
		let mut historical = Vec::new();
		let mut forecast = Vec::new();

		for name in measurement_names {
			// Skip derivations
			if self.schema.get_derivation(name).is_some() {
				continue;
			}

			if let Some(measurement_data) = self.measurements.get(name) {
				match measurement_data.temporality() {
					DataTemporality::Historical => historical.push(name.clone()),
					DataTemporality::Forecast => forecast.push(name.clone()),
				}
			}
		}

		(historical, forecast)
	}

	/// Collect all unique subjects from measurements
	fn collect_all_subjects(
		&self,
		measurement_names: &[CanonicalColumnName],
		subject_col: &str,
	) -> EtlResult<Vec<String>> {
		let mut all_subjects: Vec<String> = Vec::new();

		for measurement_name in measurement_names {
			// Skip derivations
			if self.schema.get_derivation(measurement_name).is_some() {
				continue;
			}

			if let Some(measurement_data) = self.measurements.get(measurement_name) &&
				let Ok(md_df) = measurement_data.fragment().as_dataframe() &&
				let Ok(subjects) = md_df.column(subject_col) &&
				let Ok(unique) = subjects.unique()
			{
				for i in 0..unique.len() {
					if let Ok(AnyValue::String(s)) = unique.get(i) {
						if !all_subjects.contains(&s.to_string()) {
							all_subjects.push(s.to_string());
						}
					} else if let Ok(AnyValue::StringOwned(s)) = unique.get(i) {
						let s_str = s.to_string();
						if !all_subjects.contains(&s_str) {
							all_subjects.push(s_str);
						}
					}
				}
			}
		}

		all_subjects.sort();
		Ok(all_subjects)
	}

	/// Compute time bounds for historical measurements
	///
	/// Uses request time range, constrained to actual data range, capped at now
	fn compute_historical_bounds(
		&self,
		measurement_names: &[CanonicalColumnName],
		request: &EtlUnitSubsetRequest,
		time_col: &str,
		now_ms: i64,
	) -> EtlResult<Option<(i64, i64)>> {
		// Start with the request's window (if any), clipped to now.
		let (req_start, req_end): (Option<i64>, Option<i64>) = match &request.time_range {
			Some(tr) => (
				tr.start.map(|t| t.timestamp_millis()),
				tr.end.map(|t| t.timestamp_millis()),
			),
			None => (None, None),
		};

		// Compute data bounds **intersected with the request window**. This
		// yields the range of observations that actually fall inside the
		// caller's asked-for window — not the full workbench span. That way
		// the master grid covers "where data exists for this request", not
		// "where data exists in the cache," which avoids tens of thousands
		// of null grid cells when the request window is wider than the
		// observations within it.
		let (data_min, data_max) = self.get_time_range_for_measurements_in_window(
			measurement_names, time_col, req_start, req_end,
		)?;

		let data_min = match data_min {
			Some(t) => t,
			None => return Ok(None),
		};
		let data_max = data_max.unwrap_or(now_ms);

		// Historical must not extend past now. Intersection with request
		// already happened above; `req_end` is already baked into `data_max`
		// because `get_time_range_for_measurements_in_window` clipped to it.
		let min_time = data_min;
		let max_time = data_max.min(now_ms);

		if min_time > max_time {
			return Ok(None);
		}

		info!(min_time = min_time, max_time = max_time, "Historical time bounds");

		Ok(Some((min_time, max_time)))
	}

	/// Compute time bounds for forecast measurements within the request's
	/// window. Previously this ignored the request time range entirely,
	/// which made the master grid expand to cover far-future forecast
	/// data even when the caller only asked for a past window — blowing
	/// the grid out to thousands of null cells per historical measurement.
	fn compute_forecast_bounds(
		&self,
		measurement_names: &[CanonicalColumnName],
		request: &EtlUnitSubsetRequest,
		time_col: &str,
		now_ms: i64,
	) -> EtlResult<Option<(i64, i64)>> {
		let (req_start, req_end): (Option<i64>, Option<i64>) = match &request.time_range {
			Some(tr) => (
				tr.start.map(|t| t.timestamp_millis()),
				tr.end.map(|t| t.timestamp_millis()),
			),
			None => (None, None),
		};

		// Only forecast observations inside the request window count.
		let (data_min, data_max) = self.get_time_range_for_measurements_in_window(
			measurement_names, time_col, req_start, req_end,
		)?;

		let data_max = match data_max {
			Some(t) => t,
			None => return Ok(None),
		};

		// Forecast range: from data_min (or now if later) to data_max.
		// Both already clipped to the request window above.
		let min_time = data_min.unwrap_or(now_ms);
		let max_time = data_max;

		if min_time > max_time {
			return Ok(None);
		}

		info!(min_time = min_time, max_time = max_time, "Forecast time bounds");

		Ok(Some((min_time, max_time)))
	}

	/// Combine historical and forecast bounds into final grid range
	fn combine_time_bounds(
		&self,
		historical: Option<(i64, i64)>,
		forecast: Option<(i64, i64)>,
		_now_ms: i64,
	) -> EtlResult<(i64, i64)> {
		match (historical, forecast) {
			(Some((h_min, h_max)), Some((f_min, f_max))) => {
				// Both: span from historical min to forecast max
				let grid_min = h_min.min(f_min);
				let grid_max = h_max.max(f_max);

				info!(
					historical_range = format!("{} - {}", h_min, h_max),
					forecast_range = format!("{} - {}", f_min, f_max),
					combined_range = format!("{} - {}", grid_min, grid_max),
					"Combined historical + forecast time range"
				);

				Ok((grid_min, grid_max))
			}
			(Some((h_min, h_max)), None) => {
				// Historical only
				Ok((h_min, h_max))
			}
			(None, Some((f_min, f_max))) => {
				// Forecast only
				Ok((f_min, f_max))
			}
			(None, None) => {
				// No data - return error
				Err(EtlError::Config("No time range could be determined from measurements".into()))
			}
		}
	}

	/// Get min/max time across multiple measurements
	fn get_time_range_for_measurements(
		&self,
		measurement_names: &[CanonicalColumnName],
		time_col: &str,
	) -> EtlResult<(Option<i64>, Option<i64>)> {
		self.get_time_range_for_measurements_in_window(
			measurement_names, time_col, None, None,
		)
	}

	/// Get min/max time across multiple measurements, restricted to an
	/// optional `[window_start, window_end]` window (inclusive). When both
	/// bounds are `None`, this is equivalent to
	/// [`get_time_range_for_measurements`].
	///
	/// Used by [`compute_historical_bounds`] so that the master grid spans
	/// only the part of the data that falls inside the caller's request
	/// window — preventing grids that are orders of magnitude wider than
	/// the actual observations.
	fn get_time_range_for_measurements_in_window(
		&self,
		measurement_names: &[CanonicalColumnName],
		time_col: &str,
		window_start: Option<i64>,
		window_end: Option<i64>,
	) -> EtlResult<(Option<i64>, Option<i64>)> {
		let mut overall_min: Option<i64> = None;
		let mut overall_max: Option<i64> = None;

		for measurement_name in measurement_names {
			if let Some(measurement_data) = self.measurements.get(measurement_name) &&
				let Ok(md_df) = measurement_data.fragment().as_dataframe() &&
				let Ok(time_series) = md_df.column(time_col) &&
				let Ok(ca) = time_series.datetime()
			{
				let phys = ca.physical();
				// When a window is provided, scan physical i64s directly and
				// track the min/max of timestamps that fall inside
				// `[window_start, window_end]`. This is O(n) per measurement
				// but runs only during subset planning, not in the hot loop.
				match (window_start, window_end) {
					(None, None) => {
						if let Some(min_t) = phys.min() {
							overall_min = Some(overall_min.map_or(min_t, |m| m.min(min_t)));
						}
						if let Some(max_t) = phys.max() {
							overall_max = Some(overall_max.map_or(max_t, |m| m.max(max_t)));
						}
					}
					_ => {
						let ws = window_start.unwrap_or(i64::MIN);
						let we = window_end.unwrap_or(i64::MAX);
						let mut local_min: Option<i64> = None;
						let mut local_max: Option<i64> = None;
						for v in phys.into_iter().flatten() {
							if v < ws || v > we {
								continue;
							}
							local_min = Some(local_min.map_or(v, |m| m.min(v)));
							local_max = Some(local_max.map_or(v, |m| m.max(v)));
						}
						if let Some(m) = local_min {
							overall_min = Some(overall_min.map_or(m, |o| o.min(m)));
						}
						if let Some(m) = local_max {
							overall_max = Some(overall_max.map_or(m, |o| o.max(m)));
						}
					}
				}
			}
		}

		Ok((overall_min, overall_max))
	}

	/// Apply subject filter to a list of subjects
	fn apply_subject_filter_to_list(
		&self,
		mut subjects: Vec<String>,
		filter: Option<&crate::request::SubjectFilter>,
	) -> Vec<String> {
		if let Some(subject_filter) = filter {
			match subject_filter {
				crate::request::SubjectFilter::Include(values) => {
					let include: Vec<String> = values
						.iter()
						.filter_map(|v| v.as_str().map(String::from))
						.collect();
					subjects.retain(|s| include.contains(s));
				}
				crate::request::SubjectFilter::Exclude(values) => {
					let exclude: Vec<String> = values
						.iter()
						.filter_map(|v| v.as_str().map(String::from))
						.collect();
					subjects.retain(|s| !exclude.contains(s));
				}
			}
		}
		subjects
	}

	//88

	// =========================================================================
	// Internal: Resampling
	// =========================================================================

	/// Resample measurement data to the target interval
	///
	/// - If TTL == interval: align times to interval boundaries
	/// - If TTL < interval (downsample): aggregate to coarser grid
	/// - If TTL > interval (upsample): truncate time to interval boundaries (the asof join will
	///   broadcast values forward within TTL)
	fn resample_measurement(
		&self,
		data: &DataFrame,
		measurement_name: &CanonicalColumnName,
		kind: MeasurementKind,
		subject_col: &str,
		time_col: &str,
		target_interval: Duration,
		unit_ttl: Duration,
	) -> EtlResult<DataFrame> {
		self.resample_measurement_with_components(
			data, measurement_name, kind, subject_col, time_col,
			target_interval, unit_ttl, &[],
		)
	}

	/// Resample a measurement to the target interval grid.
	///
	/// When `component_cols` is non-empty, they are included in the
	/// group-by so signal policy is applied PER COMPONENT. This is
	/// critical for measurements like `engines_on_count` where the
	/// crush (sum across components) must happen AFTER gridding, not
	/// before — otherwise the crush collapses the component dimension
	/// on irregular raw timestamps and the grid is sparse.
	fn resample_measurement_with_components(
		&self,
		data: &DataFrame,
		measurement_name: &CanonicalColumnName,
		kind: MeasurementKind,
		subject_col: &str,
		time_col: &str,
		target_interval: Duration,
		unit_ttl: Duration,
		component_cols: &[&str],
	) -> EtlResult<DataFrame> {
		let interval_ms = target_interval.as_millis() as i64;
		let value_col = measurement_name.as_str();

		// Detect timezone from the data's time column so we preserve it after truncation
		let time_tz = data
			.column(time_col)
			.ok()
			.and_then(|c| match c.dtype() {
				DataType::Datetime(_, tz) => tz.clone(),
				_ => None,
			});

		// Time truncation expression - used in all cases to ensure times align
		let truncate_time_expr = (col(time_col).cast(DataType::Int64) / lit(interval_ms) *
			lit(interval_ms))
		.cast(DataType::Datetime(TimeUnit::Milliseconds, time_tz))
		.alias(time_col);

		// Build group-by columns: always [subject, time], plus any components
		let mut group_cols: Vec<Expr> = vec![col(subject_col), col(time_col)];
		for comp in component_cols {
			group_cols.push(col(*comp));
		}

		if unit_ttl == target_interval {
			// Same interval - align times to boundaries AND aggregate.
			//
			// Truncation alone is unsafe: if the source's sample rate is
			// finer than its TTL (e.g., 1-second telemetry with a 60-second
			// TTL), multiple raw observations within one TTL window all
			// truncate to the same boundary. Without aggregation, downstream
			// (subject, time) join keys are duplicated and the LEFT JOIN
			// multiplies rows by the duplicate count.
			//
			// This is structurally identical to the `unit_ttl < target_interval`
			// downsample branch — the only difference is intent. Both must
			// reduce to one value per (subject, time[, component]) bucket.
			debug!(
				measurement = %measurement_name,
				ttl_ms = unit_ttl.as_millis(),
				interval_ms = interval_ms,
				components = ?component_cols,
				"Same interval: aligning times to boundaries with aggregation"
			);
			let agg = kind.default_aggregation();
			let agg_expr = build_agg_expr(value_col, agg);
			data
				.clone()
				.lazy()
				.with_column(truncate_time_expr)
				.group_by(group_cols)
				.agg([agg_expr])
				.collect()
				.map_err(Into::into)
		} else if unit_ttl > target_interval {
			// Upsample: coarse → fine
			// Truncate time to interval boundaries so asof join can find matches
			// The actual broadcasting happens via asof join with TTL tolerance.
			//
			// When components are present, we also need to deduplicate per
			// (subject, time, component) to prevent the downstream crush
			// from double-counting values that truncated to the same boundary.
			debug!(
				measurement = %measurement_name,
				from_ttl_ms = unit_ttl.as_millis(),
				to_interval_ms = target_interval.as_millis(),
				components = ?component_cols,
				"Upsample: truncating time to interval boundaries for asof join"
			);
			let truncated = data
				.clone()
				.lazy()
				.with_column(truncate_time_expr);
			if component_cols.is_empty() {
				truncated.collect().map_err(Into::into)
			} else {
				// Deduplicate: keep last value per (subject, time, component)
				let agg = kind.default_aggregation();
				let agg_expr = build_agg_expr(value_col, agg);
				truncated
					.group_by(group_cols)
					.agg([agg_expr])
					.collect()
					.map_err(Into::into)
			}
		} else {
			// Downsample: fine → coarse (aggregate)
			debug!(
				measurement = %measurement_name,
				from_ttl_ms = unit_ttl.as_millis(),
				to_interval_ms = target_interval.as_millis(),
				components = ?component_cols,
				"Downsample: aggregating to coarser interval"
			);
			let agg = kind.default_aggregation();
			let agg_expr = build_agg_expr(value_col, agg);

			data
				.clone()
				.lazy()
				.with_column(truncate_time_expr)
				.group_by(group_cols)
				.agg([agg_expr])
				.collect()
				.map_err(Into::into)
		}
	}

	// =========================================================================
	// Internal: Wide Join (grouped-join optimization)
	// =========================================================================

	/// Whether a `SourceJoin` is eligible for the wide-path executor.
	///
	/// The wide path handles the common case where multiple measurements
	/// from a single source share signal-policy parameters and want to
	/// be brought onto the master grid in one LEFT JOIN. It does **not**
	/// handle:
	///
	/// - **Single-member joins** — the per-measurement loop is fine and
	///   batching has nothing to optimize.
	/// - **Component-bearing sources** — need a per-measurement crush
	///   before the join. In practice this branch is also defended by
	///   the single-member check above, because each unpivoted fragment
	///   has its own Arc identity and therefore lives in its own
	///   `SourceContext`. Engines, for example, post-unpivot have their
	///   own `SourceJoin` with one member. The explicit check is here
	///   for defense in depth in case future plumbing produces a
	///   multi-member component source.
	/// All three branches of `resample_wide` (signal-only, downsample,
	/// upsample) are now wired, so the eligibility predicate accepts
	/// any non-trivial multi-column non-component join.
	fn is_wide_join_eligible(
		&self,
		src_join: &crate::plan::join::SourceJoin,
		_target_interval: Duration,
	) -> bool {
		if src_join.columns.len() <= 1 {
			return false;
		}
		if src_join.right_source.has_components() {
			return false;
		}
		true
	}

	/// Execute a wide LEFT JOIN: bring every value column from one
	/// `SourceJoin` onto the cumulative left frame in a single pass.
	///
	/// Caller must check [`Universe::is_wide_join_eligible`] before
	/// calling. Returns the new cumulative frame plus the diagnostic
	/// trace entries this stage produced.
	fn execute_wide_source_join(
		&self,
		src_join: &crate::plan::join::SourceJoin,
		filtered_cache: &std::collections::HashMap<usize, DataFrame>,
		cumulative: DataFrame,
		mode: SignalPolicyMode,
		target_interval: Duration,
	) -> EtlResult<(DataFrame, Vec<crate::subset::stages::StageDiag>)> {
		use crate::subset::stages::{StageDiag, SubsetStage};

		let subject_col = self.schema.subject.as_str();
		let time_col = self.schema.time.as_str();

		// Pull the cached filtered DataFrame for this source.
		let source_key_raw = src_join.right_source_key().as_raw();
		let cached = filtered_cache.get(&source_key_raw).ok_or_else(|| {
			EtlError::Config(format!(
				"Wide join: no filter cache entry for source key {}",
				src_join.right_source_key()
			))
		})?;

		// Narrow the cached frame to (subject, time, all value columns).
		// Each binding's physical name is selected from the cache and
		// aliased to its canonical name for downstream consumers.
		let phys_subject = src_join.right_source.subject.physical.as_str();
		let phys_time = src_join.right_source.time.physical.as_str();

		let mut select_cols = vec![
			col(phys_subject).alias(subject_col),
			col(phys_time).alias(time_col),
		];
		for jc in &src_join.columns {
			select_cols.push(
				col(jc.binding.physical.as_str()).alias(jc.binding.canonical.as_str()),
			);
		}
		let narrowed = cached.clone().lazy().select(select_cols).collect()?;

		// Determine effective TTL for the wide path. Schema validation
		// guarantees every measurement has a signal policy with a TTL.
		let unit_ttl = Duration::from_millis(
			src_join.signal_config.ttl_ms.expect(
				"Wide-join source has no TTL. Every measurement must have a \
				 signal policy with ttl_secs configured. This is validated at \
				 schema build time — if you see this panic, a measurement was \
				 constructed without schema validation."
			) as u64,
		);

		// Resample wide: signal-only/downsample uses one group_by with
		// per-column aggregations; upsample uses truncate-only and lets
		// the asof join below broadcast values forward. Skip in raw
		// mode — same contract as the per-measurement path.
		let resampled = if mode == SignalPolicyMode::Skip {
			narrowed
		} else {
			self.resample_wide(
				&narrowed,
				src_join,
				subject_col,
				time_col,
				target_interval,
				unit_ttl,
			)?
		};

		// One LEFT JOIN brings every value column into the cumulative
		// frame. `join_measurement_df` handles multi-column right sides
		// in both its equi-join branch (signal-only/downsample) and its
		// asof branch (upsample, where TTL > target_interval).
		let join_start = std::time::Instant::now();
		let mut joined = self.join_measurement_df(
			cumulative,
			resampled,
			subject_col,
			time_col,
			unit_ttl,
			target_interval,
		)?;
		let join_elapsed = join_start.elapsed().as_micros() as u64;

		// Apply per-column null fills (`null_value_extension`) for any
		// member that declared one.
		let mut diags: Vec<StageDiag> = Vec::new();
		let measurement_names: Vec<String> = src_join
			.columns
			.iter()
			.map(|jc| jc.unit.name().as_str().to_string())
			.collect();
		diags.push(StageDiag {
			stage:      SubsetStage::WideJoin {
				measurements: measurement_names,
				source:       src_join.right_source.source_name.as_str().to_string(),
			},
			rows_after: joined.height(),
			elapsed_us: join_elapsed,
			notes: Vec::new(),
		});

		for jc in src_join.columns_with_join_fills() {
			if let Some(ref fill) = jc.binding.join_null_fill {
				let fill_start = std::time::Instant::now();
				joined = apply_null_fill(&joined, jc.binding.canonical.as_str(), fill)?;
				diags.push(StageDiag {
					stage:      SubsetStage::FillNull {
						column: jc.binding.canonical.as_str().to_string(),
						value:  format!("{:?}", fill),
					},
					rows_after: joined.height(),
					elapsed_us: fill_start.elapsed().as_micros() as u64,
					notes: Vec::new(),
				});
			}
		}

		Ok((joined, diags))
	}

	/// Wide resample: like [`Universe::resample_measurement`] but for N
	/// value columns at once.
	///
	/// Branches identically to the per-measurement function:
	///
	/// - **`unit_ttl <= target_interval`** (signal-only or downsample) —
	///   truncate the time column to the target grid, then one
	///   `group_by([subject, time]).agg([…])` pass with one aggregation
	///   expression per member. Different members can use different
	///   aggregations in one pass — Polars handles per-column aggs
	///   natively.
	///
	/// - **`unit_ttl > target_interval`** (upsample) — truncate-only,
	///   no `group_by`. The wide right side is then asof-joined onto
	///   the cumulative left frame by [`Universe::join_measurement_df`],
	///   whose `join_asof_by` brings every right-side value column
	///   onto the left in one operation. The TTL becomes the asof
	///   tolerance, broadcasting sparse upstream values forward within
	///   the staleness window.
	fn resample_wide(
		&self,
		data: &DataFrame,
		src_join: &crate::plan::join::SourceJoin,
		subject_col: &str,
		time_col: &str,
		target_interval: Duration,
		unit_ttl: Duration,
	) -> EtlResult<DataFrame> {
		let interval_ms = target_interval.as_millis() as i64;

		let time_tz = data
			.column(time_col)
			.ok()
			.and_then(|c| match c.dtype() {
				DataType::Datetime(_, tz) => tz.clone(),
				_ => None,
			});

		let truncate_time_expr = (col(time_col).cast(DataType::Int64) / lit(interval_ms)
			* lit(interval_ms))
		.cast(DataType::Datetime(TimeUnit::Milliseconds, time_tz))
		.alias(time_col);

		if unit_ttl > target_interval {
			// Upsample: coarse → fine. Truncate only; asof join will
			// broadcast sparse values forward within TTL tolerance.
			debug!(
				source = %src_join.right_source.source_name,
				from_ttl_ms = unit_ttl.as_millis(),
				to_interval_ms = interval_ms,
				value_columns = src_join.columns.len(),
				"Wide resample (upsample): truncate-only, asof join handles broadcast"
			);
			return data
				.clone()
				.lazy()
				.with_column(truncate_time_expr)
				.collect()
				.map_err(Into::into);
		}

		// Signal-only (ttl == interval) or downsample (ttl < interval).
		// Both reduce to one value per (subject, time) bucket via
		// per-member aggregation in a single group_by pass.
		let mut agg_exprs: Vec<Expr> = Vec::with_capacity(src_join.columns.len());
		for jc in &src_join.columns {
			let canonical = jc.binding.canonical.as_str();
			let md = self.measurements.get(jc.unit.name()).ok_or_else(|| {
				EtlError::UnitNotFound(format!(
					"Wide resample: measurement '{}' not found in universe",
					jc.unit.name()
				))
			})?;
			let agg = md.unit.signal_aggregation();
			agg_exprs.push(build_agg_expr(canonical, agg));
		}

		debug!(
			source = %src_join.right_source.source_name,
			interval_ms = interval_ms,
			value_columns = src_join.columns.len(),
			"Wide resample: truncate + group_by + per-column aggs"
		);

		data.clone()
			.lazy()
			.with_column(truncate_time_expr)
			.group_by([col(subject_col), col(time_col)])
			.agg(agg_exprs)
			.collect()
			.map_err(Into::into)
	}

	// =========================================================================
	// Internal: Joins
	// =========================================================================

	/// Left join a measurement DataFrame on (subject, time)
	///
	/// Uses asof join when the measurement needs upsampling (TTL > interval),
	/// which broadcasts values forward within the TTL tolerance.
	/// Uses regular equi-join when TTL <= interval (times align exactly after resampling).
	fn join_measurement_df(
		&self,
		left: DataFrame,
		right: DataFrame,
		subject_col: &str,
		time_col: &str,
		unit_ttl: Duration,
		target_interval: Duration,
	) -> EtlResult<DataFrame> {


		// Get new columns from right (exclude join keys)
        let tmp = left.sort([subject_col, time_col], SortMultipleOptions::default())?;
		debug!(
			left = ?tmp.head(Some(7)),
			"Left dataframe"
		);

        let tmp = right.sort([subject_col, time_col], SortMultipleOptions::default())?;
		debug!(
			right = ?tmp.head(Some(7)),
			"Right dataframe"
		);

		if unit_ttl > target_interval {
			// Upsample case: use asof join to broadcast values forward within TTL
			use polars::prelude::AsofJoinBy;

			let ttl_ms = unit_ttl.as_millis() as i64;

			debug!(
				ttl_ms = ttl_ms,
				interval_ms = target_interval.as_millis(),
				left_rows = left.height(),
				right_rows = right.height(),
				"Using asof join for upsampling"
			);

			// Both dataframes must be sorted by the asof key (time) within each group
			let left_sorted = left
				.clone()
				.lazy()
				.sort([subject_col, time_col], SortMultipleOptions::default())
				.collect()?;

			let right_sorted = right
				.clone()
				.lazy()
				.sort([subject_col, time_col], SortMultipleOptions::default())
				.collect()?;

			// Asof join: for each left row, find the most recent right row within tolerance
			// join_asof_by(other, left_on, right_on, left_by, right_by, strategy, tolerance, allow_eq,
			// check_sortedness)
			let tolerance = Some(AnyValue::Duration(ttl_ms, TimeUnit::Milliseconds));

			left_sorted
				.join_asof_by(
					&right_sorted,
					time_col,
					time_col,
					[subject_col],
					[subject_col],
					AsofStrategy::Backward,
					tolerance,
					true,  // allow_eq: allow exact matches
					false, // check_sortedness: we already sorted
				)
				.map_err(Into::into)
		} else {
			// Downsample or same interval: regular equi-join (times align exactly)
			debug!(
				ttl_ms = unit_ttl.as_millis(),
				interval_ms = target_interval.as_millis(),
				left_rows = left.height(),
				right_rows = right.height(),
				"Using regular equi-join"
			);

			let right_new_cols: Vec<String> = right
				.get_column_names()
				.iter()
				.filter(|c| c.as_str() != subject_col && c.as_str() != time_col)
				.map(|c| c.to_string())
				.collect();

			// Build select: all left + new right
			let mut select_exprs: Vec<Expr> = left
				.get_column_names()
				.iter()
				.map(|c| col(c.as_str()))
				.collect();

			for rc in &right_new_cols {
				select_exprs.push(col(rc.as_str()));
			}

			left
				.lazy()
				.join(
					right.lazy(),
					[col(subject_col), col(time_col)],
					[col(subject_col), col(time_col)],
					JoinArgs::new(JoinType::Left),
				)
				.select(select_exprs)
				.collect()
				.map_err(Into::into)
		}
	}

	/// Left join a quality DataFrame on (subject)
	fn join_quality_df(
		&self,
		left: DataFrame,
		right: DataFrame,
		subject_col: &str,
	) -> EtlResult<DataFrame> {
		// Get new columns from right (exclude join key)
		let right_new_cols: Vec<String> = right
			.get_column_names()
			.iter()
			.filter(|c| c.as_str() != subject_col)
			.map(|c| c.to_string())
			.collect();

		// Build select: all left + new right
		let mut select_exprs: Vec<Expr> = left
			.get_column_names()
			.iter()
			.map(|c| col(c.as_str()))
			.collect();

		for rc in &right_new_cols {
			select_exprs.push(col(rc.as_str()));
		}

		left
			.lazy()
			.join(right.lazy(), [col(subject_col)], [col(subject_col)], JoinArgs::new(JoinType::Left))
			.select(select_exprs)
			.collect()
			.map_err(Into::into)
	}

	// =========================================================================
	// Internal: Filters
	// =========================================================================

	/// Apply time range filter
	fn apply_time_filter(
		&self,
		df: DataFrame,
		time_col: &str,
		range: &crate::request::TimeRange,
	) -> EtlResult<DataFrame> {
		let mut lf = df.lazy();

		if let Some(start) = range.start {
			let start_ms = start.timestamp_millis();
			lf = lf.filter(col(time_col).gt_eq(lit(start_ms)));
		}
		if let Some(end) = range.end {
			let end_ms = end.timestamp_millis();
			lf = lf.filter(col(time_col).lt(lit(end_ms)));
		}

		lf.collect().map_err(Into::into)
	}

	/// Apply subject filter
	fn apply_subject_filter(
		&self,
		df: DataFrame,
		subject_col: &str,
		filter: &crate::request::SubjectFilter,
	) -> EtlResult<DataFrame> {
		let lf = df.lazy();

		match filter {
			crate::request::SubjectFilter::Include(values) => {
				let strings: Vec<String> = values
					.iter()
					.filter_map(|v| v.as_str().map(String::from))
					.collect();
				let series = Series::new("_filter".into(), strings);
				lf.filter(col(subject_col).is_in(lit(series).implode(), true))
					.collect()
					.map_err(Into::into)
			}
			crate::request::SubjectFilter::Exclude(values) => {
				let strings: Vec<String> = values
					.iter()
					.filter_map(|v| v.as_str().map(String::from))
					.collect();
				let series = Series::new("_filter".into(), strings);
				lf.filter(col(subject_col).is_in(lit(series), true).not())
					.collect()
					.map_err(Into::into)
			}
		}
	}

	// =========================================================================
	// Internal: Metadata Building
	// =========================================================================

	/// Build measurement metadata for subset response
	fn build_measurement_metas(&self, names: &[CanonicalColumnName]) -> Vec<MeasurementMeta> {
		names
			.iter()
			.filter_map(|name| {
				if let Some(measurement_data) = self.measurements.get(name) {
					let hints = measurement_data.unit.effective_chart_hints();
					Some(
						MeasurementMeta::new(name.clone(), measurement_data.unit.kind)
							.with_null_value_configured(measurement_data.unit.null_value.is_some())
							.with_chart_hints(hints),
					)
				} else if let Some(derivation) = self.schema.get_derivation(name) {
					let hints = derivation.effective_chart_hints();
					// Derivations don't have their own null_value; they inherit
					// the null-value discipline of their inputs.
					Some(MeasurementMeta::new(name.clone(), derivation.kind).with_chart_hints(hints))
				} else {
					None
				}
			})
			.collect()
	}

	/// Build quality metadata for subset response
	fn build_quality_metas(&self, names: &[CanonicalColumnName]) -> Vec<QualityMeta> {
		names
			.iter()
			.filter_map(|name| {
				self.qualities.get(name).map(|quality_data| {
					let hints = quality_data.unit.effective_chart_hints();
					QualityMeta::new(name.clone()).with_chart_hints(hints)
				})
			})
			.collect()
	}
}

// =============================================================================
// Helpers
// =============================================================================

/// Compute the transitive closure of derivation dependencies, returning
/// a list where every requested name is included alongside the base
/// measurements (and other derivations) it depends on.
///
/// Walks `requested` in order, depth-first per item, deduping by name.
/// Dependencies appear before their dependents in the result, so
/// downstream code can rely on the order for any topological consumer.
///
/// # Why this exists
///
/// A request like `measurements: [sump, any_engine_running]` includes a
/// derivation (`any_engine_running`) but not the base columns it
/// references (e.g., `engine_1`, `engine_2`). Without expansion, the
/// per-measurement filter/join loop never brings those columns into the
/// result, and the post-join derivation evaluator fails its column
/// lookup. Expansion makes the dependency set explicit before the
/// pipeline runs.
fn expand_derivation_dependencies(
	requested: &[CanonicalColumnName],
	schema: &EtlSchema,
) -> Vec<CanonicalColumnName> {
	use std::collections::HashSet;

	fn visit(
		name: &CanonicalColumnName,
		schema: &EtlSchema,
		seen: &mut HashSet<CanonicalColumnName>,
		out: &mut Vec<CanonicalColumnName>,
	) {
		if !seen.insert(name.clone()) {
			return;
		}
		// If this name is a derivation, visit its inputs first so
		// dependencies land in `out` before the derivation that uses them.
		if let Some(deriv) = schema.get_derivation(name) {
			for dep in deriv.input_columns() {
				visit(dep, schema, seen, out);
			}
		}
		out.push(name.clone());
	}

	let mut seen: HashSet<CanonicalColumnName> = HashSet::new();
	let mut out: Vec<CanonicalColumnName> = Vec::with_capacity(requested.len());
	for name in requested {
		visit(name, schema, &mut seen, &mut out);
	}
	out
}

/// Build aggregation expression for a column
fn build_agg_expr(col_name: &str, agg: Aggregate) -> Expr {
	match agg {
		Aggregate::Mean => col(col_name).mean().alias(col_name),
		Aggregate::Sum => col(col_name).sum().alias(col_name),
		Aggregate::Min => col(col_name).min().alias(col_name),
		Aggregate::Max => col(col_name).max().alias(col_name),
		Aggregate::Any => col(col_name).max().alias(col_name), // max of 0/1 = any
		Aggregate::All => col(col_name).min().alias(col_name), // min of 0/1 = all
		Aggregate::Count => col(col_name).count().alias(col_name),
		Aggregate::First => col(col_name).first().alias(col_name),
		Aggregate::Last => col(col_name).last().alias(col_name),
		_ => col(col_name).mean().alias(col_name),
	}
}

/// Apply null fill to a column
fn apply_null_fill(df: &DataFrame, col_name: &str, null_val: &NullValue) -> EtlResult<DataFrame> {
	df.clone()
		.lazy()
		.with_column(
			col(col_name)
				.fill_null(null_val.into_expr())
				.alias(col_name),
		)
		.collect()
		.map_err(Into::into)
}
