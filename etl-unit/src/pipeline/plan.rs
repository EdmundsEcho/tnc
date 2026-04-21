//! Planning types for the measurement processing pipeline.
//!
//! A `MeasurementPlan` is built from the schema + request before
//! execution begins. It captures WHAT to do — the pipeline phases
//! capture HOW.

use std::sync::Arc;
use std::time::Duration;

use polars::prelude::DataFrame;

use crate::aggregation::Aggregate;
use crate::column::CanonicalColumnName;
use crate::error::{EtlError, EtlResult};
use crate::request::EtlUnitSubsetRequest;
use crate::signal_policy::SignalPolicy;
use crate::source::SignalPolicyMode;
use crate::unit::{MeasurementKind, MeasurementUnit, NullValue};
use crate::universe::{MeasurementData, Universe};

/// How to handle the component dimension.
#[derive(Debug, Clone)]
pub enum ComponentStrategy {
	/// Keep component dimension — one output series per component value.
	/// If `filter` is Some, only include the listed component values.
	/// Default behavior when components are present.
	Series { filter: Option<Vec<String>> },

	/// Collapse component dimension after optional filter — one output
	/// series. Uses the specified aggregation (e.g., Sum for engine count).
	Rollup {
		filter:      Option<Vec<String>>,
		aggregation: Aggregate,
	},

	/// Measurement has no component dimension.
	None,
}

/// How to join this measurement onto the cumulative grid.
#[derive(Debug, Clone)]
pub enum JoinStrategy {
	/// Equi-join on (subject, time). Used when signal policy has
	/// already aligned the data to the grid interval.
	Equi,

	/// Asof join with tolerance. Used when the measurement's native
	/// rate is coarser than the grid and values should be broadcast
	/// forward within the tolerance window.
	Asof { tolerance_ms: i64 },
}

/// A fully resolved plan for processing one measurement through
/// the pipeline. Built by the planner, consumed by the executor.
#[derive(Debug, Clone)]
pub struct MeasurementPlan {
	/// Canonical measurement name (e.g., "engines_on_count").
	pub name: CanonicalColumnName,

	/// The measurement's kind (Measure, Categorical, Binary).
	pub kind: MeasurementKind,

	/// Schema subject column name (e.g., "station_name").
	pub subject_col: String,

	/// Schema time column name (e.g., "timestamp").
	pub time_col: String,

	/// Component columns (e.g., ["engine"]). Empty if no components.
	pub component_cols: Vec<CanonicalColumnName>,

	/// The measurement's signal policy, if any.
	pub signal_policy: Option<SignalPolicy>,

	/// Full MeasurementUnit — needed by `apply_signal_policy()`.
	pub unit: MeasurementUnit,

	/// What to do with the component dimension.
	pub component_strategy: ComponentStrategy,

	/// How to join onto the cumulative grid.
	pub join_strategy: JoinStrategy,

	/// Within-source fill value: cells in the measurement's own grid
	/// that are not covered by any valid observation (after signal
	/// policy + TTL). Applied before any join.
	pub null_value: Option<NullValue>,

	/// Post-join fill value: grid cells the measurement does not
	/// produce at all (e.g., outside its data range, subjects not
	/// covered). Applied after joining onto the cumulative grid.
	pub null_value_extension: Option<NullValue>,

	/// The pre-computed aligned data from `compute_aligned()`.
	/// `Some` when signal policy was applied during the universe build.
	/// The pipeline uses this instead of re-applying signal policy.
	pub aligned_data: Option<Arc<DataFrame>>,

	/// The raw fragment data for this measurement.
	/// Used as fallback when aligned_data is None, and for
	/// the Skip (raw) mode.
	pub raw_data: Arc<DataFrame>,
}

/// Build measurement plans from the universe and a subset request.
///
/// One plan per measurement. Derivations are excluded (they're
/// computed post-join from the schema). Measurements already handled
/// by the wide-join pass can be excluded via `exclude`.
pub fn build_measurement_plans(
	universe: &Universe,
	request: &EtlUnitSubsetRequest,
	mode: SignalPolicyMode,
	interval: Duration,
	exclude: &std::collections::HashSet<CanonicalColumnName>,
) -> EtlResult<Vec<MeasurementPlan>> {
	let schema = &universe.schema;
	let subject_col = schema.subject.as_str().to_string();
	let time_col = schema.time.as_str().to_string();

	// Resolve which measurements to include
	let measurement_names: Vec<&CanonicalColumnName> = if request.measurements.is_empty() {
		universe.measurements.keys().collect()
	} else {
		request.measurements.iter().collect()
	};

	let mut plans = Vec::new();

	for name in measurement_names {
		// Skip derivations
		if schema.get_derivation(name).is_some() {
			continue;
		}
		// Skip measurements handled by wide-join
		if exclude.contains(name) {
			continue;
		}

		let md = universe.measurements.get(name).ok_or_else(|| {
			EtlError::UnitNotFound(format!("Measurement '{}' not found", name))
		})?;

		// Determine component strategy.
		// Default: Rollup with the measurement's configured crush_aggregation
		// (this matches the existing behavior). Series mode will be the
		// future default once the subset request supports it.
		let component_strategy = if md.has_components() {
			ComponentStrategy::Rollup {
				filter:      None,
				aggregation: md.unit.signal_aggregation(),
			}
		} else {
			ComponentStrategy::None
		};

		// Determine join strategy.
		// When aligned data is available (signal policy was pre-computed),
		// the data is on a regular grid → equi-join.
		// When raw (skip mode), use asof with TTL tolerance.
		let join_strategy = if mode == SignalPolicyMode::Apply && md.is_aligned() {
			JoinStrategy::Equi
		} else {
			let ttl_ms = md.ttl().as_millis() as i64;
			let interval_ms = interval.as_millis() as i64;
			if ttl_ms > interval_ms {
				JoinStrategy::Asof { tolerance_ms: ttl_ms }
			} else {
				JoinStrategy::Equi
			}
		};

		// Get aligned data as Arc for zero-copy sharing.
		// Only in Apply mode.
		let aligned_data = if mode == SignalPolicyMode::Apply {
			md.aligned().map(|df| Arc::new(df.clone()))
		} else {
			None
		};

		// Raw fragment data. as_dataframe() may materialize a ColumnRef
		// (one-time cost), but subsequent plans for the same source share
		// the underlying Arc via ColumnRef's source pointer.
		let raw_data = Arc::new(
			md.fragment().as_dataframe().map_err(EtlError::Polars)?
		);

		plans.push(MeasurementPlan {
			name: name.clone(),
			kind: md.unit.kind,
			subject_col: subject_col.clone(),
			time_col: time_col.clone(),
			component_cols: md.unit.components.clone(),
			signal_policy: md.unit.signal_policy.clone(),
			unit: md.unit.clone(),
			component_strategy,
			join_strategy,
			null_value: md.unit.null_value.clone(),
			null_value_extension: md.unit.null_value_extension.clone(),
			aligned_data,
			raw_data,
		});
	}

	Ok(plans)
}
