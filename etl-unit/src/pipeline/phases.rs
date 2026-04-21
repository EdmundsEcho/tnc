//! Typestate phases for the measurement processing pipeline.
//!
//! Each phase is a distinct type. Transitions consume `self` and return
//! `(NextPhase, PhaseDiag)`. You cannot call crush on `RawData` or
//! join on `Filtered` — the compiler prevents it.
//!
//! Memory lifecycle:
//! - `RawData`: holds `Arc<DataFrame>` (shared source, zero-copy)
//! - `Filtered`: holds owned DataFrame (filtered subset, small)
//! - `SignalApplied`: holds owned DataFrame (dense grid, largest phase)
//! - `Crushed`/`Expanded`: holds owned DataFrame (post-component processing)
//! - `Joined`: holds the cumulative grid with this measurement added
//! - `Complete`: holds the final cumulative grid with null fill applied

use std::sync::Arc;
use std::time::Instant;

use polars::prelude::*;
use tracing::{debug, info};

use crate::aggregation::Aggregate;
use crate::error::{EtlError, EtlResult};
use crate::unit::NullValue;

use super::plan::{ComponentStrategy, JoinStrategy, MeasurementPlan};

// ============================================================================
// Diagnostics
// ============================================================================

/// A detected mismatch between expected and actual output.
#[derive(Debug, Clone)]
pub struct Drift {
	pub field:    &'static str,
	pub expected: String,
	pub actual:   String,
	pub severity: DriftSeverity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriftSeverity {
	/// Informational — within tolerance but notable.
	Info,
	/// Warning — outside tolerance, data may be wrong.
	Warn,
	/// Error — invariant violated, data IS wrong.
	Error,
}

/// Measured diagnostics for a single pipeline phase.
/// Every field is MEASURED from the actual data, not echoed from config.
/// `drifts` flags any mismatch between actual output and expected output.
#[derive(Debug, Clone)]
pub struct PhaseDiag {
	pub phase:       &'static str,
	pub input_rows:  usize,
	pub output_rows: usize,
	pub elapsed_us:  u64,
	pub null_count:  usize,
	pub notes:       Vec<String>,
	/// Detected mismatches between expected and actual.
	pub drifts:      Vec<Drift>,
}

impl PhaseDiag {
	fn new(phase: &'static str) -> Self {
		Self {
			phase,
			input_rows:  0,
			output_rows: 0,
			elapsed_us:  0,
			null_count:  0,
			notes:       Vec::new(),
			drifts:      Vec::new(),
		}
	}

	/// Check if any drift has Error severity.
	pub fn has_errors(&self) -> bool {
		self.drifts.iter().any(|d| d.severity == DriftSeverity::Error)
	}

	/// Check if any drift has Warn or Error severity.
	pub fn has_warnings(&self) -> bool {
		self.drifts.iter().any(|d| matches!(d.severity, DriftSeverity::Warn | DriftSeverity::Error))
	}
}

/// Measure the actual sample rate of a DataFrame by computing the
/// median time delta between consecutive rows for a single subject.
fn measure_sample_rate_ms(df: &DataFrame, time_col: &str, subject_col: &str) -> Option<f64> {
	if df.height() < 2 {
		return None;
	}

	// Pick the first subject to avoid cross-subject gaps
	let subjects = df.column(subject_col).ok()?;
	let first_val = subjects.get(0).ok()?;
	let first_str = match &first_val {
		AnyValue::String(s) => s.to_string(),
		AnyValue::StringOwned(s) => s.to_string(),
		_ => return None,
	};

	let filtered = df.clone().lazy()
		.filter(col(subject_col).eq(lit(first_str)))
		.sort([time_col], SortMultipleOptions::default())
		.collect().ok()?;

	if filtered.height() < 2 {
		return None;
	}

	// Extract physical i64 values from the datetime column
	let tc = filtered.column(time_col).ok()?;
	let physical = tc.to_physical_repr();
	let i64_ca = physical.i64().ok()?;

	let mut deltas: Vec<i64> = Vec::with_capacity(filtered.height() - 1);
	for i in 1..i64_ca.len() {
		if let (Some(a), Some(b)) = (i64_ca.get(i - 1), i64_ca.get(i)) {
			let delta = b - a;
			if delta > 0 {
				deltas.push(delta);
			}
		}
	}

	if deltas.is_empty() {
		return None;
	}

	deltas.sort_unstable();
	let median = deltas[deltas.len() / 2];
	Some(median as f64)
}

// ============================================================================
// Sealed trait — unifies all phases for generic diagnostic code
// ============================================================================

mod sealed {
	pub trait Sealed {}
	impl Sealed for super::RawData {}
	impl Sealed for super::Filtered {}
	impl Sealed for super::SignalApplied {}
	impl Sealed for super::Crushed {}
	impl Sealed for super::Expanded {}
	impl Sealed for super::Joined {}
	impl Sealed for super::Complete {}
}

/// Common interface across all pipeline phases. Sealed — only the
/// phase types in this module implement it.
pub trait ProcessingPhase: sealed::Sealed {
	fn phase_name(&self) -> &'static str;
	fn row_count(&self) -> usize;
}

// ============================================================================
// Phase types
// ============================================================================

/// Raw measurement data. Holds an `Arc<DataFrame>` — zero-copy
/// reference to the shared source fragment or pre-computed aligned data.
pub struct RawData {
	data:           Arc<DataFrame>,
	measurement:    String,
	use_aligned:    bool,
}

/// Time- and subject-filtered data. Owns a DataFrame scoped to the
/// subset window.
pub struct Filtered {
	data:        DataFrame,
	measurement: String,
}

/// Signal-policy-applied data. Dense grid with every interval boundary
/// represented. Components (if any) are preserved as partition columns.
/// This is typically the largest intermediate DataFrame.
pub struct SignalApplied {
	data:        DataFrame,
	measurement: String,
}

/// Component dimension collapsed via aggregation (Rollup strategy)
/// or passed through for non-component measurements.
pub struct Crushed {
	data:        DataFrame,
	measurement: String,
}

/// Component dimension expanded into per-value series (Series strategy).
/// The DataFrame retains the component column for downstream series
/// extraction.
pub struct Expanded {
	data:        DataFrame,
	measurement: String,
}

/// Measurement joined onto the cumulative grid.
pub struct Joined {
	grid:        DataFrame,
	measurement: String,
}

/// Null-fill applied. Terminal phase.
pub struct Complete {
	grid:        DataFrame,
	diag:        PhaseDiag,
}

// ============================================================================
// ProcessingPhase impl for each type
// ============================================================================

impl ProcessingPhase for RawData {
	fn phase_name(&self) -> &'static str { "raw" }
	fn row_count(&self) -> usize { self.data.height() }
}
impl ProcessingPhase for Filtered {
	fn phase_name(&self) -> &'static str { "filtered" }
	fn row_count(&self) -> usize { self.data.height() }
}
impl ProcessingPhase for SignalApplied {
	fn phase_name(&self) -> &'static str { "signal_applied" }
	fn row_count(&self) -> usize { self.data.height() }
}
impl ProcessingPhase for Crushed {
	fn phase_name(&self) -> &'static str { "crushed" }
	fn row_count(&self) -> usize { self.data.height() }
}
impl ProcessingPhase for Expanded {
	fn phase_name(&self) -> &'static str { "expanded" }
	fn row_count(&self) -> usize { self.data.height() }
}
impl ProcessingPhase for Joined {
	fn phase_name(&self) -> &'static str { "joined" }
	fn row_count(&self) -> usize { self.grid.height() }
}
impl ProcessingPhase for Complete {
	fn phase_name(&self) -> &'static str { "complete" }
	fn row_count(&self) -> usize { self.grid.height() }
}

// ============================================================================
// Phase transitions
// ============================================================================

impl RawData {
	/// Create a new RawData phase from a measurement plan.
	/// Uses aligned data if available (signal policy pre-computed),
	/// otherwise raw fragment data.
	pub fn new(plan: &MeasurementPlan) -> EtlResult<Self> {
		let (data, use_aligned) = if let Some(ref aligned) = plan.aligned_data {
			(Arc::clone(aligned), true)
		} else {
			(Arc::clone(&plan.raw_data), false)
		};
		tracing::info!(
			measurement = plan.name.as_str(),
			use_aligned = use_aligned,
			columns = ?data.get_column_names_str(),
			rows = data.height(),
			"Pipeline RawData: input columns"
		);
		Ok(Self {
			data,
			measurement: plan.name.as_str().to_string(),
			use_aligned,
		})
	}

	/// Filter to the subset's time window and subjects.
	///
	/// Consumes self. The filtered DataFrame is owned (materialized
	/// from the lazy filter on the shared Arc).
	pub fn filter(
		self,
		time_col: &str,
		subject_col: &str,
		time_bounds: (i64, i64),
		subjects: Option<&[String]>,
	) -> EtlResult<(Filtered, PhaseDiag)> {
		let start = Instant::now();
		let input_rows = self.data.height();

		let (start_ms, end_ms) = time_bounds;
		let tz = Some(polars::prelude::TimeZone::UTC);
		let mut lf = (*self.data).clone().lazy()
			.filter(
				col(time_col)
					.gt_eq(lit(start_ms).cast(DataType::Datetime(TimeUnit::Milliseconds, tz.clone())))
					.and(col(time_col)
						.lt_eq(lit(end_ms).cast(DataType::Datetime(TimeUnit::Milliseconds, tz))))
			);

		if let Some(subjects) = subjects {
			let series = Series::new("_sf".into(), subjects);
			lf = lf.filter(col(subject_col).is_in(lit(series).implode(), false));
		}

		let data = lf.collect()?;
		let output_rows = data.height();

		let mut diag = PhaseDiag::new("filter");
		diag.input_rows = input_rows;
		diag.output_rows = output_rows;
		diag.elapsed_us = start.elapsed().as_micros() as u64;
		if self.use_aligned {
			diag.notes.push("source: aligned (signal policy pre-applied)".into());
		} else {
			diag.notes.push("source: raw fragment".into());
		}

		debug!(
			measurement = self.measurement.as_str(),
			input_rows, output_rows,
			used_aligned = self.use_aligned,
			"Pipeline: filter"
		);

		Ok((
			Filtered { data, measurement: self.measurement },
			diag,
		))
	}
}

impl Filtered {
	/// Apply signal policy, producing a dense time grid.
	///
	/// If the plan's `aligned_data` was used (the data is already
	/// signal-policy-applied and filtered), this is a pass-through.
	/// Otherwise, runs `apply_signal_policy()` on the filtered raw data.
	pub fn apply_signal_policy(
		self,
		plan: &MeasurementPlan,
	) -> EtlResult<(SignalApplied, PhaseDiag)> {
		let start = Instant::now();
		let input_rows = self.data.height();

		// If we started from aligned data, signal policy is already
		// applied. The filter phase already scoped it to the subset
		// window. Pass through.
		let (data, was_precomputed) = if plan.aligned_data.is_some() {
			(self.data, true)
		} else if plan.signal_policy.is_some() {
			let (result, _stats) = crate::polars_fns::apply_signal_policy(
				self.data,
				&plan.unit,
				"pipeline",
			)?;
			// Apply null_value_extension for grid nulls
			let filled = if let Some(ref nv) = plan.null_value {
				let fill_expr: Expr = nv.clone().into();
				let value_col = plan.name.as_str();
				result.lazy()
					.with_column(col(value_col).fill_null(fill_expr).alias(value_col))
					.collect()?
			} else {
				result
			};
			(filled, false)
		} else {
			(self.data, false)
		};

		let output_rows = data.height();
		let null_count = data.column(plan.name.as_str())
			.map(|c| c.null_count())
			.unwrap_or(0);

		let mut diag = PhaseDiag::new("signal_policy");
		diag.input_rows = input_rows;
		diag.output_rows = output_rows;
		diag.null_count = null_count;
		diag.elapsed_us = start.elapsed().as_micros() as u64;
		if was_precomputed {
			diag.notes.push("pre-computed (pass-through from aligned cache)".into());
		}

		// Capture the grid's time properties — useful for cross-checking
		// against the master grid's BuildMasterGrid diagnostic.
		if let Ok(tc) = data.column(&plan.time_col) {
			let phys = tc.to_physical_repr();
			let ca = phys.i64().cloned();
			let (tmin, tmax) = ca
				.as_ref()
				.map(|a| (a.min().unwrap_or(0), a.max().unwrap_or(0)))
				.unwrap_or((0, 0));
			let unique_times = tc.n_unique().unwrap_or(0);
			diag.notes.push(format!(
				"output time range (ms) = [{}, {}], unique times = {}",
				tmin, tmax, unique_times,
			));
		}
		if let Some(rate) = plan.unit.sample_rate_ms {
			diag.notes.push(format!("configured sample_rate_ms = {}", rate));
		}
		if let Some(ref sp) = plan.unit.signal_policy {
			diag.notes.push(format!(
				"configured ttl_ms = {}",
				sp.ttl().as_millis(),
			));
		}

		// --- Drift detection: verify actual sample rate matches config ---
		if let Some(expected_ms) = plan.unit.sample_rate_ms {
			if let Some(actual_ms) = measure_sample_rate_ms(
				&data, &plan.time_col, &plan.subject_col,
			) {
				let expected = expected_ms as f64;
				let ratio = actual_ms / expected;
				// Tolerance: within 10% of expected
				if ratio < 0.9 || ratio > 1.1 {
					let drift = Drift {
						field:    "sample_rate_ms",
						expected: format!("{:.0}ms", expected),
						actual:   format!("{:.0}ms", actual_ms),
						severity: if ratio > 2.0 || ratio < 0.5 {
							DriftSeverity::Error
						} else {
							DriftSeverity::Warn
						},
					};
					tracing::warn!(
						measurement = self.measurement.as_str(),
						expected_ms = expected,
						actual_ms = actual_ms,
						ratio = ratio,
						severity = ?drift.severity,
						"DRIFT: signal_policy output sample rate does not match config"
					);
					diag.drifts.push(drift);
				} else {
					diag.notes.push(format!("sample_rate: {:.0}ms (ok)", actual_ms));
				}
			}
		}

		// Drift: output should have MORE rows than input (dense grid fills gaps)
		// unless input was already dense. Flag if output < input.
		if output_rows < input_rows && plan.signal_policy.is_some() && !was_precomputed {
			diag.drifts.push(Drift {
				field:    "output_rows",
				expected: format!(">= {} (input)", input_rows),
				actual:   format!("{}", output_rows),
				severity: DriftSeverity::Warn,
			});
		}

		debug!(
			measurement = self.measurement.as_str(),
			input_rows, output_rows, null_count,
			precomputed = was_precomputed,
			drifts = diag.drifts.len(),
			"Pipeline: signal_policy"
		);

		Ok((
			SignalApplied { data, measurement: self.measurement },
			diag,
		))
	}
}

impl SignalApplied {
	/// Crush: collapse component dimension via aggregation (Rollup).
	/// Optionally filter to specific component values first.
	pub fn crush(
		self,
		aggregation: Aggregate,
		component_filter: Option<&[String]>,
		plan: &MeasurementPlan,
	) -> EtlResult<(Crushed, PhaseDiag)> {
		let start = Instant::now();
		let input_rows = self.data.height();

		let subject_col = plan.subject_col.as_str();
		let time_col = plan.time_col.as_str();
		let value_col = plan.name.as_str();

		// Optional component filter
		let filtered = if let Some(values) = component_filter {
			if let Some(comp_col) = plan.component_cols.first() {
				let series = Series::new("_cf".into(), values);
				self.data.lazy()
					.filter(col(comp_col.as_str()).is_in(lit(series).implode(), false))
					.collect()?
			} else {
				self.data
			}
		} else {
			self.data
		};

		// Crush: group by (subject, time), aggregate value
		let agg_expr = build_agg_expr(value_col, aggregation);
		let data = filtered.lazy()
			.group_by([col(subject_col), col(time_col)])
			.agg([agg_expr])
			.collect()?;

		let output_rows = data.height();

		let mut diag = PhaseDiag::new("crush");
		diag.input_rows = input_rows;
		diag.output_rows = output_rows;
		diag.null_count = data.column(value_col).map(|c| c.null_count()).unwrap_or(0);
		diag.elapsed_us = start.elapsed().as_micros() as u64;
		diag.notes.push(format!("aggregation: {:?}", aggregation));

		// Capture crushed frame's time grid properties — this is what gets
		// joined onto the master grid next. Unique times here should equal
		// the master grid's n_time_points when alignment is correct.
		if let Ok(tc) = data.column(&plan.time_col) {
			let phys = tc.to_physical_repr();
			let ca = phys.i64().cloned();
			let (tmin, tmax) = ca
				.as_ref()
				.map(|a| (a.min().unwrap_or(0), a.max().unwrap_or(0)))
				.unwrap_or((0, 0));
			let unique_times = tc.n_unique().unwrap_or(0);
			diag.notes.push(format!(
				"crushed time range (ms) = [{}, {}], unique times = {}",
				tmin, tmax, unique_times,
			));
		}

		// --- Drift detection: crush should reduce row count ---
		let n_components = plan.component_cols.len().max(1);
		if n_components > 1 && output_rows > input_rows / n_components + 1 {
			// After crushing N component values, output should be
			// roughly input/N. If significantly more, the crush
			// didn't collapse the component dimension properly.
			diag.drifts.push(Drift {
				field:    "crush_ratio",
				expected: format!("~{} (input/{} components)", input_rows / n_components, n_components),
				actual:   format!("{}", output_rows),
				severity: DriftSeverity::Warn,
			});
			tracing::warn!(
				measurement = self.measurement.as_str(),
				input_rows, output_rows,
				expected_approx = input_rows / n_components,
				n_components,
				"DRIFT: crush did not reduce rows as expected"
			);
		}

		// Drift: verify sample rate is preserved through crush
		if let Some(expected_ms) = plan.unit.sample_rate_ms {
			if let Some(actual_ms) = measure_sample_rate_ms(
				&data, &plan.time_col, &plan.subject_col,
			) {
				let expected = expected_ms as f64;
				let ratio = actual_ms / expected;
				if ratio < 0.9 || ratio > 1.1 {
					let drift = Drift {
						field:    "post_crush_sample_rate_ms",
						expected: format!("{:.0}ms", expected),
						actual:   format!("{:.0}ms", actual_ms),
						severity: if ratio > 2.0 || ratio < 0.5 {
							DriftSeverity::Error
						} else {
							DriftSeverity::Warn
						},
					};
					tracing::warn!(
						measurement = self.measurement.as_str(),
						expected_ms = expected,
						actual_ms = actual_ms,
						"DRIFT: post-crush sample rate does not match config"
					);
					diag.drifts.push(drift);
				}
			}
		}

		debug!(
			measurement = self.measurement.as_str(),
			input_rows, output_rows,
			aggregation = ?aggregation,
			drifts = diag.drifts.len(),
			"Pipeline: crush"
		);

		Ok((
			Crushed { data, measurement: self.measurement },
			diag,
		))
	}

	/// Expand: keep component dimension, producing per-value series.
	/// Optionally filter to specific component values.
	pub fn expand(
		self,
		component_filter: Option<&[String]>,
		plan: &MeasurementPlan,
	) -> EtlResult<(Expanded, PhaseDiag)> {
		let start = Instant::now();
		let input_rows = self.data.height();

		let data = if let Some(values) = component_filter {
			if let Some(comp_col) = plan.component_cols.first() {
				let series = Series::new("_cf".into(), values);
				self.data.lazy()
					.filter(col(comp_col.as_str()).is_in(lit(series).implode(), false))
					.collect()?
			} else {
				self.data
			}
		} else {
			self.data
		};

		let output_rows = data.height();
		let n_components = if let Some(comp_col) = plan.component_cols.first() {
			data.column(comp_col.as_str())
				.map(|c| c.n_unique().unwrap_or(0))
				.unwrap_or(0)
		} else {
			0
		};

		let mut diag = PhaseDiag::new("expand");
		diag.input_rows = input_rows;
		diag.output_rows = output_rows;
		diag.elapsed_us = start.elapsed().as_micros() as u64;
		diag.notes.push(format!("component_values: {}", n_components));

		debug!(
			measurement = self.measurement.as_str(),
			input_rows, output_rows, n_components,
			"Pipeline: expand"
		);

		Ok((
			Expanded { data, measurement: self.measurement },
			diag,
		))
	}

	/// No-component pass-through. Measurement has no component dimension.
	pub fn skip_components(self) -> EtlResult<(Crushed, PhaseDiag)> {
		let rows = self.data.height();
		let mut diag = PhaseDiag::new("skip_components");
		diag.input_rows = rows;
		diag.output_rows = rows;
		Ok((
			Crushed { data: self.data, measurement: self.measurement },
			diag,
		))
	}
}

// ============================================================================
// Join phase — shared between Crushed and Expanded
// ============================================================================

/// Trait for types that can be joined onto the cumulative grid.
/// Implemented by both `Crushed` and `Expanded`.
pub trait Joinable: sealed::Sealed {
	fn into_join_data(self) -> (DataFrame, String);
}

impl Joinable for Crushed {
	fn into_join_data(self) -> (DataFrame, String) {
		(self.data, self.measurement)
	}
}

impl Joinable for Expanded {
	fn into_join_data(self) -> (DataFrame, String) {
		(self.data, self.measurement)
	}
}

impl Crushed {
	/// Join this measurement onto the cumulative grid.
	pub fn join_onto(
		self,
		grid: DataFrame,
		plan: &MeasurementPlan,
	) -> EtlResult<(Joined, PhaseDiag)> {
		join_impl(self.data, self.measurement, grid, plan)
	}

	/// Accessor for diagnostics before consuming.
	pub fn diagnostics(&self) -> Vec<PhaseDiag> { Vec::new() }
}

impl Expanded {
	/// Join this measurement onto the cumulative grid.
	/// For expanded (per-component) data, the component column is
	/// carried through the join. The caller handles splitting into
	/// separate series post-join.
	pub fn join_onto(
		self,
		grid: DataFrame,
		plan: &MeasurementPlan,
	) -> EtlResult<(Joined, PhaseDiag)> {
		join_impl(self.data, self.measurement, grid, plan)
	}

	pub fn diagnostics(&self) -> Vec<PhaseDiag> { Vec::new() }
}

fn join_impl(
	right: DataFrame,
	measurement: String,
	left: DataFrame,
	plan: &MeasurementPlan,
) -> EtlResult<(Joined, PhaseDiag)> {
	let start = Instant::now();
	let input_left = left.height();
	let input_right = right.height();

	let subject_col = plan.subject_col.as_str();
	let time_col = plan.time_col.as_str();
	let value_col = plan.name.as_str();

	// Clone the (subject, time) key columns of each side up-front so we
	// can emit coverage diagnostics after the join consumes the frames.
	// Small, cheap: two-column projection, typically << value data.
	let left_keys_for_diag = left.select([time_col]).ok();
	let right_keys_for_diag = right.select([time_col]).ok();
	let right_time_bounds_for_diag = right.column(time_col).ok().map(|c| {
		let phys = c.to_physical_repr();
		let ca = phys.i64().cloned();
		ca.as_ref()
			.map(|a| (a.min().unwrap_or(0), a.max().unwrap_or(0)))
			.unwrap_or((0, 0))
	}).unwrap_or((0, 0));
	let left_time_n_unique = left.column(time_col).ok().and_then(|c| c.n_unique().ok()).unwrap_or(0);
	let right_time_n_unique = right.column(time_col).ok().and_then(|c| c.n_unique().ok()).unwrap_or(0);

	// Get columns from right that aren't join keys
	let right_value_cols: Vec<String> = right.get_column_names_str()
		.into_iter()
		.filter(|n| *n != subject_col && *n != time_col)
		.map(|s| s.to_string())
		.collect();

	let grid = match &plan.join_strategy {
		JoinStrategy::Equi => {
			// Standard equi-join on (subject, time)
			let left_on = [subject_col, time_col];
			let right_on = [subject_col, time_col];

			// Select only the value columns from right to avoid duplicate
			// subject/time columns
			let mut right_select = vec![
				col(subject_col),
				col(time_col),
			];
			right_select.extend(right_value_cols.iter().map(|c| col(c.as_str())));

			let right_selected = right.lazy().select(right_select).collect()?;

			left.join(
				&right_selected,
				left_on,
				right_on,
				JoinArgs::new(JoinType::Left),
				None,
			)?
		}
		JoinStrategy::Asof { tolerance_ms } => {
			use polars::prelude::AsofStrategy;
			let left_sorted = left.sort([subject_col, time_col], SortMultipleOptions::default())?;
			let right_sorted = right.sort([subject_col, time_col], SortMultipleOptions::default())?;
			let tolerance = Some(AnyValue::Duration(*tolerance_ms, TimeUnit::Milliseconds));

			left_sorted.join_asof_by(
				&right_sorted,
				time_col,
				time_col,
				[subject_col],
				[subject_col],
				AsofStrategy::Backward,
				tolerance,
				true,
				false,
			)?
		}
	};

	let output_rows = grid.height();
	let null_count = grid.column(value_col)
		.map(|c| c.null_count())
		.unwrap_or(0);
	let matched_rows = output_rows.saturating_sub(null_count);

	let mut diag = PhaseDiag::new("join");
	diag.input_rows = input_left;
	diag.output_rows = output_rows;
	diag.null_count = null_count;
	diag.elapsed_us = start.elapsed().as_micros() as u64;
	diag.notes.push(format!(
		"strategy = {:?}",
		plan.join_strategy
	));
	diag.notes.push(format!(
		"left (grid) rows = {}, right (measurement) rows = {}",
		input_left, input_right,
	));
	diag.notes.push(format!(
		"matched = {}, unmatched (null value_col) = {} ({}%)",
		matched_rows,
		null_count,
		if output_rows > 0 {
			100 * null_count / output_rows
		} else { 0 },
	));

	// Coverage diagnostic: compare the right side's time-key set to the
	// left (master) grid's time-key set. Right-only times mean the
	// measurement has rows that can't join to the grid; grid-only times
	// mean the grid has rows the measurement couldn't fill. Either one
	// is a grid-alignment smoking gun.
	diag.notes.push(format!(
		"unique times — left (grid) = {}, right (measurement) = {}",
		left_time_n_unique, right_time_n_unique,
	));
	diag.notes.push(format!(
		"right time range (ms) = [{}, {}]",
		right_time_bounds_for_diag.0, right_time_bounds_for_diag.1,
	));

	if let (Some(left_times), Some(right_times)) = (left_keys_for_diag, right_keys_for_diag) {
		let lf_left = left_times.unique_stable(None, polars::frame::UniqueKeepStrategy::First, None)
			.map(|df| df.lazy());
		let lf_right = right_times.unique_stable(None, polars::frame::UniqueKeepStrategy::First, None)
			.map(|df| df.lazy());
		if let (Ok(lf_l), Ok(lf_r)) = (lf_left, lf_right) {
			if let Ok(orphan) = lf_r.clone().join(
				lf_l.clone(), [col(time_col)], [col(time_col)],
				JoinArgs::new(JoinType::Anti),
			).collect() {
				diag.notes.push(format!(
					"right-only times (no match in master grid) = {}",
					orphan.height(),
				));
			}
			if let Ok(missing) = lf_l.join(
				lf_r, [col(time_col)], [col(time_col)],
				JoinArgs::new(JoinType::Anti),
			).collect() {
				diag.notes.push(format!(
					"grid-only times (measurement absent) = {}",
					missing.height(),
				));
			}
		}
	}

	debug!(
		measurement = measurement.as_str(),
		left_rows = input_left,
		right_rows = input_right,
		output_rows,
		null_count,
		matched_rows,
		strategy = ?plan.join_strategy,
		"Pipeline: join"
	);

	Ok((
		Joined { grid, measurement },
		diag,
	))
}

impl Joined {
	/// Apply null_value_extension if configured.
	pub fn fill_nulls(self, plan: &MeasurementPlan) -> EtlResult<(Complete, PhaseDiag)> {
		let start = Instant::now();
		let value_col = plan.name.as_str();
		let input_nulls = self.grid.column(value_col)
			.map(|c| c.null_count())
			.unwrap_or(0);

		let grid = if let Some(ref nve) = plan.null_value_extension {
			apply_null_fill(&self.grid, value_col, nve)?
		} else {
			self.grid
		};

		let output_nulls = grid.column(value_col)
			.map(|c| c.null_count())
			.unwrap_or(0);

		let mut diag = PhaseDiag::new("fill_nulls");
		diag.input_rows = grid.height();
		diag.output_rows = grid.height();
		diag.null_count = output_nulls;
		diag.elapsed_us = start.elapsed().as_micros() as u64;
		if input_nulls > 0 {
			diag.notes.push(format!("filled: {} → {} nulls", input_nulls, output_nulls));
		}

		Ok((Complete { grid, diag: diag.clone() }, diag))
	}
}

impl Complete {
	pub fn diag(&self) -> &PhaseDiag {
		&self.diag
	}

	pub fn into_dataframe(self) -> DataFrame {
		self.grid
	}
}

// ============================================================================
// Helpers (moved from universe_of_etlunits.rs)
// ============================================================================

pub(crate) fn build_agg_expr(col_name: &str, agg: Aggregate) -> Expr {
	match agg {
		Aggregate::Mean => col(col_name).mean().alias(col_name),
		Aggregate::Sum => col(col_name).sum().alias(col_name),
		Aggregate::Min => col(col_name).min().alias(col_name),
		Aggregate::Max => col(col_name).max().alias(col_name),
		Aggregate::Any => col(col_name).max().alias(col_name),
		Aggregate::All => col(col_name).min().alias(col_name),
		Aggregate::Count => col(col_name).count().alias(col_name),
		Aggregate::First => col(col_name).first().alias(col_name),
		Aggregate::Last => col(col_name).last().alias(col_name),
		_ => col(col_name).mean().alias(col_name),
	}
}

pub(crate) fn apply_null_fill(
	df: &DataFrame,
	col_name: &str,
	null_val: &NullValue,
) -> EtlResult<DataFrame> {
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
