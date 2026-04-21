//! Apply [`ResamplingPlan`]s to a processed subset DataFrame, producing
//! an interval-aggregated DataFrame plus per-cell diagnostics.
//!
//! The imperative companion to [`super::planner`]: the planner decides
//! what each measurement's path is (purely from configuration); this
//! module executes it against real Polars data.
//!
//! # What gets tracked per (subject, bucket, measurement)
//!
//! - **value**     — the aggregated value (from each plan's aggregation fn)
//! - **N**         — count of non-null cells in the bucket (contributors)
//! - **null_count** — cells in the bucket whose value was null
//! - **min / max** — observed extremes inside the bucket
//! - **stderr**    — sample standard error of the mean: `std(ddof=1) / sqrt(N)`
//!
//! # Honesty caveat
//!
//! The aggregate consumes whatever the upstream subset_df already holds.
//! If that DataFrame had upsample forward-fill applied (because the
//! AlignmentSpec action was `Upsample`), the N count will include the
//! forward-filled cells — it doesn't yet distinguish observed from
//! upsampled. Each plan's `path` is recorded alongside its stats so
//! callers can see whether N is inflated by upsampling.

use std::collections::HashMap;

use polars::prelude::*;
use serde::{Deserialize, Serialize};

use super::{
	IntervalBucket,
	planner::{ResamplingPath, ResamplingPlan},
};
use crate::{
	CanonicalColumnName, aggregation::Aggregate,
	error::{EtlError, EtlResult},
};

// ============================================================================
// Public types
// ============================================================================

/// Per-cell statistics for one `(subject, bucket_start, measurement)`.
///
/// Populated by [`apply_interval`] alongside the aggregated value
/// DataFrame. Surfaced on the subset's diagnostics so the UI and
/// analytics can display the basis for each aggregate value (N,
/// stderr, min, max) and judge its fairness.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntervalStats {
	pub subject: String,
	/// Bucket start as epoch milliseconds (UTC).
	pub bucket_start_ms: i64,
	pub measurement: CanonicalColumnName,
	pub path: ResamplingPath,
	pub n: usize,
	pub null_count: usize,
	pub value: Option<f64>,
	pub stderr: Option<f64>,
	pub min: Option<f64>,
	pub max: Option<f64>,
}

/// Output of [`apply_interval`].
pub struct IntervalAggregateOutput {
	/// Bucketed DataFrame: `(subject, time, measurement_cols…)`. Same
	/// shape as the input subset_df, just fewer rows. Column order:
	/// `subject_col`, `time_col`, then each plan's measurement in the
	/// order they appear in `plans`.
	pub data: DataFrame,
	/// Flat list of stats keyed by `(subject, bucket, measurement)`.
	pub stats: Vec<IntervalStats>,
}

// ============================================================================
// Truncate spec
// ============================================================================

impl IntervalBucket {
	/// Polars duration string for `col(time).dt().truncate(...)`.
	/// Calendar-aware for `Months`/`Weeks`/`Days`/`Hours`; fixed for
	/// `Fixed` (millisecond-precise, epoch-aligned).
	pub fn truncate_spec(&self) -> String {
		match self {
			Self::Months(n) => format!("{n}mo"),
			Self::Weeks(n) => format!("{n}w"),
			Self::Days(n) => format!("{n}d"),
			Self::Hours(n) => format!("{n}h"),
			Self::Fixed { duration_ms } => format!("{duration_ms}ms"),
		}
	}
}

// ============================================================================
// Entry point
// ============================================================================

/// Aggregate a processed subset into the given interval buckets.
///
/// Time column must be `Datetime` (any time unit). Subject column must
/// be `String`. Every measurement named in `plans` must exist in
/// `subset_df` with a numeric dtype that supports mean/std/min/max.
///
/// Rows with no observations in a bucket are not emitted (no placeholder
/// rows for empty buckets). Callers that need a fully-populated interval
/// grid should post-join against a master interval grid.
pub fn apply_interval(
	subset_df: &DataFrame,
	plans: &[ResamplingPlan],
	bucket: &IntervalBucket,
	subject_col: &str,
	time_col: &str,
) -> EtlResult<IntervalAggregateOutput> {
	if plans.is_empty() {
		return Err(EtlError::Config(
			"apply_interval: plans is empty; nothing to aggregate".into(),
		));
	}

	// Validate columns exist before we start building lazy expressions.
	subset_df.column(subject_col).map_err(|e| {
		EtlError::DataProcessing(format!(
			"apply_interval: subject column '{subject_col}' missing: {e}"
		))
	})?;
	subset_df.column(time_col).map_err(|e| {
		EtlError::DataProcessing(format!(
			"apply_interval: time column '{time_col}' missing: {e}"
		))
	})?;
	for plan in plans {
		let name = plan.measurement.as_str();
		subset_df.column(name).map_err(|e| {
			EtlError::DataProcessing(format!(
				"apply_interval: measurement column '{name}' missing: {e}"
			))
		})?;
	}

	let truncate_spec = bucket.truncate_spec();

	// Build one composite group_by with per-measurement agg + stats
	// expressions. This runs all measurements in a single pass.
	let mut agg_exprs: Vec<Expr> = Vec::with_capacity(plans.len() * 6);
	for plan in plans {
		let name = plan.measurement.as_str();
		agg_exprs.push(
			aggregation_expr(plan.aggregation, name).alias(name),
		);
		agg_exprs.push(col(name).count().alias(n_col(name)));
		agg_exprs.push(
			col(name).null_count().alias(null_count_col(name)),
		);
		// sample std: ddof=1. Returns null when n <= 1, which matches
		// the stderr = null outcome we want for single-observation buckets.
		agg_exprs.push(col(name).std(1).alias(std_col(name)));
		agg_exprs.push(col(name).min().alias(min_col(name)));
		agg_exprs.push(col(name).max().alias(max_col(name)));
	}

	let grouped = subset_df
		.clone()
		.lazy()
		.with_column(
			col(time_col)
				.dt()
				.truncate(lit(truncate_spec.as_str()))
				.alias(time_col),
		)
		.group_by([col(subject_col), col(time_col)])
		.agg(agg_exprs)
		.sort([subject_col, time_col], SortMultipleOptions::default())
		.collect()
		.map_err(|e| {
			EtlError::DataProcessing(format!(
				"apply_interval: aggregation failed: {e}"
			))
		})?;

	// Extract per-cell stats into a flat list, and build the "clean"
	// main DataFrame by dropping the stat sidecar columns.
	let stats = extract_stats(&grouped, plans, subject_col, time_col)?;
	let data = drop_stat_columns(grouped, plans, subject_col, time_col)?;

	Ok(IntervalAggregateOutput { data, stats })
}

// ============================================================================
// Implementation helpers
// ============================================================================

fn aggregation_expr(agg: Aggregate, col_name: &str) -> Expr {
	match agg {
		Aggregate::Mean => col(col_name).mean(),
		Aggregate::Sum => col(col_name).sum(),
		Aggregate::Min => col(col_name).min(),
		Aggregate::Max => col(col_name).max(),
		Aggregate::Any => col(col_name).max(), // 0/1 values: max == OR
		Aggregate::All => col(col_name).min(), // 0/1 values: min == AND
		Aggregate::Count => col(col_name).count().cast(DataType::Float64),
		Aggregate::First => col(col_name).first(),
		Aggregate::Last => col(col_name).last(),
		// MostRecent / LeastRecent / LinearTrend / Auto: fall back to
		// mean for interval aggregation. These are component-dimension
		// aggregations; they're not meaningful over a time bucket of a
		// single-column value. Callers wanting bucket-level
		// most-recent/first should use First.
		Aggregate::MostRecent | Aggregate::LeastRecent | Aggregate::LinearTrend | Aggregate::Auto => {
			col(col_name).mean()
		}
	}
}

fn n_col(name: &str) -> String { format!("__{name}__n") }
fn null_count_col(name: &str) -> String { format!("__{name}__nulls") }
fn std_col(name: &str) -> String { format!("__{name}__std") }
fn min_col(name: &str) -> String { format!("__{name}__min") }
fn max_col(name: &str) -> String { format!("__{name}__max") }

/// Pull the stat sidecar columns out of the grouped frame into a flat
/// list of [`IntervalStats`], one row per (subject, bucket, measurement).
fn extract_stats(
	df: &DataFrame,
	plans: &[ResamplingPlan],
	subject_col: &str,
	time_col: &str,
) -> EtlResult<Vec<IntervalStats>> {
	// Unpack the grouped frame into parallel iterators. Every plan
	// contributes 6 columns: value, n, null, std, min, max.
	let subject = df
		.column(subject_col)
		.map_err(|e| EtlError::DataProcessing(format!("subject column missing: {e}")))?
		.str()
		.map_err(|e| EtlError::DataProcessing(format!("subject column is not String: {e}")))?
		.clone();
	let time_phys = df
		.column(time_col)
		.map_err(|e| EtlError::DataProcessing(format!("time column missing: {e}")))?
		.to_physical_repr()
		.i64()
		.map_err(|e| EtlError::DataProcessing(format!("time column is not i64-backed: {e}")))?
		.clone();

	let rows = df.height();
	let mut stats = Vec::with_capacity(rows * plans.len());

	// Build per-measurement columns up front so we iterate once.
	struct PerMeasurement<'a> {
		measurement: &'a CanonicalColumnName,
		path: ResamplingPath,
		value: Float64Chunked,
		n: IdxCa,
		nulls: IdxCa,
		std: Float64Chunked,
		min: Float64Chunked,
		max: Float64Chunked,
	}

	let mut per_m: Vec<PerMeasurement> = Vec::with_capacity(plans.len());
	for plan in plans {
		let name = plan.measurement.as_str();
		per_m.push(PerMeasurement {
			measurement: &plan.measurement,
			path: plan.path,
			value: cast_f64(df, name)?,
			n: cast_idx(df, &n_col(name))?,
			nulls: cast_idx(df, &null_count_col(name))?,
			std: cast_f64(df, &std_col(name))?,
			min: cast_f64(df, &min_col(name))?,
			max: cast_f64(df, &max_col(name))?,
		});
	}

	for i in 0..rows {
		let Some(subj) = subject.get(i) else { continue };
		let Some(ts) = time_phys.get(i) else { continue };

		for m in &per_m {
			let n = m.n.get(i).unwrap_or(0) as usize;
			let null_count = m.nulls.get(i).unwrap_or(0) as usize;
			let value = m.value.get(i);
			let std = m.std.get(i);
			let min = m.min.get(i);
			let max = m.max.get(i);
			// stderr = std / sqrt(n)
			let stderr = match (std, n) {
				(Some(s), n) if n > 0 => Some(s / (n as f64).sqrt()),
				_ => None,
			};

			stats.push(IntervalStats {
				subject: subj.to_string(),
				bucket_start_ms: ts,
				measurement: m.measurement.clone(),
				path: m.path,
				n,
				null_count,
				value,
				stderr,
				min,
				max,
			});
		}
	}

	Ok(stats)
}

fn drop_stat_columns(
	mut df: DataFrame,
	plans: &[ResamplingPlan],
	_subject_col: &str,
	_time_col: &str,
) -> EtlResult<DataFrame> {
	let mut drop_names: Vec<String> = Vec::with_capacity(plans.len() * 5);
	for plan in plans {
		let name = plan.measurement.as_str();
		drop_names.push(n_col(name));
		drop_names.push(null_count_col(name));
		drop_names.push(std_col(name));
		drop_names.push(min_col(name));
		drop_names.push(max_col(name));
	}
	for name in &drop_names {
		df = df
			.drop(name)
			.map_err(|e| EtlError::DataProcessing(format!("drop column '{name}': {e}")))?;
	}
	Ok(df)
}

fn cast_f64(df: &DataFrame, name: &str) -> EtlResult<Float64Chunked> {
	let series = df
		.column(name)
		.map_err(|e| EtlError::DataProcessing(format!("column '{name}' missing: {e}")))?
		.as_materialized_series();
	series
		.cast(&DataType::Float64)
		.map_err(|e| EtlError::DataProcessing(format!("cast '{name}' to f64: {e}")))?
		.f64()
		.map_err(|e| EtlError::DataProcessing(format!("'{name}' not f64 after cast: {e}")))
		.map(|ca| ca.clone())
}

fn cast_idx(df: &DataFrame, name: &str) -> EtlResult<IdxCa> {
	let series = df
		.column(name)
		.map_err(|e| EtlError::DataProcessing(format!("column '{name}' missing: {e}")))?
		.as_materialized_series();
	series
		.idx()
		.map(|ca| ca.clone())
		.or_else(|_| {
			series
				.cast(&polars::prelude::IDX_DTYPE)
				.map_err(|e| EtlError::DataProcessing(format!("cast '{name}' to IDX: {e}")))?
				.idx()
				.map(|ca| ca.clone())
				.map_err(|e| {
					EtlError::DataProcessing(format!(
						"'{name}' not IDX after cast: {e}"
					))
				})
		})
}

// Satisfy the unused-import lint when tests are compiled out.
#[allow(dead_code)]
fn _hashmap_unused() -> HashMap<String, Aggregate> {
	HashMap::new()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
	use chrono::{TimeZone as _, Utc};

	use super::*;
	use crate::interval::planner::AggregationSource;

	// ------------------------------------------------------------------------
	// Fixture helpers
	// ------------------------------------------------------------------------

	fn ts(hours_from_epoch: i64) -> i64 {
		Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap().timestamp_millis()
			+ hours_from_epoch * 3_600_000
	}

	fn build_df(
		subjects: &[&str],
		timestamps_ms: &[i64],
		columns: &[(&str, &[Option<f64>])],
	) -> DataFrame {
		assert_eq!(subjects.len(), timestamps_ms.len());
		for (_, values) in columns {
			assert_eq!(values.len(), subjects.len());
		}
		let time_ca = Int64Chunked::new("time".into(), timestamps_ms)
			.into_datetime(TimeUnit::Milliseconds, Some(polars::prelude::TimeZone::UTC));
		let mut cols: Vec<Column> = vec![
			Column::new("subject".into(), subjects),
			time_ca.into_column(),
		];
		for (name, values) in columns {
			cols.push(Column::new((*name).into(), *values));
		}
		DataFrame::new(cols).unwrap()
	}

	fn plan(name: &str, agg: Aggregate, path: ResamplingPath) -> ResamplingPlan {
		ResamplingPlan {
			measurement: CanonicalColumnName::new(name),
			path,
			target_rate_ms: 3_600_000,
			native_rate_ms: Some(60_000),
			aggregation: agg,
			aggregation_source: AggregationSource::Schema,
			reason: String::from("test fixture"),
		}
	}

	// ------------------------------------------------------------------------
	// Single measurement, clean aggregate
	// ------------------------------------------------------------------------

	#[test]
	fn aggregates_single_measurement_into_monthly_buckets() {
		// 4 observations in January, 3 in February. Mean.
		let df = build_df(
			&["A"; 7],
			&[
				ts(0), ts(24),  ts(48),  ts(72),     // Jan: 1.0, 2.0, 3.0, 4.0
				ts(24 * 32), ts(24 * 33), ts(24 * 34), // Feb: 10.0, 20.0, 30.0
			],
			&[("sump", &[
				Some(1.0), Some(2.0), Some(3.0), Some(4.0),
				Some(10.0), Some(20.0), Some(30.0),
			])],
		);

		let plans = vec![plan("sump", Aggregate::Mean, ResamplingPath::Aggregate)];
		let out = apply_interval(
			&df, &plans, &IntervalBucket::Months(1), "subject", "time",
		)
		.unwrap();

		assert_eq!(out.data.height(), 2, "2 months → 2 rows");

		// Stats: one row per (subject, bucket, measurement) = 2 rows.
		assert_eq!(out.stats.len(), 2);

		// January: mean of 1..4 = 2.5, N=4, min=1, max=4
		let jan = &out.stats[0];
		assert_eq!(jan.subject, "A");
		assert_eq!(jan.n, 4);
		assert_eq!(jan.null_count, 0);
		assert_eq!(jan.value, Some(2.5));
		assert_eq!(jan.min, Some(1.0));
		assert_eq!(jan.max, Some(4.0));
		assert!(jan.stderr.is_some());

		// February: mean of 10, 20, 30 = 20.0, N=3, min=10, max=30
		let feb = &out.stats[1];
		assert_eq!(feb.n, 3);
		assert_eq!(feb.value, Some(20.0));
		assert_eq!(feb.min, Some(10.0));
		assert_eq!(feb.max, Some(30.0));
	}

	// ------------------------------------------------------------------------
	// Stderr correctness
	// ------------------------------------------------------------------------

	#[test]
	fn stderr_equals_sample_std_over_sqrt_n() {
		// Values [1, 2, 3, 4]: mean=2.5, sample std=sqrt(1.6667)≈1.2910
		// stderr = 1.2910 / sqrt(4) ≈ 0.6455
		let df = build_df(
			&["A"; 4],
			&[ts(0), ts(1), ts(2), ts(3)],
			&[("x", &[Some(1.0), Some(2.0), Some(3.0), Some(4.0)])],
		);
		let plans = vec![plan("x", Aggregate::Mean, ResamplingPath::Aggregate)];

		let out = apply_interval(
			&df, &plans, &IntervalBucket::Months(1), "subject", "time",
		)
		.unwrap();

		assert_eq!(out.stats.len(), 1);
		let s = &out.stats[0];
		assert_eq!(s.n, 4);
		assert_eq!(s.value, Some(2.5));

		// Sum of squared deviations from mean 2.5 = 1.5² + 0.5² + 0.5² + 1.5² = 5
		// Sample variance (ddof=1) = 5 / (4-1) = 5/3 ≈ 1.6667
		// Sample std = sqrt(5/3) ≈ 1.2910
		// stderr = 1.2910 / sqrt(4) ≈ 0.6455
		let expected_std = (5.0_f64 / 3.0).sqrt();
		let expected_stderr = expected_std / 4.0_f64.sqrt();
		let actual = s.stderr.expect("stderr should be computed");
		assert!(
			(actual - expected_stderr).abs() < 1e-6,
			"stderr mismatch: expected {expected_stderr}, got {actual}",
		);
	}

	#[test]
	fn stderr_is_none_when_n_is_one() {
		// Single observation in bucket: sample std is null → stderr null.
		let df = build_df(&["A"; 1], &[ts(0)], &[("x", &[Some(5.0)])]);
		let plans = vec![plan("x", Aggregate::Mean, ResamplingPath::Aggregate)];

		let out = apply_interval(
			&df, &plans, &IntervalBucket::Months(1), "subject", "time",
		)
		.unwrap();

		assert_eq!(out.stats.len(), 1);
		assert_eq!(out.stats[0].n, 1);
		assert_eq!(out.stats[0].stderr, None, "stderr undefined for N=1");
	}

	// ------------------------------------------------------------------------
	// Null handling
	// ------------------------------------------------------------------------

	#[test]
	fn nulls_do_not_contribute_to_n_but_are_counted_in_null_count() {
		// 4 rows in one month: 2 values, 2 nulls. Mean should be of the
		// 2 observed values only; N=2; null_count=2.
		let df = build_df(
			&["A"; 4],
			&[ts(0), ts(1), ts(2), ts(3)],
			&[("x", &[Some(10.0), None, Some(20.0), None])],
		);
		let plans = vec![plan("x", Aggregate::Mean, ResamplingPath::Aggregate)];

		let out = apply_interval(
			&df, &plans, &IntervalBucket::Months(1), "subject", "time",
		)
		.unwrap();

		assert_eq!(out.stats.len(), 1);
		let s = &out.stats[0];
		assert_eq!(s.n, 2, "N counts only non-null");
		assert_eq!(s.null_count, 2, "null_count counts only null");
		assert_eq!(s.value, Some(15.0), "mean of 10 and 20");
	}

	#[test]
	fn all_null_bucket_produces_null_value_and_zero_n() {
		let df = build_df(&["A"; 2], &[ts(0), ts(1)], &[("x", &[None, None])]);
		let plans = vec![plan("x", Aggregate::Mean, ResamplingPath::Aggregate)];

		let out = apply_interval(
			&df, &plans, &IntervalBucket::Months(1), "subject", "time",
		)
		.unwrap();

		assert_eq!(out.stats.len(), 1);
		let s = &out.stats[0];
		assert_eq!(s.n, 0);
		assert_eq!(s.null_count, 2);
		assert_eq!(s.value, None);
		assert_eq!(s.min, None);
		assert_eq!(s.max, None);
		assert_eq!(s.stderr, None);
	}

	// ------------------------------------------------------------------------
	// Multiple measurements with different aggregations
	// ------------------------------------------------------------------------

	#[test]
	fn multiple_measurements_respect_per_plan_aggregation() {
		// sump uses Mean, engines_on_count uses Sum, within the same month.
		let df = build_df(
			&["A"; 3],
			&[ts(0), ts(24), ts(48)],
			&[
				("sump", &[Some(2.0), Some(4.0), Some(6.0)]),
				("engines_on_count", &[Some(1.0), Some(1.0), Some(0.0)]),
			],
		);
		let plans = vec![
			plan("sump", Aggregate::Mean, ResamplingPath::Aggregate),
			plan("engines_on_count", Aggregate::Sum, ResamplingPath::Aggregate),
		];

		let out = apply_interval(
			&df, &plans, &IntervalBucket::Months(1), "subject", "time",
		)
		.unwrap();

		assert_eq!(out.data.height(), 1);
		assert_eq!(out.stats.len(), 2);

		let sump = out.stats.iter().find(|s| s.measurement.as_str() == "sump").unwrap();
		assert_eq!(sump.value, Some(4.0)); // (2+4+6)/3
		assert_eq!(sump.n, 3);

		let engines = out.stats
			.iter()
			.find(|s| s.measurement.as_str() == "engines_on_count")
			.unwrap();
		assert_eq!(engines.value, Some(2.0)); // 1+1+0
		assert_eq!(engines.n, 3);
	}

	// ------------------------------------------------------------------------
	// Multi-subject separation
	// ------------------------------------------------------------------------

	#[test]
	fn subjects_are_aggregated_independently() {
		let df = build_df(
			&["A", "A", "B", "B"],
			&[ts(0), ts(24), ts(0), ts(24)],
			&[("x", &[Some(1.0), Some(3.0), Some(10.0), Some(30.0)])],
		);
		let plans = vec![plan("x", Aggregate::Mean, ResamplingPath::Aggregate)];

		let out = apply_interval(
			&df, &plans, &IntervalBucket::Months(1), "subject", "time",
		)
		.unwrap();

		assert_eq!(out.data.height(), 2, "2 subjects × 1 month = 2 rows");
		assert_eq!(out.stats.len(), 2);

		let a = out.stats.iter().find(|s| s.subject == "A").unwrap();
		let b = out.stats.iter().find(|s| s.subject == "B").unwrap();
		assert_eq!(a.value, Some(2.0));
		assert_eq!(b.value, Some(20.0));
	}

	// ------------------------------------------------------------------------
	// Main DataFrame shape
	// ------------------------------------------------------------------------

	#[test]
	fn main_dataframe_has_only_subject_time_and_measurement_columns() {
		let df = build_df(
			&["A"; 2],
			&[ts(0), ts(1)],
			&[
				("sump", &[Some(2.0), Some(4.0)]),
				("engines_on_count", &[Some(1.0), Some(0.0)]),
			],
		);
		let plans = vec![
			plan("sump", Aggregate::Mean, ResamplingPath::Aggregate),
			plan("engines_on_count", Aggregate::Sum, ResamplingPath::Aggregate),
		];

		let out = apply_interval(
			&df, &plans, &IntervalBucket::Months(1), "subject", "time",
		)
		.unwrap();

		let names: Vec<&str> = out.data.get_column_names_str().into_iter().collect();
		assert!(names.contains(&"subject"));
		assert!(names.contains(&"time"));
		assert!(names.contains(&"sump"));
		assert!(names.contains(&"engines_on_count"));
		// Sidecar stat columns should NOT appear.
		for stat_col in ["__sump__n", "__sump__std", "__sump__min", "__sump__max", "__sump__nulls"] {
			assert!(
				!names.contains(&stat_col),
				"main DataFrame should not contain stat column '{stat_col}'",
			);
		}
	}

	// ------------------------------------------------------------------------
	// Path preserved in stats
	// ------------------------------------------------------------------------

	#[test]
	fn plan_path_survives_into_stats() {
		let df = build_df(&["A"; 2], &[ts(0), ts(1)], &[("x", &[Some(1.0), Some(2.0)])]);
		let plans = vec![plan("x", Aggregate::Mean, ResamplingPath::Upsample)];

		let out = apply_interval(
			&df, &plans, &IntervalBucket::Months(1), "subject", "time",
		)
		.unwrap();

		assert_eq!(out.stats[0].path, ResamplingPath::Upsample);
	}

	// ------------------------------------------------------------------------
	// Input validation
	// ------------------------------------------------------------------------

	#[test]
	fn errors_when_plans_is_empty() {
		let df = build_df(&["A"], &[ts(0)], &[("x", &[Some(1.0)])]);
		let err = apply_interval(
			&df, &[], &IntervalBucket::Months(1), "subject", "time",
		);
		assert!(err.is_err());
	}

	#[test]
	fn errors_when_measurement_column_missing() {
		let df = build_df(&["A"], &[ts(0)], &[("x", &[Some(1.0)])]);
		let plans = vec![plan("nope", Aggregate::Mean, ResamplingPath::Aggregate)];
		let err = apply_interval(
			&df, &plans, &IntervalBucket::Months(1), "subject", "time",
		);
		assert!(err.is_err());
	}
}
