//! Interval-based reporting: aggregate per-measurement data into coarse
//! time buckets (monthly, weekly, daily, or fixed durations) for summary
//! charts and tables.
//!
//! This is a separate concern from LTTB decimation:
//!
//! - **LTTB** picks representative sample points to fit a chart's pixel
//!   budget. No aggregation — original values are preserved.
//! - **Interval reporting** aggregates every observation in a bucket into
//!   one output value (mean, sum, max, …) using the measurement's
//!   configured `signal_aggregation` (or a per-request override).
//!
//! Each output cell carries its own *N* (count of non-null contributing
//! observations), which varies by measurement because native sample rates
//! differ. A monthly report over mixed-rate measurements produces the
//! same number of rows per measurement (one per bucket) but different N
//! per cell — SCADA at 60s has ~43,200 contributors per month, while
//! historical precipitation at 1h has ~720. N is observed-only: nulls
//! (including those that weren't filled because no `null_value` was
//! configured) never contribute.
//!
//! # Module layout
//!
//! - [`planner`] — pure decision logic: given a measurement + interval,
//!   produce a [`ResamplingPlan`] describing how its data maps to the
//!   report grid. No polars work — fully unit-testable in isolation.
//! - `aggregate` (future) — the imperative side that applies the plan
//!   to a DataFrame and captures per-cell N in diagnostics.
//! - `diag` (future) — `IntervalCellDiag` roll-up surfaced on
//!   `SubsetInfo`.

pub mod aggregate;
pub mod planner;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{CanonicalColumnName, aggregation::Aggregate, unit::NullValue};

pub use aggregate::{IntervalAggregateOutput, IntervalStats, apply_interval};
pub use planner::{
	AggregationSource, ResamplingPath, ResamplingPlan, ResamplingPlanner,
};

// ============================================================================
// ReportInterval — what the request asks for
// ============================================================================

/// Request-side specification of a report interval. When present on an
/// `EtlUnitSubsetRequest`, the subset pipeline aggregates each
/// measurement into buckets described by `bucket`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportInterval {
	pub bucket: IntervalBucket,
	#[serde(default)]
	pub strategy: RateStrategy,
	/// Override each measurement's `signal_aggregation` for this request.
	/// Keyed by the measurement's canonical name. Absent keys fall back
	/// to the schema-configured aggregation.
	#[serde(default)]
	pub aggregation_override: Option<HashMap<CanonicalColumnName, Aggregate>>,
	#[serde(default)]
	pub empty_bucket: EmptyBucketPolicy,
}

/// How time is bucketed for aggregation. Calendar-aware variants
/// (`Months`/`Weeks`/`Days`/`Hours`) use Polars' `dt.truncate` downstream
/// so bucket boundaries land on real calendar edges. `Fixed` produces
/// uniform, epoch-aligned buckets.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IntervalBucket {
	Months(u32),
	Weeks(u32),
	Days(u32),
	Hours(u32),
	Fixed { duration_ms: i64 },
}

impl IntervalBucket {
	/// Approximate duration in milliseconds. Months/weeks use canonical
	/// averages (30-day month, 7-day week) — fine for the *ordering*
	/// comparisons the planner makes against a measurement's native
	/// sample rate. The imperative bucket boundary math uses Polars'
	/// calendar-aware `dt.truncate`, not this value.
	pub fn approximate_ms(&self) -> i64 {
		const MS_PER_SECOND: i64 = 1_000;
		const MS_PER_MINUTE: i64 = 60 * MS_PER_SECOND;
		const MS_PER_HOUR: i64 = 60 * MS_PER_MINUTE;
		const MS_PER_DAY: i64 = 24 * MS_PER_HOUR;
		const MS_PER_WEEK: i64 = 7 * MS_PER_DAY;
		const MS_PER_MONTH_AVG: i64 = 30 * MS_PER_DAY;

		match self {
			Self::Months(n) => i64::from(*n) * MS_PER_MONTH_AVG,
			Self::Weeks(n) => i64::from(*n) * MS_PER_WEEK,
			Self::Days(n) => i64::from(*n) * MS_PER_DAY,
			Self::Hours(n) => i64::from(*n) * MS_PER_HOUR,
			Self::Fixed { duration_ms } => *duration_ms,
		}
	}
}

/// How each measurement should be resampled into the interval grid when
/// the native rate doesn't match. Applied per-measurement by the
/// planner. Strategies differ only in the middle case
/// (`native > interval`); for `native <= interval` every strategy
/// aggregates, and for `native == interval` every strategy passes through.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RateStrategy {
	/// Aggregate where native ≤ interval; sparse where native > interval.
	/// Most honest default — no fabricated values.
	#[default]
	Auto,
	/// Aggregate where possible; upsample (per the measurement's declared
	/// `upsample_strategy`) where native > interval. Falls back to sparse
	/// if no upsample strategy is configured.
	Upsample,
	/// Keep every measurement at its native rate — cells outside the
	/// native cadence on the interval grid are null.
	Native,
	/// Aggregate where possible, sparse otherwise. Identical to `Auto`
	/// today; exists as an explicit override when `Auto`'s default
	/// changes in the future.
	AggregateOrSparse,
}

/// What goes in a bucket that no observation landed in.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EmptyBucketPolicy {
	/// Leave the bucket's value as null.
	#[default]
	Null,
	/// Use each measurement's configured `null_value` (if any).
	FromConfig,
	/// Fill every empty bucket with this explicit value.
	Value(NullValue),
}
