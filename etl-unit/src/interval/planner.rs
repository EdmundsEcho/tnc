//! Per-measurement resampling decision for interval reports.
//!
//! The [`ResamplingPlanner`] is a pure function wrapped in a struct: it
//! takes one measurement's configuration and the request's
//! [`ReportInterval`], and returns a [`ResamplingPlan`] describing what
//! the imperative pipeline should do. No polars work, no side effects —
//! the decision is independently testable.
//!
//! # Decision rule (per measurement, given a report interval)
//!
//! Let `native = unit.sample_rate_ms` and `target = interval.approximate_ms`.
//!
//! - `native == target`       → [`ResamplingPath::Passthrough`]
//! - `native <  target`       → [`ResamplingPath::Aggregate`]  (always — only honest option)
//! - `native >  target`       → depends on [`RateStrategy`]:
//!     * `Auto` → [`ResamplingPath::Upsample`] when the measurement's
//!                schema declared an `upsample_strategy` (the author
//!                opted into upsampling for this measurement); otherwise
//!                [`ResamplingPath::Sparse`]. This is the honest default:
//!                follow schema intent, never fabricate values the
//!                author didn't sanction.
//!     * `AggregateOrSparse` → [`ResamplingPath::Sparse`] always; an
//!                             explicit override of `Auto` for callers
//!                             who want to ignore the schema's upsample
//!                             declaration.
//!     * `Upsample` → [`ResamplingPath::Upsample`] when
//!                    `upsample_strategy` is declared; falls back to
//!                    [`ResamplingPath::Sparse`] otherwise (can't force
//!                    what's not configured).
//!     * `Native`   → [`ResamplingPath::Sparse`] always.
//!
//! The aggregation function used by [`ResamplingPath::Aggregate`] comes
//! from the request override when present (keyed by measurement name),
//! otherwise from the measurement's schema-configured
//! `signal_aggregation`.

use serde::{Deserialize, Serialize};

use super::{RateStrategy, ReportInterval};
use crate::{CanonicalColumnName, aggregation::Aggregate, unit::MeasurementUnit};

// ============================================================================
// Plan output types
// ============================================================================

/// How a single measurement's data maps onto the interval grid. Derived
/// purely from the measurement config and the request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResamplingPlan {
	pub measurement: CanonicalColumnName,
	pub path: ResamplingPath,
	/// The bucket's approximate length in ms. Purely informative — the
	/// actual boundaries come from [`super::IntervalBucket`]'s
	/// calendar-aware truncation at apply time.
	pub target_rate_ms: i64,
	/// The measurement's native sample rate in ms (copied for
	/// diagnostics — `Option` because it's theoretically possible for a
	/// measurement to arrive without one, even though schema validation
	/// rejects that today).
	pub native_rate_ms: Option<i64>,
	pub aggregation: Aggregate,
	pub aggregation_source: AggregationSource,
	/// One-sentence human-readable explanation, surfaced in diagnostics.
	pub reason: String,
}

/// The per-measurement resampling action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResamplingPath {
	/// `native_rate == target`: no resampling. The measurement's signal
	/// policy output lands directly on the report grid.
	Passthrough,
	/// `native_rate < target`: fold many native cells into one report
	/// cell via the chosen aggregation. The only honest option when the
	/// native has more resolution than the report.
	Aggregate,
	/// `native_rate > target`: fill intermediate report cells from the
	/// most recent native cell (forward-fill or interpolate, per the
	/// measurement's `upsample_strategy`). Fabricates cells; use with
	/// care.
	Upsample,
	/// `native_rate > target`: keep the measurement at its native rate.
	/// Cells on the report grid that don't align with a native
	/// observation remain null. Most honest at the cost of visual
	/// density.
	Sparse,
}

/// Where the aggregation function came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregationSource {
	/// From `MeasurementUnit::signal_aggregation()`.
	Schema,
	/// From `ReportInterval::aggregation_override` for this measurement.
	Override,
}

// ============================================================================
// Planner
// ============================================================================

/// Pure decision machinery for resampling one measurement onto a report
/// interval. All inputs the decision depends on live here; `plan()`
/// returns a [`ResamplingPlan`] without touching a DataFrame.
pub struct ResamplingPlanner<'m, 'i> {
	unit: &'m MeasurementUnit,
	interval: &'i ReportInterval,
}

impl<'m, 'i> ResamplingPlanner<'m, 'i> {
	pub fn new(unit: &'m MeasurementUnit, interval: &'i ReportInterval) -> Self {
		Self { unit, interval }
	}

	pub fn plan(&self) -> ResamplingPlan {
		let native_rate_ms = self.unit.sample_rate_ms;
		let target_rate_ms = self.interval.bucket.approximate_ms();

		let (aggregation, aggregation_source) = self.choose_aggregation();
		let (path, reason) = self.choose_path(native_rate_ms, target_rate_ms);

		ResamplingPlan {
			measurement: self.unit.name.clone(),
			path,
			target_rate_ms,
			native_rate_ms,
			aggregation,
			aggregation_source,
			reason,
		}
	}

	fn choose_aggregation(&self) -> (Aggregate, AggregationSource) {
		if let Some(ref overrides) = self.interval.aggregation_override
			&& let Some(agg) = overrides.get(&self.unit.name)
		{
			return (*agg, AggregationSource::Override);
		}
		(self.unit.signal_aggregation(), AggregationSource::Schema)
	}

	fn choose_path(
		&self,
		native_rate_ms: Option<i64>,
		target_rate_ms: i64,
	) -> (ResamplingPath, String) {
		let Some(native) = native_rate_ms else {
			return (
				ResamplingPath::Passthrough,
				"no native sample rate configured — passthrough".into(),
			);
		};

		if native == target_rate_ms {
			return (
				ResamplingPath::Passthrough,
				format!("native rate {native}ms matches interval"),
			);
		}

		if native < target_rate_ms {
			return (
				ResamplingPath::Aggregate,
				format!(
					"native {native}ms finer than interval {target_rate_ms}ms — aggregate \
					 (no upsample/downsample choice needed)"
				),
			);
		}

		// native > target: the middle case where strategy and schema
		// config combine.
		let has_upsample_strategy = self.unit.upsample_strategy.is_some();
		match self.interval.strategy {
			RateStrategy::Auto => {
				if has_upsample_strategy {
					(
						ResamplingPath::Upsample,
						format!(
							"native {native}ms coarser than interval {target_rate_ms}ms; \
							 schema declares upsample_strategy — honoring author intent"
						),
					)
				} else {
					(
						ResamplingPath::Sparse,
						format!(
							"native {native}ms coarser than interval {target_rate_ms}ms; \
							 no upsample_strategy declared on measurement — sparse"
						),
					)
				}
			}
			RateStrategy::Upsample => {
				if has_upsample_strategy {
					(
						ResamplingPath::Upsample,
						format!(
							"native {native}ms coarser than interval {target_rate_ms}ms, \
							 strategy=Upsample with declared upsample_strategy — forward-fill"
						),
					)
				} else {
					(
						ResamplingPath::Sparse,
						format!(
							"native {native}ms coarser than interval {target_rate_ms}ms, \
							 strategy=Upsample but measurement has no upsample_strategy — \
							 cannot force what isn't configured; sparse"
						),
					)
				}
			}
			RateStrategy::AggregateOrSparse => (
				ResamplingPath::Sparse,
				format!(
					"native {native}ms coarser than interval {target_rate_ms}ms, \
					 strategy=AggregateOrSparse — sparse (ignores schema's upsample_strategy)"
				),
			),
			RateStrategy::Native => (
				ResamplingPath::Sparse,
				format!(
					"native {native}ms coarser than interval {target_rate_ms}ms, \
					 strategy=Native — sparse on report grid"
				),
			),
		}
	}
}

// ============================================================================
// Unit tests
// ============================================================================

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use super::*;
	use crate::{
		MeasurementKind, ResampleStrategy, interval::IntervalBucket,
		signal_policy::SignalPolicy, unit::MeasurementUnit,
	};

	// ------------------------------------------------------------------------
	// Fixture helpers
	// ------------------------------------------------------------------------

	fn unit_named(
		name: &str,
		kind: MeasurementKind,
		native_rate_ms: i64,
	) -> MeasurementUnit {
		MeasurementUnit::new("subject", "time", name, kind)
			.with_signal_policy(SignalPolicy::instant())
			.with_sample_rate_ms(native_rate_ms)
	}

	fn sump_unit() -> MeasurementUnit {
		unit_named("sump", MeasurementKind::Measure, 60_000) // 60s
	}

	fn precip_unit() -> MeasurementUnit {
		unit_named("historical_precip", MeasurementKind::Measure, 3_600_000) // 1h
	}

	fn monthly_auto() -> ReportInterval {
		ReportInterval {
			bucket: IntervalBucket::Months(1),
			strategy: RateStrategy::Auto,
			aggregation_override: None,
			empty_bucket: super::super::EmptyBucketPolicy::Null,
		}
	}

	fn five_min_auto() -> ReportInterval {
		ReportInterval {
			bucket: IntervalBucket::Fixed { duration_ms: 5 * 60_000 },
			strategy: RateStrategy::Auto,
			aggregation_override: None,
			empty_bucket: super::super::EmptyBucketPolicy::Null,
		}
	}

	fn one_min_auto() -> ReportInterval {
		ReportInterval {
			bucket: IntervalBucket::Fixed { duration_ms: 60_000 },
			strategy: RateStrategy::Auto,
			aggregation_override: None,
			empty_bucket: super::super::EmptyBucketPolicy::Null,
		}
	}

	// ------------------------------------------------------------------------
	// Path: Aggregate (native finer than interval)
	// ------------------------------------------------------------------------

	#[test]
	fn aggregate_when_native_finer_than_interval() {
		// sump is 60s; monthly bucket is much coarser.
		let plan = ResamplingPlanner::new(&sump_unit(), &monthly_auto()).plan();
		assert_eq!(plan.path, ResamplingPath::Aggregate);
		assert_eq!(plan.aggregation, Aggregate::Mean);
		assert_eq!(plan.aggregation_source, AggregationSource::Schema);
	}

	#[test]
	fn aggregate_path_independent_of_strategy_when_native_finer() {
		// Every strategy picks Aggregate when native < interval — there's
		// no other honest option.
		for strategy in [
			RateStrategy::Auto,
			RateStrategy::Upsample,
			RateStrategy::Native,
			RateStrategy::AggregateOrSparse,
		] {
			let interval = ReportInterval {
				bucket: IntervalBucket::Months(1),
				strategy,
				aggregation_override: None,
				empty_bucket: super::super::EmptyBucketPolicy::Null,
			};
			let plan = ResamplingPlanner::new(&sump_unit(), &interval).plan();
			assert_eq!(
				plan.path,
				ResamplingPath::Aggregate,
				"strategy {strategy:?} must choose Aggregate when native < interval",
			);
		}
	}

	// ------------------------------------------------------------------------
	// Path: Passthrough (native == interval)
	// ------------------------------------------------------------------------

	#[test]
	fn passthrough_when_native_matches_interval() {
		// sump is 60s; request interval is also 60s.
		let plan = ResamplingPlanner::new(&sump_unit(), &one_min_auto()).plan();
		assert_eq!(plan.path, ResamplingPath::Passthrough);
	}

	// ------------------------------------------------------------------------
	// Path: Sparse (native coarser than interval, Auto default)
	// ------------------------------------------------------------------------

	#[test]
	fn auto_sparse_when_native_coarser_and_no_upsample_declared() {
		// precip is 1h with NO upsample_strategy declared. 5-minute
		// interval. Auto honors schema intent: no upsample_strategy →
		// Sparse (honest; doesn't fabricate).
		let plan = ResamplingPlanner::new(&precip_unit(), &five_min_auto()).plan();
		assert_eq!(plan.path, ResamplingPath::Sparse);
		assert!(plan.reason.contains("no upsample_strategy declared"));
	}

	#[test]
	fn auto_upsamples_when_schema_declares_upsample_strategy() {
		// Same precip (1h) but WITH upsample_strategy = ForwardFill
		// declared in the schema. The author has opted in to
		// upsampling this measurement. Auto honors that — no need for
		// the caller to pass strategy=Upsample explicitly.
		let precip = precip_unit().with_upsample(ResampleStrategy::ForwardFill);
		let plan = ResamplingPlanner::new(&precip, &five_min_auto()).plan();
		assert_eq!(
			plan.path,
			ResamplingPath::Upsample,
			"Auto + schema upsample_strategy → Upsample (honor author intent)",
		);
		assert!(plan.reason.contains("honoring author intent"));
	}

	#[test]
	fn aggregate_or_sparse_ignores_schema_upsample_declaration() {
		// Even with upsample_strategy declared, AggregateOrSparse forces
		// Sparse. It's the explicit "never upsample" override for callers
		// who want to ignore the schema's opt-in.
		let precip = precip_unit().with_upsample(ResampleStrategy::ForwardFill);
		let interval = ReportInterval {
			bucket: IntervalBucket::Fixed { duration_ms: 5 * 60_000 },
			strategy: RateStrategy::AggregateOrSparse,
			aggregation_override: None,
			empty_bucket: super::super::EmptyBucketPolicy::Null,
		};
		let plan = ResamplingPlanner::new(&precip, &interval).plan();
		assert_eq!(plan.path, ResamplingPath::Sparse);
		assert!(plan.reason.contains("ignores schema's upsample_strategy"));
	}

	#[test]
	fn sparse_when_native_strategy_and_native_coarser() {
		let interval = ReportInterval {
			bucket: IntervalBucket::Fixed { duration_ms: 5 * 60_000 },
			strategy: RateStrategy::Native,
			aggregation_override: None,
			empty_bucket: super::super::EmptyBucketPolicy::Null,
		};
		let plan = ResamplingPlanner::new(&precip_unit(), &interval).plan();
		assert_eq!(plan.path, ResamplingPath::Sparse);
	}

	// ------------------------------------------------------------------------
	// Path: Upsample (native coarser, strategy requests it, strategy available)
	// ------------------------------------------------------------------------

	#[test]
	fn upsample_when_strategy_requests_and_measurement_declares_upsample() {
		let precip = precip_unit().with_upsample(ResampleStrategy::ForwardFill);
		let interval = ReportInterval {
			bucket: IntervalBucket::Fixed { duration_ms: 5 * 60_000 },
			strategy: RateStrategy::Upsample,
			aggregation_override: None,
			empty_bucket: super::super::EmptyBucketPolicy::Null,
		};
		let plan = ResamplingPlanner::new(&precip, &interval).plan();
		assert_eq!(plan.path, ResamplingPath::Upsample);
	}

	#[test]
	fn falls_back_to_sparse_when_upsample_requested_but_not_declared() {
		// precip has no upsample_strategy. Request Upsample — we can't.
		// The planner falls back to Sparse rather than silently doing
		// nothing surprising.
		let interval = ReportInterval {
			bucket: IntervalBucket::Fixed { duration_ms: 5 * 60_000 },
			strategy: RateStrategy::Upsample,
			aggregation_override: None,
			empty_bucket: super::super::EmptyBucketPolicy::Null,
		};
		let plan = ResamplingPlanner::new(&precip_unit(), &interval).plan();
		assert_eq!(plan.path, ResamplingPath::Sparse);
		assert!(plan.reason.contains("no upsample_strategy"));
	}

	// ------------------------------------------------------------------------
	// Aggregation source: schema vs override
	// ------------------------------------------------------------------------

	#[test]
	fn aggregation_uses_schema_default_when_no_override() {
		let plan = ResamplingPlanner::new(&sump_unit(), &monthly_auto()).plan();
		assert_eq!(plan.aggregation, Aggregate::Mean);
		assert_eq!(plan.aggregation_source, AggregationSource::Schema);
	}

	#[test]
	fn aggregation_override_wins_over_schema_default() {
		let mut overrides = HashMap::new();
		overrides.insert(CanonicalColumnName::new("sump"), Aggregate::Max);

		let interval = ReportInterval {
			bucket: IntervalBucket::Months(1),
			strategy: RateStrategy::Auto,
			aggregation_override: Some(overrides),
			empty_bucket: super::super::EmptyBucketPolicy::Null,
		};
		let plan = ResamplingPlanner::new(&sump_unit(), &interval).plan();
		assert_eq!(plan.aggregation, Aggregate::Max);
		assert_eq!(plan.aggregation_source, AggregationSource::Override);
	}

	#[test]
	fn aggregation_override_for_different_measurement_is_ignored() {
		// Override keyed on "precip" shouldn't affect sump's plan.
		let mut overrides = HashMap::new();
		overrides.insert(
			CanonicalColumnName::new("historical_precip"),
			Aggregate::Sum,
		);

		let interval = ReportInterval {
			bucket: IntervalBucket::Months(1),
			strategy: RateStrategy::Auto,
			aggregation_override: Some(overrides),
			empty_bucket: super::super::EmptyBucketPolicy::Null,
		};
		let plan = ResamplingPlanner::new(&sump_unit(), &interval).plan();
		assert_eq!(plan.aggregation, Aggregate::Mean);
		assert_eq!(plan.aggregation_source, AggregationSource::Schema);
	}

	// ------------------------------------------------------------------------
	// Plan metadata
	// ------------------------------------------------------------------------

	#[test]
	fn plan_records_measurement_name_and_rates() {
		let plan = ResamplingPlanner::new(&sump_unit(), &monthly_auto()).plan();
		assert_eq!(plan.measurement, CanonicalColumnName::new("sump"));
		assert_eq!(plan.native_rate_ms, Some(60_000));
		assert_eq!(plan.target_rate_ms, IntervalBucket::Months(1).approximate_ms());
		assert!(!plan.reason.is_empty(), "reason must be populated for diagnostics");
	}

	// ------------------------------------------------------------------------
	// IntervalBucket approximate_ms sanity
	// ------------------------------------------------------------------------

	#[test]
	fn interval_bucket_approximate_ms_orders_correctly() {
		// The planner only needs ordering correctness, not exact values.
		let minute = IntervalBucket::Fixed { duration_ms: 60_000 }.approximate_ms();
		let hour = IntervalBucket::Hours(1).approximate_ms();
		let day = IntervalBucket::Days(1).approximate_ms();
		let week = IntervalBucket::Weeks(1).approximate_ms();
		let month = IntervalBucket::Months(1).approximate_ms();

		assert!(minute < hour);
		assert!(hour < day);
		assert!(day < week);
		assert!(week < month);
	}
}
