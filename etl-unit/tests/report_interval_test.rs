//! End-to-end test for request → subset → interval-bucketed output.
//!
//! Builds a tiny Universe, runs a subset request carrying a
//! `ReportInterval`, and asserts on the bucketed DataFrame and the
//! per-cell stats that land on `SubsetInfo::interval_stats`.

use chrono::{TimeZone as _, Utc};
use etl_unit::{
	CanonicalColumnName, EtlSchema, EtlUnitSubsetRequest, MeasurementKind,
	aggregation::Aggregate,
	interval::{IntervalBucket, RateStrategy, ReportInterval},
	request::TimeRange,
	signal_policy::SignalPolicy,
	source::{BoundSource, EtlUniverseBuildPlan},
	universe::{Universe, UniverseBuilder, alignment::AlignmentSpec},
};
use polars::prelude::*;
use std::time::Duration;

const SUBJECT_COL: &str = "station_id";
const TIME_COL: &str = "observation_time";

fn ts(month: u32, day: u32, hour: u32) -> i64 {
	Utc.with_ymd_and_hms(2025, month, day, hour, 0, 0)
		.unwrap()
		.timestamp_millis()
}

/// Two subjects, two measurements (sump at 60s, fuel at 60s), scattered
/// across January and February 2025.
fn build_test_universe() -> Universe {
	// 8 observations: 4 in Jan for subject A, 2 in Jan for subject B,
	// 2 in Feb for subject A.
	let subjects: Vec<&str> = vec!["A", "A", "A", "A", "B", "B", "A", "A"];
	let times: Vec<i64> = vec![
		ts(1, 5, 0),  // A Jan
		ts(1, 10, 0), // A Jan
		ts(1, 15, 0), // A Jan
		ts(1, 20, 0), // A Jan
		ts(1, 5, 0),  // B Jan
		ts(1, 25, 0), // B Jan
		ts(2, 5, 0),  // A Feb
		ts(2, 10, 0), // A Feb
	];
	let sump: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 10.0, 20.0, 100.0, 200.0];
	let fuel: Vec<f64> = vec![90.0, 80.0, 70.0, 60.0, 50.0, 40.0, 30.0, 20.0];

	let time_ca = Int64Chunked::new(TIME_COL.into(), &times)
		.into_datetime(TimeUnit::Milliseconds, Some(polars::prelude::TimeZone::UTC));

	let df = DataFrame::new(vec![
		Column::new(SUBJECT_COL.into(), &subjects),
		time_ca.into_column(),
		Column::new("sump".into(), &sump),
		Column::new("fuel".into(), &fuel),
	])
	.unwrap();

	let schema = EtlSchema::new("interval_test")
		.subject(SUBJECT_COL)
		.time(TIME_COL)
		.measurement("sump", MeasurementKind::Measure)
		.with_policy(SignalPolicy::instant().with_ttl(Duration::from_secs(60)))
		.with_sample_rate(60_000)
		.measurement("fuel", MeasurementKind::Measure)
		.with_policy(SignalPolicy::instant().with_ttl(Duration::from_secs(60)))
		.with_sample_rate(60_000)
		.build()
		.unwrap();

	let plan = EtlUniverseBuildPlan::new(schema.clone())
		.source(BoundSource::identity("default", df, &schema));
	let mut universe = UniverseBuilder::build(&plan).expect("universe should build");

	let names: Vec<CanonicalColumnName> =
		schema.measurements.iter().map(|m| m.name.clone()).collect();
	let spec = AlignmentSpec::compute(&universe.measurements, &names);
	universe.ensure_aligned(spec).expect("alignment");
	universe
}

fn full_window(request: EtlUnitSubsetRequest) -> EtlUnitSubsetRequest {
	let start = Utc.with_ymd_and_hms(2024, 12, 1, 0, 0, 0).unwrap();
	let end = Utc.with_ymd_and_hms(2025, 3, 31, 0, 0, 0).unwrap();
	request.time_range(TimeRange::new(Some(start), Some(end)))
}

fn monthly_report(strategy: RateStrategy) -> ReportInterval {
	ReportInterval {
		bucket: IntervalBucket::Months(1),
		strategy,
		aggregation_override: None,
		empty_bucket: etl_unit::interval::EmptyBucketPolicy::Null,
	}
}

// ============================================================================
// The request carries ReportInterval → output is bucketed
// ============================================================================

#[test]
fn request_with_report_interval_produces_bucketed_output() {
	let universe = build_test_universe();
	let request = full_window(
		EtlUnitSubsetRequest::new()
			.measurements(vec!["sump".into(), "fuel".into()])
			.report_interval(monthly_report(RateStrategy::Auto)),
	);

	let subset = universe.subset(&request).expect("subset ok");

	// Row count reflects every (subject, bucket) pair for which the
	// master grid has any cells — even if all-null for a given subject.
	// The master grid spans Jan 5 → Feb 10 (union of all observations),
	// so subject B (observed only in January) still has Feb cells in the
	// grid — those cells are all null for B, producing a (B, Feb) row
	// with N=0. Total = 4 rows: A/Jan, A/Feb, B/Jan, B/Feb.
	assert_eq!(subset.dataframe().height(), 4);

	// Columns: subject, time, sump, fuel. Sidecar stat columns must not leak.
	let names: Vec<&str> = subset
		.dataframe()
		.get_column_names_str()
		.into_iter()
		.collect();
	assert!(names.contains(&SUBJECT_COL));
	assert!(names.contains(&TIME_COL));
	assert!(names.contains(&"sump"));
	assert!(names.contains(&"fuel"));
	assert!(!names.iter().any(|c| c.starts_with("__")), "no sidecar stat columns");
}

#[test]
fn interval_stats_capture_per_cell_n_min_max_value() {
	let universe = build_test_universe();
	let request = full_window(
		EtlUnitSubsetRequest::new()
			.measurements(vec!["sump".into()])
			.report_interval(monthly_report(RateStrategy::Auto)),
	);

	let subset = universe.subset(&request).expect("subset ok");

	// 4 (subject, bucket) rows × 1 measurement = 4 stats rows.
	// The fourth row is (B, Feb): no observations → N=0, value=None.
	assert_eq!(subset.info.interval_stats.len(), 4);

	// Verify the N=0 row exists and reflects the "no observation" state.
	let b_feb = subset
		.info
		.interval_stats
		.iter()
		.find(|s| s.subject == "B" && s.bucket_start_ms == ts(2, 1, 0))
		.expect("B/Feb stats row should exist");
	assert_eq!(b_feb.n, 0, "no observations in B/Feb");
	assert_eq!(b_feb.value, None, "aggregate over zero contributors is null");

	// Subject A, January: values 1, 2, 3, 4 → N=4, min=1, max=4, mean=2.5
	let a_jan = subset
		.info
		.interval_stats
		.iter()
		.find(|s| {
			s.subject == "A"
				&& s.measurement == CanonicalColumnName::new("sump")
				&& s.bucket_start_ms == ts(1, 1, 0)
		})
		.expect("A/Jan stats should exist");
	assert_eq!(a_jan.n, 4);
	assert_eq!(a_jan.null_count, 0);
	assert_eq!(a_jan.value, Some(2.5));
	assert_eq!(a_jan.min, Some(1.0));
	assert_eq!(a_jan.max, Some(4.0));
	assert!(a_jan.stderr.is_some(), "N > 1 → stderr computed");

	// Subject B, January: values 10, 20 → N=2, min=10, max=20, mean=15
	let b_jan = subset
		.info
		.interval_stats
		.iter()
		.find(|s| s.subject == "B" && s.measurement == CanonicalColumnName::new("sump"))
		.expect("B/Jan stats should exist");
	assert_eq!(b_jan.n, 2);
	assert_eq!(b_jan.value, Some(15.0));
	assert_eq!(b_jan.min, Some(10.0));
	assert_eq!(b_jan.max, Some(20.0));

	// Subject A, February: values 100, 200 → N=2, mean=150
	let a_feb = subset
		.info
		.interval_stats
		.iter()
		.find(|s| {
			s.subject == "A"
				&& s.measurement == CanonicalColumnName::new("sump")
				&& s.bucket_start_ms == ts(2, 1, 0)
		})
		.expect("A/Feb stats should exist");
	assert_eq!(a_feb.n, 2);
	assert_eq!(a_feb.value, Some(150.0));
}

#[test]
fn stage_trace_records_report_interval_stage() {
	let universe = build_test_universe();
	let request = full_window(
		EtlUnitSubsetRequest::new()
			.measurements(vec!["sump".into(), "fuel".into()])
			.report_interval(monthly_report(RateStrategy::Auto)),
	);

	let subset = universe.subset(&request).expect("subset ok");

	let has_report_stage = subset.info.stage_trace.iter().any(|stage| {
		matches!(&stage.stage, etl_unit::subset::stages::SubsetStage::ReportInterval { .. },)
	});
	assert!(has_report_stage, "stage_trace must include ReportInterval");
}

#[test]
fn without_report_interval_subset_is_unchanged() {
	let universe = build_test_universe();
	let request = full_window(EtlUnitSubsetRequest::new().measurements(vec!["sump".into()]));

	let subset = universe.subset(&request).expect("subset ok");

	// No interval ⇒ no interval_stats; no bucketed DataFrame.
	assert!(subset.info.interval_stats.is_empty());

	// Stage trace must NOT include a ReportInterval stage.
	let has_report_stage = subset.info.stage_trace.iter().any(|stage| {
		matches!(&stage.stage, etl_unit::subset::stages::SubsetStage::ReportInterval { .. },)
	});
	assert!(!has_report_stage);
}

#[test]
fn aggregation_override_wins_at_bucket_level() {
	let universe = build_test_universe();

	// Override sump's aggregation to Max (default is Mean for Measure kind).
	let mut overrides = std::collections::HashMap::new();
	overrides.insert(CanonicalColumnName::new("sump"), Aggregate::Max);

	let report = ReportInterval {
		bucket: IntervalBucket::Months(1),
		strategy: RateStrategy::Auto,
		aggregation_override: Some(overrides),
		empty_bucket: etl_unit::interval::EmptyBucketPolicy::Null,
	};

	let request = full_window(
		EtlUnitSubsetRequest::new()
			.measurements(vec!["sump".into()])
			.report_interval(report),
	);

	let subset = universe.subset(&request).expect("subset ok");

	let a_jan = subset
		.info
		.interval_stats
		.iter()
		.find(|s| s.subject == "A" && s.bucket_start_ms == ts(1, 1, 0))
		.expect("A/Jan stats should exist");
	// With Max override, value should be 4.0 (max of 1,2,3,4) not 2.5 (mean).
	assert_eq!(a_jan.value, Some(4.0));
	// Source is recorded as override for transparency.
	assert_eq!(a_jan.path, etl_unit::interval::ResamplingPath::Aggregate,);
}
