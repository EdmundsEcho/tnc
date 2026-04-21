//! Documents the contract between **raw** and **processed** subsets, and how
//! `null_value` and `ttl` shape the processed output.
//!
//! These tests are intentionally tiny and fully self-contained — the goal is
//! to capture the *semantics* of the pipeline so future refactors that
//! change row counts or null patterns trip a clear assertion.
//!
//! # The contract
//!
//! - **Raw** (`Universe::subset_raw`) returns one row per actual observed
//!   timestamp. No grid expansion. Nulls in the source survive.
//! - **Processed** (`Universe::subset`) builds a dense time grid at
//!   `sample_rate` covering the observation span (clipped to the request
//!   window) and projects each measurement onto it. Per-cell behavior:
//!     * `null_value` configured: cells with no covering observation get
//!       filled with `null_value`. With this set, the processed column is
//!       never null.
//!     * `null_value` absent: cells with no covering observation stay
//!       null. The chart shows a gap; LTTB drops them.
//! - **TTL** extends each observation's validity to `[t, t + TTL)`. Cells
//!   inside that window are filled by forward-fill before `null_value`
//!   applies. The number of cells an observation carries forward is
//!   `floor((TTL - 1) / sample_rate_ms)`.

use chrono::{DateTime, TimeZone as _, Utc};
use etl_unit::{
	CanonicalColumnName, EtlSchema, EtlUnitSubsetRequest, MeasurementKind,
	request::TimeRange,
	signal_policy::SignalPolicy,
	source::{BoundSource, EtlUniverseBuildPlan},
	unit::NullValue,
	universe::{Universe, UniverseBuilder, alignment::AlignmentSpec},
};
use polars::prelude::*;
use std::time::Duration;

// ============================================================================
// Test fixture builders
// ============================================================================

const SUBJECT_COL: &str = "station_id";
const TIME_COL: &str = "observation_time";
const SUBJECT: &str = "S";

/// `t = base_time() + minutes` — a tidy, repeatable epoch for tests.
fn base_time() -> DateTime<Utc> {
	Utc.with_ymd_and_hms(2026, 1, 1, 9, 0, 0).unwrap()
}

/// Build a one-station source DataFrame with observations at the listed
/// minute offsets. `values` may contain `f64::NAN` to represent a null
/// observation (Polars treats NaN as a value, not null — so we encode
/// "missing in source" by *omitting* the row instead).
fn one_station_source(value_col: &str, minute_offsets: &[i64], values: &[f64]) -> DataFrame {
	assert_eq!(minute_offsets.len(), values.len());
	let times: Vec<i64> = minute_offsets
		.iter()
		.map(|m| base_time().timestamp_millis() + m * 60_000)
		.collect();
	let subjects: Vec<&str> = vec![SUBJECT; minute_offsets.len()];

	let time_ca = Int64Chunked::new(TIME_COL.into(), &times)
		.into_datetime(TimeUnit::Milliseconds, Some(polars::prelude::TimeZone::UTC));

	DataFrame::new(vec![
		Column::new(SUBJECT_COL.into(), &subjects),
		time_ca.into_column(),
		Column::new(value_col.into(), values),
	])
	.unwrap()
}

/// Build a Universe from a schema + raw DataFrame using identity binding
/// (column names already match the schema's canonical names), then
/// pre-align so the master grid uses each measurement's `sample_rate` as
/// the interval. Without this step, the subset path falls back to
/// `longest_ttl` for the grid interval — fine for production where
/// alignment is computed up-front, but tests must wire it explicitly.
fn build(schema: &EtlSchema, df: DataFrame) -> Universe {
	let plan = EtlUniverseBuildPlan::new(schema.clone())
		.source(BoundSource::identity("default", df, schema));
	let mut universe = UniverseBuilder::build(&plan).expect("universe should build");
	let names: Vec<CanonicalColumnName> =
		schema.measurements.iter().map(|m| m.name.clone()).collect();
	let spec = AlignmentSpec::compute(&universe.measurements, &names);
	universe
		.ensure_aligned(spec)
		.expect("alignment should compute");
	universe
}

/// Subset request covering the full observation window — wide enough that
/// the request window doesn't clip out any observations. Tests that want
/// to assert on grid sizing should use this so the sole driver of the grid
/// is the observation span.
fn full_window_request(measurement: &str) -> EtlUnitSubsetRequest {
	let start = base_time() - chrono::Duration::hours(1);
	let end = base_time() + chrono::Duration::hours(2);
	EtlUnitSubsetRequest::new()
		.measurements(vec![measurement.into()])
		.time_range(TimeRange::new(Some(start), Some(end)))
}

// ============================================================================
// Raw vs Processed: cell counts
// ============================================================================

/// Raw subsets return one row per observed (subject, timestamp) — no grid
/// expansion, no null-fill, no carry-forward.
#[test]
fn raw_returns_one_row_per_observation() {
	// 4 observations at minutes 0, 1, 3, 4 — a deliberate gap at minute 2.
	let df = one_station_source("sump", &[0, 1, 3, 4], &[1.0, 1.5, 2.0, 2.5]);
	let schema = EtlSchema::new("raw_test")
		.subject(SUBJECT_COL)
		.time(TIME_COL)
		.measurement("sump", MeasurementKind::Measure)
		.with_policy(SignalPolicy::instant().with_ttl(Duration::from_secs(60)))
		.with_sample_rate(60_000)
		.build()
		.unwrap();

	let universe = build(&schema, df);
	let raw = universe.subset_raw(&full_window_request("sump")).unwrap();

	assert_eq!(raw.dataframe().height(), 4, "raw subset must have exactly one row per observation",);
	let sump_col = raw.dataframe().column("sump").unwrap();
	assert_eq!(sump_col.null_count(), 0, "raw observations were all non-null");
}

/// Processed subsets build a dense grid covering the observation span. With
/// no `null_value` configured, gaps inside the span stay null.
#[test]
fn processed_dense_grid_no_null_value_keeps_gaps_as_null() {
	let df = one_station_source("sump", &[0, 1, 3, 4], &[1.0, 1.5, 2.0, 2.5]);
	let schema = EtlSchema::new("proc_no_nv")
		.subject(SUBJECT_COL)
		.time(TIME_COL)
		.measurement("sump", MeasurementKind::Measure)
		// TTL == sample_rate ⇒ no forward-fill (floor((60000-1)/60000) == 0)
		.with_policy(SignalPolicy::instant().with_ttl(Duration::from_secs(60)))
		.with_sample_rate(60_000)
		.build()
		.unwrap();

	let universe = build(&schema, df);
	let processed = universe.subset(&full_window_request("sump")).unwrap();

	// Span = [minute 0, minute 4] inclusive @ 60s grid → 5 cells.
	assert_eq!(
		processed.dataframe().height(),
		5,
		"dense grid must cover every minute from min to max observation",
	);

	let sump_col = processed.dataframe().column("sump").unwrap();
	assert_eq!(
		sump_col.null_count(),
		1,
		"the gap at minute 2 must be null when no null_value is configured",
	);
}

/// Processed subsets fill dense-grid gaps with `null_value` when configured.
#[test]
fn processed_dense_grid_with_null_value_fills_gaps() {
	let df = one_station_source("sump", &[0, 1, 3, 4], &[1.0, 1.5, 2.0, 2.5]);
	let schema = EtlSchema::new("proc_with_nv")
		.subject(SUBJECT_COL)
		.time(TIME_COL)
		.measurement("sump", MeasurementKind::Measure)
		.with_policy(SignalPolicy::instant().with_ttl(Duration::from_secs(60)))
		.with_sample_rate(60_000)
		.with_null_value(NullValue::Float(0.0))
		.build()
		.unwrap();

	let universe = build(&schema, df);
	let processed = universe.subset(&full_window_request("sump")).unwrap();

	assert_eq!(processed.dataframe().height(), 5, "dense grid still 5 cells");
	assert_eq!(
		processed.dataframe().column("sump").unwrap().null_count(),
		0,
		"null_value = 0 must fill the gap at minute 2",
	);

	// Verify the gap cell holds null_value (0.0), not the surrounding
	// observations.
	let sump = processed
		.dataframe()
		.sort([TIME_COL], SortMultipleOptions::default())
		.unwrap();
	let values: Vec<Option<f64>> = sump
		.column("sump")
		.unwrap()
		.f64()
		.unwrap()
		.into_iter()
		.collect();
	assert_eq!(
		values,
		vec![Some(1.0), Some(1.5), Some(0.0), Some(2.0), Some(2.5)],
		"gap at minute 2 filled with null_value, not forward-filled",
	);
}

// ============================================================================
// TTL forward-fill semantics
// ============================================================================

/// `TTL == sample_rate` ⇒ each observation covers exactly its own cell,
/// nothing forward. `floor((60000 - 1) / 60000) == 0`.
#[test]
fn ttl_equal_sample_rate_does_not_carry_forward() {
	// Observations at minute 0 and minute 3 — gaps at 1, 2.
	let df = one_station_source("sump", &[0, 3], &[1.0, 4.0]);
	let schema = EtlSchema::new("ttl_eq_sr")
		.subject(SUBJECT_COL)
		.time(TIME_COL)
		.measurement("sump", MeasurementKind::Measure)
		.with_policy(SignalPolicy::instant().with_ttl(Duration::from_secs(60)))
		.with_sample_rate(60_000)
		.build()
		.unwrap();

	let universe = build(&schema, df);
	let processed = universe.subset(&full_window_request("sump")).unwrap();

	// 4 cells: 0, 1, 2, 3.
	assert_eq!(processed.dataframe().height(), 4);
	let values: Vec<Option<f64>> = processed
		.dataframe()
		.sort([TIME_COL], SortMultipleOptions::default())
		.unwrap()
		.column("sump")
		.unwrap()
		.f64()
		.unwrap()
		.into_iter()
		.collect();
	assert_eq!(
		values,
		vec![Some(1.0), None, None, Some(4.0)],
		"TTL == sample_rate: no forward-fill; gaps stay null",
	);
}

/// `TTL = 90s` with `sample_rate = 60s` ⇒ `floor(89_999 / 60_000) == 1` cell
/// of forward-fill. An observation at minute `t` carries to minute `t+1`
/// only.
#[test]
fn ttl_one_cell_carry_forward() {
	let df = one_station_source("sump", &[0, 3], &[1.0, 4.0]);
	let schema = EtlSchema::new("ttl_90")
		.subject(SUBJECT_COL)
		.time(TIME_COL)
		.measurement("sump", MeasurementKind::Measure)
		.with_policy(SignalPolicy::instant().with_ttl(Duration::from_millis(90_000)))
		.with_sample_rate(60_000)
		.build()
		.unwrap();

	let universe = build(&schema, df);
	let processed = universe.subset(&full_window_request("sump")).unwrap();

	let values: Vec<Option<f64>> = processed
		.dataframe()
		.sort([TIME_COL], SortMultipleOptions::default())
		.unwrap()
		.column("sump")
		.unwrap()
		.f64()
		.unwrap()
		.into_iter()
		.collect();
	assert_eq!(
		values,
		vec![Some(1.0), Some(1.0), None, Some(4.0)],
		"observation at minute 0 carries forward 1 cell (to minute 1); minute 2 expires",
	);
}

/// `TTL = 121s` with `sample_rate = 60s` ⇒ `floor(120_999 / 60_000) == 2`
/// cells of forward-fill. An observation at minute `t` carries to `t+1`
/// AND `t+2`.
#[test]
fn ttl_two_cell_carry_forward() {
	let df = one_station_source("sump", &[0, 3], &[1.0, 4.0]);
	let schema = EtlSchema::new("ttl_121")
		.subject(SUBJECT_COL)
		.time(TIME_COL)
		.measurement("sump", MeasurementKind::Measure)
		.with_policy(SignalPolicy::instant().with_ttl(Duration::from_millis(121_000)))
		.with_sample_rate(60_000)
		.build()
		.unwrap();

	let universe = build(&schema, df);
	let processed = universe.subset(&full_window_request("sump")).unwrap();

	let values: Vec<Option<f64>> = processed
		.dataframe()
		.sort([TIME_COL], SortMultipleOptions::default())
		.unwrap()
		.column("sump")
		.unwrap()
		.f64()
		.unwrap()
		.into_iter()
		.collect();
	assert_eq!(
		values,
		vec![Some(1.0), Some(1.0), Some(1.0), Some(4.0)],
		"observation at minute 0 carries forward 2 cells (1 and 2); minute 3 has its own observation",
	);
}

/// TTL forward-fill happens BEFORE `null_value` fill, so an observation
/// reaches its TTL-bounded cells with the actual value, and only cells
/// beyond TTL fall back to `null_value`.
#[test]
fn ttl_carries_first_then_null_value_fills_remainder() {
	// Observations at minutes 0 and 5. Gap minutes 1..=4.
	let df = one_station_source("sump", &[0, 5], &[1.0, 6.0]);
	let schema = EtlSchema::new("ttl_then_nv")
		.subject(SUBJECT_COL)
		.time(TIME_COL)
		.measurement("sump", MeasurementKind::Measure)
		// TTL = 90s carries observation at minute 0 to minute 1 only.
		// Minutes 2, 3, 4 fall back to null_value.
		.with_policy(SignalPolicy::instant().with_ttl(Duration::from_millis(90_000)))
		.with_sample_rate(60_000)
		.with_null_value(NullValue::Float(-1.0))
		.build()
		.unwrap();

	let universe = build(&schema, df);
	let processed = universe.subset(&full_window_request("sump")).unwrap();

	let values: Vec<Option<f64>> = processed
		.dataframe()
		.sort([TIME_COL], SortMultipleOptions::default())
		.unwrap()
		.column("sump")
		.unwrap()
		.f64()
		.unwrap()
		.into_iter()
		.collect();
	assert_eq!(
		values,
		vec![
			Some(1.0),  // minute 0: observation
			Some(1.0),  // minute 1: TTL forward-fill (within 90s of t=0)
			Some(-1.0), // minute 2: TTL expired → null_value
			Some(-1.0), // minute 3: null_value
			Some(-1.0), // minute 4: null_value
			Some(6.0),  // minute 5: observation
		],
		"TTL fills first within the staleness window, null_value fills the rest",
	);
	assert_eq!(
		processed.dataframe().column("sump").unwrap().null_count(),
		0,
		"with null_value configured, no nulls survive into the processed output",
	);
}

// ============================================================================
// Raw vs Processed cardinality contract
// ============================================================================

/// The defining contract: under any configuration, processed cardinality is
/// determined solely by the master grid (sample_rate × observation span),
/// while raw cardinality is determined solely by the count of observations.
/// Adding `null_value` changes the *null pattern* of processed, never the
/// row count.
#[test]
fn processed_cardinality_independent_of_null_value() {
	let observations = &[0, 1, 3, 4, 6, 7, 9]; // 7 obs, gaps at 2, 5, 8
	let values = &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0];

	// Variant A: no null_value
	let schema_a = EtlSchema::new("nv_off")
		.subject(SUBJECT_COL)
		.time(TIME_COL)
		.measurement("sump", MeasurementKind::Measure)
		.with_policy(SignalPolicy::instant().with_ttl(Duration::from_secs(60)))
		.with_sample_rate(60_000)
		.build()
		.unwrap();

	// Variant B: null_value = 0
	let schema_b = EtlSchema::new("nv_on")
		.subject(SUBJECT_COL)
		.time(TIME_COL)
		.measurement("sump", MeasurementKind::Measure)
		.with_policy(SignalPolicy::instant().with_ttl(Duration::from_secs(60)))
		.with_sample_rate(60_000)
		.with_null_value(NullValue::Float(0.0))
		.build()
		.unwrap();

	let df_a = one_station_source("sump", observations, values);
	let df_b = one_station_source("sump", observations, values);

	let req = full_window_request("sump");
	let proc_a = build(&schema_a, df_a).subset(&req).unwrap();
	let proc_b = build(&schema_b, df_b).subset(&req).unwrap();

	// Both must have the exact same number of rows (the master grid).
	// Span = minutes 0..=9 inclusive @ 60s = 10 cells.
	assert_eq!(proc_a.dataframe().height(), 10);
	assert_eq!(proc_b.dataframe().height(), 10);
	assert_eq!(
		proc_a.dataframe().height(),
		proc_b.dataframe().height(),
		"null_value affects nulls, not row count",
	);

	// What changes is the null count.
	let nulls_a = proc_a.dataframe().column("sump").unwrap().null_count();
	let nulls_b = proc_b.dataframe().column("sump").unwrap().null_count();
	assert_eq!(nulls_a, 3, "without null_value: 3 gap cells stay null");
	assert_eq!(nulls_b, 0, "with null_value: gaps filled, no nulls survive");
}

/// Raw and processed disagree on row count exactly when the dense grid
/// has more cells than there are observations. The difference is the
/// number of grid cells without a covering observation.
#[test]
fn raw_processed_row_count_difference_equals_unobserved_cells() {
	let df = one_station_source("sump", &[0, 1, 3, 4], &[1.0, 1.5, 2.0, 2.5]);
	let schema = EtlSchema::new("delta")
		.subject(SUBJECT_COL)
		.time(TIME_COL)
		.measurement("sump", MeasurementKind::Measure)
		.with_policy(SignalPolicy::instant().with_ttl(Duration::from_secs(60)))
		.with_sample_rate(60_000)
		.build()
		.unwrap();

	let req = full_window_request("sump");
	let universe = build(&schema, df);
	let raw = universe.subset_raw(&req).unwrap();
	let processed = universe.subset(&req).unwrap();

	let raw_rows = raw.dataframe().height();
	let proc_rows = processed.dataframe().height();
	let proc_nulls = processed.dataframe().column("sump").unwrap().null_count();

	assert_eq!(raw_rows, 4);
	assert_eq!(proc_rows, 5);
	assert_eq!(
		proc_rows - raw_rows,
		proc_nulls,
		"the rows processed adds beyond raw equal the null-filled gap cells",
	);
}
