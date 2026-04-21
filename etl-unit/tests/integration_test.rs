//! Integration tests for synapse-etl-unit
//!
//! These tests verify the complete workflow:
//! 1. Define EtlSchema (logical structure with canonical names)
//! 2. Bind sources via EtlUniverseBuildPlan (physical column mappings)
//! 3. Build Universe (expensive, once)
//! 4. Execute subset requests (cheap, many times)
//! 5. Verify derived fields and aggregations

use std::{path::PathBuf, sync::Once};

use etl_unit::{
	Derivation, EtlSchema, EtlUnitSubsetRequest, MeasurementKind, PointwiseExpr, UnpivotConfig,
	source::{BoundSource, EtlUniverseBuildPlan},
	universe::UniverseBuilder,
};
use polars::prelude::*;
// use tracing_subscriber;

#[allow(dead_code)]
static INIT: Once = Once::new();

/// Initialize tracing for tests. Call this at the start of any test that needs logging.
#[allow(dead_code)]
fn init_tracing() {
	INIT.call_once(|| {
		tracing_subscriber::fmt()
			.with_env_filter(
				tracing_subscriber::EnvFilter::from_default_env()
					.add_directive("synapse_etl_unit=debug".parse().unwrap()),
			)
			.with_test_writer()
			.init();
	});
}

/// Get the path to a fixture file
fn fixture_path(filename: &str) -> PathBuf {
	let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
	path.push("tests");
	path.push("fixtures");
	if !path.exists() {
		std::fs::create_dir_all(&path).expect("Failed to create fixtures directory");
	}
	path.push(filename);
	path
}

// Each fixture is materialized exactly once across the parallel test
// run via `OnceLock`. Without this guarantee, multiple tests would race
// to write the same file path and Polars could read a partially-written
// file from a sibling thread. Each helper returns the cached PathBuf
// after the first call.
static SCHEMA_FIXTURE: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
static REQUEST_FIXTURE: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
static PUMP_CSV_FIXTURE: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();

/// Helper to ensure the schema fixture exists with correct JSON content
/// NOTE: Unpivots are defined at the source level, not schema level
fn ensure_schema_fixture() -> PathBuf {
	SCHEMA_FIXTURE
		.get_or_init(|| {
			let path = fixture_path("pump_telemetry_schema.json");
			let json_content = r#"{
      "name": "pump_telemetry",
      "subject": "station_id",
      "time": "observation_time",
      "qualities": [],
      "measurements": [
        { "name": "sump", "kind": "measure" },
        { "name": "fuel", "kind": "measure" },
        { "name": "engine_1", "kind": "categorical" },
        { "name": "engine_2", "kind": "categorical" }
      ],
      "derivations": [
        {
          "name": "any_engine_running",
          "computation": {
            "pointwise": {
              "type": "any_on",
              "inputs": ["engine_1", "engine_2"]
            }
          },
          "kind": "categorical"
        },
        {
          "name": "engines_running_count",
          "computation": {
            "pointwise": {
              "type": "count_non_zero",
              "inputs": ["engine_1", "engine_2"]
            }
          },
          "kind": "count"
        }
      ]
    }"#;
			std::fs::write(&path, json_content).expect("Failed to write schema fixture");
			path
		})
		.clone()
}

/// Helper to ensure the subset request fixture exists
fn ensure_subset_request_fixture() -> PathBuf {
	REQUEST_FIXTURE
		.get_or_init(|| {
			let path = fixture_path("subset_request.json");
			let json_content = r#"{
      "NOTE": "Auto-generated",
      "measurements": ["sump", "fuel", "any_engine_running"],
      "qualities": [],
      "subject_filter": {
        "type": "Include",
        "values": ["Station_A"]
      }
    }"#;
			std::fs::write(&path, json_content).expect("Failed to write subset request fixture");
			path
		})
		.clone()
}

/// Helper to ensure mock data CSV exists
fn ensure_pump_data_csv() -> PathBuf {
	PUMP_CSV_FIXTURE
		.get_or_init(|| materialize_pump_data_csv())
		.clone()
}

fn materialize_pump_data_csv() -> PathBuf {
	let path = fixture_path("pump_data_sample.csv");
	if !path.exists() {
		use std::io::Write;
		let mut file = std::fs::File::create(&path).expect("Failed to create CSV file");
		writeln!(file, "station_id,observation_time,sump,fuel,engine_1,engine_2").unwrap();
		// Generate mock data
		for i in 0..20 {
			// engine_1 alternates, engine_2 is on for last 10
			let e1 = if i % 2 == 0 {
				1
			} else {
				0
			};
			let e2 = if i >= 10 {
				1
			} else {
				0
			};
			writeln!(file, "Station_A,2023-10-27 10:00:{:02}.000,10.{},80.0,{},{}", i, i, e1, e2)
				.unwrap();
		}
	}
	path
}

/// Load the sample pump data CSV with proper datetime parsing
fn load_pump_data() -> DataFrame {
	let path = ensure_pump_data_csv();

	CsvReadOptions::default()
		.with_has_header(true)
		.try_into_reader_with_file_path(Some(path))
		.expect("Failed to create CSV reader")
		.finish()
		.expect("Failed to read CSV")
		.lazy()
		.with_column(col("observation_time").str().to_datetime(
			Some(TimeUnit::Milliseconds),
			None,
			StrptimeOptions {
				format: Some("%Y-%m-%d %H:%M:%S%.f".into()),
				..Default::default()
			},
			lit("raise"),
		))
		.collect()
		.expect("Failed to parse datetime")
}

/// Helper to build a universe from schema and DataFrame
fn build_universe(schema: &EtlSchema, df: DataFrame) -> etl_unit::universe::Universe {
	let plan = EtlUniverseBuildPlan::new(schema.clone())
		.source(BoundSource::identity("default", df, schema));

	UniverseBuilder::build(&plan).expect("Failed to build universe")
}

/// Test loading a schema from JSON file
#[test]
fn test_load_schema_from_json() {
	let path = ensure_schema_fixture();
	let schema = EtlSchema::from_json_file(&path).expect("Failed to load schema");

	// Verify schema name
	assert_eq!(schema.name, "pump_telemetry");

	// Verify default columns (now using CanonicalColumnName)
	assert_eq!(schema.subject.as_str(), "station_id");
	assert_eq!(schema.time.as_str(), "observation_time");

	// Verify measurements
	assert!(schema.has_measurement("sump"));
	assert!(schema.has_measurement("fuel"));
	assert!(schema.has_measurement("engine_1"));
	assert!(schema.has_measurement("engine_2"));

	// Verify derivations (accessed via has_measurement which checks derivations too)
	assert!(schema.get_derivation("any_engine_running").is_some());
	assert!(schema.get_derivation("engines_running_count").is_some());
}

/// Test that signal policies are properly loaded
#[test]
fn test_schema_signal_policies() {
	use std::time::Duration;

	use etl_unit::signal_policy::{SignalPolicy, WindowStrategy};

	// Build schema programmatically with signal policies
	let schema = EtlSchema::new("policy_test")
		.subject("station_id")
		.time("observation_time")
		.measurement("sump", MeasurementKind::Measure)
		.with_policy(SignalPolicy {
			max_staleness: Duration::from_secs(60),
			windowing: WindowStrategy::Instant,
			time_format: Some("%Y-%m-%d %H:%M:%S".into()),
		})
		.measurement("fuel", MeasurementKind::Measure)
		.with_policy(SignalPolicy {
			max_staleness: Duration::from_secs(60),
			windowing: WindowStrategy::Sliding {
				duration: Duration::from_secs(30),
				min_samples: 3,
			},
			time_format: Some("%Y-%m-%d %H:%M:%S".into()),
		})
		.build()
		.unwrap();

	// Check sump has instant windowing
	let sump = schema.get_measurement("sump").expect("sump not found");
	let policy = sump
		.signal_policy
		.as_ref()
		.expect("sump should have signal policy");
	assert_eq!(policy.max_staleness.as_secs(), 60);
	assert!(matches!(policy.windowing, WindowStrategy::Instant));

	// Check fuel has sliding window
	let fuel = schema.get_measurement("fuel").expect("fuel not found");
	let fuel_policy = fuel
		.signal_policy
		.as_ref()
		.expect("fuel should have signal policy");
	assert_eq!(fuel_policy.max_staleness.as_secs(), 60);
	if let WindowStrategy::Sliding {
		duration,
		min_samples,
	} = &fuel_policy.windowing
	{
		assert_eq!(duration.as_secs(), 30);
		assert_eq!(*min_samples, 3);
	} else {
		panic!("Expected sliding window for fuel");
	}
}

/// Test loading subset request from JSON
#[test]
fn test_load_subset_request_from_json() {
	let path = ensure_subset_request_fixture();
	let content = std::fs::read_to_string(&path).expect("Failed to read file");
	let request: EtlUnitSubsetRequest =
		serde_json::from_str(&content).expect("Failed to parse JSON");

	assert_eq!(
		request.measurements,
		vec!["sump".into(), "fuel".into(), "any_engine_running".into()]
	);
	assert!(request.subject_filter.is_some());
}

/// Test executing a subset request with derived fields
#[test]
fn test_execute_subset_with_derivations() {
	let schema_path = ensure_schema_fixture();
	let schema = EtlSchema::from_json_file(&schema_path).expect("Failed to load schema");

	let df = load_pump_data();

	// Build universe (Phase 1: expensive, once)
	let universe = build_universe(&schema, df);

	// Create a request for measurements including a derived field
	let request = EtlUnitSubsetRequest::new().measurements(vec![
		"sump".into(),
		"engine_1".into(),
		"engine_2".into(),
		"any_engine_running".into(),
	]);

	// Execute subset (Phase 2: cheap, repeatable)
	let subset = universe.subset(&request).expect("Subset failed");

	// Check that we got the expected columns
	let df = &subset.data;
	assert!(df.column("sump").is_ok());
	assert!(df.column("engine_1").is_ok());
	assert!(df.column("engine_2").is_ok());
	assert!(df.column("any_engine_running").is_ok());

	// Verify derivation logic: any_engine_running should be 1 when either engine is on
	let any_engine = df.column("any_engine_running").unwrap();
	let engine_1 = df.column("engine_1").unwrap();
	let engine_2 = df.column("engine_2").unwrap();

	// Check that any_engine_running is correctly derived
	for i in 0..df.height().min(20) {
		let e1: Option<f64> = engine_1.get(i).ok().and_then(|v| v.try_extract().ok());
		let e2: Option<f64> = engine_2.get(i).ok().and_then(|v| v.try_extract().ok());
		let any: Option<i32> = any_engine.get(i).ok().and_then(|v| v.try_extract().ok());

		if let (Some(e1_val), Some(e2_val), Some(any_val)) = (e1, e2, any) {
			let expected = if e1_val > 0.0 || e2_val > 0.0 {
				1
			} else {
				0
			};
			assert_eq!(any_val, expected, "any_engine_running mismatch at row {}", i);
		}
	}
}

/// Test building schema programmatically and executing
#[test]
fn test_programmatic_schema_with_derivations() {
	let df = load_pump_data();

	// Build schema programmatically using the new builder pattern
	// Note: .subject() now takes just the canonical name
	let schema = EtlSchema::new("test_schema")
		.subject("station_id")
		.time("observation_time")
		.measurement("sump", MeasurementKind::Measure)
		.measurement("fuel", MeasurementKind::Measure)
		.measurement("engine_1", MeasurementKind::Categorical)
		.measurement("engine_2", MeasurementKind::Categorical)
		.with_derivation(Derivation::pointwise(
			"any_engine",
			PointwiseExpr::any_on(vec!["engine_1", "engine_2"]),
		))
		.with_derivation(Derivation::pointwise(
			"engine_count",
			PointwiseExpr::count_non_zero(vec!["engine_1", "engine_2"]),
		))
		.build()
		.unwrap();

	// Verify schema structure
	assert!(schema.get_derivation("any_engine").is_some());
	assert!(schema.get_derivation("engine_count").is_some());

	// Build universe and execute request
	let universe = build_universe(&schema, df);

	let request = EtlUnitSubsetRequest::new().measurements(vec![
		"sump".into(),
		"any_engine".into(),
		"engine_count".into(),
	]);

	let subset = universe.subset(&request).expect("Subset failed");

	// Verify output
	let df = &subset.data;
	assert!(df.column("sump").is_ok());
	assert!(df.column("any_engine").is_ok());
	assert!(df.column("engine_count").is_ok());
}

/// Test measurement metadata in SubsetUniverse
#[test]
fn test_subset_universe_metadata() {
	let schema_path = ensure_schema_fixture();
	let schema = EtlSchema::from_json_file(&schema_path).expect("Failed to load schema");

	let df = load_pump_data();
	let universe = build_universe(&schema, df);

	let request =
		EtlUnitSubsetRequest::new().measurements(vec!["sump".into(), "any_engine_running".into()]);

	let subset = universe.subset(&request).expect("Subset failed");

	// Check metadata via get_measurement helper
	let sump_meta = subset
		.get_measurement("sump")
		.expect("sump metadata missing");
	assert_eq!(sump_meta.kind, MeasurementKind::Measure);

	let any_engine_meta = subset
		.get_measurement("any_engine_running")
		.expect("any_engine_running metadata missing");
	// The schema fixture declares this derivation with `"kind": "categorical"`,
	// and `MeasurementMeta` reads the kind directly from the derivation's
	// declared field (see `Universe::build_measurement_metas`).
	assert_eq!(any_engine_meta.kind, MeasurementKind::Categorical);
}

/// Test schema serialization roundtrip
#[test]
fn test_schema_serialization_roundtrip() {
	// Build a schema programmatically
	let schema = EtlSchema::new("roundtrip_test")
		.subject("station_id")
		.time("observation_time")
		.measurement("sump", MeasurementKind::Measure)
		.measurement("engine_1", MeasurementKind::Categorical)
		.with_derivation(Derivation::pointwise("any_engine", PointwiseExpr::any_on(vec!["engine_1"])))
		.build()
		.unwrap();

	// Serialize to JSON
	let json = serde_json::to_string_pretty(&schema).expect("Failed to serialize");

	// Deserialize back
	let deserialized: EtlSchema = serde_json::from_str(&json).expect("Failed to deserialize");

	// Verify key properties
	assert_eq!(deserialized.name, schema.name);
	assert_eq!(deserialized.measurements.len(), schema.measurements.len());
	assert_eq!(deserialized.derivations.len(), schema.derivations.len());
	assert!(deserialized.get_derivation("any_engine").is_some());
}

/// Test count_non_zero derivation
#[test]
fn test_count_non_zero_derivation() {
	let df = load_pump_data();

	let schema = EtlSchema::new("count_test")
		.subject("station_id")
		.time("observation_time")
		.measurement("engine_1", MeasurementKind::Categorical)
		.measurement("engine_2", MeasurementKind::Categorical)
		.with_derivation(
			Derivation::pointwise(
				"engines_running",
				PointwiseExpr::count_non_zero(vec!["engine_1", "engine_2"]),
			)
			.with_kind(MeasurementKind::Count),
		)
		.build()
		.unwrap();

	let universe = build_universe(&schema, df);

	let request = EtlUnitSubsetRequest::new().measurements(vec!["engines_running".into()]);

	let subset = universe.subset(&request).expect("Subset failed");

	let engines_running = subset.data.column("engines_running").unwrap();

	// All values should be 0, 1, or 2 (count of engines)
	for i in 0..subset.data.height().min(20) {
		let val: Option<i32> = engines_running
			.get(i)
			.ok()
			.and_then(|v| v.try_extract().ok());
		if let Some(count) = val {
			assert!((0..=2).contains(&count), "Invalid engine count: {}", count);
		}
	}
}

/// Test that requesting all measurements works (empty list = all)
#[test]
fn test_request_all_measurements() {
	let schema_path = ensure_schema_fixture();
	let schema = EtlSchema::from_json_file(&schema_path).expect("Failed to load schema");

	let df = load_pump_data();
	let universe = build_universe(&schema, df);

	// Empty measurements list means "all base + derivations"
	let request = EtlUnitSubsetRequest::new();

	let subset = universe.subset(&request).expect("Subset failed");

	// Should have all base measurements and derivations
	assert!(subset.data.column("sump").is_ok());
	assert!(subset.data.column("fuel").is_ok());
	assert!(subset.data.column("engine_1").is_ok());
	assert!(subset.data.column("engine_2").is_ok());
	assert!(subset.data.column("any_engine_running").is_ok());
	assert!(subset.data.column("engines_running_count").is_ok());
}

/// Test unpivot execution
/// NOTE: Unpivots are now defined at the source level (BoundSource), not schema level
#[test]
fn test_execute_with_unpivot() {
	let df = load_pump_data();

	// Build schema with measurement that will be produced by unpivot
	// The unpivot itself is defined on the source
	let schema = EtlSchema::new("unpivot_test")
        .subject("station_id")
        .time("observation_time")
        .measurement("engine_status", MeasurementKind::Categorical)
        .with_component("engine_id")  // <-- Fixed: component is engine_id, not engine_status
        .build()
        .unwrap();

	// Define unpivot at source level
	let unpivot = UnpivotConfig::creates("engine_status", MeasurementKind::Categorical)
        .subject("station_id", "station_id")
        .time("observation_time", "observation_time")
        .component("engine_id")  // <-- Added: declare the component
        .from_source("engine_1", [("engine_id", "1")])
        .from_source("engine_2", [("engine_id", "2")])
        .build();

	// Build plan with unpivot on source
	let plan = EtlUniverseBuildPlan::new(schema.clone())
		.source(BoundSource::identity("default", df, &schema).unpivot(unpivot));

	let universe = UniverseBuilder::build(&plan).expect("Failed to build universe");

	let request = EtlUnitSubsetRequest::new().measurements(vec!["engine_status".into()]);

	let subset = universe.subset(&request).expect("Subset failed");

	// The unpivoted measurement should appear in the output. Components
	// are always crushed during subset (see universe::universe_of_etlunits
	// module docs), so the engine_id component column is not present —
	// engine_status is collapsed back to one row per (station, time).
	assert!(subset.data.column("engine_status").is_ok());
	assert!(
		subset.data.column("engine_id").is_err(),
		"engine_id should be crushed out of the subset output"
	);

	// Mock CSV has 20 rows for Station_A spanning 20 distinct one-second
	// timestamps. The schema declares no signal_policy on engine_status,
	// so the default TTL (60s) becomes the resample target interval.
	// All 20 timestamps fall inside one 60s bucket, so the resample
	// collapses them to a single (station, time) pair, and the component
	// crush yields one row.
	assert_eq!(
		subset.data.height(),
		1,
		"Expected 1 row after 60s-bucket resample + crush, got {}",
		subset.data.height()
	);
}

/// Verify that derivation dependencies pulled into the pipeline by
/// `expand_derivation_dependencies` do not leak into the result frame.
///
/// A request for `["sump", "any_engine_running"]` causes the executor
/// to pull `engine_1` and `engine_2` into the pipeline (the derivation
/// references them) — but the user only asked for `sump` and
/// `any_engine_running`. The output projection should drop the
/// dependency-only columns before returning.
#[test]
fn test_subset_projects_to_requested_columns_only() {
	let schema_path = ensure_schema_fixture();
	let schema = EtlSchema::from_json_file(&schema_path).expect("Failed to load schema");

	let df = load_pump_data();
	let universe = build_universe(&schema, df);

	let request =
		EtlUnitSubsetRequest::new().measurements(vec!["sump".into(), "any_engine_running".into()]);
	let subset = universe.subset(&request).expect("Subset failed");

	// The user-requested columns are present.
	assert!(subset.data.column("sump").is_ok(), "sump should be present");
	assert!(
		subset.data.column("any_engine_running").is_ok(),
		"any_engine_running should be present"
	);

	// Dependency-only columns are NOT in the result.
	assert!(
		subset.data.column("engine_1").is_err(),
		"engine_1 was a dependency, not requested — should be projected out"
	);
	assert!(
		subset.data.column("engine_2").is_err(),
		"engine_2 was a dependency, not requested — should be projected out"
	);
	assert!(subset.data.column("fuel").is_err(), "fuel was not requested — should be absent");

	// Subject and time keys are always preserved.
	assert!(subset.data.column("station_id").is_ok());
	assert!(subset.data.column("observation_time").is_ok());

	// Metadata reflects only the requested measurements.
	let names: Vec<&str> = subset
		.measurements
		.iter()
		.map(|m| m.column.as_str())
		.collect();
	assert_eq!(names.len(), 2, "metadata should list only requested measurements: {:?}", names);
	assert!(names.contains(&"sump"));
	assert!(names.contains(&"any_engine_running"));
}

/// Verify the wide-join path handles a four-measurement SCADA-style
/// request — exactly the shape that motivated the optimization.
#[test]
fn test_wide_join_handles_four_member_group() {
	use etl_unit::subset::stages::SubsetStage;

	let df = load_pump_data();
	let schema = EtlSchema::new("scada_four_member")
		.subject("station_id")
		.time("observation_time")
		.measurement("sump", MeasurementKind::Measure)
		.measurement("fuel", MeasurementKind::Measure)
		.measurement("engine_1", MeasurementKind::Categorical)
		.measurement("engine_2", MeasurementKind::Categorical)
		.build()
		.unwrap();
	let universe = build_universe(&schema, df);

	let request = EtlUnitSubsetRequest::new().measurements(vec![
		"sump".into(),
		"fuel".into(),
		"engine_1".into(),
		"engine_2".into(),
	]);
	let subset = universe.subset(&request).expect("Subset failed");

	// All four columns present.
	for col in ["sump", "fuel", "engine_1", "engine_2"] {
		assert!(subset.data.column(col).is_ok(), "missing column {}", col);
	}

	// Exactly one wide_join carrying all four members; zero per-measurement joins.
	let mut wide_members: Vec<String> = Vec::new();
	let mut narrow: Vec<String> = Vec::new();
	for diag in &subset.info.stage_trace {
		match &diag.stage {
			SubsetStage::WideJoin {
				measurements,
				..
			} => wide_members.extend(measurements.clone()),
			SubsetStage::JoinMeasurement {
				measurement,
			} => narrow.push(measurement.clone()),
			_ => {}
		}
	}
	assert_eq!(wide_members.len(), 4, "expected 4 wide members, got {:?}", wide_members);
	for name in ["sump", "fuel", "engine_1", "engine_2"] {
		assert!(
			wide_members.iter().any(|m| m == name),
			"wide_join missing {} (members: {:?})",
			name,
			wide_members
		);
	}
	assert!(
		narrow.is_empty(),
		"all four members should be wide-joined, found narrow joins for: {:?}",
		narrow
	);
}

/// Verify the wide path handles the upsample case (`ttl > interval`).
/// Two SCADA measurements with the default 60s TTL are queried with an
/// explicit 10s interval — the wide path's truncate-only branch +
/// asof-join branch in `join_measurement_df` must produce a single
/// `wide_join` stage with both members and a result frame at the
/// finer grid.
#[test]
fn test_wide_join_handles_upsample_case() {
	use etl_unit::Interval;
	use etl_unit::subset::stages::SubsetStage;
	use std::time::Duration;

	let df = load_pump_data();
	let schema = EtlSchema::new("wide_upsample_test")
		.subject("station_id")
		.time("observation_time")
		.measurement("sump", MeasurementKind::Measure)
		.measurement("fuel", MeasurementKind::Measure)
		.build()
		.unwrap();
	let universe = build_universe(&schema, df);

	// 10s interval forces ttl(60s default) > interval(10s) → upsample.
	let request = EtlUnitSubsetRequest::new()
		.measurements(vec!["sump".into(), "fuel".into()])
		.interval(Interval::new(Duration::from_secs(10)));

	let subset = universe
		.subset(&request)
		.expect("upsample wide subset failed");

	assert!(subset.data.column("sump").is_ok());
	assert!(subset.data.column("fuel").is_ok());

	// Single wide_join carrying both members; zero per-measurement joins.
	let mut wide_count = 0usize;
	let mut wide_members: Vec<String> = Vec::new();
	let mut narrow: Vec<String> = Vec::new();
	for diag in &subset.info.stage_trace {
		match &diag.stage {
			SubsetStage::WideJoin {
				measurements,
				..
			} => {
				wide_count += 1;
				wide_members.extend(measurements.clone());
			}
			SubsetStage::JoinMeasurement {
				measurement,
			} => narrow.push(measurement.clone()),
			_ => {}
		}
	}
	assert_eq!(wide_count, 1, "upsample case should produce one wide_join, got {}", wide_count);
	assert!(wide_members.iter().any(|m| m == "sump"));
	assert!(wide_members.iter().any(|m| m == "fuel"));
	assert!(
		narrow.is_empty(),
		"upsample wide path should fully replace per-measurement loop, found narrow: {:?}",
		narrow
	);
}

/// Negative case: a single-member request takes the per-measurement
/// path. The wide path requires `columns.len() > 1` to have anything
/// to batch.
#[test]
fn test_wide_join_skips_single_member_request() {
	use etl_unit::subset::stages::SubsetStage;

	let df = load_pump_data();
	let schema = EtlSchema::new("single_member_test")
		.subject("station_id")
		.time("observation_time")
		.measurement("sump", MeasurementKind::Measure)
		.measurement("fuel", MeasurementKind::Measure)
		.build()
		.unwrap();
	let universe = build_universe(&schema, df);

	let request = EtlUnitSubsetRequest::new().measurements(vec!["sump".into()]);
	let subset = universe.subset(&request).expect("Subset failed");

	let has_wide = subset
		.info
		.stage_trace
		.iter()
		.any(|d| matches!(d.stage, SubsetStage::WideJoin { .. }));
	let has_narrow_sump = subset.info.stage_trace.iter().any(|d| {
		matches!(
			&d.stage,
			SubsetStage::JoinMeasurement { measurement } if measurement == "sump"
		)
	});
	assert!(!has_wide, "single-member request should not take the wide path");
	assert!(has_narrow_sump, "sump should be processed by the per-measurement loop");
}

/// Verify the wide-join path fires for a multi-measurement request
/// drawing from a single source.
///
/// Checks that:
/// 1. The stage trace contains a `wide_join` entry naming both members.
/// 2. There are *no* `join_measurement` entries for those members
///    (proving the per-measurement loop skipped them).
/// 3. The result row count and column set match what the per-measurement
///    path would have produced.
#[test]
fn test_wide_join_fires_for_shared_source() {
	use etl_unit::subset::stages::SubsetStage;

	let df = load_pump_data();
	let schema = EtlSchema::new("wide_join_test")
		.subject("station_id")
		.time("observation_time")
		.measurement("sump", MeasurementKind::Measure)
		.measurement("fuel", MeasurementKind::Measure)
		.build()
		.unwrap();
	let universe = build_universe(&schema, df);

	let request = EtlUnitSubsetRequest::new().measurements(vec!["sump".into(), "fuel".into()]);
	let subset = universe.subset(&request).expect("Subset failed");

	// Result has both columns.
	assert!(subset.data.column("sump").is_ok());
	assert!(subset.data.column("fuel").is_ok());

	// Stage trace contains exactly one wide_join entry naming both members,
	// and zero join_measurement entries for them.
	let mut wide_join_count = 0usize;
	let mut wide_join_members: Vec<String> = Vec::new();
	let mut narrow_join_members: Vec<String> = Vec::new();
	for diag in &subset.info.stage_trace {
		match &diag.stage {
			SubsetStage::WideJoin {
				measurements,
				..
			} => {
				wide_join_count += 1;
				wide_join_members.extend(measurements.iter().cloned());
			}
			SubsetStage::JoinMeasurement {
				measurement,
			} => {
				narrow_join_members.push(measurement.clone());
			}
			_ => {}
		}
	}
	assert_eq!(
		wide_join_count,
		1,
		"Expected exactly one wide_join stage, got {}. Stage trace: {:?}",
		wide_join_count,
		subset
			.info
			.stage_trace
			.iter()
			.map(|d| &d.stage)
			.collect::<Vec<_>>()
	);
	assert!(
		wide_join_members.contains(&"sump".to_string()),
		"wide_join did not include sump (members: {:?})",
		wide_join_members
	);
	assert!(
		wide_join_members.contains(&"fuel".to_string()),
		"wide_join did not include fuel (members: {:?})",
		wide_join_members
	);
	assert!(
		narrow_join_members.is_empty(),
		"sump and fuel should have been wide-joined, not per-measurement. \
		 Found narrow joins for: {:?}",
		narrow_join_members
	);
}

/// Test two-phase execution model
#[test]
fn test_two_phase_execution() {
	let df = load_pump_data();

	let schema = EtlSchema::new("two_phase_test")
		.subject("station_id")
		.time("observation_time")
		.measurement("sump", MeasurementKind::Measure)
		.measurement("fuel", MeasurementKind::Measure)
		.build()
		.unwrap();

	// Phase 1: Build universe (expensive, once)
	let universe = build_universe(&schema, df);

	// Verify universe properties
	assert_eq!(universe.schema().name, "two_phase_test");
	assert!(universe.has_measurement("sump"));
	assert!(universe.has_measurement("fuel"));

	// Phase 2: Multiple cheap subsets
	let subset1 = universe
		.subset(&EtlUnitSubsetRequest::new().measurements(vec!["sump".into()]))
		.expect("Subset 1 failed");

	let subset2 = universe
		.subset(&EtlUnitSubsetRequest::new().measurements(vec!["fuel".into()]))
		.expect("Subset 2 failed");

	let subset_both = universe
		.subset(&EtlUnitSubsetRequest::new().measurements(vec!["sump".into(), "fuel".into()]))
		.expect("Subset both failed");

	// Verify results
	assert_eq!(subset1.measurements.len(), 1);
	assert_eq!(subset2.measurements.len(), 1);
	assert_eq!(subset_both.measurements.len(), 2);

	// All should have the same row count (from same universe)
	assert_eq!(subset1.data.height(), subset2.data.height());
	assert_eq!(subset1.data.height(), subset_both.data.height());
}

/// Test Universe build info audit trail
#[test]
fn test_universe_build_info() {
	let df = load_pump_data();

	let schema = EtlSchema::new("build_info_test")
		.subject("station_id")
		.time("observation_time")
		.measurement("sump", MeasurementKind::Measure)
		.measurement("engine_1", MeasurementKind::Categorical)
		.with_derivation(Derivation::pointwise("any_engine", PointwiseExpr::any_on(vec!["engine_1"])))
		.build()
		.unwrap();

	let universe = build_universe(&schema, df);

	let build_info = universe.build_info();

	// Verify audit trail
	assert_eq!(build_info.schema_name, "build_info_test");
	assert!(!build_info.sources_used.is_empty());
	assert!(build_info.row_count > 0);
	assert!(build_info.subject_count > 0);
	assert!(build_info.build_duration.as_nanos() > 0);
}

/*
/// Test subset_many for batch queries
#[test]
fn test_subset_many() {
	let df = load_pump_data();

	let schema = EtlSchema::new("batch_test")
		.subject("station_id")
		.time("observation_time")
		.measurement("sump", MeasurementKind::Measure)
		.measurement("fuel", MeasurementKind::Measure)
		.measurement("engine_1", MeasurementKind::Categorical)
		.build()
		.unwrap();

	let universe = build_universe(&schema, df);

	let requests = vec![
		EtlUnitSubsetRequest::new().measurements(vec!["sump".into()]),
		EtlUnitSubsetRequest::new().measurements(vec!["fuel".into()]),
		EtlUnitSubsetRequest::new().measurements(vec!["engine_1".into()]),
	];

	let results = universe
		.subset_many(&requests)
		.expect("Batch subset failed");

	assert_eq!(results.len(), 3);
	assert_eq!(results[0].measurements[0].column, "sump".into());
	assert_eq!(results[1].measurements[0].column, "fuel".into());
	assert_eq!(results[2].measurements[0].column, "engine_1".into());
} */

// =============================================================================
// Qualities-only and QualityFilter Tests
// =============================================================================

use etl_unit::QualityFilter;
use etl_unit::universe::{QualityData, Universe};

/// Build a universe with only quality data (no measurement data), for quality-only tests.
fn build_quality_only_universe() -> Universe {
	let schema = EtlSchema::new("quality_test")
		.subject("station_id")
		.time("observation_time")
		.quality("region")
		.build()
		.unwrap();

	let mut universe = Universe::from_schema(schema.clone());

	// Quality data: 3 stations, 2 regions
	let quality_df = df! {
		"station_id" => ["Station_A", "Station_B", "Station_C"],
		"region" => ["North", "South", "North"]
	}
	.unwrap();

	let quality_unit = schema.get_quality("region").unwrap().clone();
	universe
		.qualities
		.insert("region".into(), QualityData::new(quality_unit, quality_df));

	universe
}

/// Build a universe with measurements (from pump CSV) and a quality.
fn build_universe_with_quality() -> Universe {
	// The pump CSV only has Station_A
	let schema = EtlSchema::new("quality_test")
		.subject("station_id")
		.time("observation_time")
		.measurement("sump", MeasurementKind::Measure)
		.quality("region")
		.build()
		.unwrap();

	let df = load_pump_data();
	let mut universe = build_universe(&schema, df);

	// Quality data — Station_A is in the measurement data
	let quality_df = df! {
		"station_id" => ["Station_A"],
		"region" => ["North"]
	}
	.unwrap();

	let quality_unit = schema.get_quality("region").unwrap().clone();
	universe
		.qualities
		.insert("region".into(), QualityData::new(quality_unit, quality_df));

	universe
}

/// Test qualities-only subset: 0 measurements + 1 quality → subject × quality DataFrame
#[test]
fn test_qualities_only_subset() {
	let universe = build_quality_only_universe();

	// Request with 0 measurements and 1 quality
	let request = EtlUnitSubsetRequest::new().qualities(vec!["region".into()]);

	let subset = universe
		.subset(&request)
		.expect("Qualities-only subset failed");

	// Should have no measurements and 1 quality
	assert!(!subset.has_measurements());
	assert!(subset.has_qualities());
	assert_eq!(subset.qualities.len(), 1);
	assert_eq!(subset.qualities[0].column, "region".into());

	// Should have no time column
	assert!(subset.time_column().is_none());

	// Should have subject and quality columns
	let df = subset.dataframe();
	assert!(df.column("station_id").is_ok());
	assert!(df.column("region").is_ok());

	// Should have 3 subjects
	assert_eq!(subset.info.subject_count, 3);
	assert_eq!(df.height(), 3);
}

/// Test quality filter in main subset path: measurements + quality + quality_filter
#[test]
fn test_quality_filter_with_measurements() {
	let universe = build_universe_with_quality();

	// Request measurements + quality + filter to only "North" region
	let request = EtlUnitSubsetRequest::new()
		.measurements(vec!["sump".into()])
		.qualities(vec!["region".into()])
		.quality_filter(QualityFilter {
			quality: "region".into(),
			values: vec!["North".to_string()],
		});

	let subset = universe
		.subset(&request)
		.expect("Quality filter subset failed");

	// Should have filtered to North stations only (Station_A is "North")
	let df = subset.dataframe();
	let region_col = df.column("region").unwrap();

	// All region values should be "North"
	for i in 0..df.height() {
		if let Ok(polars::prelude::AnyValue::String(v)) = region_col.get(i) {
			assert_eq!(v, "North", "Expected all regions to be North, got {} at row {}", v, i);
		}
	}

	// Station_A is North
	assert_eq!(subset.info.subject_count, 1);
}

/// Test quality filter on qualities-only path
#[test]
fn test_quality_filter_qualities_only() {
	let universe = build_quality_only_universe();

	// Request qualities only, filtered to "South"
	let request = EtlUnitSubsetRequest::new()
		.qualities(vec!["region".into()])
		.quality_filter(QualityFilter {
			quality: "region".into(),
			values: vec!["South".to_string()],
		});

	let subset = universe
		.subset(&request)
		.expect("Quality filter subset failed");

	assert!(!subset.has_measurements());
	assert!(subset.time_column().is_none());

	// Only Station_B is South
	let df = subset.dataframe();
	assert_eq!(df.height(), 1);
	assert_eq!(subset.info.subject_count, 1);
}

/// Test quality filter with multiple values
#[test]
fn test_quality_filter_multiple_values() {
	let universe = build_quality_only_universe();

	// Filter to both North and South (= all)
	let request = EtlUnitSubsetRequest::new()
		.qualities(vec!["region".into()])
		.quality_filter(QualityFilter {
			quality: "region".into(),
			values: vec!["North".to_string(), "South".to_string()],
		});

	let subset = universe
		.subset(&request)
		.expect("Quality filter subset failed");
	assert_eq!(subset.info.subject_count, 3);

	// Filter to only North (Station_A, Station_C)
	let request = EtlUnitSubsetRequest::new()
		.qualities(vec!["region".into()])
		.quality_filter(QualityFilter {
			quality: "region".into(),
			values: vec!["North".to_string()],
		});

	let subset = universe
		.subset(&request)
		.expect("Quality filter subset failed");
	assert_eq!(subset.info.subject_count, 2);
}
