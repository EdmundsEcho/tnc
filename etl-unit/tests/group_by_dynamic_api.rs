//! API integration tests for signal policy grid validation
//!
//! Tests grid generation using real data from the station-data API.
//! Requires server running on localhost:3000.
//!
//! Run with: cargo test -p synapse-etl-unit --test group_by_dynamic_api_tests -- --nocapture

use etl_unit::{MeasurementUnit, SignalPolicy};
use polars::prelude::*;
use serial_test::serial;
use std::sync::Once;

use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

mod common;
use crate::common::{
	calculate_stats_with_measurement, print_validation_report_with_measurement,
	report_signal_distribution,
};

// =============================================================================
// Constants
// =============================================================================

const BASE_URL: &str = "http://localhost:3000/api/station-data";

/// Time format for parsing string timestamps from API responses
const TIME_FMT: &str = "%Y-%m-%d %H:%M:%S";

// Initialize tracing once for all tests
static INIT: Once = Once::new();

fn init_tracing() {
	INIT.call_once(|| {
		let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));

		let _ = fmt()
			.with_env_filter(filter)
			.with_test_writer()
			.with_target(true)
			.with_level(true)
			.with_line_number(true)
			.with_file(true)
			.try_init();
	});
}

// =============================================================================
// URL Builder Helper
// =============================================================================

#[derive(Default)]
struct StationDataRequest {
	format: Option<&'static str>,
	group_by: Option<&'static str>,
	measurements: Vec<&'static str>,
	last_hours: Option<u32>,
	include_meta: bool,
	// Note: interval is intentionally NOT included - we want raw signals
}

impl StationDataRequest {
	fn new() -> Self {
		Self::default()
	}

	fn format(mut self, fmt: &'static str) -> Self {
		self.format = Some(fmt);
		self
	}

	fn group_by(mut self, group: &'static str) -> Self {
		self.group_by = Some(group);
		self
	}

	fn measurements(mut self, measurements: Vec<&'static str>) -> Self {
		self.measurements = measurements;
		self
	}

	fn last_hours(mut self, hours: u32) -> Self {
		self.last_hours = Some(hours);
		self
	}

	fn include_meta(mut self) -> Self {
		self.include_meta = true;
		self
	}

	fn build(&self) -> String {
		let mut params: Vec<String> = Vec::new();

		if let Some(fmt) = self.format {
			params.push(format!("format={}", fmt));
		}
		if let Some(group) = self.group_by {
			params.push(format!("group_by={}", group));
		}
		if !self.measurements.is_empty() {
			params.push(format!("measurements={}", self.measurements.join(",")));
		}
		if let Some(hours) = self.last_hours {
			params.push(format!("last_hours={}", hours));
		}
		if self.include_meta {
			params.push("include_meta=true".to_string());
		}

		if params.is_empty() {
			BASE_URL.to_string()
		} else {
			format!("{}?{}", BASE_URL, params.join("&"))
		}
	}
}

// =============================================================================
// HTTP Fetch Helper
// =============================================================================

fn fetch_json(url: &str) -> Option<serde_json::Value> {
	println!("  Fetching: {}", url);

	let response = match reqwest::blocking::get(url) {
		Ok(r) => r,
		Err(e) => {
			println!("  ⚠️  Could not connect to API: {:?}", e);
			println!("      Make sure the server is running on localhost:3000");
			return None;
		}
	};

	let status = response.status();
	if !status.is_success() {
		println!("  ERROR: HTTP {}", status);
		if let Ok(text) = response.text() {
			println!("  Response body: {}", text);
		}
		return None;
	}

	match response.json() {
		Ok(j) => Some(j),
		Err(e) => {
			println!("  ERROR: Could not parse JSON: {:?}", e);
			None
		}
	}
}

// =============================================================================
// Timestamp Parsing Helper
// =============================================================================

/// Parse timestamp string "2025-12-10 15:31:59" to milliseconds since epoch
fn parse_timestamp_str(ts_str: &str) -> Option<i64> {
	chrono::NaiveDateTime::parse_from_str(ts_str, TIME_FMT)
		.ok()
		.map(|dt| dt.and_utc().timestamp_millis())
}

// =============================================================================
// Meta Extraction Helpers
// =============================================================================

/// Validate that the response contains raw signals (no interval applied)
fn validate_raw_signals(json: &serde_json::Value) -> Result<(), String> {
	let has_interval = json
		.get("meta")
		.and_then(|m| m.get("sections"))
		.and_then(|s| s.get("request"))
		.and_then(|r| r.get("has_interval"))
		.and_then(|v| v.as_bool())
		.unwrap_or(false);

	if has_interval {
		let interval = json
			.get("meta")
			.and_then(|m| m.get("sections"))
			.and_then(|s| s.get("request"))
			.and_then(|r| r.get("interval"))
			.and_then(|v| v.as_str())
			.unwrap_or("unknown");
		return Err(format!(
			"Response has interval='{}'. Expected raw signals (no interval).",
			interval
		));
	}

	Ok(())
}

/// Extract MeasurementUnit by name from meta.sections.units.measurements
fn extract_measurement_unit(json: &serde_json::Value, name: &str) -> Option<MeasurementUnit> {
	let measurements = json
		.get("meta")
		.and_then(|m| m.get("sections"))
		.and_then(|s| s.get("units"))
		.and_then(|u| u.get("measurements"))
		.and_then(|m| m.as_array())?;

	for measurement_json in measurements {
		let measurement_name = measurement_json.get("name").and_then(|n| n.as_str())?;
		if measurement_name == name {
			// Deserialize the MeasurementUnit directly
			return serde_json::from_value(measurement_json.clone()).ok();
		}
	}

	None
}

/// Extract presentation metadata (total_subjects, group_keys)
fn extract_presentation_meta(json: &serde_json::Value) -> Option<(usize, Vec<String>)> {
	let presentation = json.get("presentation")?;

	let total_subjects = presentation
		.get("total_subjects")
		.and_then(|v| v.as_u64())
		.map(|n| n as usize)
		.unwrap_or(0);

	let group_keys: Vec<String> = presentation
		.get("group_keys")
		.and_then(|v| v.as_array())
		.map(|arr| {
			arr.iter()
				.filter_map(|v| v.as_str().map(String::from))
				.collect()
		})
		.unwrap_or_default();

	Some((total_subjects, group_keys))
}

// =============================================================================
// SignalPolicy Helper
// =============================================================================

/// Create a default SignalPolicy for testing
fn create_test_signal_policy(ttl_secs: u32) -> SignalPolicy {
	SignalPolicy::instant().with_ttl(ttl_secs)
}

/// Clone a MeasurementUnit and attach a SignalPolicy
fn measurement_with_policy(measurement: &MeasurementUnit, policy: SignalPolicy) -> MeasurementUnit {
	let mut m = measurement.clone();
	m.signal_policy = Some(policy);
	m
}

// =============================================================================
// DataFrame Builder from API Response
// =============================================================================

/// Build DataFrame from chart API response with column names matching MeasurementUnit.
///
/// Handles the response structure with include_meta=true:
/// { "meta": {...}, "presentation": { "charts": [...] } }
fn build_dataframe_for_measurement(
	json: &serde_json::Value,
	measurement: &MeasurementUnit,
) -> Option<DataFrame> {
	let subject_col = measurement.subject.as_str();
	let time_col = measurement.time.as_str();
	let value_col = measurement.value.as_str();

	// Get the chart label for this measurement (from chart_hints)
	let expected_label = measurement
		.chart_hints
		.as_ref()
		.and_then(|h| h.label.as_deref())
		.unwrap_or(value_col);

	// Handle nested structure: presentation.charts when include_meta=true
	let charts = json
		.get("presentation")
		.and_then(|p| p.get("charts"))
		.and_then(|c| c.as_array())?;

	let mut all_subjects: Vec<String> = Vec::new();
	let mut all_timestamps: Vec<i64> = Vec::new();
	let mut all_values: Vec<Option<f64>> = Vec::new();

	for chart in charts {
		let subject_name = chart
			.get("options")
			.and_then(|o| o.get("plugins"))
			.and_then(|p| p.get("title"))
			.and_then(|t| t.get("text"))
			.and_then(|t| t.as_str())
			.unwrap_or("unknown");

		let data = chart.get("data")?;
		let labels = data.get("labels")?.as_array()?;

		// Find the dataset matching our measurement by label
		let datasets = data.get("datasets")?.as_array()?;
		let dataset = datasets.iter().find(|ds| {
			ds.get("label")
				.and_then(|l| l.as_str())
				.map(|l| l == expected_label)
				.unwrap_or(false)
		});

		let values = match dataset {
			Some(ds) => ds.get("data").and_then(|d| d.as_array())?,
			None => {
				println!(
					"  WARNING: Dataset with label '{}' not found for subject '{}'",
					expected_label, subject_name
				);
				continue;
			}
		};

		for (label, value) in labels.iter().zip(values.iter()) {
			let ts_str = label.as_str().unwrap_or("");
			if let Some(ts_ms) = parse_timestamp_str(ts_str) {
				all_subjects.push(subject_name.to_string());
				all_timestamps.push(ts_ms);
				all_values.push(value.as_f64());
			}
		}
	}

	if all_timestamps.is_empty() {
		println!("  ERROR: No timestamps parsed from charts");
		return None;
	}

	let df = df! {
		subject_col => &all_subjects,
		time_col => &all_timestamps,
		value_col => &all_values
	}
	.ok()?
	.lazy()
	.with_column(col(time_col).cast(DataType::Datetime(TimeUnit::Milliseconds, None)))
	.sort([subject_col, time_col], SortMultipleOptions::default())
	.collect()
	.ok()?;

	Some(df)
}

// =============================================================================
// Integration Tests
// =============================================================================

/// Test sump measurement signal → observation transformation.
///
/// This test:
/// 1. Fetches raw signals from the API (no interval parameter)
/// 2. Validates we received signals, not observations
/// 3. Deserializes MeasurementUnit from API meta
/// 4. Attaches a SignalPolicy (60s TTL)
/// 5. Applies signal policy to create observation grid
/// 6. Validates grid completeness
#[test]
#[serial]
fn test_sump_signal_to_observation() {
	// init_tracing();

	println!("\n");
	println!("================================================================================");
	println!("  TEST: Sump Signal → Observation Transformation");
	println!("================================================================================");
	println!();

	// 1. Build request for raw signals (no interval!)
	let url = StationDataRequest::new()
		.format("chart")
		.group_by("by_subject")
		.measurements(vec!["sump"])
		.last_hours(1)
		.include_meta()
		.build();

	// 2. Fetch API response
	let json = match fetch_json(&url) {
		Some(j) => j,
		None => {
			println!("  ⚠️  Skipping test - API not available");
			return;
		}
	};

	// 3. Validate we got signals, not observations
	if let Err(e) = validate_raw_signals(&json) {
		panic!("ERROR: {}", e);
	}
	println!("  ✓ Received raw signals (no interval)");

	// 4. Extract MeasurementUnit for sump from meta
	let sump_meta = extract_measurement_unit(&json, "sump")
		.expect("MeasurementUnit 'sump' not found in API response");

	println!("  ✓ Found MeasurementUnit: {}", sump_meta.name.as_str());
	println!("    subject: {}", sump_meta.subject.as_str());
	println!("    time:    {}", sump_meta.time.as_str());
	println!("    value:   {}", sump_meta.value.as_str());
	if let Some(hints) = &sump_meta.chart_hints {
		if let Some(label) = &hints.label {
			println!("    label:   {}", label);
		}
	}

	// 5. Attach SignalPolicy (60s TTL)
	let signal_policy = create_test_signal_policy(60);
	let sump_measurement = measurement_with_policy(&sump_meta, signal_policy);

	println!("  ✓ Attached SignalPolicy: TTL=60s, SampleRate=6s");

	// 6. Get presentation metadata
	if let Some((total_subjects, _group_keys)) = extract_presentation_meta(&json) {
		println!("  ✓ Subjects in response: {}", total_subjects);
	}

	// 7. Build DataFrame from raw signals
	let signals_df = build_dataframe_for_measurement(&json, &sump_measurement)
		.expect("Failed to build signals DataFrame");

	println!("\n  RAW SIGNALS:");
	println!("    shape:    {:?}", signals_df.shape());
	println!("    columns:  {:?}", signals_df.get_column_names());
	println!();
	println!("{}", signals_df.head(Some(10)));

	// 8. Apply signal policy to create observation grid (from common module)
	let observations_df = SignalPolicy::apply(signals_df.clone(), &sump_measurement)
		.expect("Failed to apply signal policy");

	println!("\n  OBSERVATIONS:");
	println!("    shape:    {:?}", observations_df.shape());
	println!();
	println!("{}", observations_df.head(Some(15)));

	// 9. Calculate stats and report (from common module)
	let stats = calculate_stats_with_measurement(&signals_df, &observations_df, &sump_measurement)
		.expect("Failed to calculate stats");

	// 10. Print signal distribution
	report_signal_distribution(&observations_df, sump_measurement.subject.as_str());

	// 11. Print validation report
	print_validation_report_with_measurement(&stats, &sump_measurement);

	// 12. Assert grid completeness
	assert!(
		stats.is_grid_complete(),
		"Grid is not complete: {} actual vs {} expected observations",
		stats.actual_observations,
		stats.expected_observations
	);
}
