//! Common utilities for signal policy grid validation tests
//!
//! This module provides shared test utilities for validation and reporting.
//! The core signal policy algorithm is in the library via `SignalPolicy::apply()`.

use etl_unit::{EtlTimeRange, MeasurementUnit};
use polars::prelude::*;

// =============================================================================
// Validation
// =============================================================================

/// Count unique values in a column.
pub fn count_unique(df: &DataFrame, col_name: &str) -> usize {
	df.column(col_name)
		.ok()
		.and_then(|col| col.n_unique().ok())
		.unwrap_or(0)
}

// =============================================================================
// Reporting
// =============================================================================

/// Statistics from signal policy validation.
pub struct SignalPolicyStats {
	pub input_signals: usize,
	pub actual_observations: usize,
	pub expected_observations: usize,
	pub valid_observations: usize,
	pub null_observations: usize,
	pub fill_rate: f64,
	pub grid_cells: usize,
	pub num_subjects: usize,
	pub num_component_combos: usize,
	pub partitions: usize,
	pub duration_ms: u64,
	pub ttl_ms: u64,
}

impl SignalPolicyStats {
	pub fn is_grid_complete(&self) -> bool {
		self.actual_observations == self.expected_observations
	}
}

/// Calculate signal policy statistics from input and result DataFrames.
///
/// # Arguments
/// * `input_df` - DataFrame with raw signals
/// * `result_df` - DataFrame with observation grid
/// * `time_col` - Name of the timestamp column
/// * `value_col` - Name of the value column (the library outputs this directly, not
///   `{value_col}_mean`)
/// * `subject_col` - Name of the subject column
/// * `ttl_ms` - Grid cell size in milliseconds
/// * `num_component_combos` - Number of component combinations (1 if no components)
pub fn calculate_stats_from_parts(
	input_df: &DataFrame,
	result_df: &DataFrame,
	time_col: &str,
	value_col: &str,
	subject_col: &str,
	ttl_ms: u64,
	num_component_combos: usize,
) -> Option<SignalPolicyStats> {
	let input_signals = input_df.height();
	let actual_observations = result_df.height();

	// Library outputs value_col directly (not {value_col}_mean)
	let null_observations = result_df
		.column(value_col)
		.map(|c| c.null_count())
		.unwrap_or(0);
	let valid_observations = actual_observations.saturating_sub(null_observations);

	let fill_rate = if actual_observations > 0 {
		valid_observations as f64 / actual_observations as f64 * 100.0
	} else {
		0.0
	};

	let time_range = EtlTimeRange::extract_time_range_from_parts(input_df, time_col, None).ok()?;
	let duration_ms = time_range.duration_ms;

	let grid_cells = if ttl_ms > 0 {
		(duration_ms as f64 / ttl_ms as f64).ceil() as usize
	} else {
		0
	};

	let num_subjects = count_unique(input_df, subject_col);
	let partitions = num_subjects * num_component_combos;
	let expected_observations = grid_cells * partitions;

	Some(SignalPolicyStats {
		input_signals,
		actual_observations,
		expected_observations,
		valid_observations,
		null_observations,
		fill_rate,
		grid_cells,
		num_subjects,
		num_component_combos,
		partitions,
		duration_ms,
		ttl_ms,
	})
}

/// Calculate signal policy statistics using MeasurementUnit.
pub fn calculate_stats_with_measurement(
	input_df: &DataFrame,
	result_df: &DataFrame,
	measurement: &MeasurementUnit,
) -> Option<SignalPolicyStats> {
	let time_col = measurement.time.as_str();
	let value_col = measurement.name.as_str(); // Library uses measurement.name as value column
	let subject_col = measurement.subject.as_str();

	let ttl_ms = measurement
		.signal_policy
		.as_ref()
		.map(|p| p.ttl().as_millis() as u64)
		.unwrap_or(60_000);

	let num_component_combos = if measurement.components.is_empty() {
		1
	} else {
		// Count unique component combinations from input data
		1 // TODO: Calculate from data if needed
	};

	calculate_stats_from_parts(
		input_df,
		result_df,
		time_col,
		value_col,
		subject_col,
		ttl_ms,
		num_component_combos,
	)
}

/// Report signal distribution per partition (signals per cell).
///
/// Note: This requires `signal_count` column which the library doesn't produce.
/// For library output, use `report_signal_distribution_simple` instead.
pub fn report_signal_distribution(result_df: &DataFrame, subject_col: &str) {
	println!("\n  SIGNAL DISTRIBUTION PER PARTITION:");
	println!("    {:<20} {:>12} {:>12}", "Subject", "Cells", "Nulls");
	println!("    {}", "─".repeat(48));

	// Get unique subjects
	let subjects: Vec<String> = result_df
		.column(subject_col)
		.ok()
		.and_then(|c| c.str().ok())
		.map(|s| {
			let mut set: std::collections::HashSet<String> = std::collections::HashSet::new();
			for v in s.into_iter().flatten() {
				set.insert(v.to_string());
			}
			let mut vec: Vec<String> = set.into_iter().collect();
			vec.sort();
			vec
		})
		.unwrap_or_default();

	let mut total_cells = 0usize;
	let mut total_nulls = 0usize;

	for subject in &subjects {
		let subject_data = result_df
			.clone()
			.lazy()
			.filter(col(subject_col).eq(lit(subject.as_str())))
			.collect()
			.unwrap_or_default();

		let cells = subject_data.height();
		// Count nulls in the value column (first non-partition column after time)
		let value_col = result_df
			.get_column_names()
			.iter()
			.find(|&name| *name != subject_col && *name != "grid_time" && !name.ends_with("_right"))
			.map(|s| s.to_string())
			.unwrap_or_default();

		let nulls = if !value_col.is_empty() {
			subject_data
				.column(&value_col)
				.map(|c| c.null_count())
				.unwrap_or(0)
		} else {
			0
		};

		total_cells += cells;
		total_nulls += nulls;

		println!("    {:<20} {:>12} {:>12}", subject, cells, nulls);
	}

	println!("    {}", "─".repeat(48));
	println!("    {:<20} {:>12} {:>12}", "TOTALS:", total_cells, total_nulls);
}

/// Print full validation report.
pub fn print_validation_report(
	stats: &SignalPolicyStats,
	measurement_name: &str,
	subject_col: &str,
	time_col: &str,
	value_col: &str,
) {
	println!("\n================================================================================");
	println!("  SIGNAL POLICY VALIDATION: {}", measurement_name);
	println!("================================================================================");
	println!();
	println!("  CONFIGURATION:");
	println!("    measurement:        {}", measurement_name);
	println!("    subject_col:        {}", subject_col);
	println!("    time_col:           {}", time_col);
	println!("    value_col:          {}", value_col);
	println!("    ttl:                {} ms ({} seconds)", stats.ttl_ms, stats.ttl_ms / 1000);

	println!("\n  TIME RANGE:");
	println!("    duration:           {} ms", stats.duration_ms);

	println!("\n  GRID CALCULATION:");
	println!(
		"    grid_cells:         ceil({} ms / {} ms) = {} cells",
		stats.duration_ms, stats.ttl_ms, stats.grid_cells
	);
	println!("    subjects:           {}", stats.num_subjects);
	println!("    component_combos:   {}", stats.num_component_combos);
	println!(
		"    partitions:         {} subjects × {} combos = {}",
		stats.num_subjects, stats.num_component_combos, stats.partitions
	);
	println!(
		"    expected:           {} cells × {} partitions = {} observations",
		stats.grid_cells, stats.partitions, stats.expected_observations
	);

	println!("\n  RESULTS:");
	println!("    input_signals:      {}", stats.input_signals);
	println!("    actual_observations: {}", stats.actual_observations);
	println!("    valid_observations: {}", stats.valid_observations);
	println!("    null_observations:  {}", stats.null_observations);
	println!("    fill_rate:          {:.1}%", stats.fill_rate);

	// Validation
	println!("\n  VALIDATION:");
	if stats.is_grid_complete() {
		println!(
			"    ✅ Observation count: {} == {}",
			stats.actual_observations, stats.expected_observations
		);
	} else {
		println!(
			"    🚫 Observation count: {} != {} (diff: {})",
			stats.actual_observations,
			stats.expected_observations,
			(stats.actual_observations as i64 - stats.expected_observations as i64).abs()
		);
	}

	if stats.fill_rate >= 90.0 {
		println!("    ✅ Fill rate: {:.1}% (≥90%)", stats.fill_rate);
	} else if stats.fill_rate >= 50.0 {
		println!("    ⚠️  Fill rate: {:.1}% (50-90%)", stats.fill_rate);
	} else {
		println!("    🚫 Fill rate: {:.1}% (<50%)", stats.fill_rate);
	}

	println!();
	if stats.is_grid_complete() {
		println!("  ✅ PASS - Grid is complete");
	} else {
		println!("  🚫 FAIL - Grid mismatch");
	}
}

/// Print validation report using MeasurementUnit.
pub fn print_validation_report_with_measurement(
	stats: &SignalPolicyStats,
	measurement: &MeasurementUnit,
) {
	print_validation_report(
		stats,
		measurement.name.as_str(),
		measurement.subject.as_str(),
		measurement.time.as_str(),
		measurement.name.as_str(),
	);
}
