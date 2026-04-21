//! Synthetic data tests for group_by_dynamic behavior
//!
//! Tests grid generation with subjects and components using synthetic data.
//! No API dependency required.
//!
//! Run with: cargo test -p synapse-etl-unit --test group_by_dynamic_synthetic_tests -- --nocapture
//!
//! Note: Tests use #[serial] to ensure sequential execution for clean output.

use etl_unit::SignalPolicy;
use polars::prelude::*;
use serial_test::serial;

// =============================================================================
// Validation Test - Guaranteed Complete Coverage (100% fill rate)
// =============================================================================

/// This test generates synthetic data with GUARANTEED complete coverage:
/// - Every partition has at least 1 signal in every grid cell
/// - Different partitions have different signal counts (1-5 signals per cell)
/// - Result: 100% fill rate, zero null observations
///
/// This is the ONLY test where we can assert both:
/// 1. `actual_observations == expected_observations`
/// 2. `null_observations == 0`
///
/// Setup:
/// - 2 subjects (warehouses): WH-A, WH-B
/// - 1 component (zone): Zone 1, Zone 2, Zone 3
/// - partitions = 2 warehouses × 3 zones = 6 partitions
/// - time_span = 5 minutes = 300,000 ms
/// - ttl = 60,000 ms (1 minute)
/// - grid_cells = ceil(300000/60000) = 5 cells
/// - expected_observations = 5 cells × 6 partitions = 30 observations
/// - expected_nulls = 0
#[test]
#[serial]
fn test_guaranteed_complete_coverage() {
	println!("\n");
	println!("================================================================================");
	println!("  TEST: Guaranteed Complete Coverage (100% Fill Rate)");
	println!("================================================================================");
	println!();
	println!("  Every partition has at least 1 signal in every grid cell.");
	println!("  This guarantees 100% fill rate and zero null observations.");
	println!();

	// Configuration
	let base_ts: i64 = 1765324800000;
	let ttl_ms: i64 = 60_000; // 1 minute per cell

	// Grid: 5 cells covering [0, 300000) ms
	// Cell 0: [0, 60000)
	// Cell 1: [60000, 120000)
	// Cell 2: [120000, 180000)
	// Cell 3: [180000, 240000)
	// Cell 4: [240000, 300000)

	// Dimensions
	let warehouses = ["WH-A", "WH-B"];
	let zones = ["Zone 1", "Zone 2", "Zone 3"];

	let num_subjects = warehouses.len();
	let num_component_combos = zones.len();
	let partitions = num_subjects * num_component_combos; // 2 × 3 = 6
	let grid_cells = 5; // ceil(300000/60000) = 5
	let expected_observations = grid_cells * partitions; // 5 × 6 = 30

	println!("  CONFIGURATION:");
	println!("    base_ts:              {} ms", base_ts);
	println!("    ttl:                  {} ms (1 minute)", ttl_ms);
	println!("    grid_cells:           {} cells", grid_cells);
	println!();
	println!("  DIMENSIONS:");
	println!("    warehouses (subjects): {} {:?}", warehouses.len(), warehouses);
	println!("    zones (component):     {} {:?}", zones.len(), zones);
	println!(
		"    partitions:            {} × {} = {} partitions",
		num_subjects, num_component_combos, partitions
	);
	println!();
	println!("  EXPECTED:");
	println!(
		"    observations:          {} cells × {} partitions = {} observations",
		grid_cells, partitions, expected_observations
	);
	println!("    null_observations:     0 (guaranteed coverage)");
	println!("    fill_rate:             100.0%");
	println!();

	// Generate synthetic data with GUARANTEED coverage
	// Each partition gets a different number of signals per cell, but always ≥1
	let mut timestamps: Vec<i64> = Vec::new();
	let mut warehouse_col: Vec<&str> = Vec::new();
	let mut zone_col: Vec<&str> = Vec::new();
	let mut temperature: Vec<f64> = Vec::new();

	// Signal pattern per partition (signals per cell)
	// Total signals varies, but every cell has at least 1
	// Only 5 cells now (0-4), not 6
	let signal_patterns: Vec<(&str, &str, Vec<usize>)> = vec![
		// (warehouse, zone, [signals in cell 0, cell 1, cell 2, cell 3, cell 4])
		("WH-A", "Zone 1", vec![1, 1, 1, 1, 1]), // 5 signals total (minimum)
		("WH-A", "Zone 2", vec![3, 2, 1, 2, 3]), // 11 signals total
		("WH-A", "Zone 3", vec![5, 5, 5, 5, 5]), // 25 signals total (dense)
		("WH-B", "Zone 1", vec![2, 1, 3, 1, 2]), // 9 signals total
		("WH-B", "Zone 2", vec![1, 2, 2, 2, 1]), // 8 signals total
		("WH-B", "Zone 3", vec![4, 3, 2, 1, 2]), // 12 signals total
	];

	println!("  SIGNAL DISTRIBUTION PER PARTITION:");
	println!("    {:<8} {:<8} {:^21} {:>6}", "Subject", "Zone", "Signals/Cell [0-4]", "Total");
	println!("    ──────── ──────── ───────────────────── ──────");

	let mut total_signals = 0;
	for (warehouse, zone, signals_per_cell) in &signal_patterns {
		let partition_total: usize = signals_per_cell.iter().sum();
		total_signals += partition_total;

		// Format the signals per cell compactly
		let cells_str = signals_per_cell
			.iter()
			.map(|n| n.to_string())
			.collect::<Vec<_>>()
			.join(", ");

		println!("    {:<8} {:<8} [{:^19}] {:>6}", warehouse, zone, cells_str, partition_total);

		// Generate signals for each cell
		// IMPORTANT: Signals can land ANYWHERE within the cell (not just boundaries)
		// The truncate_to_grid function handles alignment
		for (cell_idx, &num_signals) in signals_per_cell.iter().enumerate() {
			let cell_start = cell_idx as i64 * ttl_ms;

			for signal_idx in 0..num_signals {
				// Spread signals throughout the cell (middle, not boundaries)
				// This proves the truncation approach works regardless of signal position
				let offset_within_cell = if num_signals == 1 {
					ttl_ms / 2 // Single signal in middle of cell
				} else {
					// Multiple signals: spread evenly, starting from 10% into cell
					let usable_range = ttl_ms - 2000; // Leave 1s margin at each end
					1000 + (signal_idx as i64 * usable_range) / (num_signals as i64)
				};

				let ts = base_ts + cell_start + offset_within_cell;
				timestamps.push(ts);
				warehouse_col.push(warehouse);
				zone_col.push(zone);
				// Temperature varies by cell and signal for variety
				temperature.push(20.0 + (cell_idx as f64) + (signal_idx as f64 * 0.1));
			}
		}
	}
	println!("    ──────── ──────── ───────────────────── ──────");
	println!("    {:>8} {:>8} {:>21} {:>6}", "", "", "TOTAL:", total_signals);
	println!();

	// Create DataFrame
	let df = df! {
		"warehouse" => &warehouse_col,
		"zone" => &zone_col,
		"timestamp" => &timestamps,
		"temperature" => &temperature
	}
	.unwrap()
	.lazy()
	.with_column(col("timestamp").cast(DataType::Datetime(TimeUnit::Milliseconds, None)))
	.sort(["warehouse", "zone", "timestamp"], SortMultipleOptions::default())
	.collect()
	.unwrap();

	println!("  INPUT DATAFRAME: {:?}", df.shape());
	println!("{}", df.head(Some(12)));
	println!();

	// Apply signal policy with complete grid (uses library's SignalPolicy::apply_from_parts)
	let result = SignalPolicy::apply_from_parts(
		df.clone(),
		"timestamp",
		"temperature",
		&["warehouse", "zone"],
		ttl_ms,
		None,
	)
	.unwrap();

	println!("  RESULT DATAFRAME: {:?}", result.shape());
	println!("{}", result);
	println!();

	// Count observations
	let actual_observations = result.height();
	let valid_count = result
		.column("temperature")
		.map(|c| c.len() - c.null_count())
		.unwrap_or(0);
	let null_count = actual_observations - valid_count;
	let fill_rate = valid_count as f64 / actual_observations as f64 * 100.0;

	println!("  VALIDATION:");
	println!("    expected_observations: {}", expected_observations);
	println!("    actual_observations:   {}", actual_observations);
	println!("    valid_observations:    {}", valid_count);
	println!("    null_observations:     {}", null_count);
	println!("    fill_rate:             {:.1}%", fill_rate);
	println!();

	// Verify grid uniformity per partition
	println!("  GRID CELLS PER PARTITION:");
	let partition_counts = result
		.clone()
		.lazy()
		.group_by([col("warehouse"), col("zone")])
		.agg([
			col("grid_time").count().alias("grid_cells"),
			col("temperature").count().alias("valid_cells"),
		])
		.sort(["warehouse", "zone"], SortMultipleOptions::default())
		.collect()
		.unwrap();
	println!("{}", partition_counts);

	// Assertions
	println!();
	let obs_ok = actual_observations == expected_observations;
	let null_ok = null_count == 0;
	let fill_ok = (fill_rate - 100.0).abs() < 0.01;

	if obs_ok {
		println!("  ✅ Observation count: {} == {}", actual_observations, expected_observations);
	} else {
		println!("  🚫 Observation count: {} != {}", actual_observations, expected_observations);
	}

	if null_ok {
		println!("  ✅ Null observations: {} == 0", null_count);
	} else {
		println!("  🚫 Null observations: {} != 0", null_count);
	}

	if fill_ok {
		println!("  ✅ Fill rate: {:.1}% == 100.0%", fill_rate);
	} else {
		println!("  🚫 Fill rate: {:.1}% != 100.0%", fill_rate);
	}

	assert_eq!(
		actual_observations, expected_observations,
		"Observation count mismatch: got {} but expected {}",
		actual_observations, expected_observations
	);
	assert_eq!(null_count, 0, "Expected 0 null observations but got {}", null_count);
}
