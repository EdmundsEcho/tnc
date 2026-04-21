//! Diagnostic tests for group_by_dynamic behavior
//!
//! Run with: cargo test -p synapse-etl-unit test_group_by_dynamic_diagnostic -- --nocapture

#[cfg(test)]
mod group_by_dynamic_tests {
	use polars::prelude::*;

	/// Test basic group_by_dynamic behavior with known data
	#[test]
	fn test_group_by_dynamic_diagnostic() {
		// Create test data: 10 signals over 90 seconds
		// With TTL=30s, we expect 3 grid cells: [0-30), [30-60), [60-90)
		let timestamps_ms: Vec<i64> = vec![
			0,      // Cell 0
			10_000, // Cell 0
			20_000, // Cell 0
			35_000, // Cell 1
			40_000, // Cell 1
			50_000, // Cell 1
			65_000, // Cell 2
			70_000, // Cell 2
			80_000, // Cell 2
			85_000, // Cell 2
		];
		let values: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];

		let df = df! {
			"station" => vec!["A"; 10],
			"timestamp" => &timestamps_ms,
			"value" => &values
		}
		.unwrap();

		// Cast to Datetime
		let df = df
			.lazy()
			.with_column(col("timestamp").cast(DataType::Datetime(TimeUnit::Milliseconds, None)))
			.collect()
			.unwrap();

		println!("=== INPUT DATA ===");
		println!("Shape: {:?}", df.shape());
		println!("Schema: {:?}", df.schema());
		println!("{}", df);

		let time_col = df.column("timestamp").unwrap();
		println!("\nTime column dtype: {:?}", time_col.dtype());
		println!("First timestamp: {:?}", time_col.get(0));
		println!("Last timestamp: {:?}", time_col.get(df.height() - 1));

		// Test different Duration formats
		println!("\n=== DURATION PARSING ===");
		let dur_30s = Duration::parse("30s");
		let dur_30000ms = Duration::parse("30000ms");
		let dur_30000000us = Duration::parse("30000000us");

		println!("Duration::parse(\"30s\") = {:?}", dur_30s);
		println!("Duration::parse(\"30000ms\") = {:?}", dur_30000ms);
		println!("Duration::parse(\"30000000us\") = {:?}", dur_30000000us);

		// Test group_by_dynamic with different configurations
		println!("\n=== TEST 1: group_by_dynamic with 30s Duration ===");
		let result1 = df
			.clone()
			.lazy()
			.group_by_dynamic(
				col("timestamp"),
				vec![],
				DynamicGroupOptions {
					index_column:       "timestamp".into(),
					every:              Duration::parse("30s"),
					period:             Duration::parse("30s"),
					offset:             Duration::parse("0s"),
					closed_window:      ClosedWindow::Left,
					label:              Label::Left,
					include_boundaries: true, // Show boundaries for debugging
					start_by:           StartBy::DataPoint,
				},
			)
			.agg([col("value").mean().alias("value_mean"), col("value").count().alias("signal_count")])
			.collect();

		match result1 {
			Ok(result) => {
				println!("SUCCESS! Output shape: {:?}", result.shape());
				println!("{}", result);
			}
			Err(e) => {
				println!("ERROR: {:?}", e);
			}
		}

		// Test 2: Try with ms duration string
		println!("\n=== TEST 2: group_by_dynamic with 30000ms Duration ===");
		let result2 = df
			.clone()
			.lazy()
			.group_by_dynamic(
				col("timestamp"),
				vec![],
				DynamicGroupOptions {
					index_column:       "timestamp".into(),
					every:              Duration::parse("30000ms"),
					period:             Duration::parse("30000ms"),
					offset:             Duration::parse("0ms"),
					closed_window:      ClosedWindow::Left,
					label:              Label::Left,
					include_boundaries: true,
					start_by:           StartBy::DataPoint,
				},
			)
			.agg([col("value").mean().alias("value_mean"), col("value").count().alias("signal_count")])
			.collect();

		match result2 {
			Ok(result) => {
				println!("SUCCESS! Output shape: {:?}", result.shape());
				println!("{}", result);
			}
			Err(e) => {
				println!("ERROR: {:?}", e);
			}
		}

		// Test 3: Use Default for options
		println!("\n=== TEST 3: group_by_dynamic with Default options ===");
		let result3 = df
			.clone()
			.lazy()
			.group_by_dynamic(
				col("timestamp"),
				vec![],
				DynamicGroupOptions {
					every: Duration::parse("30s"),
					period: Duration::parse("30s"),
					..Default::default()
				},
			)
			.agg([col("value").mean().alias("value_mean"), col("value").count().alias("signal_count")])
			.collect();

		match result3 {
			Ok(result) => {
				println!("SUCCESS! Output shape: {:?}", result.shape());
				println!("{}", result);
			}
			Err(e) => {
				println!("ERROR: {:?}", e);
			}
		}

		// Test 4: start_by variants
		println!("\n=== TEST 4: group_by_dynamic with StartBy::WindowBound ===");
		let result4 = df
			.clone()
			.lazy()
			.group_by_dynamic(
				col("timestamp"),
				vec![],
				DynamicGroupOptions {
					index_column:       "timestamp".into(),
					every:              Duration::parse("30s"),
					period:             Duration::parse("30s"),
					offset:             Duration::parse("0s"),
					closed_window:      ClosedWindow::Left,
					label:              Label::Left,
					include_boundaries: true,
					start_by:           StartBy::WindowBound,
				},
			)
			.agg([col("value").mean().alias("value_mean"), col("value").count().alias("signal_count")])
			.collect();

		match result4 {
			Ok(result) => {
				println!("SUCCESS! Output shape: {:?}", result.shape());
				println!("{}", result);
			}
			Err(e) => {
				println!("ERROR: {:?}", e);
			}
		}

		// Expected results:
		// Cell [0-30): values 1, 2, 3 → mean = 2.0, count = 3
		// Cell [30-60): values 4, 5, 6 → mean = 5.0, count = 3
		// Cell [60-90): values 7, 8, 9, 10 → mean = 8.5, count = 4
		println!("\n=== EXPECTED ===");
		println!("3 rows with:");
		println!("  [0-30ms):  mean=2.0, count=3");
		println!("  [30-60ms): mean=5.0, count=3");
		println!("  [60-90ms): mean=8.5, count=4");
	}

	/// Test with actual millisecond epoch timestamps (realistic data)
	#[test]
	fn test_group_by_dynamic_epoch_timestamps() {
		// Simulate real timestamps: epoch milliseconds for Dec 10, 2025
		// Base: 2025-12-10 00:00:00 UTC = 1765324800000 ms
		let base_ts: i64 = 1765324800000;

		let timestamps_ms: Vec<i64> = vec![
			base_ts,
			base_ts + 10_000, // +10s
			base_ts + 20_000, // +20s
			base_ts + 35_000, // +35s
			base_ts + 40_000, // +40s
			base_ts + 50_000, // +50s
			base_ts + 65_000, // +65s
			base_ts + 70_000, // +70s
			base_ts + 80_000, // +80s
			base_ts + 85_000, // +85s
		];
		let values: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];

		let df = df! {
			"station" => vec!["A"; 10],
			"timestamp" => &timestamps_ms,
			"value" => &values
		}
		.unwrap();

		// Cast to Datetime
		let df = df
			.lazy()
			.with_column(col("timestamp").cast(DataType::Datetime(TimeUnit::Milliseconds, None)))
			.collect()
			.unwrap();

		println!("=== EPOCH TIMESTAMP TEST ===");
		println!("Base timestamp: {} ms", base_ts);
		println!("Input shape: {:?}", df.shape());
		println!("{}", df);

		let result = df
			.clone()
			.lazy()
			.group_by_dynamic(
				col("timestamp"),
				vec![],
				DynamicGroupOptions {
					index_column:       "timestamp".into(),
					every:              Duration::parse("30s"),
					period:             Duration::parse("30s"),
					offset:             Duration::parse("0s"),
					closed_window:      ClosedWindow::Left,
					label:              Label::Left,
					include_boundaries: true,
					start_by:           StartBy::DataPoint,
				},
			)
			.agg([col("value").mean().alias("value_mean"), col("value").count().alias("signal_count")])
			.collect();

		match result {
			Ok(result) => {
				println!("SUCCESS! Output shape: {:?}", result.shape());
				println!("{}", result);

				// Verify we got 3 or 4 rows (depending on boundary handling)
				assert!(
					result.height() >= 3 && result.height() <= 4,
					"Expected 3-4 grid cells, got {}",
					result.height()
				);
			}
			Err(e) => {
				println!("ERROR: {:?}", e);
				panic!("group_by_dynamic failed: {:?}", e);
			}
		}
	}

	/// Test with partition column (mimics real usage with station_name)
	#[test]
	fn test_group_by_dynamic_with_partition() {
		let timestamps_ms: Vec<i64> = vec![
			// Station A
			0, 10_000, 20_000, 35_000, 40_000, // Station B
			0, 15_000, 45_000, 50_000, 75_000,
		];
		let stations: Vec<&str> = vec!["A", "A", "A", "A", "A", "B", "B", "B", "B", "B"];
		let values: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 20.0, 30.0, 40.0, 50.0];

		let df = df! {
			"station" => &stations,
			"timestamp" => &timestamps_ms,
			"value" => &values
		}
		.unwrap()
		.lazy()
		.with_column(col("timestamp").cast(DataType::Datetime(TimeUnit::Milliseconds, None)))
		.sort(["station", "timestamp"], SortMultipleOptions::default())
		.collect()
		.unwrap();

		println!("=== PARTITION TEST ===");
		println!("Input: {:?}", df.shape());
		println!("{}", df);

		// Use partition column in group_by
		let result = df
			.clone()
			.lazy()
			.group_by_dynamic(
				col("timestamp"),
				vec![col("station")], // Group by station
				DynamicGroupOptions {
					index_column:       "timestamp".into(),
					every:              Duration::parse("30s"),
					period:             Duration::parse("30s"),
					offset:             Duration::parse("0s"),
					closed_window:      ClosedWindow::Left,
					label:              Label::Left,
					include_boundaries: true,
					start_by:           StartBy::DataPoint,
				},
			)
			.agg([col("value").mean().alias("value_mean"), col("value").count().alias("signal_count")])
			.collect();

		match result {
			Ok(result) => {
				println!("SUCCESS! Output shape: {:?}", result.shape());
				println!("{}", result);
			}
			Err(e) => {
				println!("ERROR: {:?}", e);
				panic!("group_by_dynamic with partition failed: {:?}", e);
			}
		}
	}

	/// Test what happens if data is not sorted
	#[test]
	fn test_group_by_dynamic_unsorted_data() {
		// Intentionally unsorted timestamps
		let timestamps_ms: Vec<i64> = vec![50_000, 10_000, 80_000, 20_000, 65_000];
		let values: Vec<f64> = vec![5.0, 1.0, 8.0, 2.0, 6.0];

		let df = df! {
			"station" => vec!["A"; 5],
			"timestamp" => &timestamps_ms,
			"value" => &values
		}
		.unwrap()
		.lazy()
		.with_column(col("timestamp").cast(DataType::Datetime(TimeUnit::Milliseconds, None)))
		.collect()
		.unwrap();

		println!("=== UNSORTED DATA TEST ===");
		println!("Input (unsorted): {:?}", df.shape());
		println!("{}", df);

		let result = df
			.clone()
			.lazy()
			.group_by_dynamic(
				col("timestamp"),
				vec![],
				DynamicGroupOptions {
					index_column:       "timestamp".into(),
					every:              Duration::parse("30s"),
					period:             Duration::parse("30s"),
					offset:             Duration::parse("0s"),
					closed_window:      ClosedWindow::Left,
					label:              Label::Left,
					include_boundaries: true,
					start_by:           StartBy::DataPoint,
				},
			)
			.agg([col("value").mean().alias("value_mean"), col("value").count().alias("signal_count")])
			.collect();

		match result {
			Ok(result) => {
				println!("SUCCESS (unsorted)! Output shape: {:?}", result.shape());
				println!("{}", result);
			}
			Err(e) => {
				println!("ERROR (unsorted): {:?}", e);
				println!("This may be expected - group_by_dynamic requires sorted data");
			}
		}

		// Now try with sorted data
		println!("\n--- After sorting ---");
		let df_sorted = df
			.lazy()
			.sort(["timestamp"], SortMultipleOptions::default())
			.collect()
			.unwrap();

		println!("Input (sorted): {:?}", df_sorted.shape());
		println!("{}", df_sorted);

		let result_sorted = df_sorted
			.clone()
			.lazy()
			.group_by_dynamic(
				col("timestamp"),
				vec![],
				DynamicGroupOptions {
					index_column:       "timestamp".into(),
					every:              Duration::parse("30s"),
					period:             Duration::parse("30s"),
					offset:             Duration::parse("0s"),
					closed_window:      ClosedWindow::Left,
					label:              Label::Left,
					include_boundaries: true,
					start_by:           StartBy::DataPoint,
				},
			)
			.agg([col("value").mean().alias("value_mean"), col("value").count().alias("signal_count")])
			.collect();

		match result_sorted {
			Ok(result) => {
				println!("SUCCESS (sorted)! Output shape: {:?}", result.shape());
				println!("{}", result);
			}
			Err(e) => {
				println!("ERROR (sorted): {:?}", e);
			}
		}
	}
}
