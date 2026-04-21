//! Instant Signal Policy
//!
//! The Instant policy transforms irregular time series data into a regular grid
//! of observations. Each grid cell spans a TTL-sized window, and all signals
//! within that window are aggregated to produce a single observation.
//!
//! # Core Concepts
//!
//! - **Grid**: A regular series of time points spaced at TTL intervals
//! - **Grid Cell**: A TTL-duration window assigned to each grid point
//! - **Observation**: The result of aggregating all signals in a grid cell (N of 1)
//! - **Signal**: A raw data point from the source
//!
//! # How It Works
//!
//! 1. **Truncate**: Timestamps are truncated to grid cell boundaries
//! 2. **Aggregate**: Simple group_by on truncated time + partitions
//! 3. **Complete Grid**: Cross join of time grid × unique partitions
//! 4. **Fill**: Left join ensures all cells exist (missing = null)
//!
//! # Example
//!
//! Given signals at irregular intervals with TTL = 30 seconds:
//!
//! ```text
//! Time:     0s    10s   20s   35s   40s   75s
//! Signals:  3     5     4     7     8     9
//!
//! Grid cells (TTL = 30s):
//!   [0s-30s)  → signals: 3, 5, 4  → mean: 4.0
//!   [30s-60s) → signals: 7, 8    → mean: 7.5
//!   [60s-90s) → signals: 9       → mean: 9.0
//!
//! Output: 3 observations at t=0s, t=30s, t=60s
//! ```
//!
//! # Grid Size Invariant
//!
//! For a given time range and TTL:
//! ```text
//! grid_points = ceil(time_range / TTL)
//! ```
//!
//! Every measurement with the same time bounds and TTL produces the same
//! number of grid cells, ensuring consistent observation counts across
//! measurements. Some cells may be null (no signals), but the grid structure
//! is deterministic.

use polars::prelude::*;
use tracing::{debug, info, instrument};

use super::time_range::extract_time_range_from_parts;
use crate::{EtlError, EtlResult, MeasurementKind, MeasurementUnit};

// =============================================================================
// Public Entry Point (from_parts)
// =============================================================================

/// Apply Instant policy using explicit parameters (no MeasurementUnit required).
///
/// This is the low-level API useful for tests and callers without a MeasurementUnit.
/// Uses mean aggregation (suitable for Measure kind).
///
/// # Arguments
///
/// * `df` - Source DataFrame with raw signal data
/// * `time_col` - Name of the timestamp column (must be Datetime type)
/// * `value_col` - Name of the value column to aggregate
/// * `partition_cols` - Columns that define partitions (subject + components)
/// * `ttl_ms` - Grid cell size in milliseconds
/// * `time_format` - Optional time format for parsing String timestamps
///
/// # Returns
///
/// DataFrame with regular time grid where each row is one observation.
#[instrument(skip(df), fields(input_rows = df.height()))]
pub(crate) fn apply_instant_policy_from_parts(
	df: DataFrame,
	time_col: &str,
	value_col: &str,
	partition_cols: &[&str],
	ttl_ms: i64,
	time_format: Option<&str>,
) -> EtlResult<DataFrame> {
	debug!(
		time_col = time_col,
		value_col = value_col,
		partition_cols = ?partition_cols,
		ttl_ms = ttl_ms,
		time_format = ?time_format,
		"Applying instant policy from parts"
	);

	// 1. Extract time range (pass time_format for String columns)
	let time_range = extract_time_range_from_parts(&df, time_col, time_format)?;

	debug!(
		start_ts = time_range.start_ts,
		end_ts = time_range.end_ts,
		duration_ms = time_range.duration_ms,
		"Time range extracted"
	);

	// 2. If time column is String, we need to parse it before truncation
	let df_with_parsed_time = ensure_datetime_column(&df, time_col, time_format)?;

	// 3. Truncate timestamps to grid cell boundaries
	let df_with_grid_time = truncate_to_grid(&df_with_parsed_time, time_col, ttl_ms)?;

	// 4. Aggregate using simple group_by (mean aggregation)
	let mut group_cols: Vec<Expr> = vec![col("grid_time")];
	group_cols.extend(partition_cols.iter().map(|c| col(*c)));

	let aggregated = df_with_grid_time
		.lazy()
		.group_by(group_cols)
		.agg([col(value_col).mean().alias(value_col)])
		.collect()?;

	debug!(aggregated_rows = aggregated.height(), "Aggregated signals to grid cells");

	// 5. Create the complete time grid
	let grid = create_time_grid(time_range.start_ts, time_range.end_ts, ttl_ms);

	debug!(grid_cells = grid.len(), first = grid.first(), last = grid.last(), "Generated time grid");

	// Match the timezone of the aggregated grid_time column (may be UTC or None).
	let grid_tz = match aggregated.column("grid_time").map(|c| c.dtype().clone()) {
		Ok(DataType::Datetime(_, tz)) => tz,
		_ => None,
	};

	// Build grid using ChunkedArray to preserve timezone (polars 0.51 .cast() drops tz)
	let grid_ca =
		Int64Chunked::new("grid_time".into(), &grid).into_datetime(TimeUnit::Milliseconds, grid_tz);
	let grid_df = DataFrame::new(vec![grid_ca.into_column()])?;

	// 6–9. Per-partition reindex: join grid with each partition's data independently
	// to avoid the peak memory spike of a full cross-join (G × N).
	// `from_parts` does not know the measurement's TTL (it's a low-level
	// helper used by tests that exercise the grid machinery directly), so
	// no TTL forward-fill is applied. Tests assert on the raw grid shape.
	let result = per_partition_reindex(
		&grid_df, &aggregated, partition_cols, "grid_time", None,
	)?;

	debug!(
		output_rows = result.height(),
		output_cols = result.width(),
		"Instant policy from parts complete"
	);

	Ok(result)
}

// =============================================================================
// Instant Policy Entry Point
// =============================================================================

/// Apply Instant policy: aggregate signals into TTL-sized grid cells.
///
/// # Algorithm
///
/// 1. **Truncate**: Add `grid_time` column by truncating timestamps to TTL boundaries
/// 2. **Aggregate**: Simple `group_by([grid_time, ...partitions])` with mean/max
/// 3. **Create Grid**: Generate complete time grid × all unique partitions (cross join)
/// 4. **Fill**: Left join grid with aggregated data (missing cells = null)
///
/// This approach is robust because:
/// - No reliance on `group_by_dynamic` with `StartBy::DataPoint`
/// - Signals anywhere within a cell map to the same grid time
/// - Complete grid guarantees consistent observation counts
///
/// # Arguments
///
/// * `df` - Source DataFrame with irregular time series data
/// * `measurement` - Measurement unit containing policy configuration
///
/// # Returns
///
/// DataFrame with regular time grid where each row is one observation
/// (the aggregation of all signals in a TTL window).
#[instrument(skip(df, measurement), fields(
    measurement = %measurement.name,
    input_rows = df.height(),
))]
pub(crate) fn apply_instant_policy(
	df: DataFrame,
	measurement: &MeasurementUnit,
) -> EtlResult<DataFrame> {
	info!("🟢🟢 Now dispatched to instant policy fn '{}'", measurement.name);
	let policy = measurement
		.signal_policy
		.as_ref()
		.ok_or_else(|| EtlError::SignalPolicy("Instant policy called without policy".into()))?;

	// Extract column names
	let time_col = measurement.time.as_str();
	let value_col = measurement.name.as_str();
	let subject_col = measurement.subject.as_str();

	// Build partition columns: subject + components
	let mut partition_cols: Vec<&str> = vec![subject_col];
	for comp in &measurement.components {
		partition_cols.push(comp.as_str());
	}

	// Grid interval: the measurement's declared native sample rate.
	// Schema validation guarantees this is always set.
	let grid_interval_ms = measurement.sample_rate_ms.ok_or_else(|| {
		EtlError::Config(format!(
			"Measurement '{}' has no sample_rate. \
			 Every measurement must declare a sample_rate in its TOML config.",
			measurement.name,
		))
	})?;
	let ttl_ms = policy.ttl().as_millis() as i64;

	// Get time_format from signal policy
	let time_format = policy.time_format.as_deref();

	// Determine aggregation based on measurement kind
	let measurement_kind = measurement.kind;

	debug!(
		time_col = time_col,
		value_col = value_col,
		partition_cols = ?partition_cols,
		grid_interval_ms = grid_interval_ms,
		ttl_ms = ttl_ms,
		sample_rate_ms = ?measurement.sample_rate_ms,
		time_format = ?time_format,
		measurement_kind = ?measurement_kind,
		"🟣 Applying instant policy"
	);

	// 1. Extract time range (pass time_format for String columns)
	let time_range = extract_time_range_from_parts(&df, time_col, time_format)?;

	debug!(
		start_ts = time_range.start_ts,
		end_ts = time_range.end_ts,
		duration_ms = time_range.duration_ms,
		"...Time range extracted"
	);

	// 2. If time column is String, we need to parse it before truncation
	let df_with_parsed_time = ensure_datetime_column(&df, time_col, time_format)?;

	// 3. Truncate timestamps to grid cell boundaries (at native sample rate)
	let df_with_grid_time = truncate_to_grid(&df_with_parsed_time, time_col, grid_interval_ms)?;

	// 4. Aggregate using simple group_by
	let mut group_cols: Vec<Expr> = vec![col("grid_time")];
	group_cols.extend(partition_cols.iter().map(|c| col(*c)));

	// Build aggregation expression based on measurement kind
	let agg_expr = match measurement_kind {
		MeasurementKind::Binary => col(value_col).max().alias(value_col),
		_ => col(value_col).mean().alias(value_col),
	};

	let aggregated = df_with_grid_time
		.lazy()
		.group_by(group_cols)
		.agg([agg_expr])
		.collect()?;

	debug!(aggregated_rows = aggregated.height(), "Aggregated signals to grid cells");

	// 5. Create the complete time grid (at native sample rate)
	let grid = create_time_grid(time_range.start_ts, time_range.end_ts, grid_interval_ms);

	debug!(grid_cells = grid.len(), first = grid.first(), last = grid.last(), "Generated time grid");

	// Match the timezone of the aggregated grid_time column (may be UTC or None).
	let grid_tz = match aggregated.column("grid_time").map(|c| c.dtype().clone()) {
		Ok(DataType::Datetime(_, tz)) => tz,
		_ => None,
	};

	// Build grid using ChunkedArray to preserve timezone (polars 0.51 .cast() drops tz)
	let grid_ca =
		Int64Chunked::new("grid_time".into(), &grid).into_datetime(TimeUnit::Milliseconds, grid_tz);
	let grid_df = DataFrame::new(vec![grid_ca.into_column()])?;

	// TTL-aware validity window.
	//
	// An observation at grid time `t` is valid for `[t, t + TTL)`. For a
	// grid at cadence `grid_interval_ms`, the cells covered by that
	// observation (including `t` itself) span `ceil(TTL / grid_interval_ms)`.
	// The number of *additional* cells after `t` that remain within the
	// observation's validity is `floor((TTL - 1) / grid_interval_ms)`.
	//
	// Example: sample_rate = 60s, TTL = 90s  →  floor(89/60) = 1 cell forward.
	//          sample_rate = 60s, TTL = 60s  →  floor(59/60) = 0 (no carry).
	//          sample_rate = 60s, TTL = 121s →  floor(120/60) = 2 cells forward.
	let ttl_forward_fill_limit: Option<u32> = if ttl_ms > 0 && grid_interval_ms > 0 {
		let n = ((ttl_ms - 1) / grid_interval_ms) as u32;
		if n > 0 { Some(n) } else { None }
	} else {
		None
	};

	// 6–9. Per-partition reindex: join grid with each partition's data independently
	// to avoid the peak memory spike of a full cross-join (G × N).
	// The TTL forward-fill is applied inside this step, per partition, so
	// validity never leaks across (subject, components) boundaries.
	let reindexed = per_partition_reindex(
		&grid_df, &aggregated, &partition_cols, "grid_time", ttl_forward_fill_limit,
	)?;

	// Rename grid_time back to the original time column name and sort
	let mut sort_cols = vec![time_col.to_string()];
	sort_cols.extend(partition_cols.iter().map(|s| s.to_string()));

	let result = reindexed
		.lazy()
		.rename(["grid_time"], [time_col], true)
		.sort(sort_cols, SortMultipleOptions::default())
		.collect()?;

	debug!(
		output_rows = result.height(),
		output_cols = result.width(),
		ttl_forward_fill_limit = ?ttl_forward_fill_limit,
		"Instant policy complete"
	);

	Ok(result)
}

// =============================================================================
// Batched Instant Policy
// =============================================================================

/// Apply instant signal policy to multiple value columns at once.
///
/// All columns must share the same TTL, subject/time columns, and partition
/// structure. This avoids redundant truncate + group_by + reindex passes
/// when multiple measurements share the same grid parameters.
///
/// Each `(col_name, kind)` pair specifies a value column and its measurement
/// kind (which determines aggregation: mean for Measure, max for Binary).
///
/// NOTE: Not yet used. Signal policy is currently applied per-measurement
/// during subset, not batched during extraction. This batched approach
/// would improve performance when multiple measurements share the same
/// grid parameters. Revisit when signal policy per-subset becomes a
/// performance bottleneck.
#[instrument(skip(df), fields(input_rows = df.height(), value_cols = value_cols.len()))]
pub(crate) fn apply_instant_policy_batched(
	df: DataFrame,
	time_col: &str,
	value_cols: &[(&str, MeasurementKind)],
	partition_cols: &[&str],
	ttl_ms: i64,
	time_format: Option<&str>,
) -> EtlResult<DataFrame> {
	if value_cols.is_empty() {
		return Ok(df);
	}

	debug!(
		time_col = time_col,
		value_cols = ?value_cols.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
		partition_cols = ?partition_cols,
		ttl_ms = ttl_ms,
		"Applying batched instant policy"
	);

	// 1. Extract time range
	let time_range = extract_time_range_from_parts(&df, time_col, time_format)?;

	// 2. Ensure datetime column
	let df_with_parsed_time = ensure_datetime_column(&df, time_col, time_format)?;

	// 3. Truncate to grid
	let df_with_grid_time = truncate_to_grid(&df_with_parsed_time, time_col, ttl_ms)?;

	// 4. Build multi-column aggregation in one group_by pass
	let mut group_cols: Vec<Expr> = vec![col("grid_time")];
	group_cols.extend(partition_cols.iter().map(|c| col(*c)));

	let agg_exprs: Vec<Expr> = value_cols
		.iter()
		.map(|(name, kind)| match kind {
			MeasurementKind::Binary => col(*name).max().alias(*name),
			_ => col(*name).mean().alias(*name),
		})
		.collect();

	let aggregated = df_with_grid_time
		.lazy()
		.group_by(group_cols)
		.agg(agg_exprs)
		.collect()?;

	debug!(aggregated_rows = aggregated.height(), "Batched aggregation complete");

	// 5. Create time grid
	let grid = create_time_grid(time_range.start_ts, time_range.end_ts, ttl_ms);

	let grid_tz = match aggregated.column("grid_time").map(|c| c.dtype().clone()) {
		Ok(DataType::Datetime(_, tz)) => tz,
		_ => None,
	};

	let grid_ca =
		Int64Chunked::new("grid_time".into(), &grid).into_datetime(TimeUnit::Milliseconds, grid_tz);
	let grid_df = DataFrame::new(vec![grid_ca.into_column()])?;

	// 6. Per-partition reindex
	// Batched path is unused today; no per-measurement TTL is threaded in,
	// so we pass None. Revisit if this path is wired into production.
	let reindexed = per_partition_reindex(
		&grid_df, &aggregated, partition_cols, "grid_time", None,
	)?;

	// 7. Rename grid_time back to original time column and sort
	let mut sort_cols = vec![time_col.to_string()];
	sort_cols.extend(partition_cols.iter().map(|s| s.to_string()));

	let result = reindexed
		.lazy()
		.rename(["grid_time"], [time_col], true)
		.sort(sort_cols, SortMultipleOptions::default())
		.collect()?;

	debug!(
		output_rows = result.height(),
		output_cols = result.width(),
		"Batched instant policy complete"
	);

	Ok(result)
}

// =============================================================================
// Per-Partition Reindex
// =============================================================================

/// Reindex aggregated data onto a complete time grid, one partition at a time.
///
/// Instead of `cross_join(grid, all_partitions)` which allocates `G × N` rows
/// up front, this iterates each unique partition (station), left-joins the grid
/// with just that partition's data (`G × 1`), adds the partition column values
/// as literals, and vconcat's the results.
///
/// Output is identical to the cross-join approach but peak memory drops from
/// `G × N` to `G × 1`.
fn per_partition_reindex(
	grid_df: &DataFrame,
	aggregated: &DataFrame,
	partition_cols: &[&str],
	grid_time_col: &str,
	ttl_forward_fill_limit: Option<u32>,
) -> EtlResult<DataFrame> {
	// Split aggregated data by partition columns
	let partition_col_strs: Vec<PlSmallStr> =
		partition_cols.iter().map(|c| PlSmallStr::from(*c)).collect();
	let partition_groups = aggregated.partition_by(partition_col_strs.clone(), true)?;

	debug!(
		partition_count = partition_groups.len(),
		grid_size = grid_df.height(),
		ttl_forward_fill_limit = ?ttl_forward_fill_limit,
		"Per-partition reindex"
	);

	let mut results: Vec<DataFrame> = Vec::with_capacity(partition_groups.len());

	for group in &partition_groups {
		// Extract this partition's key values from first row
		let partition_values: Vec<(&str, AnyValue<'static>)> = partition_cols
			.iter()
			.map(|&pc| {
				let c = group.column(pc).expect("partition column must exist");
				(pc, c.get(0).expect("group must have rows").into_static())
			})
			.collect();

		// Select only grid_time + value columns (drop partition cols for the join)
		let value_cols: Vec<&str> = group
			.get_column_names_str()
			.into_iter()
			.filter(|c| *c != grid_time_col && !partition_cols.contains(c))
			.collect();

		let mut select_exprs = vec![col(grid_time_col)];
		select_exprs.extend(value_cols.iter().map(|c| col(*c)));

		let group_data = group.clone().lazy().select(select_exprs).collect()?;

		// Left join: grid × this partition's data. Ensure the result is
		// sorted by grid_time so the downstream forward-fill propagates
		// values in chronological order within this partition.
		let mut joined_lf = grid_df
			.clone()
			.lazy()
			.join(
				group_data.lazy(),
				[col(grid_time_col)],
				[col(grid_time_col)],
				JoinArgs::new(JoinType::Left),
			)
			.sort([grid_time_col], SortMultipleOptions::default());

		// Add partition columns as literal values
		for (name, val) in &partition_values {
			let dtype = group.column(name).unwrap().dtype().clone();
			let scalar = Scalar::new(dtype, val.clone());
			joined_lf = joined_lf.with_column(
				lit(scalar).alias(*name),
			);
		}

		let mut partition_df = joined_lf.collect()?;

		// TTL-aware forward-fill within this partition.
		//
		// An observation at grid time `t` has validity `[t, t + TTL)` per the
		// signal-policy contract. For a grid at cadence `sample_rate`, that
		// observation covers at most `floor((TTL - 1) / sample_rate)` cells
		// beyond its own. We implement the validity window by forward-filling
		// null value cells with the most recent non-null value, capped at
		// that many consecutive fills.
		//
		// This runs per partition (each (subject, ...components) independently),
		// which is exactly what "validity lives with the observation" requires —
		// we never carry an engine=1 observation across into engine=2.
		if let Some(limit) = ttl_forward_fill_limit {
			if limit > 0 {
				partition_df =
					partition_df.fill_null(FillNullStrategy::Forward(Some(limit)))?;
			}
		}

		results.push(partition_df);
	}

	if results.is_empty() {
		// No data: build an empty grid with all columns
		let mut empty = grid_df.clone();
		for &pc in partition_cols {
			if let Ok(c) = aggregated.column(pc) {
				let empty_col = c.clear();
				empty.with_column(empty_col)?;
			}
		}
		// Add empty value columns
		for c in aggregated.get_column_names_str() {
			if c != grid_time_col && !partition_cols.contains(&c) {
				if let Ok(col) = aggregated.column(c) {
					empty.with_column(col.clear())?;
				}
			}
		}
		return Ok(empty.slice(0, 0));
	}

	// Vertically concatenate all partition results
	let mut sort_cols = vec![grid_time_col.to_string()];
	sort_cols.extend(partition_cols.iter().map(|s| s.to_string()));

	let lfs: Vec<LazyFrame> = results.into_iter().map(|df| df.lazy()).collect();
	let result = concat(
		lfs.as_slice(),
		UnionArgs {
			parallel: true,
			rechunk: false,
			to_supertypes: true,
			..Default::default()
		},
	)?
	.sort(sort_cols, SortMultipleOptions::default())
	.collect()?;

	Ok(result)
}

// =============================================================================
// Grid Functions
// =============================================================================

/// Ensure the time column is Datetime type, parsing from String if necessary.
///
/// If the column is already Datetime, returns the DataFrame unchanged.
/// If the column is String and time_format is provided, parses it.
/// If the column is String and no time_format, returns an error.
fn ensure_datetime_column(
	df: &DataFrame,
	time_col: &str,
	time_format: Option<&str>,
) -> EtlResult<DataFrame> {
	let series = df.column(time_col).map_err(|e| {
		EtlError::DataProcessing(format!("Time column '{}' missing: {}", time_col, e))
	})?;

	match series.dtype() {
		DataType::Datetime(..) => {
			// Already Datetime, no conversion needed
			Ok(df.clone())
		}
		DataType::String => {
			// Need to parse String to Datetime
			let fmt = time_format.ok_or_else(|| {
				EtlError::DataProcessing(format!(
					"Column '{}' is String but no time_format was provided for parsing",
					time_col
				))
			})?;

			debug!(
				time_col = time_col,
				time_format = fmt,
				"Parsing string time column to datetime for grid truncation"
			);

			let options = StrptimeOptions {
				format: Some(PlSmallStr::from_str(fmt)),
				strict: false,
				..Default::default()
			};

			df.clone()
				.lazy()
				.with_column(
					col(time_col)
						.str()
						.to_datetime(Some(TimeUnit::Milliseconds), None, options, lit("raise"))
						.alias(time_col),
				)
				.collect()
				.map_err(|e| {
					EtlError::DataProcessing(format!(
						"Failed to parse time column '{}' with format '{}': {}",
						time_col, fmt, e
					))
				})
		}
		other => Err(EtlError::DataProcessing(format!(
			"Unsupported time column type for grid truncation: {:?}",
			other
		))),
	}
}

/// Truncate timestamps to grid cell boundaries.
///
/// This is the key to reliable grid alignment. By truncating BEFORE aggregation,
/// we ensure all signals within a cell map to the same grid time, regardless
/// of where they fall within the cell.
///
/// # Example with ttl_ms = 60_000 (1 minute):
/// ```text
/// 00:00:00 → 00:00:00 (at boundary)
/// 00:00:30 → 00:00:00 (middle of cell)
/// 00:00:59 → 00:00:00 (end of cell)
/// 00:01:00 → 00:01:00 (next cell)
/// 00:01:45 → 00:01:00
/// ```
fn truncate_to_grid(df: &DataFrame, time_col: &str, ttl_ms: i64) -> PolarsResult<DataFrame> {
	let interval_ms_str = format!("{}ms", ttl_ms);
	df.clone()
		.lazy()
		.with_column(
			col(time_col)
				.dt()
				.truncate(lit(interval_ms_str))
				.alias("grid_time"),
		)
		.collect()
}

/// Create a complete time grid from start to end with given interval.
///
/// Grid is aligned to TTL boundaries (truncated to TTL).
///
/// # Grid Size Formula
/// ```text
/// grid_cells = ceil(duration_ms / ttl_ms)
/// ```
fn create_time_grid(start_ms: i64, end_ms: i64, ttl_ms: i64) -> Vec<i64> {
	if ttl_ms <= 0 {
		return Vec::new();
	}

	let mut grid = Vec::new();

	// Align grid start to TTL boundary (truncate)
	let aligned_start = (start_ms / ttl_ms) * ttl_ms;
	let mut current = aligned_start;

	while current <= end_ms {
		grid.push(current);
		current += ttl_ms;
	}

	grid
}
