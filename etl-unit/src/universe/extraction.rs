//! Phase 1: Fragment Extraction
//!
//! Transforms source-level data into canonical fragments.

use polars::prelude::*;
use tracing::{debug, info, instrument};

use std::collections::HashMap;

use crate::{
	EtlError, MeasurementKind, MeasurementUnit, QualityUnit, TruthMapping,
	column::{CanonicalColumnName, SourceColumnName},
	error::EtlResult,
	schema::EtlSchema,
	signal_policy::WindowStrategy,
	source::{BoundSource, EtlUniverseBuildPlan},
	universe::fragment::{
		EtlUnitFragment, FragmentAccumulator, MeasurementFragment, QualityFragment,
	},
	universe::measurement_storage::{DeferredTransform, FragmentRef},
	unpivot::UnpivotConfig,
};

// =============================================================================
// Main Entry Point
// =============================================================================

/// Extract fragments from all sources in the plan.
#[instrument(skip(plan), fields(sources = plan.sources.len()))]
pub fn extract_all_fragments(plan: &EtlUniverseBuildPlan) -> EtlResult<FragmentAccumulator> {
	let mut accumulator = FragmentAccumulator::new();

	for source in &plan.sources {
		debug!(source = %source.name, "Extracting from source (always raw)");
		let fragments = extract_source_fragments(source, &plan.schema)?;
		accumulator.add_all(fragments);
	}

	Ok(accumulator)
}

// =============================================================================
// Measurement Batching
// =============================================================================

/// Key for grouping measurements into batches that share signal policy params.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct BatchKey {
	ttl_ms: i64,
	time_col: String,
	subject_col: String,
	/// Sorted component columns
	partition_cols: Vec<String>,
}

/// A batch of measurements that can share a single signal policy pass.
struct MeasurementBatch<'a> {
	/// (measurement, source_value_col, canonical_value_col)
	members: Vec<(&'a MeasurementUnit, String, String)>,
}

/// Plan measurement batches for a source: group compatible measurements.
///
/// Measurements are batchable if they share:
/// - Instant windowing strategy
/// - Same TTL value
/// - Same subject and time columns
/// - Same component (partition) columns
/// - No unpivot required
///
/// Returns `(batches, unbatched)` where unbatched measurements get individual
/// signal policy passes (non-Instant windowing, or columns not found in source).
fn plan_measurement_batches<'a>(
	schema: &'a EtlSchema,
	source: &BoundSource,
) -> (Vec<MeasurementBatch<'a>>, Vec<&'a MeasurementUnit>) {
	let mut batch_map: HashMap<BatchKey, MeasurementBatch<'a>> = HashMap::new();
	let mut unbatched: Vec<&'a MeasurementUnit> = Vec::new();

	for measurement in &schema.measurements {
		// Check if this measurement can be batched
		let batchable = match &measurement.signal_policy {
			Some(policy) => matches!(policy.windowing, WindowStrategy::Instant),
			None => false,
		};

		if !batchable {
			unbatched.push(measurement);
			continue;
		}

		// Check that source provides the necessary columns
		let value_source = match resolve_source_column(source, &measurement.value) {
			Some(s) => s.as_str().to_string(),
			None => {
				// Source doesn't provide this measurement
				continue;
			}
		};

		let subject_source = match resolve_source_column(source, &schema.subject) {
			Some(s) => s.as_str().to_string(),
			None => {
				unbatched.push(measurement);
				continue;
			}
		};

		let time_source = match resolve_source_column(source, &schema.time) {
			Some(s) => s.as_str().to_string(),
			None => {
				unbatched.push(measurement);
				continue;
			}
		};

		let policy = measurement.signal_policy.as_ref().unwrap();
		let ttl_ms = policy.ttl().as_millis() as i64;

		let mut partition_cols = vec![subject_source.clone()];
		for comp in &measurement.components {
			if let Some(comp_source) = resolve_source_column(source, comp) {
				partition_cols.push(comp_source.as_str().to_string());
			}
		}
		partition_cols.sort();

		let key = BatchKey {
			ttl_ms,
			time_col: time_source,
			subject_col: subject_source,
			partition_cols,
		};

		let batch = batch_map.entry(key.clone()).or_insert_with(|| MeasurementBatch {
			members: Vec::new(),
		});

		batch.members.push((measurement, value_source, measurement.name.as_str().to_string()));
	}

	let batches: Vec<MeasurementBatch<'a>> = batch_map.into_values().collect();
	(batches, unbatched)
}

/// Extract all fragments from a single source.
#[instrument(skip(source, schema), fields(source = %source.name))]
pub fn extract_source_fragments(
	source: &BoundSource,
	schema: &EtlSchema,
) -> EtlResult<Vec<EtlUnitFragment>> {
	let mut fragments = Vec::new();

	// 1. Plan measurement batches
	let (batches, unbatched) = plan_measurement_batches(schema, source);

	info!(
		measurements = schema.measurements.len(),
		batches = batches.len(),
		unbatched = unbatched.len(),
		"👉 Extracting measurements from source"
	);

	// 2. Extract each measurement individually (always raw — signal policy is lazy).
	for batch in &batches {
		for (measurement, _, _) in &batch.members {
			if let Some(fragment) = extract_measurement(source, measurement, schema)? {
				fragments.push(fragment.into());
			}
		}
	}

	// 3. Extract unbatched measurements individually.
	for measurement in &unbatched {
		if let Some(fragment) = extract_measurement(source, measurement, schema)? {
			fragments.push(fragment.into());
		}
	}

	// 4. Execute unpivots (structural — always applied).
	for unpivot_config in &source.unpivots {
		let fragment = execute_unpivot(source, unpivot_config, schema)?;

		// No signal policy during extraction — it's applied lazily via
		// MeasurementData::data_for(Apply) at subset time.
		fragments.push(fragment.into());
	}

	// 5. Extract qualities
	for quality in &schema.qualities {
		if let Some(fragment) = extract_quality(source, quality, schema)? {
			fragments.push(fragment.into());
		}
	}

	debug!(
		 source = %source.name,
		 count = fragments.len(),
		 "✅ Source extraction complete"
	);

	Ok(fragments)
}

// =============================================================================
// Measurement Extraction
// =============================================================================

/// Extract a measurement fragment from a source.
///
/// Returns `None` if the source doesn't provide this measurement.
#[instrument(
    skip(source, measurement, schema),
    fields(
        measurement = %measurement.name,
        source = %source.name,
        schema_subject = %schema.subject,
        schema_time = %schema.time,
        value_column = %measurement.value,
    )
)]
/// Extract a measurement fragment from a source (always raw).
///
/// Signal policy is NOT applied during extraction — it is applied
/// lazily at subset time via `MeasurementData::data_for(Apply)`.
pub fn extract_measurement(
	source: &BoundSource,
	measurement: &MeasurementUnit,
	schema: &EtlSchema,
) -> EtlResult<Option<MeasurementFragment>> {
	debug!(
		 measurement = ?measurement.etl_unit_signature(),
		 source_columns = ?source.dataframe_columns(),
		 source_mappings = ?source.columns.keys().map(|k| k.as_str()).collect::<Vec<_>>(),
		 unpivot_count = source.unpivots.len(),
		 "👉 Extracting measurement from source"
	);

	// Resolve value column (source → canonical)
	let value_source = match resolve_source_column(source, &measurement.value) {
		Some(s) => s,
		None => {
			debug!(
				 source = %source.name,
				 measurement = %measurement.name,
				 value_column = %measurement.value,
				 "🦀 Source does not provide column - no mapping found"
			);
			return Ok(None);
		}
	};
	// Resolve subject column
	let subject_source = match resolve_source_column(source, &schema.subject) {
		Some(s) => s,
		None => {
			debug!(
				 source = %source.name,
				 subject = %schema.subject,
				 "Source does not provide subject column"
			);
			return Ok(None);
		}
	};

	// Resolve time column
	let time_source = match resolve_source_column(source, &schema.time) {
		Some(s) => s,
		None => {
			debug!(
				 source = %source.name,
				 time = %schema.time,
				 "⏰ Source does not provide time column"
			);
			return Ok(None);
		}
	};

	// Resolve available components
	let mut components = Vec::new();
	let mut component_mappings = Vec::new();
	for comp in &measurement.components {
		if let Some(comp_source) = resolve_source_column(source, comp) {
			component_mappings.push((comp_source, comp.clone()));
			components.push(comp.clone());
		}
	}

	// Build column mappings for the ColumnRef
	use crate::universe::measurement_storage::{ColumnRefData, DataSourceName};
	use crate::column::SourceColumnName;

	let mut column_mappings = vec![
		(SourceColumnName::new(subject_source.as_str()), schema.subject.clone()),
		(SourceColumnName::new(time_source.as_str()), schema.time.clone()),
	];
	for (src, canon) in &component_mappings {
		column_mappings.push((SourceColumnName::new(src.as_str()), canon.clone()));
	}

	// Build deferred transform for shape-preserving operations.
	// These are applied lazily at materialization, not now.
	let transform = build_deferred_transform(measurement);

	let fragment_ref = FragmentRef::ColumnRef(ColumnRefData {
		source: std::sync::Arc::clone(&source.data),
		value_column: SourceColumnName::new(value_source.as_str()),
		canonical_name: measurement.name.clone(),
		source_name: DataSourceName::new(&source.name),
		column_mappings,
		transform,
	});

	debug!(
		 source_rows = source.data.height(),
		 components = components.len(),
		 has_transform = fragment_ref.has_transform(),
		 "Extracted measurement (ColumnRef)"
	);

	Ok(Some(MeasurementFragment::with_ref(
		measurement.name.clone(),
		&source.name,
		measurement.kind,
		components,
		fragment_ref,
	)))
}

// =============================================================================
// Deferred Transforms
// =============================================================================

/// Build a deferred transform for a measurement, if it needs one.
///
/// Returns `Some(DeferredTransform)` for measurements that need
/// shape-preserving transformations (binary truth mapping, null
/// substitution). These are applied lazily when the ColumnRef
/// is materialized, not during extraction.
///
/// Returns `None` for measurements that need no transformation.
fn build_deferred_transform(
	measurement: &MeasurementUnit,
) -> Option<DeferredTransform> {
	let needs_truth_mapping = measurement.kind == MeasurementKind::Binary
		&& measurement.truth_mapping.is_some();
	let needs_null_fill = measurement.null_value.is_some();

	if !needs_truth_mapping && !needs_null_fill {
		return None;
	}

	// Capture what we need in the closure
	let col_name = measurement.name.as_str().to_string();
	let truth_mapping = if needs_truth_mapping {
		measurement.truth_mapping.clone()
	} else {
		None
	};
	let null_value = measurement.null_value.clone();

	let label = match (needs_truth_mapping, needs_null_fill) {
		(true, true) => format!("{}: truth_mapping + null_fill", col_name),
		(true, false) => format!("{}: truth_mapping", col_name),
		(false, true) => format!("{}: null_fill", col_name),
		_ => unreachable!(),
	};

	Some(DeferredTransform::new(label, move |lf: LazyFrame| {
		let mut result = lf;

		// Apply truth mapping: convert source values to 0/1
		if let Some(ref mapping) = truth_mapping {
			let expr = build_truth_mapping_expr(&col_name, mapping);
			result = result.with_column(expr.alias(&col_name));
		}

		// Apply null fill: substitute nulls with default value
		if let Some(ref null_val) = null_value {
			let fill_expr: Expr = null_val.clone().into();
			result = result.with_column(
				col(&col_name).fill_null(fill_expr).alias(&col_name),
			);
		}

		result
	}))
}

// =============================================================================
// Quality Extraction
// =============================================================================

/// Extract a quality fragment from a source.
///
/// Returns `None` if the source doesn't provide this quality.
#[instrument(skip(source, quality, schema), fields(quality = %quality.name, source = %source.name))]
pub fn extract_quality(
	source: &BoundSource,
	quality: &QualityUnit,
	schema: &EtlSchema,
) -> EtlResult<Option<QualityFragment>> {
	// Resolve value column
	let value_source = match resolve_source_column(source, &quality.value) {
		Some(s) => s,
		None => {
			debug!("This source does not provide the quality");
			return Ok(None);
		}
	};

	// Resolve subject column
	let subject_source = match resolve_source_column(source, &schema.subject) {
		Some(s) => s,
		None => {
			debug!(
				 source = %source.name,
				 subject = %schema.subject,
				 "Source does not provide subject column"
			);
			return Ok(None);
		}
	};

	// Extract and deduplicate (qualities are time-invariant)
	let mut data = (*source.data)
		.clone()
		.lazy()
		.select([
			col(subject_source.as_str()).alias(schema.subject.as_str()),
			col(value_source.as_str()).alias(quality.name.as_str()),
		])
		.unique(Some(schema.subject.clone().into()), UniqueKeepStrategy::First)
		.collect()?;

	debug!(rows = data.height(), "Extracted quality");

	// Apply null value substitution
	if let Some(ref null_val) = quality.null_value {
		data = data
			.lazy()
			.with_column(
				col(quality.name.as_str())
					.fill_null(null_val.into_expr())
					.alias(quality.name.as_str()),
			)
			.collect()?;
	}

	Ok(Some(QualityFragment::new(quality.name.clone(), &source.name, data)))
}

// =============================================================================
// Unpivot Execution
// =============================================================================

/// Execute an unpivot to produce a measurement fragment.
#[instrument(skip(source, config, schema), fields(unpivot = %config.measurement_name(), source = %source.name))]
pub fn execute_unpivot(
	source: &BoundSource,
	config: &UnpivotConfig,
	schema: &EtlSchema,
) -> EtlResult<MeasurementFragment> {
	let subject_source = resolve_source_column(source, &schema.subject).ok_or_else(|| {
		EtlError::MissingColumn(format!(
			"Source '{}' has no mapping for subject column '{}'",
			source.name, schema.subject
		))
	})?;

	let time_source = resolve_source_column(source, &schema.time).ok_or_else(|| {
		EtlError::MissingColumn(format!(
			"Source '{}' has no mapping for time column '{}'",
			source.name, schema.time
		))
	})?;

	debug!(
		 subject = %subject_source,
		 time = %time_source,
		 "Executing unpivot"
	);

	let mut fragment = config.execute(&source.data, &source.name)?;

	// If the schema defines this measurement as Binary, apply truth mapping
	if let Some(measurement) = schema.get_measurement(config.measurement_name())
		&& measurement.kind == MeasurementKind::Binary
	{
		let df = fragment.materialize()?;
		let transformed_df = apply_truth_mapping(&df, measurement)?;
		fragment.fragment = FragmentRef::Materialized(transformed_df);
	}

	{
		let peek_df = fragment.materialize()?;
		debug!(
			 peek = ?peek_df.head(Some(5)),
			 "👀 Unpivot fragment data peek"
		);
	}

	// Report null counts by station
	if let Ok(null_counts) = fragment
		.materialize()?
		.clone()
		.lazy()
		.group_by([col("station_name")])
		.agg([
			col(PlSmallStr::from(&fragment.components[0]))
				.null_count()
				.alias("null_count"),
			col(PlSmallStr::from(&fragment.components[0]))
				.count()
				.alias("total_count"),
		])
		.collect()
	{
		debug!(
			 measurement = %fragment.unit_name,
			 component = %fragment.components[0],
			 null_by_station = ?null_counts,
			 "📊 Null counts by station after unpivot"
		);
	}

	Ok(fragment)
}

// =============================================================================
// Binary Truth Mapping
// =============================================================================

/// Apply truth mapping to convert source values to 0/1 for Binary measurements.
///
/// This enables proper Any/All aggregation semantics by normalizing values
/// to a consistent boolean representation (0 = false, 1 = true).
fn apply_truth_mapping(df: &DataFrame, measurement: &MeasurementUnit) -> EtlResult<DataFrame> {
	let col_name = measurement.name.as_str();

	let Some(mapping) = measurement.truth_mapping.as_ref() else {
		debug!(
			 measurement = %col_name,
			 "No explicit truth mapping configured, returning DataFrame unchanged"
		);
		return Ok(df.clone());
	};

	debug!(
		 measurement = %col_name,
		 true_values = ?mapping.true_values,
		 false_values = ?mapping.false_values,
		 "Applying binary truth mapping"
	);

	// Build the conversion expression based on the mapping
	let expr = build_truth_mapping_expr(col_name, mapping);

	let result = df
		.clone()
		.lazy()
		.with_column(expr.alias(col_name))
		.collect()?;

	debug!(
		 measurement = %col_name,
		 dtype = ?result.column(col_name)?.dtype(),
		 "Truth mapping applied"
	);

	Ok(result)
}

/// Build a Polars expression to convert values based on TruthMapping.
///
/// Returns an expression that maps:
/// - true_values → 1
/// - false_values → 0 (or anything not in true_values if false_values is None)
/// - other values → null (if both true and false values are specified)
fn build_truth_mapping_expr(col_name: &str, mapping: &TruthMapping) -> Expr {
	// If no true values defined, can't do mapping - return as-is
	if mapping.true_values.is_empty() {
		debug!(col = col_name, "No true values defined, keeping original");
		return col(col_name);
	}

	// Build the is_true check
	let true_check = build_is_in_expr(col_name, &mapping.true_values);

	if let Some(ref false_values) = mapping.false_values {
		// Explicit false values: true_values → 1, false_values → 0, else → null
		let false_check = build_is_in_expr(col_name, false_values);

		when(true_check)
			.then(lit(1i32))
			.when(false_check)
			.then(lit(0i32))
			.otherwise(lit(NULL))
	} else {
		// No explicit false values: true_values → 1, everything else → 0
		when(true_check).then(lit(1i32)).otherwise(lit(0i32))
	}
}

/// Build an is_in expression for a list of JSON values.
fn build_is_in_expr(col_name: &str, values: &[serde_json::Value]) -> Expr {
	// Group values by type for proper comparison
	let mut string_values: Vec<String> = Vec::new();
	let mut int_values: Vec<i64> = Vec::new();
	let mut float_values: Vec<f64> = Vec::new();
	let mut bool_values: Vec<bool> = Vec::new();

	for v in values {
		match v {
			serde_json::Value::String(s) => string_values.push(s.clone()),
			serde_json::Value::Number(n) => {
				if let Some(i) = n.as_i64() {
					int_values.push(i);
				} else if let Some(f) = n.as_f64() {
					float_values.push(f);
				}
			}
			serde_json::Value::Bool(b) => bool_values.push(*b),
			_ => {}
		}
	}

	// Build OR expression for all value types
	let mut checks: Vec<Expr> = Vec::new();

	if !string_values.is_empty() {
		let series = Series::new("_check".into(), string_values);
		// Use lit(series).implode() to avoid deprecation warning
		checks.push(
			col(col_name)
				.cast(DataType::String)
				.is_in(lit(series).implode(), false),
		);
	}

	if !int_values.is_empty() {
		let series = Series::new("_check".into(), int_values);
		checks.push(
			col(col_name)
				.cast(DataType::Int64)
				.is_in(lit(series).implode(), false),
		);
	}

	if !float_values.is_empty() {
		let series = Series::new("_check".into(), float_values);
		checks.push(
			col(col_name)
				.cast(DataType::Float64)
				.is_in(lit(series).implode(), false),
		);
	}

	if !bool_values.is_empty() {
		for b in bool_values {
			checks.push(col(col_name).eq(lit(b)));
		}
	}

	// Combine all checks with OR
	if checks.is_empty() {
		lit(false)
	} else {
		checks.into_iter().reduce(|a, b| a.or(b)).unwrap()
	}
}

// =============================================================================
// Helpers
// =============================================================================

/// Resolve a canonical column name to its source column name via BoundSource mapping.
///
/// Looks up the canonical column in the BoundSource's column mappings to find
/// the corresponding source column name that exists in the DataFrame.
///
/// # Arguments
/// * `source` - The BoundSource containing column mappings and the DataFrame
/// * `canonical` - The canonical column name to resolve
///
/// # Returns
/// * `Some(SourceColumnName)` - The mapped source column name if:
///   1. An explicit mapping exists in source.columns, AND
///   2. The mapped source column exists in the DataFrame
/// * `None` - If no mapping exists or the mapped column doesn't exist in the DataFrame
fn resolve_source_column<'b>(
	source: &'b BoundSource,
	canonical: &CanonicalColumnName,
) -> Option<&'b SourceColumnName> {
	// Check explicit binding
	let source_col = source.get_source_column(canonical)?;

	// Verify the bound column actually exists in the DataFrame
	if source.data.column(source_col.as_str()).is_ok() {
		Some(source_col)
	} else {
		None
	}
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
	use super::*;
	use crate::unit::MeasurementKind;

	fn make_schema() -> EtlSchema {
		EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("value", MeasurementKind::Measure)
			.quality("name")
			.build()
			.unwrap()
	}

	fn make_source_df() -> DataFrame {
		df! {
			 "station" => ["A", "B"],
			 "timestamp" => [100i64, 200],
			 "value" => [1.0, 2.0],
			 "name" => ["Station A", "Station B"]
		}
		.unwrap()
	}

	#[test]
	fn test_extract_measurement() {
		let schema = make_schema();
		let df = make_source_df();
		let source = BoundSource::identity("test", df, &schema);

		let measurement = schema.get_measurement("value").unwrap();
		let fragment = extract_measurement(&source, measurement, &schema)
			.unwrap()
			.unwrap();

		assert_eq!(fragment.unit_name.as_str(), "value");
		assert_eq!(fragment.source_name, "test");
		assert_eq!(fragment.fragment.height(), 2);
	}

	#[test]
	fn test_extract_quality() {
		let schema = make_schema();
		let df = make_source_df();
		let source = BoundSource::identity("test", df, &schema);

		let quality = schema.get_quality("name").unwrap();
		let fragment = extract_quality(&source, quality, &schema).unwrap().unwrap();

		assert_eq!(fragment.unit_name.as_str(), "name");
		assert_eq!(fragment.data.height(), 2);
	}

	#[test]
	fn test_extract_missing_returns_none() {
		let schema = make_schema();
		let df = df! {
			 "station" => ["A"],
			 "timestamp" => [100i64]
			 // missing "value" column
		}
		.unwrap();
		let source = BoundSource::identity("test", df, &schema);

		let measurement = schema.get_measurement("value").unwrap();
		let result = extract_measurement(&source, measurement, &schema).unwrap();

		assert!(result.is_none());
	}

	#[test]
	fn test_binary_truth_mapping_numeric() {
		let schema = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("engine_on", MeasurementKind::Binary)
			.build()
			.unwrap();

		let df = df! {
			 "station" => ["A", "A", "B", "B"],
			 "timestamp" => [100i64, 200, 100, 200],
			 "engine_on" => [1, 0, 1, 1]
		}
		.unwrap();

		let source = BoundSource::identity("test", df, &schema);
		let measurement = schema.get_measurement("engine_on").unwrap();
		let fragment = extract_measurement(&source, measurement, &schema)
			.unwrap()
			.unwrap();

		// Values should be preserved as 1/0
		let frag_df = fragment.materialize().unwrap();
		let values: Vec<i32> = frag_df
			.column("engine_on")
			.unwrap()
			.i32()
			.unwrap()
			.into_iter()
			.map(|v| v.unwrap())
			.collect();

		assert_eq!(values, vec![1, 0, 1, 1]);
	}

	#[test]
	fn test_binary_truth_mapping_strings() {
		let schema = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("engine_status", MeasurementKind::Binary)
			.with_true_values(["on", "running"])
			.with_false_values(["off", "stopped"])
			.build()
			.unwrap();

		let df = df! {
			 "station" => ["A", "A", "B", "B"],
			 "timestamp" => [100i64, 200, 100, 200],
			 "engine_status" => ["on", "off", "running", "stopped"]
		}
		.unwrap();

		let source = BoundSource::identity("test", df, &schema);
		let measurement = schema.get_measurement("engine_status").unwrap();
		let fragment = extract_measurement(&source, measurement, &schema)
			.unwrap()
			.unwrap();

		// Values should be converted to 1/0
		let frag_df = fragment.materialize().unwrap();
		let values: Vec<i32> = frag_df
			.column("engine_status")
			.unwrap()
			.i32()
			.unwrap()
			.into_iter()
			.map(|v| v.unwrap())
			.collect();

		assert_eq!(values, vec![1, 0, 1, 0]);
	}

	#[test]
	fn test_binary_truth_mapping_implicit_false() {
		// Without explicit false values, anything not "true" is false
		let schema = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("active", MeasurementKind::Binary)
			.with_true_values(["yes", "Y", "1"])
			.build()
			.unwrap();

		let df = df! {
			 "station" => ["A", "A", "B", "B", "C"],
			 "timestamp" => [100i64, 200, 100, 200, 100],
			 "active" => ["yes", "no", "Y", "N", "maybe"]
		}
		.unwrap();

		let source = BoundSource::identity("test", df, &schema);
		let measurement = schema.get_measurement("active").unwrap();
		let fragment = extract_measurement(&source, measurement, &schema)
			.unwrap()
			.unwrap();

		let frag_df = fragment.materialize().unwrap();
		let values: Vec<i32> = frag_df
			.column("active")
			.unwrap()
			.i32()
			.unwrap()
			.into_iter()
			.map(|v| v.unwrap())
			.collect();

		// "yes" and "Y" are true (1), everything else is false (0)
		assert_eq!(values, vec![1, 0, 1, 0, 0]);
	}
}
