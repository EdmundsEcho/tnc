//! SubsetExecutor: extracts SubsetUniverse views from a Universe.
//!
//! # Role in the Pipeline
//!
//! The SubsetExecutor consumes a Universe and an EtlUnitSubsetRequest to produce
//! a SubsetUniverse. The Universe stores measurements separately in HashMaps;
//! composition into a single DataFrame happens here at request time.
//!
//! ## Execution Flow
//!
//! ```text
//! EtlUnitSubsetRequest
//!         │
//!         ▼
//! ┌─────────────────────┐
//! │  Preprocess Request │  (strip component filters - always crush)
//! └─────────────────────┘
//!         │
//!         ▼
//! ┌─────────────────────┐
//! │  Universe::subset() │  (compose, resample, join, derivations)
//! └─────────────────────┘
//!         │
//!         ▼
//! ┌─────────────────────┐
//! │  Synthetic Subjects │  (optional aggregated subjects)
//! └─────────────────────┘
//!         │
//!         ▼
//!    SubsetUniverse
//! ```
//!
//! # Component Handling
//!
//! Components are always crushed (aggregated out) during subset composition.
//! Any component filters in the request are ignored. This simplifies the
//! composition logic and avoids null-introduction from joining measurements
//! with different component structures.
//!
//! # Usage
//!
//! ```rust,ignore
//! // Build universe once (expensive)
//! let universe = UniverseBuilder::build(&plan)?;
//!
//! // Query many times (cheap-ish, composes on demand)
//! let subset = SubsetExecutor::get_subset(&universe, &request, None)?;
//!
//! // Or use Universe method directly
//! let subset = universe.subset(&request)?;
//! ```

use meta_tracing::{MetaCollector, record_input_rows, record_output_rows};
use polars::prelude::*;
use serde_json::json;
use tracing::{debug, instrument};

use crate::{
	CanonicalColumnName, MeasurementKind,
	aggregation::{Aggregate, SyntheticSubject},
	error::EtlResult,
	request::EtlUnitSubsetRequest,
	schema::EtlSchema,
	subset::SubsetUniverse,
	universe::Universe,
};

/// Extracts SubsetUniverse views from a Universe.
pub struct SubsetExecutor;

impl SubsetExecutor {
	/// Execute multiple subset requests against a universe.
	#[instrument(skip(universe, requests, collector), fields(request_count = requests.len()))]
	pub fn execute(
		universe: &Universe,
		requests: &[EtlUnitSubsetRequest],
		collector: Option<&mut MetaCollector>,
	) -> EtlResult<Vec<SubsetUniverse>> {
		debug!("Executing {} subset requests", requests.len());
		for req in requests {
			debug!(request = ?req, "EtlUnitSubsetRequest.");
		}

		let results: EtlResult<Vec<SubsetUniverse>> = requests
			.iter()
			.map(|req| Self::get_subset(universe, req, None))
			.collect();

		if let Some(c) = collector
			&& let Ok(ref subsets) = results
		{
			let total_rows: usize = subsets.iter().map(|s| s.info.row_count).sum();
			c.set_output_rows(total_rows);
			c.add_section(
				"batch_execute",
				&json!({
					"request_count": requests.len(),
					"total_output_rows": total_rows,
				}),
			);
		}

		results
	}

	/// Execute a single subset request against a universe.
	#[instrument(skip(universe, request, collector), fields(
		has_interval = request.interval.is_some(),
		has_time_range = request.time_range.is_some(),
		measurement_count = request.measurements.len(),
	))]
	pub fn get_subset(
		universe: &Universe,
		request: &EtlUnitSubsetRequest,
		mut collector: Option<&mut MetaCollector>,
	) -> EtlResult<SubsetUniverse> {
		let schema = universe.schema();
		let subject_col = schema.subject.as_str();
		let time_col = schema.time.as_str();

		debug!(request = ?request, "EtlUnitSubsetRequest.");

		// Record schema info
		if let Some(ref mut c) = collector {
			c.add_section(
				"schema",
				&json!({
					"name": &schema.name,
					"subject_column": subject_col,
					"time_column": time_col,
				}),
			);
		}

		// WARN: Temporary hack
		// Preprocess request: strip component filters (we always crush)
		let sanitized_request = Self::preprocess_request(request);

		// Record request info
		if let Some(ref mut c) = collector
			&& request.component_filters.is_some()
		{
			c.add_section(
				"preprocessing",
				&json!({
					"component_filters_stripped": true,
					"reason": "Components are always crushed during composition",
				}),
			);
		}

		// Delegate to Universe::subset() for core composition
		let mut subset =
			universe.subset_with_mode(&sanitized_request, crate::SignalPolicyMode::Apply)?;

		// Record composition result
		let post_composition_rows = subset.data.height();
		record_input_rows(collector.as_deref_mut(), post_composition_rows);

		if let Some(ref mut c) = collector {
			c.add_section(
				"composition",
				&json!({
					"rows": post_composition_rows,
					"measurements": sanitized_request.measurements.iter().map(|m| m.as_str()).collect::<Vec<_>>(),
					"qualities": sanitized_request.qualities.iter().map(|q| q.as_str()).collect::<Vec<_>>(),
				}),
			);
		}

		// Create synthetic subjects if requested
		if request.has_synthetic_subjects() {
			let rows_before = subset.data.height();

			let measurement_names: Vec<CanonicalColumnName> = subset
				.measurements
				.iter()
				.map(|m| m.column.clone())
				.collect();

			subset.data = Self::create_synthetic_subjects(
				schema,
				subset.data,
				&request.synthetic_subjects,
				&measurement_names,
			)?;

			// Update info
			subset.info.row_count = subset.data.height();
			subset.info.subject_count = subset
				.data
				.column(subject_col)
				.map(|c| c.n_unique().unwrap_or(0))
				.unwrap_or(0);

			if let Some(ref mut c) = collector {
				c.add_section(
					"synthetic_subjects",
					&json!({
						"count": request.synthetic_subjects.len(),
						"rows_before": rows_before,
						"rows_after": subset.data.height(),
					}),
				);
			}
		}

		// Record final output
		record_output_rows(collector.as_deref_mut(), subset.data.height());

		if let Some(ref mut c) = collector {
			c.add_section(
				"result",
				&json!({
					"row_count": subset.data.height(),
					"subject_count": subset.info.subject_count,
					"measurement_count": subset.measurements.len(),
					"quality_count": subset.qualities.len(),
				}),
			);
		}

		debug!(
			rows = subset.data.height(),
			measurements = subset.measurements.len(),
			qualities = subset.qualities.len(),
			"Subset complete"
		);

		Ok(subset)
	}

	// =========================================================================
	// Request Preprocessing
	// =========================================================================

	/// Preprocess a request by stripping component filters.
	///
	/// Components are always crushed during composition, so component filters
	/// are ignored. This creates a sanitized copy of the request.
	fn preprocess_request(request: &EtlUnitSubsetRequest) -> EtlUnitSubsetRequest {
		let mut sanitized = request.clone();

		// Strip component filters - we always crush all components
		if sanitized.component_filters.is_some() {
			debug!("Stripping component filters from request (components always crushed)");
			sanitized.component_filters = None;
		}

		// Strip component references from synthetic subjects
		for synthetic in &mut sanitized.synthetic_subjects {
			if !synthetic.component_filters.is_empty() {
				debug!(
					synthetic = %synthetic.name_pattern,
					"Stripping component filters from synthetic subject"
				);
				synthetic.component_filters.clear();
			}
			if synthetic.group_by_component.is_some() {
				debug!(
					synthetic = %synthetic.name_pattern,
					"Stripping group_by_component from synthetic subject"
				);
				synthetic.group_by_component = None;
			}
		}

		sanitized
	}

	// =========================================================================
	// Synthetic Subjects
	// =========================================================================

	/// Create synthetic subjects by aggregating across real subjects.
	fn create_synthetic_subjects(
		schema: &EtlSchema,
		df: DataFrame,
		synthetic_subjects: &[SyntheticSubject],
		measurement_names: &[CanonicalColumnName],
	) -> EtlResult<DataFrame> {
		let mut result = df;

		for synthetic in synthetic_subjects {
			result = Self::create_single_synthetic(
				schema,
				result,
				synthetic,
				&schema.subject,
				&schema.time,
				measurement_names,
			)?;
		}

		Ok(result)
	}

	/// Create a single synthetic subject.
	fn create_single_synthetic(
		schema: &EtlSchema,
		df: DataFrame,
		synthetic: &SyntheticSubject,
		subject_col: &CanonicalColumnName,
		time_col: &CanonicalColumnName,
		measurement_names: &[CanonicalColumnName],
	) -> EtlResult<DataFrame> {
		// Build aggregation expressions for each measurement
		let mut agg_exprs: Vec<Expr> = Vec::new();
		for col_name in measurement_names {
			let aggregate = synthetic.get_aggregate(col_name);
			if let Some(agg) = aggregate {
				let kind = schema
					.get_measurement_kind(col_name)
					.unwrap_or(MeasurementKind::Measure);
				let resolved_agg = if *agg == Aggregate::Auto {
					Aggregate::resolve_auto(kind)
				} else {
					*agg
				};
				let agg_expr = Self::build_agg_expr(col_name.as_str(), resolved_agg);
				agg_exprs.push(agg_expr);
			}
		}

		if agg_exprs.is_empty() {
			return Ok(df);
		}

		// Group by time only (aggregate across all subjects)
		let group_cols: Vec<Expr> = vec![col(time_col.as_str())];

		// Add quality grouping if specified
		let mut group_cols = group_cols;
		if let Some(ref quality_col) = synthetic.group_by_quality {
			group_cols.push(col(quality_col.as_str()));
		}

		// Group and aggregate
		let aggregated = df
			.clone()
			.lazy()
			.group_by(group_cols)
			.agg(agg_exprs)
			.collect()?;

		// Add synthetic subject name
		let with_subject =
			Self::add_synthetic_subject_name(aggregated, synthetic, subject_col.as_str())?;

		// Reorder columns to match original DataFrame for concat
		let original_cols: Vec<String> = df
			.get_column_names()
			.iter()
			.map(|s| s.to_string())
			.collect();

		let select_exprs: Vec<Expr> = original_cols
			.iter()
			.filter(|c| with_subject.column(c.as_str()).is_ok())
			.map(|c| col(c.as_str()))
			.collect();

		let reordered = with_subject.lazy().select(select_exprs).collect()?;

		// Union with original data
		concat(vec![df.lazy(), reordered.lazy()], UnionArgs::default())?
			.collect()
			.map_err(Into::into)
	}

	/// Add the synthetic subject name column.
	fn add_synthetic_subject_name(
		df: DataFrame,
		synthetic: &SyntheticSubject,
		subject_col: &str,
	) -> EtlResult<DataFrame> {
		let pattern = &synthetic.name_pattern;

		// Check for quality grouping that needs name expansion
		if let Some(ref quality_col) = synthetic.group_by_quality {
			// Build name expression with quality value
			let name_expr = Self::build_name_expansion_expr(pattern, Some(quality_col));
			df.lazy()
				.with_column(name_expr.alias(subject_col))
				.collect()
				.map_err(Into::into)
		} else {
			// Simple case: use pattern as-is
			df.lazy()
				.with_column(lit(pattern.clone()).alias(subject_col))
				.collect()
				.map_err(Into::into)
		}
	}

	/// Build an expression that expands a name pattern with column values.
	fn build_name_expansion_expr(pattern: &str, quality_col: Option<&String>) -> Expr {
		let mut result_pattern = pattern.to_string();

		if let Some(quality) = quality_col {
			// Replace {quality} placeholder
			if result_pattern.contains("{quality}") {
				result_pattern = result_pattern.replace("{quality}", "\x00QUALITY\x00");
			}
			// Replace named placeholder like {zone}
			let named_placeholder = format!("{{{}}}", quality);
			if result_pattern.contains(&named_placeholder) {
				result_pattern = result_pattern.replace(&named_placeholder, "\x00QUALITY\x00");
			}
		}

		if !result_pattern.contains("\x00QUALITY\x00") {
			return lit(pattern.to_string());
		}

		// Build concat expression
		let mut parts: Vec<Expr> = Vec::new();
		let mut remaining = result_pattern.as_str();

		while !remaining.is_empty() {
			if let Some(pos) = remaining.find("\x00QUALITY\x00") {
				if pos > 0 {
					parts.push(lit(remaining[..pos].to_string()));
				}
				if let Some(quality) = quality_col {
					parts.push(col(quality.as_str()).cast(DataType::String));
				}
				remaining = &remaining[pos + "\x00QUALITY\x00".len()..];
			} else {
				if !remaining.is_empty() {
					parts.push(lit(remaining.to_string()));
				}
				break;
			}
		}

		if parts.len() == 1 {
			parts.remove(0)
		} else {
			concat_str(parts, "", false)
		}
	}

	/// Build aggregation expression for a column.
	fn build_agg_expr(col_name: &str, agg: Aggregate) -> Expr {
		match agg {
			Aggregate::Mean => col(col_name).mean().alias(col_name),
			Aggregate::Sum => col(col_name).sum().alias(col_name),
			Aggregate::Min => col(col_name).min().alias(col_name),
			Aggregate::Max => col(col_name).max().alias(col_name),
			Aggregate::Any => col(col_name).max().alias(col_name),
			Aggregate::All => col(col_name).min().alias(col_name),
			Aggregate::Count => col(col_name).count().alias(col_name),
			Aggregate::First => col(col_name).first().alias(col_name),
			Aggregate::Last => col(col_name).last().alias(col_name),
			_ => col(col_name).mean().alias(col_name),
		}
	}
}
