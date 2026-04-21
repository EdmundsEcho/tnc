//! Phase 5: Universe Assembly
//!
//! Assembles the final Universe with metadata.
//!
//! ## Type Signature
//!
//! ```text
//! assemble_universe: (
//!     HashMap<CanonicalColumnName, MeasurementData>,
//!     HashMap<CanonicalColumnName, QualityData>,
//!     EtlSchema,
//!     &EtlUniverseBuildPlan,
//!     Vec<CrushedComponent>,
//!     Duration
//! ) -> EtlResult<Universe>
//! ```

use std::{collections::HashMap, time::Duration};

use super::{
	CrushedComponent, CrushedComponentInfo, MeasurementData, QualityData, Universe,
	UniverseBuildInfo,
};
use crate::{
	CanonicalColumnName, error::EtlResult, schema::EtlSchema, source::EtlUniverseBuildPlan,
};

/// Assemble the final Universe with metadata.
pub fn assemble_universe(
	measurements: HashMap<CanonicalColumnName, MeasurementData>,
	qualities: HashMap<CanonicalColumnName, QualityData>,
	schema: EtlSchema,
	plan: &EtlUniverseBuildPlan,
	crushed_components: Vec<CrushedComponent>,
	duration: Duration,
) -> EtlResult<Universe> {
	let build_info =
		build_info(&measurements, &qualities, &schema, plan, crushed_components, duration);
	Ok(Universe::new(measurements, qualities, schema, build_info))
}

/// Build the audit/info structure.
fn build_info(
	measurements: &HashMap<CanonicalColumnName, MeasurementData>,
	qualities: &HashMap<CanonicalColumnName, QualityData>,
	schema: &EtlSchema,
	plan: &EtlUniverseBuildPlan,
	crushed_components: Vec<CrushedComponent>,
	duration: Duration,
) -> UniverseBuildInfo {
	// Count total rows and unique subjects across all measurements
	let (total_rows, subject_count) = measurements
		.values()
		.next()
		.map(|m| {
			let rows = m.fragment().height();
			let subjects = m.fragment().as_dataframe()
				.ok()
				.and_then(|df| df.column(schema.subject.as_str())
					.map(|c| c.n_unique().unwrap_or(0))
					.ok())
				.unwrap_or(0);
			(rows, subjects)
		})
		.unwrap_or((0, 0));

	// Convert CrushedComponent to CrushedComponentInfo
	let crushed_info: Vec<CrushedComponentInfo> = crushed_components
		.into_iter()
		.map(|c| {
			CrushedComponentInfo::new(
				c.measurement_name.as_str(),
				c.component_name.as_str(),
				c.input_units_without,
				c.input_units_with,
				&format!("{:?}", c.aggregation).to_lowercase(),
			)
		})
		.collect();

	// Collect signal policy stats from all measurements
	let signal_policy_stats = measurements
		.values()
		.filter_map(|m| m.signal_policy_stats().cloned())
		.collect();

	UniverseBuildInfo::builder(&schema.name)
		.sources_used(plan.sources.iter().map(|s| s.name.clone()).collect())
		.base_units_extracted(
			measurements
				.keys()
				.map(|k| k.as_str().to_string())
				.collect(),
		)
		.derivations_computed(
			schema
				.derivations
				.iter()
				.map(|d| d.name.as_str().to_string())
				.collect(),
		)
		.qualities_joined(qualities.keys().map(|k| k.as_str().to_string()).collect())
		.components_crushed(crushed_info)
		.row_count(total_rows)
		.subject_count(subject_count)
		.signal_policy_stats(signal_policy_stats)
		.build_duration(duration)
		.build()
}
