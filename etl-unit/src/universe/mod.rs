//! Universe: The materialized result of an ETL schema applied to source data.
//!
//! ## Storage Model
//!
//! The Universe stores measurements and qualities separately in HashMaps,
//! with components preserved. Composition into a single DataFrame happens
//! at subset request time.
//!
//! ```text
//! Universe
//! ├── measurements: HashMap<Name, MeasurementData>
//! │   └── each contains (MeasurementUnit, DataFrame) with components intact
//! └── qualities: HashMap<Name, QualityData>
//!     └── each contains (QualityUnit, DataFrame)
//! ```
//!
//! ## Processing Phases
//!
//! ```text
//! Phase 1: Extraction     (&EtlUniverseBuildPlan) -> FragmentAccumulator
//! Phase 2: Stacking       (FragmentAccumulator)   -> (Vec<ComposedMeasurement>, Vec<ComposedQuality>)
//! Phase 3: Assembly       (ComposedUnits)         -> Universe (HashMap storage)
//! Phase 4: Subset         (Universe, Request)     -> SubsetUniverse (composed DataFrame)
//!     - Crush components
//!     - Resample to interval
//!     - Join measurements
//!     - Compute derivations
//! ```

mod assembly;
mod derivation;
mod extraction;
mod fragment;
mod info;
pub mod measurement_storage;
mod universe_of_etlunits;

use std::{collections::HashMap, time::Instant};

pub use extraction::extract_all_fragments;
// Re-export fragment types (used by extraction, stacking)
pub use fragment::{
	ComposedMeasurement, ComposedQuality, CrushedComponent, EtlUnitFragment, FragmentAccumulator,
	MeasurementFragment, QualityFragment, stack_all_fragments,
};
// Re-export info types
pub use info::{
	CompositionStrategyKind, CompositionSummary, CrushedComponentInfo, UniverseBuildInfo,
	UniverseBuildInfoBuilder,
};
pub mod alignment;
pub use alignment::{AlignAction, AlignmentSpec, MeasurementAlignment};
pub use measurement_storage::FragmentRef;
use tracing::{debug, info, warn};
// Re-export main types from universe_of_etlunits
pub use universe_of_etlunits::{MeasurementData, MeasurementDiag, MeasurementPolicyDiag, MeasurementState, MemorySummary, QualityData, Universe};

use crate::{
	CanonicalColumnName,
	error::{EtlError, EtlResult},
	source::EtlUniverseBuildPlan,
};

// =============================================================================
// Universe Builder
// =============================================================================

/// Builds a Universe from an EtlUniverseBuildPlan.
///
/// Orchestrates the processing phases to produce a Universe with HashMap storage.
pub struct UniverseBuilder;

impl UniverseBuilder {
	/// Build a Universe from a plan.
	///
	/// ## Phases
	///
	/// 1. **Extraction**: Transform source data into canonical fragments
	/// 2. **Stacking**: Combine fragments into composed units
	/// 3. **Assembly**: Store in HashMap structure with metadata
	///
	/// Composition and derivation are deferred to subset time.
	#[tracing::instrument(skip(plan), fields(schema = %plan.schema.name, sources = plan.sources.len()))]
	pub fn build(plan: &EtlUniverseBuildPlan) -> EtlResult<Universe> {
		let start = Instant::now();
		let schema = &plan.schema;

		info!("🟢 Starting universe build");

		// =====================================================================
		// PHASE 1: Extraction
		// =====================================================================
		info!("👉 Phase 1: Extracting fragments");
		let accumulator = extraction::extract_all_fragments(plan)?;

		debug!(
			measurements = accumulator.measurement_count(),
			qualities = accumulator.quality_count(),
			total = accumulator.total_fragments(),
			"✅ Extraction complete"
		);

		if accumulator.is_empty() {
			return Err(EtlError::Config("No fragments extracted".into()));
		}

		// =====================================================================
		// PHASE 2: Stacking
		// =====================================================================
		info!("👉 Phase 2: Stacking fragments");
		let (composed_measurements, composed_qualities) =
			stack_all_fragments(accumulator, &schema.subject, &schema.time)?;

		// Collect crushing info (for reporting)
		let crushed: Vec<CrushedComponent> = composed_measurements
			.iter()
			.flat_map(|m| m.crushed_components.clone())
			.collect();

		if !crushed.is_empty() {
			warn!(count = crushed.len(), "Components crushed during stacking");
		}

		debug!(
			measurements = composed_measurements.len(),
			qualities = composed_qualities.len(),
			"✅ Stacking complete"
		);

		// =====================================================================
		// PHASE 3: Convert to HashMap Storage
		// =====================================================================
		info!("👉 Phase 3: Building Universe storage");

		let mut measurements: HashMap<CanonicalColumnName, MeasurementData> = HashMap::new();
		let mut qualities: HashMap<CanonicalColumnName, QualityData> = HashMap::new();

		// Convert ComposedMeasurement to MeasurementData
		for composed in composed_measurements {
			let name = composed.name.clone();

			// Get the full MeasurementUnit from schema if available
			let unit = schema.get_measurement(&name).cloned().unwrap_or_else(|| {
				// Fallback: construct minimal unit
				crate::MeasurementUnit::new(
					schema.subject.clone(),
					schema.time.clone(),
					name.clone(),
					composed.kind,
				)
				.with_components(
					composed
						.components
						.iter()
						.map(|c| c.as_str().to_string())
						.collect(),
				)
			});

			// Merge signal policy stats
			let stats = if composed.signal_policy_stats.len() == 1 {
				composed.signal_policy_stats.into_iter().next()
			} else if !composed.signal_policy_stats.is_empty() {
				// TODO: proper merging of multiple stats
				composed.signal_policy_stats.into_iter().next()
			} else {
				None
			};

			// Stats from stacking are informational; they'll be recomputed during alignment.
			// Start in Raw state — alignment happens in ensure_aligned().
			let _ = stats; // intentionally unused after state enum refactor
			let measurement_data = MeasurementData {
				unit,
				state: MeasurementState::Raw { fragment: composed.fragment },
			};
			measurements.insert(name, measurement_data);
		}

		// Convert ComposedQuality to QualityData
		for composed in composed_qualities {
			let name = composed.name.clone();

			// Get the full QualityUnit from schema if available
			let unit = schema.get_quality(&name).cloned().unwrap_or_else(|| {
				// Fallback: construct minimal unit
				crate::QualityUnit::new(schema.subject.clone(), name.clone())
			});

			let quality_data = QualityData::new(unit, composed.data);
			qualities.insert(name, quality_data);
		}

		debug!(
			measurements = measurements.len(),
			qualities = qualities.len(),
			"✅ Storage populated"
		);

		// =====================================================================
		// PHASE 4: Assembly (create Universe with metadata)
		// =====================================================================
		info!("👉 Phase 4: Assembling universe");
		let universe = assembly::assemble_universe(
			measurements,
			qualities,
			schema.clone(),
			plan,
			crushed,
			start.elapsed(),
		)?;

		info!(
			measurements = universe.measurement_count(),
			qualities = universe.quality_count(),
			duration_ms = universe.build_info().build_duration.as_millis(),
			"✅ Universe build complete"
		);

		Ok(universe)
	}
}
