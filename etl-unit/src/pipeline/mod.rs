//! Typed measurement processing pipeline.
//!
//! This module encapsulates the multi-phase data processing strategy
//! that transforms raw measurement data into universe-ready series.
//! Each phase is a distinct Rust type, and transitions between phases
//! are the only way to advance — the compiler enforces correct ordering.
//!
//! # Pipeline flow
//!
//! ```text
//!                                 RawData
//!                                     │ .filter(bounds, subjects)
//!                                     ▼
//!                                 Filtered
//!                                     │ .apply_signal_policy(plan)
//!                                     ▼
//!                                 SignalApplied
//!                                     │
//!                        ┌────────────┼────────────┐
//!                        │            │            │
//!                   .crush(agg)  .expand(filter) .skip_components()
//!                        │            │            │
//!                        ▼            ▼            ▼
//!                    Crushed      Expanded      Crushed
//!                        │            │            │
//!                        └────────────┼────────────┘
//!                                     │ .join_onto(grid)
//!                                     ▼
//!                                  Joined
//!                                     │ .fill_nulls(nve)
//!                                     ▼
//!                                  Complete
//! ```
//!
//! # Usage from Universe
//!
//! ```rust,ignore
//! for plan in &measurement_plans {
//!     let (grid, diags) = pipeline::execute_measurement(
//!         plan, grid, time_bounds, subjects.as_deref(),
//!     )?;
//!     all_diags.extend(diags);
//! }
//! ```

pub mod phases;
pub mod plan;

pub use phases::{
	Complete, Crushed, Drift, DriftSeverity, Expanded, Filtered, Joined,
	PhaseDiag, ProcessingPhase, RawData, SignalApplied,
};
pub use plan::{ComponentStrategy, JoinStrategy, MeasurementPlan};

use crate::error::EtlResult;
use polars::prelude::DataFrame;

/// Execute a measurement plan through the full pipeline, joining
/// the result onto the cumulative grid.
///
/// Returns the updated grid (with this measurement's column added)
/// and the diagnostics from every phase.
pub fn execute_measurement(
	plan: &MeasurementPlan,
	grid: DataFrame,
	time_bounds: (i64, i64),
	subjects: Option<&[String]>,
) -> EtlResult<(DataFrame, Vec<PhaseDiag>)> {
	let mut diags = Vec::new();

	// Phase 1: Raw → Filtered
	let raw = RawData::new(plan)?;
	let (filtered, filter_diag) = raw.filter(
		&plan.time_col, &plan.subject_col,
		time_bounds, subjects,
	)?;
	diags.push(filter_diag);

	// Phase 2: Filtered → SignalApplied
	let (signal_applied, signal_diag) = filtered.apply_signal_policy(plan)?;
	diags.push(signal_diag);

	// Phase 3: Component handling → Crushed or Expanded
	// Phase 4: Join onto grid
	let (joined, join_diag) = match &plan.component_strategy {
		ComponentStrategy::Rollup { filter, aggregation } => {
			let (crushed, crush_diag) = signal_applied.crush(
				*aggregation, filter.as_deref(), plan,
			)?;
			diags.push(crush_diag);
			crushed.join_onto(grid, plan)?
		}
		ComponentStrategy::Series { filter } => {
			let (expanded, expand_diag) = signal_applied.expand(
				filter.as_deref(), plan,
			)?;
			diags.push(expand_diag);
			expanded.join_onto(grid, plan)?
		}
		ComponentStrategy::None => {
			let (crushed, skip_diag) = signal_applied.skip_components()?;
			diags.push(skip_diag);
			crushed.join_onto(grid, plan)?
		}
	};
	diags.push(join_diag);

	// Phase 5: Null fill
	let (complete, fill_diag) = joined.fill_nulls(plan)?;
	diags.push(fill_diag);

	Ok((complete.into_dataframe(), diags))
}
