//! Phase 4: Derivation Computation
//!
//! Computes derived measurements in dependency order.
//!
//! ## Type Signature
//!
//! ```text
//! compute_all_derivations: (DataFrame, &EtlSchema) -> EtlResult<DataFrame>
//!
//! Sub-operations:
//!   topological_sort: (&[Derivation]) -> Vec<&Derivation>
//!   compute_derivation: (DataFrame, &Derivation, &EtlSchema) -> DataFrame
//! ```

use std::collections::{HashSet, VecDeque};

use polars::prelude::*;
use tracing::{debug, instrument};

use crate::{
	error::{EtlError, EtlResult},
	polars_fns,
	schema::EtlSchema,
	unit::{Computation, Derivation},
};

// =============================================================================
// Main Entry Point
// =============================================================================

/// Compute all derivations in dependency order.
///
/// ```text
/// (DataFrame, &EtlSchema) -> EtlResult<DataFrame>
/// ```
#[instrument(skip_all, fields(derivations = schema.derivations.len()))]
pub fn compute_all_derivations(mut df: DataFrame, schema: &EtlSchema) -> EtlResult<DataFrame> {
	let ordered = topological_sort(&schema.derivations)?;

	for derivation in ordered {
		debug!(derivation = %derivation.name, "Computing derivation");
		df = compute_derivation(df, derivation, schema)?;
	}

	Ok(df)
}

// =============================================================================
// Topological Sort
// =============================================================================

/// Sort derivations in dependency order.
///
/// ```text
/// (&[Derivation]) -> EtlResult<Vec<&Derivation>>
/// ```
pub fn topological_sort(derivations: &[Derivation]) -> EtlResult<Vec<&Derivation>> {
	let mut result: Vec<&Derivation> = Vec::new();
	let mut remaining: VecDeque<&Derivation> = derivations.iter().collect();
	let mut resolved: HashSet<&str> = HashSet::new();

	let max_iterations = derivations.len() * derivations.len() + 1;
	let mut iterations = 0;

	while let Some(derivation) = remaining.pop_front() {
		iterations += 1;
		if iterations > max_iterations {
			return Err(EtlError::Config("Circular dependency detected in derivations".into()));
		}

		// Check if all dependencies are resolved
		let deps = derivation.input_columns();
		let all_resolved = deps.iter().all(|dep| {
			// Resolved if:
			// 1. Already computed
			// 2. Not a derivation (base measurement)
			resolved.contains(dep.as_str()) || !derivations.iter().any(|d| d.name == **dep)
		});

		if all_resolved {
			resolved.insert(derivation.name.as_str());
			result.push(derivation);
		} else {
			remaining.push_back(derivation);
		}
	}

	Ok(result)
}

// =============================================================================
// Single Derivation
// =============================================================================

/// Compute a single derivation.
///
/// ```text
/// (DataFrame, &Derivation, &EtlSchema) -> EtlResult<DataFrame>
/// ```
pub fn compute_derivation(
	df: DataFrame,
	derivation: &Derivation,
	schema: &EtlSchema,
) -> EtlResult<DataFrame> {
	match &derivation.computation {
		Computation::Pointwise(expr) => polars_fns::compute_pointwise(df, &derivation.name, expr),
		Computation::OverTime(expr) => {
			polars_fns::compute_over_time(df, &derivation.name, expr, schema)
		}
		Computation::OverSubjects(expr) => {
			polars_fns::compute_over_subjects(df, &derivation.name, expr, schema)
		}
	}
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
	use super::*;
	use crate::unit::{MeasurementKind, PointwiseExpr};

	#[test]
	fn test_topological_sort_simple() {
		let derivations = vec![
			Derivation::pointwise("c", PointwiseExpr::sum(["a", "b"])),
			Derivation::pointwise("d", PointwiseExpr::difference("c", "a")),
		];

		let sorted = topological_sort(&derivations).unwrap();

		// c must come before d
		let c_idx = sorted.iter().position(|d| d.name.as_str() == "c").unwrap();
		let d_idx = sorted.iter().position(|d| d.name.as_str() == "d").unwrap();
		assert!(c_idx < d_idx);
	}

	#[test]
	fn test_topological_sort_no_deps() {
		let derivations = vec![
			Derivation::pointwise("x", PointwiseExpr::sum(["a", "b"])),
			Derivation::pointwise("y", PointwiseExpr::sum(["c", "d"])),
		];

		let sorted = topological_sort(&derivations).unwrap();
		assert_eq!(sorted.len(), 2);
	}

	#[test]
	fn test_compute_derivation() {
		let schema = EtlSchema::new("test")
			.subject("s")
			.time("t")
			.measurement_with_defaults("a", MeasurementKind::Measure)
			.measurement_with_defaults("b", MeasurementKind::Measure)
			.build()
			.unwrap();

		let df = df! {
			 "s" => ["X"],
			 "t" => [100i64],
			 "a" => [1.0],
			 "b" => [2.0]
		}
		.unwrap();

		let derivation = Derivation::pointwise("c", PointwiseExpr::sum(["a", "b"]));

		let result = compute_derivation(df, &derivation, &schema).unwrap();

		assert!(result.column("c").is_ok());
	}
}
