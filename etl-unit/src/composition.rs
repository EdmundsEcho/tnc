//! Composition planning for combining EtlUnits from multiple sources.
//!
//! Composition rules based on codomain (unit name = value column = codomain):
//! - Same codomain from multiple sources → Stack (union rows)
//! - Different codomains → Join on subject
//! - Component mismatch within same codomain → Reduce/crush/melt then stack
//! - Subject mismatch within same codomain → Error

use std::collections::HashMap;

use crate::{CanonicalColumnName, SourceColumnName, request::AggregationType};

/// Result of analyzing how to compose sources for a unit
#[derive(Debug, Clone)]
pub enum CompositionStrategy {
	/// Single source provides the unit directly
	Direct {
		source: String,
	},

	/// Multiple sources provide the same unit → stack (union rows)
	Stack {
		sources:    Vec<String>,
		/// Sources that need component reduction before stacking
		reductions: Vec<ComponentReduction>,
	},

	/// Error: sources have incompatible domains for the same codomain
	Incompatible {
		unit:   String,
		reason: String,
	},
}

impl CompositionStrategy {
	pub fn is_direct(&self) -> bool {
		matches!(self, Self::Direct { .. })
	}

	pub fn is_stack(&self) -> bool {
		matches!(self, Self::Stack { .. })
	}

	pub fn is_incompatible(&self) -> bool {
		matches!(self, Self::Incompatible { .. })
	}

	pub fn source_names(&self) -> Vec<&str> {
		match self {
			Self::Direct {
				source,
			} => vec![source.as_str()],
			Self::Stack {
				sources,
				..
			} => sources.iter().map(|s| s.as_str()).collect(),
			Self::Incompatible {
				..
			} => vec![],
		}
	}
}

/// A source that needs component reduction before stacking
#[derive(Debug, Clone)]
pub struct ComponentReduction {
	pub source:            String,
	/// Components to aggregate away
	pub reduce_components: Vec<SourceColumnName>,
	/// Aggregation to use (from measurement kind)
	pub aggregation:       AggregationType,
}

/// Full composition plan for a subset request
#[derive(Debug, Clone)]
pub struct CompositionPlan {
	/// Strategy for each requested unit (keyed by unit name aka codomain)
	pub unit_strategies: HashMap<CanonicalColumnName, CompositionStrategy>,

	/// Units that will be joined together (different codomains sharing subject)
	pub join_units: Vec<CanonicalColumnName>,
}

impl CompositionPlan {
	/// Create an empty plan
	pub fn new() -> Self {
		Self {
			unit_strategies: HashMap::new(),
			join_units:      Vec::new(),
		}
	}

	/// Check if this is a simple single-source, single-unit plan
	pub fn is_simple(&self) -> bool {
		self.unit_strategies.len() == 1 && self.unit_strategies.values().all(|s| s.is_direct())
	}

	/// Check if any stacking is required
	pub fn requires_stacking(&self) -> bool {
		self.unit_strategies.values().any(|s| s.is_stack())
	}

	/// Check if joining is required (multiple codomains)
	pub fn requires_joining(&self) -> bool {
		self.join_units.len() > 1
	}

	/// Check if any component reduction is required
	pub fn requires_reduction(&self) -> bool {
		self.unit_strategies.values().any(
			|s| matches!(s, CompositionStrategy::Stack { reductions, .. } if !reductions.is_empty()),
		)
	}

	/// Check if there are any errors
	pub fn has_errors(&self) -> bool {
		self.unit_strategies.values().any(|s| s.is_incompatible())
	}

	/// Get all error messages
	pub fn errors(&self) -> Vec<&str> {
		self
			.unit_strategies
			.values()
			.filter_map(|s| {
				if let CompositionStrategy::Incompatible {
					reason,
					..
				} = s
				{
					Some(reason.as_str())
				} else {
					None
				}
			})
			.collect()
	}

	/// Get all unique source names involved in this plan
	pub fn all_sources(&self) -> Vec<&str> {
		let mut sources: Vec<&str> = self
			.unit_strategies
			.values()
			.flat_map(|s| s.source_names())
			.collect();
		sources.sort();
		sources.dedup();
		sources
	}

	/// Get the strategy for a specific unit
	pub fn get_strategy(&self, unit_name: &CanonicalColumnName) -> Option<&CompositionStrategy> {
		self.unit_strategies.get(unit_name)
	}
}

impl Default for CompositionPlan {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_composition_strategy_direct() {
		let strategy = CompositionStrategy::Direct {
			source: "source_a".into(),
		};
		assert!(strategy.is_direct());
		assert!(!strategy.is_stack());
		assert_eq!(strategy.source_names(), vec!["source_a"]);
	}

	#[test]
	fn test_composition_strategy_stack() {
		let strategy = CompositionStrategy::Stack {
			sources:    vec!["source_a".into(), "source_b".into()],
			reductions: vec![],
		};
		assert!(strategy.is_stack());
		assert_eq!(strategy.source_names().len(), 2);
	}

	#[test]
	fn test_composition_plan_simple() {
		let mut plan = CompositionPlan::new();
		plan.unit_strategies.insert(
			"value_a".into(),
			CompositionStrategy::Direct {
				source: "source".into(),
			},
		);
		plan.join_units.push("value_a".into());

		assert!(plan.is_simple());
		assert!(!plan.requires_stacking());
		assert!(!plan.requires_joining());
	}

	#[test]
	fn test_composition_plan_with_join() {
		let mut plan = CompositionPlan::new();
		plan.unit_strategies.insert(
			"value_a".into(),
			CompositionStrategy::Direct {
				source: "source_a".into(),
			},
		);
		plan.unit_strategies.insert(
			"value_b".into(),
			CompositionStrategy::Direct {
				source: "source_b".into(),
			},
		);
		plan.join_units = vec!["value_a".into(), "value_b".into()];

		assert!(!plan.is_simple());
		assert!(plan.requires_joining());
		assert!(!plan.requires_stacking());
	}

	#[test]
	fn test_composition_plan_with_stack() {
		let mut plan = CompositionPlan::new();
		plan.unit_strategies.insert(
			"value_a".into(),
			CompositionStrategy::Stack {
				sources:    vec!["source_a".into(), "source_b".into()],
				reductions: vec![],
			},
		);
		plan.join_units.push("value_a".into());

		assert!(plan.requires_stacking());
		assert!(!plan.requires_joining());
	}

	#[test]
	fn test_composition_plan_errors() {
		let mut plan = CompositionPlan::new();
		plan.unit_strategies.insert(
			"value_a".into(),
			CompositionStrategy::Incompatible {
				unit:   "value_a".into(),
				reason: "No source provides this unit".into(),
			},
		);

		assert!(plan.has_errors());
		assert_eq!(plan.errors().len(), 1);
	}
}
