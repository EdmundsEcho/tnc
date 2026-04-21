//! Universe build metadata and audit information.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::polars_fns::SignalPolicyStats;

// =============================================================================
// Build Info
// =============================================================================

/// Information about how a Universe was built.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniverseBuildInfo {
	pub schema_name:          String,
	pub sources_used:         Vec<String>,
	pub base_units_extracted: Vec<String>,
	pub unpivots_applied:     Vec<String>,
	pub derivations_computed: Vec<String>,
	pub qualities_joined:     Vec<String>,
	pub components_crushed:   Vec<CrushedComponentInfo>,
	pub row_count:            usize,
	pub subject_count:        usize,
	pub build_duration:       Duration,
	pub composition_summary:  Vec<CompositionSummary>,
	pub signal_policy_stats:  Vec<SignalPolicyStats>,
}

impl UniverseBuildInfo {
	/// Create a builder.
	pub fn builder(schema_name: &str) -> UniverseBuildInfoBuilder {
		UniverseBuildInfoBuilder::new(schema_name)
	}

	/// Get info for a crushed component by name.
	pub fn get_crushed_component(&self, component: &str) -> Option<&CrushedComponentInfo> {
		self
			.components_crushed
			.iter()
			.find(|c| c.component == component)
	}
}

/// Builder for UniverseBuildInfo.
#[derive(Debug, Default)]
pub struct UniverseBuildInfoBuilder {
	schema_name:          String,
	sources_used:         Vec<String>,
	base_units_extracted: Vec<String>,
	unpivots_applied:     Vec<String>,
	derivations_computed: Vec<String>,
	qualities_joined:     Vec<String>,
	components_crushed:   Vec<CrushedComponentInfo>,
	row_count:            usize,
	subject_count:        usize,
	build_duration:       Duration,
	composition_summary:  Vec<CompositionSummary>,
	signal_policy_stats:  Vec<SignalPolicyStats>,
}

impl UniverseBuildInfoBuilder {
	pub fn new(schema_name: &str) -> Self {
		Self {
			schema_name: schema_name.to_string(),
			..Default::default()
		}
	}

	pub fn sources_used(mut self, sources: Vec<String>) -> Self {
		self.sources_used = sources;
		self
	}

	pub fn base_units_extracted(mut self, units: Vec<String>) -> Self {
		self.base_units_extracted = units;
		self
	}

	pub fn unpivots_applied(mut self, unpivots: Vec<String>) -> Self {
		self.unpivots_applied = unpivots;
		self
	}

	pub fn derivations_computed(mut self, derivations: Vec<String>) -> Self {
		self.derivations_computed = derivations;
		self
	}

	pub fn qualities_joined(mut self, qualities: Vec<String>) -> Self {
		self.qualities_joined = qualities;
		self
	}

	pub fn components_crushed(mut self, components: Vec<CrushedComponentInfo>) -> Self {
		self.components_crushed = components;
		self
	}

	/// Add a single crushed component (useful for tests).
	pub fn add_crushed_component(mut self, info: CrushedComponentInfo) -> Self {
		self.components_crushed.push(info);
		self
	}

	pub fn row_count(mut self, count: usize) -> Self {
		self.row_count = count;
		self
	}

	pub fn subject_count(mut self, count: usize) -> Self {
		self.subject_count = count;
		self
	}

	pub fn build_duration(mut self, duration: Duration) -> Self {
		self.build_duration = duration;
		self
	}

	pub fn composition_summary(mut self, summary: Vec<CompositionSummary>) -> Self {
		self.composition_summary = summary;
		self
	}

	pub fn signal_policy_stats(mut self, stats: Vec<SignalPolicyStats>) -> Self {
		self.signal_policy_stats = stats;
		self
	}

	pub fn build(self) -> UniverseBuildInfo {
		UniverseBuildInfo {
			schema_name:          self.schema_name,
			sources_used:         self.sources_used,
			base_units_extracted: self.base_units_extracted,
			unpivots_applied:     self.unpivots_applied,
			derivations_computed: self.derivations_computed,
			qualities_joined:     self.qualities_joined,
			components_crushed:   self.components_crushed,
			row_count:            self.row_count,
			subject_count:        self.subject_count,
			build_duration:       self.build_duration,
			composition_summary:  self.composition_summary,
			signal_policy_stats:  self.signal_policy_stats,
		}
	}
}

// =============================================================================
// Crushed Component Info
// =============================================================================

/// Information about a component that was crushed during stacking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrushedComponentInfo {
	pub measurement:            String,
	pub component:              String,
	pub sources_missing:        Vec<String>,
	pub sources_with_component: Vec<String>,
	pub aggregation_used:       String,
	/// Human-readable reason for crushing.
	pub reason:                 String,
}

impl CrushedComponentInfo {
	pub fn new(
		measurement: &str,
		component: &str,
		sources_missing: Vec<String>,
		sources_with_component: Vec<String>,
		aggregation_used: &str,
	) -> Self {
		let reason = format!(
			"Component '{}' not present in all sources (missing from: {})",
			component,
			sources_missing.join(", ")
		);

		Self {
			measurement: measurement.to_string(),
			component: component.to_string(),
			sources_missing,
			sources_with_component,
			aggregation_used: aggregation_used.to_string(),
			reason,
		}
	}

	/// Create with a custom reason.
	pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
		self.reason = reason.into();
		self
	}
}

// =============================================================================
// Composition Summary
// =============================================================================

/// Summary of how a unit was composed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositionSummary {
	pub unit:     String,
	pub strategy: CompositionStrategyKind,
	pub sources:  Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompositionStrategyKind {
	Direct,
	Stack,
}

impl CompositionSummary {
	pub fn direct(unit: String, source: &str) -> Self {
		Self {
			unit,
			strategy: CompositionStrategyKind::Direct,
			sources: vec![source.to_string()],
		}
	}

	pub fn stack(unit: String, sources: Vec<String>) -> Self {
		Self {
			unit,
			strategy: CompositionStrategyKind::Stack,
			sources,
		}
	}
}
