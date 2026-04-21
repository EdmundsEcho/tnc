//! `RuntimePlan` — the top-level plan for a runtime query.
//!
//! Composes an [`ExtractionCore`] (filter + optional crush) with the
//! runtime-only stages: a [`JoinPlan`] for assembling the final frame and
//! a [`DerivationPlan`] for the post-join derivation phase.
//!
//! # Signal policy is intentionally absent
//!
//! Signal policy is driven by the existing
//! [`crate::AlignmentSpec`](crate::universe::AlignmentSpec), which lives
//! on the `Universe`. The runtime executor consults `AlignmentSpec`
//! between the crush and join stages to apply per-measurement signal
//! policy and resampling. The plan layer does not re-encode this — it
//! would just be a thin wrapper. `AlignmentSpec` is left untouched by
//! this refactor.
//!
//! # Type-safe dispatch
//!
//! `RuntimePlan` and [`super::BuildPlan`] are distinct types, not
//! variants of an enum. A function that takes `&RuntimePlan` cannot be
//! called with a `BuildPlan` and vice versa. The shared substrate is
//! [`ExtractionCore`], composed by value into both top-level types.

use serde::{Deserialize, Serialize};

use crate::unit_ref::EtlUnitRef;

use super::core::ExtractionCore;
use super::join::JoinPlan;

/// Placeholder plan for the post-join derivation phase.
///
/// Today, the runtime computes derivations by walking the schema's
/// derivation table after the joined frame is assembled. This type
/// records *which* derivations are scheduled to run, but the actual
/// computation logic still consults the schema directly. A future
/// refactor can expand this into a topologically-sorted operation list
/// when derivation planning becomes performance-critical; for now the
/// type just gives `RuntimePlan` somewhere to hold the list.
///
/// All entries are expected to be [`EtlUnitRef::Derivation`] variants.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DerivationPlan {
	pub derivations: Vec<EtlUnitRef>,
}

impl DerivationPlan {
	pub fn empty() -> Self {
		Self::default()
	}

	pub fn from_derivations(derivations: Vec<EtlUnitRef>) -> Self {
		Self { derivations }
	}

	pub fn len(&self) -> usize {
		self.derivations.len()
	}

	pub fn is_empty(&self) -> bool {
		self.derivations.is_empty()
	}
}

/// The top-level plan for a runtime query.
///
/// Stages applied by the executor, in order:
///
/// 1. **Filter** (from `core.filter`) — one filter pass per source.
/// 2. **Crush** (from `core.crush`) — collapse component dimensions
///    where present. Skipped per-source when no crush is needed.
/// 3. **Signal policy** — driven by `AlignmentSpec` on the `Universe`,
///    *not* by this plan. Per-measurement TTL handling and resampling.
/// 4. **Join** (from `join`) — LEFT JOIN every source onto the master
///    grid built from `AlignmentSpec::unified_rate_ms`.
/// 5. **Derivations** (from `derivations`) — compute derived columns
///    on the joined frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimePlan {
	/// Shared filter + crush substrate.
	pub core:        ExtractionCore,
	/// Joins to perform after signal policy.
	pub join:        JoinPlan,
	/// Derivations to compute after the join.
	pub derivations: DerivationPlan,
}

impl RuntimePlan {
	/// Construct a runtime plan from its components.
	pub fn new(core: ExtractionCore, join: JoinPlan, derivations: DerivationPlan) -> Self {
		Self {
			core,
			join,
			derivations,
		}
	}

	/// Construct an empty runtime plan. Useful for tests and as a
	/// starting point for builders.
	pub fn empty() -> Self {
		Self {
			core:        ExtractionCore::empty(),
			join:        JoinPlan::empty(),
			derivations: DerivationPlan::empty(),
		}
	}

	/// Number of sources participating in this plan.
	pub fn source_count(&self) -> usize {
		self.core.source_count()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::Arc;

	use crate::plan::bindings::{CodomainBinding, ColumnBinding};
	use crate::plan::crush::CrushPlan;
	use crate::plan::filter::{FilterPlan, SourceFilter};
	use crate::plan::join::{JoinColumn, JoinKeys, SourceJoin};
	use crate::plan::source_context::{SourceContext, SourceKey, SourceMember};
	use crate::universe::measurement_storage::DataSourceName;

	fn scada() -> Arc<SourceContext> {
		Arc::new(SourceContext {
			source_name: DataSourceName::new("scada"),
			source_key:  SourceKey::from_raw(0xA1),
			subject:     ColumnBinding::new("station_id", "station_name"),
			time:        ColumnBinding::new("obs_time", "timestamp"),
			components:  vec![],
			members:     vec![SourceMember::new(
				EtlUnitRef::measurement("sump"),
				CodomainBinding::new("sump_reading", "sump"),
			)],
		})
	}

	#[test]
	fn empty_runtime_plan_is_default_everywhere() {
		let plan = RuntimePlan::empty();
		assert_eq!(plan.source_count(), 0);
		assert_eq!(plan.join.op_count(), 0);
		assert!(plan.derivations.is_empty());
	}

	#[test]
	fn derivation_plan_construction() {
		let dp = DerivationPlan::from_derivations(vec![
			EtlUnitRef::derivation("total_runtime"),
			EtlUnitRef::derivation("avg_load"),
		]);
		assert_eq!(dp.len(), 2);
		assert!(!dp.is_empty());
	}

	#[test]
	fn runtime_plan_composes_core_and_runtime_stages() {
		let scada = scada();
		let core = ExtractionCore::new(
			FilterPlan {
				filters: vec![SourceFilter::from_context(scada.clone(), None, None)],
			},
			CrushPlan::empty(),
		);
		let join = JoinPlan {
			joins: vec![SourceJoin::new(
				scada,
				JoinKeys::SubjectTime,
				crate::plan::join::GroupSignalConfig::new(None, None),
				vec![JoinColumn::new(
					EtlUnitRef::measurement("sump"),
					CodomainBinding::new("sump_reading", "sump"),
				)],
			)],
		};
		let derivations = DerivationPlan::from_derivations(vec![EtlUnitRef::derivation("d1")]);

		let plan = RuntimePlan::new(core, join, derivations);
		assert_eq!(plan.source_count(), 1);
		assert_eq!(plan.join.op_count(), 1);
		assert_eq!(plan.derivations.len(), 1);
	}

	#[test]
	fn serde_roundtrip_runtime_plan() {
		let scada = scada();
		let plan = RuntimePlan::new(
			ExtractionCore::new(
				FilterPlan {
					filters: vec![SourceFilter::from_context(scada, None, None)],
				},
				CrushPlan::empty(),
			),
			JoinPlan::empty(),
			DerivationPlan::empty(),
		);
		let json = serde_json::to_string(&plan).unwrap();
		let back: RuntimePlan = serde_json::from_str(&json).unwrap();
		assert_eq!(back.source_count(), 1);
		assert_eq!(back.join.op_count(), 0);
		assert!(back.derivations.is_empty());
	}
}
