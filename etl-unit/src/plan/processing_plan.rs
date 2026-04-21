//! `ProcessingPlan` — the top-level entry point of the planning layer.
//!
//! `ProcessingPlan` is an additive wrapper enum over the two pipeline
//! mode plans: [`RuntimePlan`] and [`BuildPlan`]. It exists for
//! documentation and diagnostic dispatch — the place a reader lands
//! when they want to know "what kinds of plans does this layer produce?"
//!
//! # Type-safe dispatch is preserved
//!
//! The two variants are also addressable as standalone types. Executors
//! continue to take `&RuntimePlan` or `&BuildPlan` directly:
//!
//! ```ignore
//! fn execute_runtime_query(plan: &RuntimePlan) { ... }
//! fn build_qualities(plan: &BuildPlan)         { ... }
//! ```
//!
//! The compiler still refuses to mix a build plan into a runtime
//! executor and vice versa. `ProcessingPlan` does not weaken that — it
//! is purely additive, used at boundaries where the caller wants one
//! type to mean "any pipeline plan" (diagnostics, top-level dispatch in
//! a CLI, generic logging).
//!
//! # Conversion
//!
//! `From<RuntimePlan>` and `From<BuildPlan>` impls let you bubble a
//! concrete plan up to the wrapper without ceremony:
//!
//! ```ignore
//! let plan = RuntimePlan::new(core, join, derivations);
//! let wrapped: ProcessingPlan = plan.into();
//! ```
//!
//! Going back down uses `as_runtime()` / `as_build()`.
//!
//! # What's not wrapped
//!
//! Stage plans — `FilterPlan`, `CrushPlan`, `JoinPlan`, `DerivationPlan`
//! — are deliberately *not* wrapped in a peer enum. They're not
//! interchangeable building blocks; they have distinct shapes consumed
//! at distinct stages. A wrapper enum would only add a runtime tag with
//! no structural value. If a future need for "render any stage to the
//! diagnostics panel" arises, it should be solved with a trait
//! (`trait StageDiag { fn summary(&self) -> String; }`), not an enum.

use serde::{Deserialize, Serialize};

use super::build::BuildPlan;
use super::core::ExtractionCore;
use super::runtime::RuntimePlan;

/// The top-level entry point for the planning layer.
///
/// One variant per pipeline mode. Each variant carries the concrete
/// top-level plan for that mode; the concrete types are also exported
/// independently for type-safe executor dispatch.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ProcessingPlan {
	/// Runtime query processing: Filter → Crush → SignalPolicy → Join → Derivations.
	Runtime(RuntimePlan),
	/// Store-builder processing: Filter → Crush → Sink.
	Build(BuildPlan),
}

impl ProcessingPlan {
	/// View the underlying plan as a runtime plan, if it is one.
	pub fn as_runtime(&self) -> Option<&RuntimePlan> {
		if let Self::Runtime(p) = self {
			Some(p)
		} else {
			None
		}
	}

	/// View the underlying plan as a build plan, if it is one.
	pub fn as_build(&self) -> Option<&BuildPlan> {
		if let Self::Build(p) = self {
			Some(p)
		} else {
			None
		}
	}

	/// The shared extraction core, regardless of variant.
	///
	/// Both `RuntimePlan` and `BuildPlan` compose an [`ExtractionCore`]
	/// (filter + crush). Diagnostics that only care about the shared
	/// substrate can use this accessor without matching on the variant.
	pub fn core(&self) -> &ExtractionCore {
		match self {
			Self::Runtime(p) => &p.core,
			Self::Build(p) => &p.core,
		}
	}

	/// Number of source contexts referenced by this plan.
	pub fn source_count(&self) -> usize {
		self.core().source_count()
	}

	/// Mode label for diagnostics. Lowercase, snake_case.
	pub fn mode(&self) -> &'static str {
		match self {
			Self::Runtime(_) => "runtime",
			Self::Build(_) => "build",
		}
	}
}

impl From<RuntimePlan> for ProcessingPlan {
	fn from(plan: RuntimePlan) -> Self {
		Self::Runtime(plan)
	}
}

impl From<BuildPlan> for ProcessingPlan {
	fn from(plan: BuildPlan) -> Self {
		Self::Build(plan)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::Arc;

	use crate::aggregation::Aggregate;
	use crate::plan::bindings::{CodomainBinding, ColumnBinding};
	use crate::plan::build::BuildSink;
	use crate::plan::crush::{Crush, CrushMember, CrushPlan};
	use crate::plan::filter::{FilterPlan, SourceFilter};
	use crate::plan::join::JoinPlan;
	use crate::plan::runtime::DerivationPlan;
	use crate::plan::source_context::{SourceContext, SourceKey, SourceMember};
	use crate::unit_ref::EtlUnitRef;
	use crate::universe::measurement_storage::DataSourceName;

	fn make_source() -> Arc<SourceContext> {
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

	fn make_runtime_plan() -> RuntimePlan {
		let src = make_source();
		RuntimePlan::new(
			ExtractionCore::new(
				FilterPlan {
					filters: vec![SourceFilter::from_context(src, None, None)],
				},
				CrushPlan::empty(),
			),
			JoinPlan::empty(),
			DerivationPlan::empty(),
		)
	}

	fn make_build_plan() -> BuildPlan {
		let src = make_source();
		BuildPlan::qualities(
			ExtractionCore::new(
				FilterPlan {
					filters: vec![SourceFilter::from_context(src.clone(), None, None)],
				},
				CrushPlan {
					crushes: vec![Crush::time(
						src,
						vec![CrushMember::new(
							EtlUnitRef::quality("station_label"),
							CodomainBinding::new("display_name", "station_label"),
							Aggregate::First,
						)],
					)],
				},
			),
			"/tmp/qualities.parquet",
		)
	}

	#[test]
	fn from_runtime_plan_via_into() {
		let rt = make_runtime_plan();
		let plan: ProcessingPlan = rt.into();
		assert!(plan.as_runtime().is_some());
		assert!(plan.as_build().is_none());
		assert_eq!(plan.mode(), "runtime");
	}

	#[test]
	fn from_build_plan_via_into() {
		let bp = make_build_plan();
		let plan: ProcessingPlan = bp.into();
		assert!(plan.as_build().is_some());
		assert!(plan.as_runtime().is_none());
		assert_eq!(plan.mode(), "build");
	}

	#[test]
	fn shared_core_accessor_works_for_both_variants() {
		let runtime: ProcessingPlan = make_runtime_plan().into();
		let build: ProcessingPlan = make_build_plan().into();

		assert_eq!(runtime.core().source_count(), 1);
		assert_eq!(build.core().source_count(), 1);
		assert_eq!(runtime.source_count(), 1);
		assert_eq!(build.source_count(), 1);
	}

	#[test]
	fn serde_roundtrip_runtime_variant() {
		let plan: ProcessingPlan = make_runtime_plan().into();
		let json = serde_json::to_string(&plan).unwrap();
		assert!(json.contains("\"mode\":\"runtime\""));
		let back: ProcessingPlan = serde_json::from_str(&json).unwrap();
		assert!(back.as_runtime().is_some());
	}

	#[test]
	fn serde_roundtrip_build_variant() {
		let plan: ProcessingPlan = make_build_plan().into();
		let json = serde_json::to_string(&plan).unwrap();
		assert!(json.contains("\"mode\":\"build\""));
		let back: ProcessingPlan = serde_json::from_str(&json).unwrap();
		assert!(back.as_build().is_some());
	}

	#[test]
	fn standalone_types_remain_usable() {
		// The whole point of the additive design: concrete types still
		// work as function parameters without going through the enum.
		let rt = make_runtime_plan();
		fn takes_runtime(p: &RuntimePlan) -> usize { p.source_count() }
		assert_eq!(takes_runtime(&rt), 1);

		let bp = make_build_plan();
		fn takes_build(p: &BuildPlan) -> usize { p.source_count() }
		assert_eq!(takes_build(&bp), 1);
	}
}
