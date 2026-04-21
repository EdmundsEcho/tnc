//! `ExtractionCore` — the shared substrate of the runtime and build pipelines.
//!
//! Both `RuntimePlan` and `BuildPlan` compose an `ExtractionCore`. The
//! core captures everything the two pipelines do *the same way*: filter
//! source DataFrames and (optionally) crush a dimension. What differs is
//! what comes after — runtime continues with signal policy + join +
//! derivations; build writes to a sink.
//!
//! # Sources are derived from the filter plan
//!
//! `ExtractionCore` does **not** carry a separate `sources` field. The
//! set of sources participating in the plan is exactly the set of
//! `Arc<SourceContext>` referenced by the filter plan, because every
//! source in the plan needs to be filtered. The
//! [`ExtractionCore::sources`] iterator walks the filter plan and yields
//! the Arc references in filter order. This eliminates the
//! redundant-state class of bug ("the sources list says 3, but the
//! filter plan only has 2 entries").
//!
//! # Construction
//!
//! Step 3 provides the structural constructor. The schema-aware builder
//! that walks a `Universe`, computes which crushes are needed, and
//! produces a complete `ExtractionCore` lives in step 4 next to the
//! executor wiring.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::crush::CrushPlan;
use super::filter::FilterPlan;
use super::source_context::{SourceContext, SourceKey};

/// The shared substrate consumed by both [`super::RuntimePlan`] and
/// [`super::BuildPlan`].
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExtractionCore {
	/// One filter pass per source.
	pub filter: FilterPlan,
	/// Dimension reductions to apply to component-bearing or
	/// time-collapsible sources. May be empty.
	pub crush:  CrushPlan,
}

impl ExtractionCore {
	/// Construct a core from its filter and crush components.
	pub fn new(filter: FilterPlan, crush: CrushPlan) -> Self {
		Self { filter, crush }
	}

	/// Construct an empty core. Useful as a starting point for builders
	/// or as a base case in tests.
	pub fn empty() -> Self {
		Self::default()
	}

	/// Iterate over the unique source contexts referenced by this core.
	///
	/// The set is determined by the filter plan: every source in the
	/// core has exactly one [`SourceFilter`](super::filter::SourceFilter)
	/// entry, and the iterator yields each one in filter order.
	pub fn sources(&self) -> impl Iterator<Item = &Arc<SourceContext>> {
		self.filter.filters.iter().map(|f| &f.source)
	}

	/// Number of unique sources in the plan.
	pub fn source_count(&self) -> usize {
		self.filter.filters.len()
	}

	/// Look up a source context by its `SourceKey`.
	pub fn lookup_source(&self, key: SourceKey) -> Option<&Arc<SourceContext>> {
		self.sources().find(|s| s.source_key == key)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plan::bindings::{CodomainBinding, ColumnBinding};
	use crate::plan::crush::{Crush, CrushMember};
	use crate::plan::filter::SourceFilter;
	use crate::plan::source_context::SourceMember;
	use crate::aggregation::Aggregate;
	use crate::unit_ref::EtlUnitRef;
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

	fn engines_unpivoted() -> Arc<SourceContext> {
		Arc::new(SourceContext {
			source_name: DataSourceName::new("scada_engines"),
			source_key:  SourceKey::from_raw(0xE9),
			subject:     ColumnBinding::identity("station_name"),
			time:        ColumnBinding::identity("timestamp"),
			components:  vec![ColumnBinding::identity("engine_number")],
			members:     vec![SourceMember::new(
				EtlUnitRef::measurement("engines_on_count"),
				CodomainBinding::new("engine_on", "engines_on_count"),
			)],
		})
	}

	#[test]
	fn empty_core_has_zero_sources() {
		let core = ExtractionCore::empty();
		assert_eq!(core.source_count(), 0);
		assert!(core.sources().next().is_none());
	}

	#[test]
	fn sources_iter_walks_the_filter_plan_in_order() {
		let s1 = scada();
		let s2 = engines_unpivoted();
		let filter = FilterPlan {
			filters: vec![
				SourceFilter::from_context(s1.clone(), None, None),
				SourceFilter::from_context(s2.clone(), None, None),
			],
		};
		let core = ExtractionCore::new(filter, CrushPlan::empty());
		assert_eq!(core.source_count(), 2);
		let keys: Vec<SourceKey> = core.sources().map(|s| s.source_key).collect();
		assert_eq!(keys, vec![s1.source_key, s2.source_key]);
	}

	#[test]
	fn lookup_source_finds_by_key() {
		let s1 = scada();
		let s2 = engines_unpivoted();
		let key2 = s2.source_key;
		let filter = FilterPlan {
			filters: vec![
				SourceFilter::from_context(s1, None, None),
				SourceFilter::from_context(s2, None, None),
			],
		};
		let core = ExtractionCore::new(filter, CrushPlan::empty());
		let found = core.lookup_source(key2).expect("should find engines source");
		assert_eq!(found.source_key, key2);
		assert_eq!(found.source_name.as_str(), "scada_engines");
	}

	#[test]
	fn lookup_source_returns_none_for_unknown_key() {
		let core = ExtractionCore::new(
			FilterPlan {
				filters: vec![SourceFilter::from_context(scada(), None, None)],
			},
			CrushPlan::empty(),
		);
		assert!(core.lookup_source(SourceKey::from_raw(0xDEAD)).is_none());
	}

	#[test]
	fn core_carries_crush_alongside_filter() {
		let engines = engines_unpivoted();
		let filter = FilterPlan {
			filters: vec![SourceFilter::from_context(engines.clone(), None, None)],
		};
		let crush = CrushPlan {
			crushes: vec![Crush::components(
				engines,
				vec![CrushMember::new(
					EtlUnitRef::measurement("engines_on_count"),
					CodomainBinding::new("engine_on", "engines_on_count"),
					Aggregate::Sum,
				)],
			)],
		};
		let core = ExtractionCore::new(filter, crush);
		assert_eq!(core.source_count(), 1);
		assert_eq!(core.crush.op_count(), 1);
		assert_eq!(core.crush.components_count(), 1);
	}

	#[test]
	fn serde_roundtrip_extraction_core() {
		let core = ExtractionCore::new(
			FilterPlan {
				filters: vec![SourceFilter::from_context(scada(), None, None)],
			},
			CrushPlan::empty(),
		);
		let json = serde_json::to_string(&core).unwrap();
		let back: ExtractionCore = serde_json::from_str(&json).unwrap();
		assert_eq!(back.source_count(), 1);
	}
}
