//! `CrushPlan` — dimension reduction operations.
//!
//! A crush collapses one dimension of a source `DataFrame` by aggregation.
//! Two variants share the same shape but operate on different dimensions:
//!
//! - [`Crush::Components`] — collapses the component dimension.
//!   Group-by keys: subject + time. Output preserves the time grid.
//!   Used at runtime for component-bearing measurements (e.g.,
//!   `engines_on_count` after the engine columns are unpivoted into a
//!   `engine_number` component column — the crush sums them back).
//!
//! - [`Crush::Time`] — collapses the time dimension.
//!   Group-by key: subject only. Output is one row per subject. This
//!   is exactly the shape of `qualities.parquet`, so the store-builder
//!   uses this variant to extract qualities from raw source data.
//!
//! Both variants reuse the same [`Aggregate`] enum on each member:
//! component crush typically uses `Sum`/`Mean`/`Max`; time crush
//! typically uses `First`. The plan does not constrain the choice — the
//! plan builder reads it from the measurement/quality config.
//!
//! # Why two variants instead of one parameter
//!
//! The surviving columns differ between the two cases (time stays vs.
//! time is collapsed), and downstream stages need to know the output
//! shape to plan joins and signal policy. Encoding the variant
//! structurally lets the executor and the next stage match on it
//! instead of branching on a runtime field.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::aggregation::Aggregate;
use crate::unit_ref::EtlUnitRef;

use super::bindings::CodomainBinding;
use super::source_context::{SourceContext, SourceKey};

/// One member of a crush operation: an etl-unit whose value column is
/// being aggregated as part of a dimension reduction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrushMember {
	/// The etl-unit identity (Measurement or Quality).
	pub unit:        EtlUnitRef,
	/// The codomain (value) column being aggregated. Carries any
	/// `join_null_fill` declared on the unit so the next stage doesn't
	/// have to look it up.
	pub value:       CodomainBinding,
	/// The aggregation function applied during the group-by collapse.
	pub aggregation: Aggregate,
}

impl CrushMember {
	pub fn new(unit: EtlUnitRef, value: CodomainBinding, aggregation: Aggregate) -> Self {
		Self {
			unit,
			value,
			aggregation,
		}
	}
}

/// One crush operation against one source.
///
/// See the module-level docs for the distinction between the two variants.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Crush {
	/// Collapse component columns. Group-by: subject + time.
	/// Used at runtime for component-bearing measurements.
	Components {
		source:  Arc<SourceContext>,
		members: Vec<CrushMember>,
	},
	/// Collapse the time dimension. Group-by: subject.
	/// Used by the store-builder to produce `qualities.parquet`.
	Time {
		source:  Arc<SourceContext>,
		members: Vec<CrushMember>,
	},
}

impl Crush {
	/// Construct a component crush.
	pub fn components(source: Arc<SourceContext>, members: Vec<CrushMember>) -> Self {
		Self::Components { source, members }
	}

	/// Construct a time crush.
	pub fn time(source: Arc<SourceContext>, members: Vec<CrushMember>) -> Self {
		Self::Time { source, members }
	}

	/// The source this crush operates on.
	pub fn source(&self) -> &Arc<SourceContext> {
		match self {
			Self::Components { source, .. } | Self::Time { source, .. } => source,
		}
	}

	/// Convenience: the source's key.
	pub fn source_key(&self) -> SourceKey {
		self.source().source_key
	}

	/// The members participating in this crush.
	pub fn members(&self) -> &[CrushMember] {
		match self {
			Self::Components { members, .. } | Self::Time { members, .. } => members,
		}
	}

	/// Whether this is a component crush.
	pub fn is_components(&self) -> bool {
		matches!(self, Self::Components { .. })
	}

	/// Whether this is a time crush.
	pub fn is_time(&self) -> bool {
		matches!(self, Self::Time { .. })
	}
}

/// A list of crush operations to apply, in order.
///
/// In the runtime path, the plan builder includes one [`Crush::Components`]
/// per source-with-components. Sources without components contribute
/// nothing — they pass through unchanged.
///
/// In the store-builder path, the plan builder includes one [`Crush::Time`]
/// per source whose members include qualities to extract.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CrushPlan {
	pub crushes: Vec<Crush>,
}

impl CrushPlan {
	/// Construct an empty plan (no crush operations needed).
	pub fn empty() -> Self {
		Self::default()
	}

	/// Number of crush operations.
	pub fn op_count(&self) -> usize {
		self.crushes.len()
	}

	/// Number of components crushes.
	pub fn components_count(&self) -> usize {
		self.crushes.iter().filter(|c| c.is_components()).count()
	}

	/// Number of time crushes.
	pub fn time_count(&self) -> usize {
		self.crushes.iter().filter(|c| c.is_time()).count()
	}

	/// Total members across all crushes.
	pub fn member_count(&self) -> usize {
		self.crushes.iter().map(|c| c.members().len()).sum()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plan::bindings::{CodomainBinding, ColumnBinding};
	use crate::plan::source_context::SourceMember;
	use crate::universe::measurement_storage::DataSourceName;

	fn make_scada_engines_unpivoted() -> Arc<SourceContext> {
		// Post-unpivot: SCADA engines now have a component column.
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

	fn make_quality_source() -> Arc<SourceContext> {
		Arc::new(SourceContext {
			source_name: DataSourceName::new("scada"),
			source_key:  SourceKey::from_raw(0xA1),
			subject:     ColumnBinding::new("station_id", "station_name"),
			time:        ColumnBinding::new("obs_time", "timestamp"),
			components:  vec![],
			members:     vec![SourceMember::new(
				EtlUnitRef::quality("station_label"),
				CodomainBinding::new("display_name", "station_label"),
			)],
		})
	}

	#[test]
	fn components_constructor() {
		let src = make_scada_engines_unpivoted();
		let key = src.source_key;
		let crush = Crush::components(
			src,
			vec![CrushMember::new(
				EtlUnitRef::measurement("engines_on_count"),
				CodomainBinding::new("engine_on", "engines_on_count"),
				Aggregate::Sum,
			)],
		);
		assert!(crush.is_components());
		assert!(!crush.is_time());
		assert_eq!(crush.source_key(), key);
		assert_eq!(crush.members().len(), 1);
		assert_eq!(crush.members()[0].aggregation, Aggregate::Sum);
	}

	#[test]
	fn time_constructor() {
		let src = make_quality_source();
		let key = src.source_key;
		let crush = Crush::time(
			src,
			vec![CrushMember::new(
				EtlUnitRef::quality("station_label"),
				CodomainBinding::new("display_name", "station_label"),
				Aggregate::First,
			)],
		);
		assert!(crush.is_time());
		assert!(!crush.is_components());
		assert_eq!(crush.source_key(), key);
	}

	#[test]
	fn empty_plan_counts_are_zero() {
		let plan = CrushPlan::empty();
		assert_eq!(plan.op_count(), 0);
		assert_eq!(plan.components_count(), 0);
		assert_eq!(plan.time_count(), 0);
		assert_eq!(plan.member_count(), 0);
	}

	#[test]
	fn mixed_plan_counts_are_correct() {
		let plan = CrushPlan {
			crushes: vec![
				Crush::components(
					make_scada_engines_unpivoted(),
					vec![CrushMember::new(
						EtlUnitRef::measurement("engines_on_count"),
						CodomainBinding::new("engine_on", "engines_on_count"),
						Aggregate::Sum,
					)],
				),
				Crush::time(
					make_quality_source(),
					vec![CrushMember::new(
						EtlUnitRef::quality("station_label"),
						CodomainBinding::new("display_name", "station_label"),
						Aggregate::First,
					)],
				),
			],
		};
		assert_eq!(plan.op_count(), 2);
		assert_eq!(plan.components_count(), 1);
		assert_eq!(plan.time_count(), 1);
		assert_eq!(plan.member_count(), 2);
	}

	#[test]
	fn serde_roundtrip_components_variant() {
		let crush = Crush::components(
			make_scada_engines_unpivoted(),
			vec![CrushMember::new(
				EtlUnitRef::measurement("engines_on_count"),
				CodomainBinding::new("engine_on", "engines_on_count"),
				Aggregate::Sum,
			)],
		);
		let json = serde_json::to_string(&crush).unwrap();
		assert!(json.contains("\"kind\":\"components\""));
		let back: Crush = serde_json::from_str(&json).unwrap();
		assert!(back.is_components());
	}

	#[test]
	fn serde_roundtrip_time_variant() {
		let crush = Crush::time(
			make_quality_source(),
			vec![CrushMember::new(
				EtlUnitRef::quality("station_label"),
				CodomainBinding::new("display_name", "station_label"),
				Aggregate::First,
			)],
		);
		let json = serde_json::to_string(&crush).unwrap();
		assert!(json.contains("\"kind\":\"time\""));
		let back: Crush = serde_json::from_str(&json).unwrap();
		assert!(back.is_time());
	}
}
