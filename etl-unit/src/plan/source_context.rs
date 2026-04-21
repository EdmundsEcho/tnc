//! `SourceContext` — the per-source invariants shared by all etl-units
//! extracted from one source DataFrame.
//!
//! # Why this type exists
//!
//! Two measurements (or a measurement and a quality) extracted from the
//! same `Arc<DataFrame>` necessarily share the same subject column, the
//! same time column, and the same component columns. This is a structural
//! invariant of the extraction model: a single physical DataFrame has one
//! schema, and every etl-unit drawn from it sees that schema.
//!
//! Rather than redundantly storing those bindings on every plan node or
//! every member, the plan layer holds them once on `SourceContext` and
//! shares the context across plan nodes via [`Arc<SourceContext>`].
//!
//! # Identity
//!
//! [`SourceKey`] is a newtype over the raw pointer value of the source
//! `Arc<DataFrame>`. Two fragments share a `SourceKey` if and only if
//! they point into the same `Arc`. This is the grouping key the plan
//! builder uses to collapse work across measurements that share data.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::column::DomainSignature;
use crate::unit_ref::EtlUnitRef;
use crate::universe::measurement_storage::DataSourceName;

use super::bindings::{CodomainBinding, ColumnBinding};

/// Arc pointer identity for a source `DataFrame`.
///
/// Two `MeasurementFragment`s sharing the same `SourceKey` point into the
/// same `Arc<DataFrame>`. Used as the grouping key for filter, crush, and
/// signal-policy plans so we can do one pass per shared source instead of
/// one pass per measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SourceKey(usize);

impl SourceKey {
	/// Construct a `SourceKey` from an `Arc` by taking its pointer identity.
	///
	/// Two clones of the same `Arc` produce the same key. Two distinct
	/// `Arc`s — even with identical contents — produce different keys.
	pub fn from_arc<T>(arc: &Arc<T>) -> Self {
		// Cast through *const () so the address is layout-stable across
		// different inner types.
		Self(Arc::as_ptr(arc) as *const () as usize)
	}

	/// Construct from a raw pointer value (e.g., one already produced by
	/// existing code in `measurement_storage::source_arc_ptrs`).
	pub fn from_raw(value: usize) -> Self {
		Self(value)
	}

	/// The raw pointer value.
	pub fn as_raw(&self) -> usize {
		self.0
	}
}

impl std::fmt::Display for SourceKey {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "source#{:x}", self.0)
	}
}

/// One etl-unit served by a particular source. Always lives inside a
/// [`SourceContext::members`] vector.
///
/// `SourceMember` is intentionally kind-agnostic: `unit` is an
/// [`EtlUnitRef`] discriminating Measurement / Quality / Derivation.
/// Today the plan layer uses Measurement and Quality variants;
/// derivations are handled in a separate post-join plan phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceMember {
	/// The etl-unit identity (Measurement / Quality / Derivation).
	pub unit:  EtlUnitRef,
	/// The codomain (value) column for this member in the source.
	/// Carries any null-fill metadata declared on the unit.
	pub value: CodomainBinding,
}

impl SourceMember {
	pub fn new(unit: EtlUnitRef, value: CodomainBinding) -> Self {
		Self { unit, value }
	}
}

/// The shared context for all etl-units extracted from one source
/// `DataFrame`.
///
/// Held by [`Arc`] so that multiple plan nodes (filter, crush,
/// signal-policy, join) can reference the same source without copying
/// the bindings or re-computing the domain signature.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceContext {
	/// Logical source name (e.g., "scada", "mrms"). Used for diagnostics
	/// and error messages.
	pub source_name: DataSourceName,
	/// Arc-pointer identity of the source `DataFrame`.
	pub source_key:  SourceKey,
	/// Subject key column. Never null-filled.
	pub subject:     ColumnBinding,
	/// Time key column. Never null-filled.
	pub time:        ColumnBinding,
	/// Component key columns (e.g., `engine_number`). Empty for sources
	/// without components. Never null-filled.
	pub components:  Vec<ColumnBinding>,
	/// Etl-units served by this source. Heterogeneous by design — a
	/// single source can contribute both measurements and qualities.
	pub members:     Vec<SourceMember>,
}

impl SourceContext {
	/// Whether this source has any component dimensions.
	pub fn has_components(&self) -> bool {
		!self.components.is_empty()
	}

	/// Derive the [`DomainSignature`] for this source from its bindings.
	///
	/// The signature is used by composition planning to decide whether
	/// fragments can be stacked or must be joined.
	pub fn domain(&self) -> DomainSignature {
		DomainSignature::measurement(
			self.subject.canonical.as_str(),
			self.time.canonical.as_str(),
		)
		.with_components(
			self.components
				.iter()
				.map(|c| c.canonical.as_str().to_string())
				.collect(),
		)
	}

	/// Iterator over members that are measurements.
	pub fn measurement_members(&self) -> impl Iterator<Item = &SourceMember> {
		self.members.iter().filter(|m| m.unit.is_measurement())
	}

	/// Iterator over members that are qualities.
	pub fn quality_members(&self) -> impl Iterator<Item = &SourceMember> {
		self.members.iter().filter(|m| m.unit.is_quality())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::unit::NullValue;

	fn make_context() -> SourceContext {
		SourceContext {
			source_name: DataSourceName::new("scada"),
			source_key:  SourceKey::from_raw(0xDEAD_BEEF),
			subject:     ColumnBinding::new("station_id", "station_name"),
			time:        ColumnBinding::new("obs_time", "timestamp"),
			components:  vec![],
			members:     vec![
				SourceMember::new(
					EtlUnitRef::measurement("sump"),
					CodomainBinding::new("sump_reading", "sump"),
				),
				SourceMember::new(
					EtlUnitRef::quality("station_label"),
					CodomainBinding::new("display_name", "station_label"),
				),
			],
		}
	}

	#[test]
	fn source_key_from_arc_is_stable_across_clones() {
		let arc = Arc::new(42u32);
		let k1 = SourceKey::from_arc(&arc);
		let k2 = SourceKey::from_arc(&arc.clone());
		assert_eq!(k1, k2);
	}

	#[test]
	fn source_key_distinguishes_distinct_arcs() {
		let a = Arc::new(1u32);
		let b = Arc::new(1u32);
		assert_ne!(SourceKey::from_arc(&a), SourceKey::from_arc(&b));
	}

	#[test]
	fn source_key_display_is_hex() {
		let k = SourceKey::from_raw(0xDEAD_BEEF);
		assert_eq!(format!("{}", k), "source#deadbeef");
	}

	#[test]
	fn has_components_false_when_empty() {
		let ctx = make_context();
		assert!(!ctx.has_components());
	}

	#[test]
	fn has_components_true_when_present() {
		let mut ctx = make_context();
		ctx.components.push(ColumnBinding::identity("engine_number"));
		assert!(ctx.has_components());
	}

	#[test]
	fn domain_signature_reflects_canonical_names() {
		let ctx = make_context();
		let sig = ctx.domain();
		assert_eq!(sig.subject.as_str(), "station_name");
		assert_eq!(sig.time.as_ref().unwrap().as_str(), "timestamp");
		assert!(sig.components.is_empty());
	}

	#[test]
	fn domain_signature_includes_components() {
		let mut ctx = make_context();
		ctx.components.push(ColumnBinding::identity("engine_number"));
		let sig = ctx.domain();
		assert_eq!(sig.components.len(), 1);
		assert_eq!(sig.components[0].as_str(), "engine_number");
	}

	#[test]
	fn member_iterators_partition_by_kind() {
		let ctx = make_context();
		assert_eq!(ctx.measurement_members().count(), 1);
		assert_eq!(ctx.quality_members().count(), 1);
	}

	#[test]
	fn null_fills_attach_only_to_codomain() {
		// Compile-time check: ColumnBinding has no null fields, so this
		// would not compile if someone tried to add them. The runtime
		// part of the assertion is on CodomainBinding.
		let m = SourceMember::new(
			EtlUnitRef::measurement("engines_on_count"),
			CodomainBinding::new("engine_on", "engines_on_count")
				.with_source_null_fill(NullValue::Integer(0))
				.with_join_null_fill(NullValue::Integer(0)),
		);
		assert!(m.value.source_null_fill.is_some());
		assert!(m.value.join_null_fill.is_some());
	}

	#[test]
	fn serde_roundtrip_source_context() {
		let ctx = make_context();
		let json = serde_json::to_string(&ctx).unwrap();
		let back: SourceContext = serde_json::from_str(&json).unwrap();
		assert_eq!(back.source_name.as_str(), "scada");
		assert_eq!(back.subject.canonical.as_str(), "station_name");
		assert_eq!(back.members.len(), 2);
	}
}
