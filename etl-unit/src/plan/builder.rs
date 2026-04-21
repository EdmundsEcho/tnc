//! Schema-aware plan construction.
//!
//! Walks a [`Universe`] and a request and produces a complete
//! [`RuntimePlan`]. The builder is the bridge between the universe-side
//! types (`MeasurementData`, `FragmentRef`, `EtlSchema`) and the
//! plan-side types (`SourceContext`, `FilterPlan`, `CrushPlan`,
//! `JoinPlan`, `DerivationPlan`).
//!
//! # Source grouping
//!
//! Measurements are grouped by `Arc<DataFrame>` pointer identity. Two
//! measurements that share an Arc share a [`SourceContext`]; the builder
//! produces one context per unique Arc.
//!
//! # What the builder does *not* do
//!
//! - **Signal policy.** Signal policy is driven by the existing
//!   [`crate::AlignmentSpec`] on the universe; the runtime executor
//!   consults it between the crush and join stages. The plan does not
//!   re-encode signal policy.
//! - **Stacked fragments.** A measurement whose fragment is `Stacked`
//!   over multiple sources currently uses the *first* source for plan
//!   identity. Cross-source stacking is a known follow-up.
//! - **Pre-processed qualities.** Today's runtime loads qualities from
//!   `qualities.parquet` (a separate path), so the plan does not include
//!   them in `SourceContext.members`. The store-builder pipeline (which
//!   produces qualities.parquet using `BuildPlan` + `Crush::Time`) is
//!   the place quality extraction lives.

use std::collections::HashMap;
use std::sync::Arc;

use crate::column::{CanonicalColumnName, SourceColumnName};
use crate::error::{EtlError, EtlResult};
use crate::request::{EtlUnitSubsetRequest, SubjectFilter};
use crate::subject::SubjectValue;
use crate::unit_ref::EtlUnitRef;
use crate::universe::measurement_storage::DataSourceName;
use crate::universe::{MeasurementData, Universe};

use super::bindings::{CodomainBinding, ColumnBinding};
use super::core::ExtractionCore;
use super::crush::{Crush, CrushMember, CrushPlan};
use super::filter::FilterPlan;
use super::join::{GroupSignalConfig, JoinColumn, JoinKeys, JoinPlan, SourceJoin};
use super::runtime::{DerivationPlan, RuntimePlan};
use super::source_context::{SourceContext, SourceKey, SourceMember};

/// Build a [`RuntimePlan`] from a universe and a subset request.
///
/// Walks the universe's measurements, groups them by source `Arc`
/// identity, and assembles `SourceContext`s + the four stage plans
/// (`FilterPlan`, `CrushPlan`, `JoinPlan`, `DerivationPlan`) wrapped in
/// an `ExtractionCore` + `RuntimePlan`.
///
/// `request.measurements` selects which measurements participate. An
/// empty list means "all measurements in the universe."
pub fn build_runtime_plan(
	universe: &Universe,
	request: &EtlUnitSubsetRequest,
) -> EtlResult<RuntimePlan> {
	let schema = universe.schema();

	// Resolve which measurement names participate.
	let measurement_names: Vec<CanonicalColumnName> = if request.measurements.is_empty() {
		(&universe.measurements).keys().cloned().collect()
	} else {
		request.measurements.clone()
	};

	// Partition into derivations vs primary measurements. Derivations
	// are computed post-join from the schema; they don't participate in
	// source grouping.
	let mut primary_names: Vec<CanonicalColumnName> = Vec::new();
	let mut derivation_refs: Vec<EtlUnitRef> = Vec::new();
	for name in &measurement_names {
		if schema.get_derivation(name).is_some() {
			derivation_refs.push(EtlUnitRef::derivation(name.clone()));
		} else {
			primary_names.push(name.clone());
		}
	}

	// Group primary measurements by source Arc identity.
	let mut by_source: HashMap<usize, Vec<&MeasurementData>> = HashMap::new();
	for name in &primary_names {
		let md = (&universe.measurements).get(name).ok_or_else(|| {
			EtlError::UnitNotFound(format!("Measurement '{}' not found", name))
		})?;
		let key = md
			.fragment()
			.source_arc_ptrs()
			.first()
			.copied()
			.unwrap_or(0);
		by_source.entry(key).or_default().push(md);
	}

	// Build one SourceContext per unique source group, in deterministic
	// order (sorted by raw key) so plan output is reproducible.
	let mut source_keys: Vec<usize> = by_source.keys().copied().collect();
	source_keys.sort_unstable();

	let mut sources: Vec<Arc<SourceContext>> = Vec::with_capacity(source_keys.len());
	for raw_key in source_keys {
		let mds = by_source
			.get(&raw_key)
			.expect("source key was just iterated from the map");
		let context = build_source_context(SourceKey::from_raw(raw_key), mds, schema)?;
		sources.push(Arc::new(context));
	}

	// Filter plan: one filter per source, with request-level filters propagated.
	let time_range = request.time_range.clone();
	let subject_set = subject_filter_to_values(request.subject_filter.as_ref());
	let filter = FilterPlan::build(sources.iter().cloned(), time_range, subject_set);

	// Crush plan: one Crush::Components per source-with-components.
	let crush = build_crush_plan(&sources, universe);

	// Join plan: one SourceJoin per source.
	let join = build_join_plan(&sources, universe);

	// Derivation plan: just records what to compute. The actual logic
	// continues to live in the schema-driven derivation system.
	let derivations = DerivationPlan::from_derivations(derivation_refs);

	let core = ExtractionCore::new(filter, crush);
	Ok(RuntimePlan::new(core, join, derivations))
}

/// Build one [`SourceContext`] for a group of measurements that share
/// an `Arc<DataFrame>`.
fn build_source_context(
	source_key: SourceKey,
	mds: &[&MeasurementData],
	schema: &crate::EtlSchema,
) -> EtlResult<SourceContext> {
	let representative = mds.first().ok_or_else(|| {
		EtlError::Config("build_source_context called with empty group".into())
	})?;
	let frag = representative.fragment();

	// Source name from the fragment, or a synthetic key-based name if
	// the fragment is materialized (no upstream identity).
	let source_name = frag
		.source_name()
		.cloned()
		.unwrap_or_else(|| DataSourceName::new(format!("source_{:x}", source_key.as_raw())));

	// Subject and time bindings.
	let subject_phys = frag
		.physical_column_name(schema.subject.as_str())
		.unwrap_or_else(|| schema.subject.as_str().to_string());
	let time_phys = frag
		.physical_column_name(schema.time.as_str())
		.unwrap_or_else(|| schema.time.as_str().to_string());

	let subject = ColumnBinding::new(
		SourceColumnName::new(subject_phys),
		schema.subject.clone(),
	);
	let time = ColumnBinding::new(
		SourceColumnName::new(time_phys),
		schema.time.clone(),
	);

	// Component bindings — taken from the first measurement that has
	// any. By construction, all measurements in a source group share the
	// same component dimensions (it's a property of the underlying
	// DataFrame schema, not of the measurement).
	let components: Vec<ColumnBinding> = representative
		.unit
		.components
		.iter()
		.map(|c| {
			let phys = frag
				.physical_column_name(c.as_str())
				.unwrap_or_else(|| c.as_str().to_string());
			ColumnBinding::new(SourceColumnName::new(phys), c.clone())
		})
		.collect();

	// Build a SourceMember per measurement in the group.
	let members: Vec<SourceMember> = mds
		.iter()
		.map(|md| build_source_member(md))
		.collect();

	Ok(SourceContext {
		source_name,
		source_key,
		subject,
		time,
		components,
		members,
	})
}

/// Project one `MeasurementData` to a [`SourceMember`], pulling the
/// physical value column from the fragment and copying the two
/// null-fill semantics from the `MeasurementUnit`.
fn build_source_member(md: &MeasurementData) -> SourceMember {
	let frag = md.fragment();
	let value_phys = frag
		.physical_column_name(md.unit.name.as_str())
		.unwrap_or_else(|| md.unit.name.as_str().to_string());

	let mut binding = CodomainBinding::new(
		SourceColumnName::new(value_phys),
		md.unit.name.clone(),
	);
	if let Some(ref fill) = md.unit.null_value {
		binding = binding.with_source_null_fill(fill.clone());
	}
	if let Some(ref fill) = md.unit.null_value_extension {
		binding = binding.with_join_null_fill(fill.clone());
	}

	SourceMember::new(EtlUnitRef::measurement(md.unit.name.clone()), binding)
}

/// Build a `CrushPlan` containing one `Crush::Components` per source
/// that has component dimensions. Sources without components contribute
/// nothing — they pass through to the join stage unchanged.
fn build_crush_plan(sources: &[Arc<SourceContext>], universe: &Universe) -> CrushPlan {
	let crushes: Vec<Crush> = sources
		.iter()
		.filter(|src| src.has_components())
		.map(|src| {
			let members: Vec<CrushMember> = src
				.members
				.iter()
				.filter_map(|m| {
					// Look up the measurement to get its declared aggregation.
					let canonical = m.unit.name();
					let mu = (&universe.measurements).get(canonical)?;
					Some(CrushMember::new(
						m.unit.clone(),
						m.value.clone(),
						mu.unit.signal_aggregation(),
					))
				})
				.collect();
			Crush::components(src.clone(), members)
		})
		.collect();
	CrushPlan { crushes }
}

/// Build a `JoinPlan` with one `SourceJoin` per `(SourceKey,
/// GroupSignalConfig)` pair. Each join brings *all* of its members onto
/// the cumulative left frame in a single LEFT JOIN — this is the
/// structural shape that enables the wide-join optimization.
///
/// Subgrouping by signal config means members of one `SourceJoin` are
/// **structurally guaranteed** to be batchable in a single signal-policy
/// + resample pass. The wide-join executor doesn't need to re-check
/// compatibility per call.
fn build_join_plan(sources: &[Arc<SourceContext>], universe: &Universe) -> JoinPlan {
	use std::collections::HashMap;

	let mut joins: Vec<SourceJoin> = Vec::new();
	for src in sources {
		// Subgroup this source's members by signal config. Preserve
		// insertion order so plan output is deterministic.
		let mut by_config: HashMap<GroupSignalConfig, Vec<JoinColumn>> = HashMap::new();
		let mut config_order: Vec<GroupSignalConfig> = Vec::new();

		for member in &src.members {
			// Skip non-measurements — qualities take a different path
			// (today they're loaded from qualities.parquet, not joined
			// here).
			if !member.unit.is_measurement() {
				continue;
			}
			let mu = match (&universe.measurements).get(member.unit.name()) {
				Some(md) => &md.unit,
				None => continue,
			};
			let config = GroupSignalConfig::new(
				mu.signal_policy.as_ref().map(|p| p.ttl().as_millis() as i64),
				mu.sample_rate_ms,
			);
			if !by_config.contains_key(&config) {
				config_order.push(config.clone());
			}
			by_config
				.entry(config)
				.or_default()
				.push(JoinColumn::new(member.unit.clone(), member.value.clone()));
		}

		for config in config_order {
			let columns = by_config.remove(&config).expect("config was just inserted");
			joins.push(SourceJoin::new(
				src.clone(),
				JoinKeys::SubjectTime,
				config,
				columns,
			));
		}
	}
	JoinPlan { joins }
}

/// Convert a request-level subject filter into the plan's
/// `Vec<SubjectValue>` representation. Returns `None` for unset filters
/// or non-`Include` variants (which the plan layer can't pre-narrow —
/// `Exclude` requires post-fetch filtering on the result).
fn subject_filter_to_values(filter: Option<&SubjectFilter>) -> Option<Vec<SubjectValue>> {
	match filter? {
		SubjectFilter::Include(values) => {
			let strings: Vec<SubjectValue> = values
				.iter()
				.filter_map(|v| v.as_str().map(|s| SubjectValue::new(s.to_string())))
				.collect();
			if strings.is_empty() {
				None
			} else {
				Some(strings)
			}
		}
		// Other variants (e.g., Exclude) are not pre-narrowable; the
		// executor handles them after the filter stage.
		_ => None,
	}
}

#[cfg(test)]
mod tests {
	// Builder tests are end-to-end and require a real Universe; they
	// live in the universe module's test suite. The plan-side
	// invariants (one source per Arc, filters carry consumers, etc.)
	// are exercised by the per-module tests already.
}
