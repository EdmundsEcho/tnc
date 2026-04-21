//! `FilterPlan` — one filter pass per unique source.
//!
//! The Filter stage reduces raw source `DataFrame`s to the request's
//! time range and subject set. Because every etl-unit extracted from a
//! source shares the same physical subject and time columns (the
//! shared-source invariant captured by [`SourceContext`]), one filter
//! pass per source serves all members of that source.
//!
//! # Optimization metric
//!
//! [`FilterPlan::pass_count`] vs [`FilterPlan::consumer_count`] tells you
//! how much work the shared-source invariant saved. Five SCADA
//! measurements + one quality reading from one SCADA `Arc` produce
//! `pass_count = 1` and `consumer_count = 6`.
//!
//! # Null fills
//!
//! Source-level null fills (the `null_value` config field) live on
//! [`CodomainBinding::source_null_fill`](super::bindings::CodomainBinding).
//! `SourceFilter` extracts them at construction time into a flat
//! [`NullFill`] list keyed by physical column, which is what the
//! filter executor needs. Members without `source_null_fill` contribute
//! nothing.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::column::SourceColumnName;
use crate::request::TimeRange;
use crate::subject::SubjectValue;
use crate::unit::NullValue;
use crate::unit_ref::EtlUnitRef;

use super::source_context::{SourceContext, SourceKey};

/// A single source-level null fill applied during the Filter stage.
///
/// Sourced from [`CodomainBinding::source_null_fill`](super::bindings::CodomainBinding).
/// One entry per (source, physical column) pair where the member declared
/// a `null_value`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NullFill {
	/// Physical column in the source `DataFrame` to fill.
	pub column: SourceColumnName,
	/// The replacement value.
	pub value:  NullValue,
}

/// One filter operation against one source.
///
/// All measurements and qualities served by the source share the
/// filtered output. The executor consults `time_range`, `subject_set`,
/// and `null_fills` and produces a single filtered (and null-filled)
/// `DataFrame` for the source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceFilter {
	/// The source being filtered. Held by `Arc` so multiple plan nodes
	/// (filter, crush, signal-policy, join) can reference the same
	/// context without copying.
	pub source: Arc<SourceContext>,
	/// Optional time range filter. `None` = no time filter (sweep all).
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub time_range: Option<TimeRange>,
	/// Optional subject filter. `None` = all subjects.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub subject_set: Option<Vec<SubjectValue>>,
	/// Source-level null fills. Empty when no member declares `null_value`.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub null_fills: Vec<NullFill>,
	/// Etl-units served by this filter pass. Diagnostics use this to
	/// report "this filter served N consumers."
	pub consumers: Vec<EtlUnitRef>,
}

impl SourceFilter {
	/// Build a `SourceFilter` from a source context, propagating null
	/// fills and consumer identities from the context's members.
	///
	/// `time_range` and `subject_set` come from the request (or from a
	/// store-builder partition coordinate).
	pub fn from_context(
		source: Arc<SourceContext>,
		time_range: Option<TimeRange>,
		subject_set: Option<Vec<SubjectValue>>,
	) -> Self {
		let null_fills = source
			.members
			.iter()
			.filter_map(|m| {
				m.value.source_null_fill.as_ref().map(|fill| NullFill {
					column: m.value.physical.clone(),
					value:  fill.clone(),
				})
			})
			.collect();
		let consumers = source.members.iter().map(|m| m.unit.clone()).collect();
		Self {
			source,
			time_range,
			subject_set,
			null_fills,
			consumers,
		}
	}

	/// The Arc-pointer identity of the underlying source.
	pub fn source_key(&self) -> SourceKey {
		self.source.source_key
	}
}

/// One filter operation per unique source. The fundamental "shared-source
/// = shared filter" optimization is encoded structurally here: there is
/// no way to construct a `FilterPlan` with two filters against the same
/// source without explicitly choosing to do so.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FilterPlan {
	pub filters: Vec<SourceFilter>,
}

impl FilterPlan {
	/// Construct an empty plan.
	pub fn empty() -> Self {
		Self::default()
	}

	/// Build a filter plan from a list of source contexts and the
	/// request-level filter parameters. Produces one [`SourceFilter`]
	/// per source in input order; null fills and consumers are
	/// derived from each source's members.
	pub fn build(
		sources: impl IntoIterator<Item = Arc<SourceContext>>,
		time_range: Option<TimeRange>,
		subject_set: Option<Vec<SubjectValue>>,
	) -> Self {
		let filters = sources
			.into_iter()
			.map(|src| SourceFilter::from_context(src, time_range.clone(), subject_set.clone()))
			.collect();
		Self { filters }
	}

	/// Number of physical filter passes — one per unique source.
	pub fn pass_count(&self) -> usize {
		self.filters.len()
	}

	/// Total number of consumers (members) across all filters.
	///
	/// `consumer_count - pass_count` is the work saved by the
	/// shared-source invariant: that many measurements/qualities did
	/// not need their own filter pass.
	pub fn consumer_count(&self) -> usize {
		self.filters.iter().map(|f| f.consumers.len()).sum()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plan::bindings::{CodomainBinding, ColumnBinding};
	use crate::plan::source_context::SourceMember;
	use crate::universe::measurement_storage::DataSourceName;

	fn make_scada() -> Arc<SourceContext> {
		Arc::new(SourceContext {
			source_name: DataSourceName::new("scada"),
			source_key:  SourceKey::from_raw(0xA1),
			subject:     ColumnBinding::new("station_id", "station_name"),
			time:        ColumnBinding::new("obs_time", "timestamp"),
			components:  vec![],
			members:     vec![
				SourceMember::new(
					EtlUnitRef::measurement("sump"),
					CodomainBinding::new("sump_reading", "sump"),
				),
				SourceMember::new(
					EtlUnitRef::measurement("discharge"),
					CodomainBinding::new("discharge_reading", "discharge"),
				),
				SourceMember::new(
					EtlUnitRef::measurement("engines_on_count"),
					CodomainBinding::new("engine_count", "engines_on_count")
						.with_source_null_fill(NullValue::Integer(0)),
				),
			],
		})
	}

	fn make_mrms() -> Arc<SourceContext> {
		Arc::new(SourceContext {
			source_name: DataSourceName::new("mrms"),
			source_key:  SourceKey::from_raw(0xB2),
			subject:     ColumnBinding::new("station_name", "station_name"),
			time:        ColumnBinding::new("timestamp", "timestamp"),
			components:  vec![],
			members:     vec![SourceMember::new(
				EtlUnitRef::measurement("historical_precip"),
				CodomainBinding::new("value_mm", "historical_precip")
					.with_source_null_fill(NullValue::Float(0.0)),
			)],
		})
	}

	#[test]
	fn from_context_lists_all_members_as_consumers() {
		let scada = make_scada();
		let f = SourceFilter::from_context(scada, None, None);
		assert_eq!(f.consumers.len(), 3);
		assert!(f.consumers.iter().any(|u| u.as_str() == "sump"));
		assert!(f.consumers.iter().any(|u| u.as_str() == "discharge"));
		assert!(f.consumers.iter().any(|u| u.as_str() == "engines_on_count"));
	}

	#[test]
	fn from_context_extracts_only_members_with_source_null_fill() {
		let scada = make_scada();
		let f = SourceFilter::from_context(scada, None, None);
		// Only engines_on_count declared a source_null_fill.
		assert_eq!(f.null_fills.len(), 1);
		assert_eq!(f.null_fills[0].column.as_str(), "engine_count");
		assert_eq!(f.null_fills[0].value, NullValue::Integer(0));
	}

	#[test]
	fn from_context_no_null_fills_when_none_declared() {
		// A source whose members all have None for source_null_fill.
		let ctx = Arc::new(SourceContext {
			source_name: DataSourceName::new("clean"),
			source_key:  SourceKey::from_raw(0xC3),
			subject:     ColumnBinding::identity("station"),
			time:        ColumnBinding::identity("time"),
			components:  vec![],
			members:     vec![SourceMember::new(
				EtlUnitRef::measurement("v"),
				CodomainBinding::new("v", "v"),
			)],
		});
		let f = SourceFilter::from_context(ctx, None, None);
		assert!(f.null_fills.is_empty());
	}

	#[test]
	fn from_context_propagates_request_filters() {
		let scada = make_scada();
		let subjects = vec![SubjectValue::new("Coastal"), SubjectValue::new("Parr")];
		let f = SourceFilter::from_context(scada, None, Some(subjects.clone()));
		assert_eq!(f.subject_set.as_deref(), Some(subjects.as_slice()));
		assert!(f.time_range.is_none());
	}

	#[test]
	fn source_key_round_trips_through_filter() {
		let scada = make_scada();
		let key = scada.source_key;
		let f = SourceFilter::from_context(scada, None, None);
		assert_eq!(f.source_key(), key);
	}

	#[test]
	fn build_creates_one_filter_per_source() {
		let plan = FilterPlan::build([make_scada(), make_mrms()], None, None);
		assert_eq!(plan.pass_count(), 2);
	}

	#[test]
	fn build_consumer_count_sums_across_sources() {
		let plan = FilterPlan::build([make_scada(), make_mrms()], None, None);
		// SCADA has 3 members, MRMS has 1.
		assert_eq!(plan.consumer_count(), 4);
	}

	#[test]
	fn pass_count_vs_consumer_count_is_the_optimization_metric() {
		// SCADA contributes the shared-source savings: 3 members → 1 pass.
		let plan = FilterPlan::build([make_scada()], None, None);
		assert_eq!(plan.pass_count(), 1);
		assert_eq!(plan.consumer_count(), 3);
		// Two consumers were "free" — they piggybacked on the same filter.
	}

	#[test]
	fn empty_plan_has_zero_passes_and_consumers() {
		let plan = FilterPlan::empty();
		assert_eq!(plan.pass_count(), 0);
		assert_eq!(plan.consumer_count(), 0);
	}

	#[test]
	fn serde_roundtrip_filter_plan() {
		let plan = FilterPlan::build([make_scada(), make_mrms()], None, None);
		let json = serde_json::to_string(&plan).unwrap();
		let back: FilterPlan = serde_json::from_str(&json).unwrap();
		assert_eq!(back.pass_count(), 2);
		assert_eq!(back.consumer_count(), 4);
	}

	#[test]
	fn serde_skips_empty_optional_fields() {
		let scada = make_scada();
		let f = SourceFilter::from_context(scada, None, None);
		let json = serde_json::to_string(&f).unwrap();
		assert!(!json.contains("time_range"));
		assert!(!json.contains("subject_set"));
		// null_fills present because SCADA has the engines fill
		assert!(json.contains("null_fills"));
	}
}
