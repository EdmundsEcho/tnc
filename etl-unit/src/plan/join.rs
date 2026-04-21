//! `JoinPlan` — composes per-source frames onto a unified grid.
//!
//! After Filter, Crush, and SignalPolicy have run, each source has been
//! reduced to a per-source `DataFrame` keyed on either `(subject, time)`
//! (measurements) or `(subject)` (qualities). The Join stage brings them
//! all onto one frame via LEFT JOINs.
//!
//! # The grid is sample-rate-derived, not source-derived
//!
//! The master grid that the joins target is built from
//! `AlignmentSpec::unified_rate_ms` and the request time range. It is
//! not a source `DataFrame`, and the plan does not name a "grid source."
//! Every source LEFT JOINs onto the synthetic grid.
//!
//! `AlignmentSpec` is owned by the universe-builder layer and is left
//! untouched by this refactor; the `JoinPlan` only describes the join
//! operations.
//!
//! # One join per source, not one join per column
//!
//! A source contributing five value columns (e.g., SCADA's `sump`,
//! `discharge`, `suction`, `fuel`, `engines_on_count`) produces **one**
//! [`SourceJoin`] with `columns.len() == 5`, not five separate joins.
//! This is the grouped-join optimization the previous TODO in
//! `universe_of_etlunits.rs` flagged as ~780ms savings for the SCADA
//! pump-station path.
//!
//! # Null fills
//!
//! `null_value_extension` (the join-induced null fill) lives on each
//! [`CodomainBinding::join_null_fill`](super::bindings::CodomainBinding)
//! and rides along inside [`JoinColumn::binding`]. The executor inspects
//! the binding when applying the join and fills the joined column in the
//! same lazy pass.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::unit_ref::EtlUnitRef;

use super::bindings::CodomainBinding;
use super::source_context::{SourceContext, SourceKey};

/// One value column carried by a [`SourceJoin`].
///
/// `binding` carries any `join_null_fill` declared on the etl-unit, so
/// the join executor doesn't need to chase down a separate null-handling
/// table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinColumn {
	/// The etl-unit identity (Measurement or Quality).
	pub unit:    EtlUnitRef,
	/// The codomain binding, including any `join_null_fill`.
	pub binding: CodomainBinding,
}

impl JoinColumn {
	pub fn new(unit: EtlUnitRef, binding: CodomainBinding) -> Self {
		Self { unit, binding }
	}

	/// Whether this column needs a join-null fill applied after the join.
	pub fn has_join_fill(&self) -> bool {
		self.binding.join_null_fill.is_some()
	}
}

/// Which key columns the LEFT JOIN uses.
///
/// Measurements join on `(subject, time)` because their rows are
/// time-stamped. Qualities join on `(subject)` only — one quality row
/// per subject is broadcast across every time row of the left frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinKeys {
	/// Join on `(subject, time)` — for measurement sources.
	SubjectTime,
	/// Join on `(subject)` only — for quality sources.
	Subject,
}

/// Signal-policy parameters that members of a [`SourceJoin`] must
/// share to be batchable on the wide-join path.
///
/// Two measurements from the same source can be processed in one
/// batched signal-policy + resample pass only if they agree on:
///
/// - **TTL** — drives the truncate-to-grid step in signal policy and
///   bounds the forward-fill staleness window. Both are frame-level
///   operations on a single time column; mixed TTLs cannot share one
///   `group_by_dynamic` pass.
/// - **Sample rate** — derives the per-measurement `AlignAction`
///   (`SignalOnly`, `Upsample`, `Downsample`, `PassThrough`) when
///   compared with the universe's `unified_rate_ms`. Members in
///   different action classes go through different code paths in the
///   resample stage and cannot be batched.
///
/// `build_join_plan` subgroups source members by `(SourceKey,
/// GroupSignalConfig)` so members of one `SourceJoin` are
/// **structurally guaranteed** batchable. The wide-join executor can
/// rely on this without re-checking compatibility per call.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GroupSignalConfig {
	/// Signal policy TTL in milliseconds. `None` = measurement has no
	/// signal policy declared.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub ttl_ms:         Option<i64>,
	/// Native sample rate in milliseconds. `None` = not declared on the
	/// measurement.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sample_rate_ms: Option<i64>,
}

impl GroupSignalConfig {
	pub fn new(ttl_ms: Option<i64>, sample_rate_ms: Option<i64>) -> Self {
		Self {
			ttl_ms,
			sample_rate_ms,
		}
	}
}

/// One LEFT JOIN against the cumulative left frame.
///
/// Brings every value column from `right_source` into the result in a
/// single join operation. The join key shape (`SubjectTime` vs `Subject`)
/// is determined by what kind of etl-units the source contributes.
///
/// All members of one `SourceJoin` share the same [`GroupSignalConfig`]
/// — that's the structural invariant the wide-join executor relies on
/// when batching signal policy + resample across multiple value columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceJoin {
	/// The source whose columns are being joined in. Held by `Arc` so
	/// the same context shared by Filter / Crush / SignalPolicy plans
	/// is reused here.
	pub right_source:  Arc<SourceContext>,
	/// Whether the join uses `(subject, time)` or `(subject)` keys.
	pub keys:          JoinKeys,
	/// Signal-policy parameters shared by every member of this join.
	pub signal_config: GroupSignalConfig,
	/// All value columns brought in by this join, in one lazy pass.
	pub columns:       Vec<JoinColumn>,
}

impl SourceJoin {
	pub fn new(
		right_source: Arc<SourceContext>,
		keys: JoinKeys,
		signal_config: GroupSignalConfig,
		columns: Vec<JoinColumn>,
	) -> Self {
		Self {
			right_source,
			keys,
			signal_config,
			columns,
		}
	}

	/// Convenience: the source's key.
	pub fn right_source_key(&self) -> SourceKey {
		self.right_source.source_key
	}

	/// Iterator over columns that need a join-null fill applied.
	pub fn columns_with_join_fills(&self) -> impl Iterator<Item = &JoinColumn> {
		self.columns.iter().filter(|c| c.has_join_fill())
	}
}

/// A list of joins to perform after signal policy. Walked in order; each
/// join brings one source's columns onto the cumulative left frame.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct JoinPlan {
	pub joins: Vec<SourceJoin>,
}

impl JoinPlan {
	/// Construct an empty plan (the store-builder path uses this).
	pub fn empty() -> Self {
		Self::default()
	}

	/// Number of join operations — one per right-side source.
	pub fn op_count(&self) -> usize {
		self.joins.len()
	}

	/// Total columns brought in across all joins.
	///
	/// `column_count - op_count` is the work saved by grouping multiple
	/// columns from one source into a single LEFT JOIN. For the SCADA
	/// pump-station path that's roughly the 780ms the previous TODO
	/// flagged.
	pub fn column_count(&self) -> usize {
		self.joins.iter().map(|j| j.columns.len()).sum()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plan::bindings::{CodomainBinding, ColumnBinding};
	use crate::plan::source_context::SourceMember;
	use crate::unit::NullValue;
	use crate::universe::measurement_storage::DataSourceName;

	fn scada_source() -> Arc<SourceContext> {
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
			],
		})
	}

	fn mrms_source() -> Arc<SourceContext> {
		Arc::new(SourceContext {
			source_name: DataSourceName::new("mrms"),
			source_key:  SourceKey::from_raw(0xB2),
			subject:     ColumnBinding::identity("station_name"),
			time:        ColumnBinding::identity("timestamp"),
			components:  vec![],
			members:     vec![SourceMember::new(
				EtlUnitRef::measurement("historical_precip"),
				CodomainBinding::new("value_mm", "historical_precip")
					.with_join_null_fill(NullValue::Float(0.0)),
			)],
		})
	}

	fn quality_source() -> Arc<SourceContext> {
		Arc::new(SourceContext {
			source_name: DataSourceName::new("scada_qualities"),
			source_key:  SourceKey::from_raw(0xC3),
			subject:     ColumnBinding::identity("station_name"),
			time:        ColumnBinding::identity("timestamp"),
			components:  vec![],
			members:     vec![SourceMember::new(
				EtlUnitRef::quality("station_label"),
				CodomainBinding::new("display_name", "station_label"),
			)],
		})
	}

	#[test]
	fn join_column_has_join_fill_reflects_binding() {
		let no_fill = JoinColumn::new(
			EtlUnitRef::measurement("sump"),
			CodomainBinding::new("sump_reading", "sump"),
		);
		assert!(!no_fill.has_join_fill());

		let with_fill = JoinColumn::new(
			EtlUnitRef::measurement("engines_on_count"),
			CodomainBinding::new("engine_on", "engines_on_count")
				.with_join_null_fill(NullValue::Integer(0)),
		);
		assert!(with_fill.has_join_fill());
	}

	#[test]
	fn one_source_join_carries_all_its_columns() {
		// SCADA join brings sump + discharge in one operation.
		let scada = scada_source();
		let key = scada.source_key;
		let join = SourceJoin::new(
			scada,
			JoinKeys::SubjectTime,
			GroupSignalConfig::new(None, None),
			vec![
				JoinColumn::new(
					EtlUnitRef::measurement("sump"),
					CodomainBinding::new("sump_reading", "sump"),
				),
				JoinColumn::new(
					EtlUnitRef::measurement("discharge"),
					CodomainBinding::new("discharge_reading", "discharge"),
				),
			],
		);
		assert_eq!(join.right_source_key(), key);
		assert_eq!(join.columns.len(), 2);
		assert_eq!(join.keys, JoinKeys::SubjectTime);
	}

	#[test]
	fn columns_with_join_fills_filters_correctly() {
		let join = SourceJoin::new(
			mrms_source(),
			JoinKeys::SubjectTime,
			GroupSignalConfig::new(None, None),
			vec![
				JoinColumn::new(
					EtlUnitRef::measurement("historical_precip"),
					CodomainBinding::new("value_mm", "historical_precip")
						.with_join_null_fill(NullValue::Float(0.0)),
				),
				JoinColumn::new(
					EtlUnitRef::measurement("dummy"),
					CodomainBinding::new("d", "dummy"),
				),
			],
		);
		let with_fills: Vec<&JoinColumn> = join.columns_with_join_fills().collect();
		assert_eq!(with_fills.len(), 1);
		assert_eq!(with_fills[0].unit.as_str(), "historical_precip");
	}

	#[test]
	fn quality_join_uses_subject_only_keys() {
		let join = SourceJoin::new(
			quality_source(),
			JoinKeys::Subject,
			GroupSignalConfig::new(None, None),
			vec![JoinColumn::new(
				EtlUnitRef::quality("station_label"),
				CodomainBinding::new("display_name", "station_label"),
			)],
		);
		assert_eq!(join.keys, JoinKeys::Subject);
	}

	#[test]
	fn empty_plan_counts() {
		let plan = JoinPlan::empty();
		assert_eq!(plan.op_count(), 0);
		assert_eq!(plan.column_count(), 0);
	}

	#[test]
	fn op_count_vs_column_count_is_the_optimization_metric() {
		// One SCADA join with 2 columns + one MRMS join with 1 column =
		// 2 join ops carrying 3 columns total.
		let plan = JoinPlan {
			joins: vec![
				SourceJoin::new(
					scada_source(),
					JoinKeys::SubjectTime,
					GroupSignalConfig::new(None, None),
					vec![
						JoinColumn::new(
							EtlUnitRef::measurement("sump"),
							CodomainBinding::new("sump_reading", "sump"),
						),
						JoinColumn::new(
							EtlUnitRef::measurement("discharge"),
							CodomainBinding::new("discharge_reading", "discharge"),
						),
					],
				),
				SourceJoin::new(
					mrms_source(),
					JoinKeys::SubjectTime,
					GroupSignalConfig::new(None, None),
					vec![JoinColumn::new(
						EtlUnitRef::measurement("historical_precip"),
						CodomainBinding::new("value_mm", "historical_precip"),
					)],
				),
			],
		};
		assert_eq!(plan.op_count(), 2);
		assert_eq!(plan.column_count(), 3);
		// One column was "free" — it piggybacked on the SCADA join.
	}

	#[test]
	fn serde_roundtrip_join_plan() {
		let plan = JoinPlan {
			joins: vec![SourceJoin::new(
				scada_source(),
				JoinKeys::SubjectTime,
				GroupSignalConfig::new(None, None),
				vec![JoinColumn::new(
					EtlUnitRef::measurement("sump"),
					CodomainBinding::new("sump_reading", "sump"),
				)],
			)],
		};
		let json = serde_json::to_string(&plan).unwrap();
		let back: JoinPlan = serde_json::from_str(&json).unwrap();
		assert_eq!(back.op_count(), 1);
		assert_eq!(back.column_count(), 1);
	}
}
