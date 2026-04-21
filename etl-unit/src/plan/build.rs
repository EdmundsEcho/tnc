//! `BuildPlan` — the top-level plan for a store-builder run.
//!
//! Composes an [`ExtractionCore`] with a [`BuildSink`] that says where
//! the result goes. The store-builder pipeline runs the same Filter and
//! Crush stages as the runtime, then writes to its sink. It does not run
//! signal policy, joins, or derivations.
//!
//! # Today's sink: qualities
//!
//! The only sink variant today is [`BuildSink::Qualities`], which writes
//! a `qualities.parquet` to a path. The crush plan in the core uses
//! [`Crush::Time`](super::crush::Crush::Time) to collapse each source's
//! time dimension to a single row per subject — exactly the shape of
//! `qualities.parquet`.
//!
//! Future sinks (measurement-store partitions, manifest updates, etc.)
//! become additional [`BuildSink`] variants without changing the rest
//! of the plan layer.
//!
//! # Type-safe dispatch
//!
//! `BuildPlan` and [`super::RuntimePlan`] are distinct types. The
//! store-builder entry point takes `&BuildPlan`; the runtime executor
//! takes `&RuntimePlan`. Neither can accept the other.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::core::ExtractionCore;

/// Where a build pipeline writes its output.
///
/// New variants can be added as additional store-builder use cases come
/// online. Each variant carries the parameters specific to that sink.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BuildSink {
	/// Write a `qualities.parquet` to the given path.
	///
	/// The build pipeline produces one row per subject (achieved by a
	/// [`Crush::Time`](super::crush::Crush::Time) operation) containing
	/// all quality columns declared on the sources.
	Qualities {
		/// Output path for the parquet file. Typically the store root
		/// (e.g. `s3-prefix/qualities.parquet` or
		/// `/var/local/store/scada/qualities.parquet`).
		path: PathBuf,
	},
}

impl BuildSink {
	/// Convenience constructor for the qualities sink.
	pub fn qualities(path: impl Into<PathBuf>) -> Self {
		Self::Qualities { path: path.into() }
	}

	/// True if this sink writes a qualities parquet file.
	pub fn is_qualities(&self) -> bool {
		matches!(self, Self::Qualities { .. })
	}
}

/// The top-level plan for a store-builder run.
///
/// Stages applied by the executor, in order:
///
/// 1. **Filter** (from `core.filter`) — usually unbounded over time and
///    subjects (a full sweep), or scoped to one partition coordinate
///    when building incrementally.
/// 2. **Crush** (from `core.crush`) — for the qualities sink, contains
///    `Crush::Time` operations that collapse each source's rows to one
///    per subject.
/// 3. **Sink** (from `sink`) — write the resulting frame to its
///    destination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildPlan {
	/// Shared filter + crush substrate.
	pub core: ExtractionCore,
	/// Where the result goes.
	pub sink: BuildSink,
}

impl BuildPlan {
	/// Construct a build plan from its components.
	pub fn new(core: ExtractionCore, sink: BuildSink) -> Self {
		Self { core, sink }
	}

	/// Convenience: build a qualities-writing plan.
	pub fn qualities(core: ExtractionCore, path: impl Into<PathBuf>) -> Self {
		Self::new(core, BuildSink::qualities(path))
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

	use crate::aggregation::Aggregate;
	use crate::plan::bindings::{CodomainBinding, ColumnBinding};
	use crate::plan::crush::{Crush, CrushMember, CrushPlan};
	use crate::plan::filter::{FilterPlan, SourceFilter};
	use crate::plan::source_context::{SourceContext, SourceKey, SourceMember};
	use crate::unit_ref::EtlUnitRef;
	use crate::universe::measurement_storage::DataSourceName;

	fn scada_with_qualities() -> Arc<SourceContext> {
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
	fn build_sink_qualities_constructor() {
		let sink = BuildSink::qualities("/tmp/qualities.parquet");
		assert!(sink.is_qualities());
		match sink {
			BuildSink::Qualities { path } => {
				assert_eq!(path.to_str().unwrap(), "/tmp/qualities.parquet");
			}
		}
	}

	#[test]
	fn qualities_constructor_assembles_full_plan() {
		let src = scada_with_qualities();
		let core = ExtractionCore::new(
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
		);
		let plan = BuildPlan::qualities(core, "/store/scada/qualities.parquet");
		assert_eq!(plan.source_count(), 1);
		assert_eq!(plan.core.crush.time_count(), 1);
		assert!(plan.sink.is_qualities());
	}

	#[test]
	fn shared_core_works_for_both_plan_types() {
		// The same ExtractionCore can be cloned into either a RuntimePlan
		// or a BuildPlan. This is the central composition guarantee.
		use crate::plan::runtime::{DerivationPlan, RuntimePlan};
		use crate::plan::join::JoinPlan;

		let src = scada_with_qualities();
		let core = ExtractionCore::new(
			FilterPlan {
				filters: vec![SourceFilter::from_context(src, None, None)],
			},
			CrushPlan::empty(),
		);

		let runtime = RuntimePlan::new(core.clone(), JoinPlan::empty(), DerivationPlan::empty());
		let build = BuildPlan::qualities(core, "/q.parquet");

		assert_eq!(runtime.source_count(), 1);
		assert_eq!(build.source_count(), 1);
	}

	#[test]
	fn serde_roundtrip_build_plan() {
		let src = scada_with_qualities();
		let plan = BuildPlan::qualities(
			ExtractionCore::new(
				FilterPlan {
					filters: vec![SourceFilter::from_context(src, None, None)],
				},
				CrushPlan::empty(),
			),
			"/q.parquet",
		);
		let json = serde_json::to_string(&plan).unwrap();
		assert!(json.contains("\"kind\":\"qualities\""));
		assert!(json.contains("/q.parquet"));
		let back: BuildPlan = serde_json::from_str(&json).unwrap();
		assert!(back.sink.is_qualities());
		assert_eq!(back.source_count(), 1);
	}
}
