//! Typed subset pipeline stages.
//!
//! Each stage describes a transformation on the subset DataFrame.
//! Stages are planned from the request, then executed sequentially.
//! Each execution produces a diagnostic entry.
//!
//! # Type-driven design
//!
//! The stage enum makes the pipeline observable:
//! - Planning produces a `Vec<SubsetStage>` — visible before execution
//! - Execution produces `Vec<StageDiag>` — matches 1:1 with the plan
//! - The diagnostic panel renders the stage trace directly

use serde::{Deserialize, Serialize};

/// A single stage in the subset pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "stage", rename_all = "snake_case")]
pub enum SubsetStage {
	/// Construct the master time grid (subject × time).
	/// All measurements are joined onto this grid downstream.
	BuildMasterGrid {
		/// Grid step size in ms — expected to equal unified_rate_ms.
		interval_ms: i64,
		/// Min grid time (ms since epoch), aligned to interval boundary.
		grid_min_ms: i64,
		/// Max grid time (ms since epoch), aligned to interval boundary.
		grid_max_ms: i64,
		/// Number of time points on the grid.
		n_time_points: usize,
		/// Number of subjects in the grid.
		n_subjects: usize,
		/// Whether the grid covers historical, forecast, or both.
		temporality: String,
	},
	/// Filter a source DataFrame by time range and/or subjects.
	/// The measurement field lists all measurements served by this filter.
	/// One filter per source group — measurements sharing a source are filtered together.
	Filter {
		measurement: String,
		has_time_filter: bool,
		has_subject_filter: bool,
	},
	/// Crush component dimensions via aggregation.
	Crush {
		measurement: String,
		aggregation: String,
	},
	/// Join measurement onto the master grid.
	JoinMeasurement {
		measurement: String,
	},
	/// Wide LEFT JOIN bringing multiple measurements from one source
	/// onto the master grid in a single pass. Replaces N consecutive
	/// `JoinMeasurement` stages when the plan layer determines the
	/// members are batchable (same source, same signal config, no
	/// component dimensions, no upsampling required).
	WideJoin {
		/// Names of every measurement brought in by this wide join, in
		/// plan order.
		measurements: Vec<String>,
		/// Logical source name (for diagnostics — the diagnostics panel
		/// shows "wide_join — scada (sump, fuel, ...)").
		source: String,
	},
	/// Typed pipeline phase for a per-measurement processing step.
	/// Replaces the inline filter/crush/join/null_fill stages for
	/// measurements handled by the pipeline module.
	Pipeline {
		measurement: String,
		phase: String,
	},
	/// Fill nulls introduced by the join.
	FillNull {
		column: String,
		value: String,
	},
	/// Join a quality column.
	JoinQuality {
		quality: String,
	},
	/// Filter by quality value.
	QualityFilter {
		quality: String,
		values: Vec<String>,
	},
	/// Compute derived measurements.
	ComputeDerivations {
		count: usize,
	},
	/// Final time range filter.
	TimeFilter,
	/// Final subject filter.
	SubjectFilter,
	/// Apply a report interval — aggregate each measurement into coarse
	/// time buckets (monthly/weekly/daily/hourly/fixed). Output is the
	/// bucketed DataFrame; per-cell stats (N, stderr, min, max) land on
	/// [`super::SubsetInfo::interval_stats`].
	ReportInterval {
		/// Polars truncate spec for the bucket, e.g. "1mo", "1w".
		bucket: String,
		/// Strategy requested (auto/upsample/native/aggregate_or_sparse).
		strategy: String,
		/// Number of distinct bucket boundaries produced.
		n_buckets: usize,
		/// Number of measurement columns aggregated.
		n_measurements: usize,
	},
}

/// Diagnostic output from executing one stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageDiag {
	/// The stage that was executed.
	pub stage: SubsetStage,
	/// Rows in the result DataFrame after this stage.
	pub rows_after: usize,
	/// Execution time in microseconds.
	pub elapsed_us: u64,
	/// Free-form structured notes. Stages use this for context-specific
	/// detail that isn't worth a dedicated variant field (time ranges,
	/// match/miss counts, null counts, grid alignment checks, etc.).
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub notes: Vec<String>,
}

impl StageDiag {
	/// Shorthand constructor without notes.
	pub fn new(stage: SubsetStage, rows_after: usize, elapsed_us: u64) -> Self {
		Self { stage, rows_after, elapsed_us, notes: Vec::new() }
	}
}
