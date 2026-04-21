//! Data Transfer Objects for JSON deserialization
//!
//! These types match the JSON schema format and are converted to runtime types.
//! The schema is purely canonical - source column mappings are handled by BoundSource.

use serde::{Deserialize, Serialize};

use crate::{MeasurementKind, PointwiseExpr};

/// Root DTO for JSON schema files
///
/// # Example JSON
///
/// ```json
/// {
///   "name": "pump_station",
///   "subject": "station",
///   "time": "timestamp",
///   "qualities": [
///     { "name": "region" }
///   ],
///   "measurements": [
///     {
///       "name": "water_level",
///       "kind": "measure"
///     },
///     {
///       "name": "engine_status",
///       "kind": "categorical",
///       "components": ["engine_id"]
///     }
///   ]
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDto {
	pub name: String,

	/// Canonical subject name
	pub subject: String,

	/// Canonical time name
	pub time: String,

	#[serde(default)]
	pub qualities: Vec<QualityDto>,

	#[serde(default)]
	pub measurements: Vec<MeasurementDto>,

	#[serde(default)]
	pub derivations: Vec<DerivationDto>,
}

// =============================================================================
// Quality DTO
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityDto {
	/// Quality canonical name (also used as unit name)
	pub name: String,

	/// Optional chart hints
	#[serde(default)]
	pub chart_hints: Option<ChartHintsDto>,
}

// =============================================================================
// Measurement DTO
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasurementDto {
	/// Measurement canonical name (also used as unit name)
	pub name: String,

	/// Measurement kind
	pub kind: MeasurementKind,

	/// Component canonical names (optional)
	#[serde(default)]
	pub components: Vec<String>,

	/// Optional signal policy
	#[serde(default)]
	pub signal_policy: Option<SignalPolicyDto>,

	/// Optional chart hints
	#[serde(default)]
	pub chart_hints: Option<ChartHintsDto>,
}

// =============================================================================
// Signal Policy DTO
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalPolicyDto {
	/// Maximum staleness duration (e.g., "60s", "5m")
	#[serde(with = "humantime_serde")]
	pub max_staleness: std::time::Duration,

	/// Windowing strategy
	pub windowing: WindowStrategyDto,

	/// Time format string (e.g., "%Y-%m-%d %H:%M:%S")
	#[serde(default)]
	pub time_format: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WindowStrategyDto {
	/// Instant value (no windowing)
	Instant,

	/// Sliding window with duration and minimum samples
	Sliding {
		#[serde(with = "humantime_serde")]
		duration:    std::time::Duration,
		min_samples: u32,
	},
}

// =============================================================================
// Chart Hints DTO
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChartHintsDto {
	#[serde(default)]
	pub label: Option<String>,

	#[serde(default)]
	pub color: Option<String>,

	#[serde(default)]
	pub stepped: Option<bool>,

	#[serde(default)]
	pub tension: Option<f32>,

	/// Axis identifier: "y", "y1", or "y2"
	#[serde(default)]
	pub axis: Option<String>,

	/// Chart type: "line", "bar", "scatter", or { "bubble": { "size": "column" } }
	#[serde(default)]
	pub chart_type: Option<ChartTypeDto>,

	/// Index type: "time", "subject", or { "quality": "column" }, { "component": "column" }
	#[serde(default)]
	pub index: Option<IndexDto>,

	/// Series grouping: "subject", or { "component": "column" }, { "quality": "column" }
	#[serde(default)]
	pub series: Option<SeriesDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChartTypeDto {
	Line,
	Bar,
	Scatter,
	Bubble {
		size: String,
	},
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexDto {
	Time,
	Subject,
	Quality(String),
	Component(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeriesDto {
	Subject,
	Quality(String),
	Component(String),
	SubjectAndComponent(String),
}

// =============================================================================
// Derivation DTO
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivationDto {
	/// Derivation name (output column)
	pub name: String,

	/// Computation definition
	pub computation: ComputationDto,

	/// Output measurement kind
	pub kind: MeasurementKind,

	/// Optional chart hints
	#[serde(default)]
	pub chart_hints: Option<ChartHintsDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputationDto {
	/// Pointwise expression - uses the runtime type directly since it's serde-compatible
	pub pointwise: PointwiseExpr,
}
