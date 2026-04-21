//! Measurement unit: subject, time, [components] → value

use serde::{Deserialize, Serialize};

use super::null_value::NullValue;
use crate::{
	aggregation::Aggregate,
	chart_hints::ChartHints,
	column::{CanonicalColumnName, DomainSignature},
	signal_policy::SignalPolicy,
};

/// What type of measurement (determines default aggregation semantics)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MeasurementKind {
	/// Discrete count of events → Sum to aggregate
	Count,
	/// Continuous value (level, temperature, pressure) → Mean to aggregate
	Measure,
	/// Pre-computed average → Mean (N weighted if weight column specified)
	Average,
	/// Discrete state with string values (status codes, labels) → Last to aggregate
	Categorical,
	/// Binary on/off state → Any to aggregate (true if any is true)
	Binary,
}

impl MeasurementKind {
	/// Returns the default aggregation function for this measurement kind.
	pub fn default_aggregation(&self) -> Aggregate {
		match self {
			MeasurementKind::Measure => Aggregate::Mean,
			MeasurementKind::Count => Aggregate::Sum,
			MeasurementKind::Categorical => Aggregate::Last,
			MeasurementKind::Average => Aggregate::Mean,
			MeasurementKind::Binary => Aggregate::Any,
		}
	}

	/// Returns the default NullValue for this measurement kind.
	///
	/// This is used when no explicit null_value is configured but one is needed.
	pub fn default_null_value(&self) -> NullValue {
		match self {
			MeasurementKind::Count => NullValue::Integer(0),
			MeasurementKind::Measure => NullValue::Float(0.0),
			MeasurementKind::Average => NullValue::Float(0.0),
			MeasurementKind::Categorical => NullValue::String(String::new()),
			MeasurementKind::Binary => NullValue::Boolean(false),
		}
	}

	/// Check if a NullValue type is compatible with this measurement kind.
	///
	/// # Compatibility Rules
	/// - Count: Integer or Float
	/// - Measure: Float or Integer
	/// - Average: Float or Integer
	/// - Categorical: String or Integer (for status codes)
	/// - Binary: Boolean or Integer (0/1)
	pub fn is_compatible_null_value(&self, value: &NullValue) -> bool {
		match (self, value) {
			// Numeric kinds accept Float or Integer
			(MeasurementKind::Count, NullValue::Integer(_)) => true,
			(MeasurementKind::Count, NullValue::Float(_)) => true,
			(MeasurementKind::Measure, NullValue::Float(_)) => true,
			(MeasurementKind::Measure, NullValue::Integer(_)) => true,
			(MeasurementKind::Average, NullValue::Float(_)) => true,
			(MeasurementKind::Average, NullValue::Integer(_)) => true,

			// Categorical accepts String or Integer (status codes)
			(MeasurementKind::Categorical, NullValue::String(_)) => true,
			(MeasurementKind::Categorical, NullValue::Integer(_)) => true,

			// Binary accepts Boolean or Integer (0/1)
			(MeasurementKind::Binary, NullValue::Boolean(_)) => true,
			(MeasurementKind::Binary, NullValue::Integer(_)) => true,

			_ => false,
		}
	}

	/// Get a human-readable description of expected NullValue types for this kind.
	pub fn expected_null_value_types(&self) -> &'static str {
		match self {
			MeasurementKind::Count => "NullValue::Integer or NullValue::Float",
			MeasurementKind::Measure => "NullValue::Float or NullValue::Integer",
			MeasurementKind::Average => "NullValue::Float or NullValue::Integer",
			MeasurementKind::Categorical => "NullValue::String or NullValue::Integer",
			MeasurementKind::Binary => "NullValue::Boolean or NullValue::Integer",
		}
	}
}

// ============================================================================
// DataTemporality
// ============================================================================

/// Whether a measurement represents historical observations or future predictions
///
/// This affects how time ranges are computed in the master grid:
/// - Historical: Time range is backward-looking from "now" (or request end)
/// - Forecast: Time range is forward-looking from "now", uses data's natural range
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataTemporality {
	/// Observations of the past (SCADA telemetry, MRMS radar, etc.)
	#[default]
	Historical,
	/// Predictions of the future (HRRR, NBM, GFS forecasts, etc.)
	Forecast,
}

impl DataTemporality {
	pub fn is_historical(&self) -> bool {
		matches!(self, DataTemporality::Historical)
	}

	pub fn is_forecast(&self) -> bool {
		matches!(self, DataTemporality::Forecast)
	}
}

impl std::fmt::Display for DataTemporality {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			DataTemporality::Historical => write!(f, "historical"),
			DataTemporality::Forecast => write!(f, "forecast"),
		}
	}
}

/// Mapping of source values to boolean true/false for Binary measurements.
///
/// During extraction, source values are converted to 0/1 based on this mapping:
/// - Values in `true_values` → 1
/// - Values in `false_values` → 0 (if specified)
/// - Other values → 0 (if `false_values` is None) or null (if `false_values` is Some)
///
/// # Example
/// ```rust,ignore
/// // Simple numeric
/// TruthMapping::numeric()  // 1 → true, 0 → false
///
/// // String-based
/// TruthMapping::new()
///     .true_values(["on", "running", "active"])
///     .false_values(["off", "stopped", "inactive"])
/// ```
#[derive(PartialEq, Eq, Debug, Clone, Default, Serialize, Deserialize)]
pub struct TruthMapping {
	/// Values that map to true (1)
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub true_values: Vec<serde_json::Value>,

	/// Values that map to false (0). If None, anything not in true_values is false.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub false_values: Option<Vec<serde_json::Value>>,
}

impl TruthMapping {
	/// Create an empty truth mapping (configure with builder methods)
	pub fn new() -> Self {
		Self::default()
	}

	/// Create a numeric truth mapping: 1 → true, 0 → false
	pub fn numeric() -> Self {
		Self {
			true_values:  vec![serde_json::Value::from(1), serde_json::Value::from(1.0)],
			false_values: Some(vec![serde_json::Value::from(0), serde_json::Value::from(0.0)]),
		}
	}

	/// Create a boolean truth mapping: true → true, false → false
	pub fn boolean() -> Self {
		Self {
			true_values:  vec![serde_json::Value::Bool(true)],
			false_values: Some(vec![serde_json::Value::Bool(false)]),
		}
	}

	/// Set the true values (builder pattern)
	pub fn with_true_values<I, V>(mut self, values: I) -> Self
	where
		I: IntoIterator<Item = V>,
		V: Into<serde_json::Value>, {
		self.true_values = values.into_iter().map(|v| v.into()).collect();
		self
	}

	/// Set the false values (builder pattern)
	pub fn with_false_values<I, V>(mut self, values: I) -> Self
	where
		I: IntoIterator<Item = V>,
		V: Into<serde_json::Value>, {
		self.false_values = Some(values.into_iter().map(|v| v.into()).collect());
		self
	}

	/// Add string values that map to true
	pub fn true_strings<I, S>(mut self, values: I) -> Self
	where
		I: IntoIterator<Item = S>,
		S: Into<String>, {
		self.true_values.extend(
			values
				.into_iter()
				.map(|s| serde_json::Value::String(s.into())),
		);
		self
	}

	/// Add string values that map to false
	pub fn false_strings<I, S>(mut self, values: I) -> Self
	where
		I: IntoIterator<Item = S>,
		S: Into<String>, {
		let false_vals = self.false_values.get_or_insert_with(Vec::new);
		false_vals.extend(
			values
				.into_iter()
				.map(|s| serde_json::Value::String(s.into())),
		);
		self
	}

	/// Check if a value should be considered true
	pub fn is_true(&self, value: &serde_json::Value) -> bool {
		self.true_values.contains(value)
	}

	/// Check if a value should be considered false
	pub fn is_false(&self, value: &serde_json::Value) -> bool {
		match &self.false_values {
			Some(false_vals) => false_vals.contains(value),
			None => !self.is_true(value),
		}
	}

	/// Check if the mapping has any true values defined
	pub fn has_true_values(&self) -> bool {
		!self.true_values.is_empty()
	}

	/// Check if the mapping has explicit false values defined
	pub fn has_false_values(&self) -> bool {
		self
			.false_values
			.as_ref()
			.map(|v| !v.is_empty())
			.unwrap_or(false)
	}
}

/// A Measurement etl-unit: subject, time, [components] → value
///
/// Measurements are time-varying observations of a subject. They may optionally
/// have component dimensions that decompose the measurement (e.g., sales by color and size).
///
/// All column references are canonical names. The mapping to source DataFrame columns
/// is handled by `BoundSource`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasurementUnit {
	/// Identifier for this unit
	pub name: CanonicalColumnName,

	/// The subject canonical name (copied from schema for domain_signature)
	pub subject: CanonicalColumnName,

	/// The time canonical name (copied from schema for domain_signature)
	pub time: CanonicalColumnName,

	/// Component canonical names (part of domain, may be empty)
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub components: Vec<CanonicalColumnName>,

	/// The measurement value canonical name
	pub value: CanonicalColumnName,

	/// Type of measurement (determines default aggregation)
	pub kind: MeasurementKind,

	/// Value to substitute for nulls in source data
	pub null_value: Option<NullValue>,

	/// Value to substitute for nulls from joins
	pub null_value_extension: Option<NullValue>,

	/// How to handle the measurement's data stream
	pub signal_policy: Option<SignalPolicy>,

	/// Presentation hints for charting
	pub chart_hints: Option<ChartHints>,

    /// TODO: Deprecate, replace with downsample_strategy
	/// How to downsample the measurement (override default that aligns with MeasurementKind)
	pub signal_aggregation: Option<Aggregate>,

	/// For Binary measurements: mapping of source values to true/false
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub truth_mapping: Option<TruthMapping>,

	/// Whether this measurement is historical or forecast
	#[serde(default)]
	pub temporality: DataTemporality,

	/// Native sample rate in milliseconds (e.g., 60_000 for 60s, 3_600_000 for 1h).
	/// Declared in config, validated against observed data.
	#[serde(default)]
	pub sample_rate_ms: Option<i64>,

	/// Strategy for upsampling to a faster target rate.
	#[serde(default)]
	pub upsample_strategy: Option<ResampleStrategy>,

	/// Strategy for downsampling to a slower target rate.
	#[serde(default)]
	pub downsample_strategy: Option<ResampleStrategy>,
}

/// Strategy for resampling a measurement to a different rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResampleStrategy {
	/// Carry the last observed value (appropriate for engine status, precipitation).
	ForwardFill,
	/// Linear interpolation between values (appropriate for gage height, temperature).
	Interpolate,
	/// Leave gaps as null (no assumption about intermediate values).
	Null,
	/// Arithmetic mean of values in the window.
	Mean,
	/// Maximum value in the window.
	Max,
	/// Minimum value in the window.
	Min,
	/// Sum of values in the window.
	Sum,
	/// Last value in the window.
	Last,
}

// Implement equality based on subject, time, components, value, signal_policy, and truth_mapping
impl PartialEq for MeasurementUnit {
	fn eq(&self, other: &Self) -> bool {
		self.subject == other.subject &&
			self.time == other.time &&
			self.components == other.components &&
			self.value == other.value &&
			self.signal_policy == other.signal_policy &&
			self.truth_mapping == other.truth_mapping
	}
}

impl Eq for MeasurementUnit {}
impl MeasurementUnit {
	/// Create a measurement unit
	///
	/// # Arguments
	/// * `subject` - The canonical subject name (from schema)
	/// * `time` - The canonical time name (from schema)
	/// * `value` - The canonical name for this measurement's value
	/// * `kind` - The type of measurement
	pub fn new(
		subject: impl Into<CanonicalColumnName>,
		time: impl Into<CanonicalColumnName>,
		value: impl Into<CanonicalColumnName>,
		kind: MeasurementKind,
	) -> Self {
		let value: CanonicalColumnName = value.into();
		Self {
			name: value.clone(),
			subject: subject.into(),
			time: time.into(),
			components: Vec::new(),
			value,
			kind,
			null_value: None,
			null_value_extension: None,
			chart_hints: None,
			signal_policy: None,
			signal_aggregation: None,
			truth_mapping: None,
			temporality: DataTemporality::default(),
			sample_rate_ms: None,
			upsample_strategy: None,
			downsample_strategy: None,
		}
	}

	/// Set this measurement as historical (default)
	pub fn historical(mut self) -> Self {
		self.temporality = DataTemporality::Historical;
		self
	}

	/// Set this measurement as forecast data
	pub fn forecast(mut self) -> Self {
		self.temporality = DataTemporality::Forecast;
		self
	}

	pub fn is_forecast(&self) -> bool {
		self.temporality.is_forecast()
	}

	pub fn is_historical(&self) -> bool {
		self.temporality.is_historical()
	}

	/// Add component canonical names
	pub fn with_components(mut self, components: Vec<impl Into<String>>) -> Self {
		self.components = components
			.into_iter()
			.map(|c| CanonicalColumnName::new(c))
			.collect();
		self
	}

	/// Add a single component
	pub fn with_component(mut self, component: impl Into<String>) -> Self {
		self.components.push(CanonicalColumnName::new(component));
		self
	}

	/// Set null value for source data
	pub fn with_null_value(mut self, value: NullValue) -> Self {
		self.null_value = Some(value);
		self
	}

	/// Set null value extension for joins
	pub fn with_null_extension(mut self, value: NullValue) -> Self {
		self.null_value_extension = Some(value);
		self
	}

	/// Set the signal policy
	pub fn with_signal_policy(mut self, policy: SignalPolicy) -> Self {
		self.signal_policy = Some(policy);
		self
	}

	/// Set chart hints
	pub fn with_chart_hints(mut self, hints: ChartHints) -> Self {
		self.chart_hints = Some(hints);
		self
	}

	/// Set the truth mapping for Binary measurements
	pub fn with_truth_mapping(mut self, mapping: TruthMapping) -> Self {
		self.truth_mapping = Some(mapping);
		self
	}

	/// Get the domain signature for this unit
	pub fn domain_signature(&self) -> DomainSignature {
		DomainSignature::measurement(self.subject.as_str(), self.time.as_str()).with_components(
			self
				.components
				.iter()
				.map(|c| c.as_str().to_string())
				.collect(),
		)
	}

	/// Get for the signal policy
	pub fn signal_aggregation(&self) -> Aggregate {
		self
			.signal_aggregation
			.unwrap_or_else(|| self.kind.default_aggregation())
	}

	/// Set the signal aggregation override
	pub fn with_signal_aggregation(mut self, agg: Aggregate) -> Self {
		self.signal_aggregation = Some(agg);
		self
	}

	pub fn with_sample_rate_ms(mut self, rate_ms: i64) -> Self {
		self.sample_rate_ms = Some(rate_ms);
		self
	}

	pub fn with_upsample(mut self, strategy: ResampleStrategy) -> Self {
		self.upsample_strategy = Some(strategy);
		self
	}

	pub fn with_downsample(mut self, strategy: ResampleStrategy) -> Self {
		self.downsample_strategy = Some(strategy);
		self
	}

	/// Get the chart hints, using defaults based on measurement kind if not set
	pub fn effective_chart_hints(&self) -> ChartHints {
		self.chart_hints.clone().unwrap_or_else(|| {
			match self.kind {
				MeasurementKind::Categorical | MeasurementKind::Binary => ChartHints::categorical(),
				_ => ChartHints::measure(),
			}
		})
	}

	/// Check if this is a Binary measurement
	pub fn is_binary(&self) -> bool {
		self.kind == MeasurementKind::Binary
	}

	/// Get the effective truth mapping for Binary measurements
	/// Returns numeric mapping (1/0) as default if not specified
	pub fn effective_truth_mapping(&self) -> Option<TruthMapping> {
		if self.kind == MeasurementKind::Binary {
			Some(
				self
					.truth_mapping
					.clone()
					.unwrap_or_else(TruthMapping::numeric),
			)
		} else {
			None
		}
	}

	/// Get the signal policy -> Option<SignalPolicy>
	pub fn signal_policy(&self) -> Option<&SignalPolicy> {
		self.signal_policy.as_ref()
	}

	/// Get the etl-unit function signature
	pub fn etl_unit_signature(&self) -> Vec<CanonicalColumnName> {
		// build a vec starting with subject, time, components then value
		let mut signature = vec![self.subject.clone(), self.time.clone()];
		if !self.components.is_empty() {
			signature.extend(self.components.iter().cloned());
		}
		signature.push(self.value.clone());
		signature
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_simple_measurement() {
		let m = MeasurementUnit::new(
			"station_id",
			"observation_time",
			"sump_ft",
			MeasurementKind::Measure,
		);

		assert_eq!(m.name, "sump_ft".into());
		assert_eq!(m.subject.as_str(), "station_id");
		assert_eq!(m.time.as_str(), "observation_time");
		assert_eq!(m.value.as_str(), "sump_ft");
		assert!(m.components.is_empty());
		assert_eq!(m.kind.default_aggregation(), Aggregate::Mean);
	}

	#[test]
	fn test_measurement_with_components() {
		let m = MeasurementUnit::new("store_id", "sale_date", "units_sold", MeasurementKind::Count)
			.with_components(vec!["color", "size"]);

		assert_eq!(m.components.len(), 2);
		assert_eq!(m.kind.default_aggregation(), Aggregate::Sum);

		let sig = m.domain_signature();
		assert_eq!(sig.components.len(), 2);
		assert_eq!(sig.components[0].as_str(), "color");
		assert_eq!(sig.components[1].as_str(), "size");
	}

	#[test]
	fn test_domain_signature() {
		let m = MeasurementUnit::new("sensor_id", "reading_time", "temp_c", MeasurementKind::Measure);

		let sig = m.domain_signature();
		assert_eq!(sig.subject.as_str(), "sensor_id");
		assert_eq!(sig.time.as_ref().map(|t| t.as_str()), Some("reading_time"));
		assert!(sig.components.is_empty());
	}

	#[test]
	fn test_categorical_chart_hints() {
		let m = MeasurementUnit::new(
			"station_id",
			"observation_time",
			"engine_1",
			MeasurementKind::Categorical,
		);

		let hints = m.effective_chart_hints();
		assert!(hints.stepped);
	}

	#[test]
	fn test_binary_measurement() {
		let m = MeasurementUnit::new(
			"station_id",
			"observation_time",
			"engine_status",
			MeasurementKind::Binary,
		);

		assert!(m.is_binary());
		assert_eq!(m.kind.default_aggregation(), Aggregate::Any);

		// Default truth mapping is numeric
		let mapping = m.effective_truth_mapping().unwrap();
		assert!(mapping.has_true_values());
	}

	#[test]
	fn test_binary_with_string_truth_mapping() {
		let m = MeasurementUnit::new(
			"station_id",
			"observation_time",
			"engine_status",
			MeasurementKind::Binary,
		)
		.with_truth_mapping(
			TruthMapping::new()
				.true_strings(["on", "running", "active"])
				.false_strings(["off", "stopped", "inactive"]),
		);

		let mapping = m.truth_mapping.as_ref().unwrap();
		assert!(mapping.is_true(&serde_json::Value::String("on".into())));
		assert!(mapping.is_true(&serde_json::Value::String("running".into())));
		assert!(mapping.is_false(&serde_json::Value::String("off".into())));
		assert!(!mapping.is_true(&serde_json::Value::String("unknown".into())));
	}

	#[test]
	fn test_truth_mapping_numeric() {
		let mapping = TruthMapping::numeric();

		assert!(mapping.is_true(&serde_json::Value::from(1)));
		assert!(mapping.is_true(&serde_json::Value::from(1.0)));
		assert!(mapping.is_false(&serde_json::Value::from(0)));
		assert!(mapping.is_false(&serde_json::Value::from(0.0)));
	}

	#[test]
	fn test_truth_mapping_implicit_false() {
		// Without explicit false values, anything not true is false
		let mapping = TruthMapping::new().true_strings(["yes"]);

		assert!(mapping.is_true(&serde_json::Value::String("yes".into())));
		assert!(mapping.is_false(&serde_json::Value::String("no".into())));
		assert!(mapping.is_false(&serde_json::Value::String("maybe".into())));
	}
}
