#![allow(clippy::tabs_in_doc_comments)]
//! EtlSchema Builder
//!
//! Provides a fluent API for constructing EtlSchema with validation at build time.

use super::EtlSchema;
use crate::{
	Aggregate, CanonicalColumnName, ChartHints, DataTemporality, Derivation, EtlError, EtlResult,
	MeasurementKind, MeasurementUnit, NullValue, QualityUnit, SignalPolicy, TruthMapping,
};

// ============================================================================
// Builder
// ============================================================================

/// Builder for constructing EtlSchema with validation
#[derive(Debug, Clone)]
pub struct EtlSchemaBuilder {
	name:         String,
	subject:      Option<CanonicalColumnName>,
	time:         Option<CanonicalColumnName>,
	qualities:    Vec<QualityUnit>,
	measurements: Vec<MeasurementUnit>,
	derivations:  Vec<Derivation>,
}

impl EtlSchemaBuilder {
	/// Create a new schema builder with the given name
	pub fn new(name: impl Into<String>) -> Self {
		Self {
			name:         name.into(),
			subject:      None,
			time:         None,
			qualities:    Vec::new(),
			measurements: Vec::new(),
			derivations:  Vec::new(),
		}
	}

	// =========================================================================
	// Subject Column
	// =========================================================================

	/// Set the canonical subject name
	pub fn subject(mut self, canonical: impl Into<String>) -> Self {
		self.subject = Some(CanonicalColumnName::new(canonical));
		self
	}

	// =========================================================================
	// Time Column
	// =========================================================================

	/// Set the canonical time name
	pub fn time(mut self, canonical: impl Into<String>) -> Self {
		self.time = Some(CanonicalColumnName::new(canonical));
		self
	}

	// =========================================================================
	// Qualities
	// =========================================================================

	/// Add a quality using the schema's subject canonical name
	pub fn quality(mut self, value: impl Into<String>) -> Self {
		let subject_canonical = self
			.subject
			.as_ref()
			.map(|s| s.as_str())
			.unwrap_or("subject");

		self
			.qualities
			.push(QualityUnit::new(subject_canonical, value));
		self
	}

	/// Set chart hints on the last added quality
	pub fn with_quality_chart_hints(mut self, hints: ChartHints) -> Self {
		if let Some(last) = self.qualities.last_mut() {
			last.chart_hints = Some(hints);
		}
		self
	}

	/// Set null value substitution for source data nulls on the last added quality
	///
	/// # Example
	/// ```rust,ignore
	/// .quality("region")
	/// .with_quality_null_value(NullValue::string("Unknown"))
	/// ```
	pub fn with_quality_null_value(mut self, value: NullValue) -> Self {
		if let Some(last) = self.qualities.last_mut() {
			last.null_value = Some(value);
		}
		self
	}

	/// Set null value substitution for nulls from joins on the last added quality
	///
	/// # Example
	/// ```rust,ignore
	/// .quality("category")
	/// .with_quality_null_value_extension(NullValue::string("Uncategorized"))
	/// ```
	pub fn with_quality_null_value_extension(mut self, value: NullValue) -> Self {
		if let Some(last) = self.qualities.last_mut() {
			last.null_value_extension = Some(value);
		}
		self
	}

	/// Add a fully-configured quality
	pub fn with_quality(mut self, quality: QualityUnit) -> Self {
		self.qualities.push(quality);
		self
	}

	// =========================================================================
	// Measurements
	// =========================================================================

	/// Add a measurement using schema's subject and time canonical names.
	///
	/// **Important**: Every measurement requires a `signal_policy` and
	/// `sample_rate` to be set before `build()`. Chain `.with_policy()`
	/// and `.with_sample_rate()` after this call:
	///
	/// ```ignore
	/// builder.measurement("sump", MeasurementKind::Measure)
	///     .with_policy(SignalPolicy::instant(Duration::from_secs(60)))
	///     .with_sample_rate(60_000)
	/// ```
	///
	/// For test code, use `.measurement_with_defaults()` which sets
	/// a 60s instant policy and 60s sample rate automatically.
	pub fn measurement(
		mut self,
		value: impl Into<CanonicalColumnName>,
		kind: MeasurementKind,
	) -> Self {
		let subject_canonical = self
			.subject
			.as_ref()
			.map(|s| s.as_str())
			.unwrap_or("subject");

		let time_canonical = self.time.as_ref().map(|t| t.as_str()).unwrap_or("time");

		self
			.measurements
			.push(MeasurementUnit::new(subject_canonical, time_canonical, value, kind));
		self
	}

	/// Add a measurement with sensible test defaults: 60s instant signal
	/// policy and 60s sample rate. Saves test boilerplate — production
	/// code should use `.measurement()` + explicit `.with_policy()` +
	/// `.with_sample_rate()`.
	pub fn measurement_with_defaults(
		self,
		value: impl Into<CanonicalColumnName>,
		kind: MeasurementKind,
	) -> Self {
		self.measurement(value, kind)
			.with_policy(SignalPolicy::instant())
			.with_sample_rate(60_000)
	}

	/// Set chart hints on the last added measurement
	pub fn with_measurement_chart_hints(mut self, hints: ChartHints) -> Self {
		if let Some(last) = self.measurements.last_mut() {
			last.chart_hints = Some(hints);
		}
		self
	}

	/// Set signal policy on the last added measurement
	pub fn with_policy(mut self, policy: SignalPolicy) -> Self {
		if let Some(last) = self.measurements.last_mut() {
			last.signal_policy = Some(policy);
		}
		self
	}

	/// Mark the current measurement as forecast data
	pub fn forecast(mut self) -> Self {
		if let Some(last) = self.measurements.last_mut() {
			last.temporality = DataTemporality::Forecast;
		}
		self
	}

	/// Mark the current measurement as historical data (default, but explicit)
	pub fn historical(mut self) -> Self {
		if let Some(last) = self.measurements.last_mut() {
			last.temporality = DataTemporality::Historical;
		}
		self
	}

	/// Add a component to the last added measurement
	pub fn with_component(mut self, component: impl Into<String>) -> Self {
		if let Some(last) = self.measurements.last_mut() {
			last.components.push(CanonicalColumnName::new(component));
		}
		self
	}

	/// Set aggregation override on the last added measurement
	pub fn with_aggregation(mut self, agg: Aggregate) -> Self {
		if let Some(last) = self.measurements.last_mut() {
			last.signal_aggregation = Some(agg);
		}
		self
	}

	/// Set the native sample rate in milliseconds on the last added measurement.
	pub fn with_sample_rate(mut self, rate_ms: i64) -> Self {
		if let Some(last) = self.measurements.last_mut() {
			last.sample_rate_ms = Some(rate_ms);
		}
		self
	}

	/// Set the upsample strategy on the last added measurement.
	pub fn with_upsample(mut self, strategy: crate::ResampleStrategy) -> Self {
		if let Some(last) = self.measurements.last_mut() {
			last.upsample_strategy = Some(strategy);
		}
		self
	}

	/// Set the downsample strategy on the last added measurement.
	pub fn with_downsample(mut self, strategy: crate::ResampleStrategy) -> Self {
		if let Some(last) = self.measurements.last_mut() {
			last.downsample_strategy = Some(strategy);
		}
		self
	}

	/// Set null value substitution for source data nulls on the last added measurement
	///
	/// This value is used to fill nulls that exist in the original source data.
	///
	/// # Example
	/// ```rust,ignore
	/// .measurement("temperature", MeasurementKind::Measure)
	/// .with_null_value(NullValue::float(0.0))  // Replace source nulls with 0.0
	/// ```
	pub fn with_null_value(mut self, value: NullValue) -> Self {
		if let Some(last) = self.measurements.last_mut() {
			last.null_value = Some(value);
		}
		self
	}

	/// Set null value substitution for nulls from joins on the last added measurement
	///
	/// This value is used to fill nulls that arise from left joins during
	/// universe composition (e.g., when a measurement has no data for a
	/// particular subject/time combination).
	///
	/// # Example
	/// ```rust,ignore
	/// .measurement("precipitation", MeasurementKind::Measure)
	/// .with_null_value_extension(NullValue::float(0.0))  // No rain = 0.0
	/// ```
	pub fn with_null_value_extension(mut self, value: NullValue) -> Self {
		if let Some(last) = self.measurements.last_mut() {
			last.null_value_extension = Some(value);
		}
		self
	}

	/// Add a fully-configured measurement
	pub fn with_measurement(mut self, measurement: MeasurementUnit) -> Self {
		self.measurements.push(measurement);
		self
	}

	// =========================================================================
	// Binary Measurement Truth Mapping
	// =========================================================================

	/// Set true values for a Binary measurement (last added measurement)
	///
	/// Values in this list will be converted to 1 (true) during extraction.
	///
	/// # Example
	/// ```rust,ignore
	/// .measurement("engine_status", MeasurementKind::Binary)
	/// .with_true_values(["on", "running", "active"])
	/// ```
	pub fn with_true_values<I, V>(mut self, values: I) -> Self
	where
		I: IntoIterator<Item = V>,
		V: Into<serde_json::Value>, {
		if let Some(last) = self.measurements.last_mut() {
			let mapping = last.truth_mapping.get_or_insert_with(TruthMapping::new);
			mapping
				.true_values
				.extend(values.into_iter().map(|v| v.into()));
		}
		self
	}

	/// Set false values for a Binary measurement (last added measurement)
	///
	/// Values in this list will be converted to 0 (false) during extraction.
	/// If not set, any value not in true_values is considered false.
	///
	/// # Example
	/// ```rust,ignore
	/// .measurement("engine_status", MeasurementKind::Binary)
	/// .with_true_values(["on", "running"])
	/// .with_false_values(["off", "stopped"])
	/// ```
	pub fn with_false_values<I, V>(mut self, values: I) -> Self
	where
		I: IntoIterator<Item = V>,
		V: Into<serde_json::Value>, {
		if let Some(last) = self.measurements.last_mut() {
			let mapping = last.truth_mapping.get_or_insert_with(TruthMapping::new);
			let false_vals = mapping.false_values.get_or_insert_with(Vec::new);
			false_vals.extend(values.into_iter().map(|v| v.into()));
		}
		self
	}

	/// Set a complete truth mapping for a Binary measurement
	///
	/// # Example
	/// ```rust,ignore
	/// .measurement("pump_status", MeasurementKind::Binary)
	/// .with_truth_mapping(TruthMapping::numeric())
	/// ```
	pub fn with_truth_mapping(mut self, mapping: TruthMapping) -> Self {
		if let Some(last) = self.measurements.last_mut() {
			last.truth_mapping = Some(mapping);
		}
		self
	}

	// =========================================================================
	// Derivations
	// =========================================================================

	/// Add a derivation
	pub fn with_derivation(mut self, derivation: Derivation) -> Self {
		self.derivations.push(derivation);
		self
	}

	/// Add a derivation with hints
	pub fn with_derivation_and_hints(
		mut self,
		mut derivation: Derivation,
		hints: ChartHints,
	) -> Self {
		derivation.chart_hints = Some(hints);
		self.derivations.push(derivation);
		self
	}

	/// Set the signal policy with custom aggregation
	pub fn with_policy_and_aggregation(mut self, policy: SignalPolicy, agg: Aggregate) -> Self {
		if let Some(last) = self.measurements.last_mut() {
			last.signal_policy = Some(policy);
			last.signal_aggregation = Some(agg);
		}
		self
	}

	// =========================================================================
	// Build
	// =========================================================================

	/// Build the schema, running structural validation
	pub fn build(self) -> EtlResult<EtlSchema> {
		// Require subject and time
		let subject = self.subject.ok_or_else(|| {
			EtlError::Config("Subject is required. Call .subject(\"canonical_name\")".into())
		})?;

		let time = self.time.ok_or_else(|| {
			EtlError::Config("Time is required. Call .time(\"canonical_name\")".into())
		})?;

		// Construct schema
		let schema = EtlSchema {
			name: self.name,
			subject,
			time,
			qualities: self.qualities,
			measurements: self.measurements,
			derivations: self.derivations,
		};

		// Validate structural constraints
		schema.validate()?;

		// Validate required fields: signal_policy and sample_rate.
		// These are not optional — every measurement must declare how
		// its data stream is processed and at what native rate it
		// samples. Defaults mask configuration errors that surface as
		// subtle data bugs (wrong grid interval, silent fallback to
		// DEFAULT_TTL).
		for m in &schema.measurements {
			if m.signal_policy.is_none() {
				return Err(EtlError::Config(format!(
					"Measurement '{}' is missing a signal policy. \
					 Every measurement must declare a signal policy \
					 (e.g., .with_policy(SignalPolicy::instant(Duration::from_secs(60)))). \
					 In TOML config, add:\n\n\
					 [measurements.{}.signal_policy]\n\
					 type = \"instant\"\n\
					 ttl_secs = 60\n",
					m.name, m.name,
				)));
			}
			if m.sample_rate_ms.is_none() {
				return Err(EtlError::Config(format!(
					"Measurement '{}' is missing a sample rate. \
					 Every measurement must declare its native sample rate \
					 (e.g., .with_sample_rate(60_000) for 60-second data). \
					 In TOML config, add:\n\n\
					 [measurements.{}]\n\
					 sample_rate = \"60s\"\n",
					m.name, m.name,
				)));
			}
		}

		// Validate signal policies
		for m in &schema.measurements {
			if let Some(policy) = &m.signal_policy {
				policy.validate()?;
			}
		}

		// Validate Binary measurements have truth mappings (warn if not, use default)
		for m in &schema.measurements {
			if m.kind == MeasurementKind::Binary && m.truth_mapping.is_none() {
				tracing::debug!(
					 measurement = %m.name,
					 "Binary measurement without explicit truth mapping, using numeric default (1=true, 0=false)"
				);
			}
		}

		// Validate NullValue types match MeasurementKind
		for m in &schema.measurements {
			if let Some(ref null_val) = m.null_value {
				validate_null_value_for_kind(&m.name, m.kind, null_val, "null_value")?;
			}
			if let Some(ref null_val) = m.null_value_extension {
				validate_null_value_for_kind(&m.name, m.kind, null_val, "null_value_extension")?;
			}
		}

		Ok(schema)
	}
}

/// Validate that a NullValue type is compatible with a MeasurementKind
fn validate_null_value_for_kind(
	measurement_name: &crate::CanonicalColumnName,
	kind: MeasurementKind,
	null_value: &NullValue,
	field_name: &str,
) -> EtlResult<()> {
	let is_valid = match (kind, null_value) {
		// Numeric kinds accept Float or Integer
		(MeasurementKind::Count, NullValue::Integer(_)) => true,
		(MeasurementKind::Count, NullValue::Float(_)) => true,
		(MeasurementKind::Measure, NullValue::Float(_)) => true,
		(MeasurementKind::Measure, NullValue::Integer(_)) => true,
		(MeasurementKind::Average, NullValue::Float(_)) => true,
		(MeasurementKind::Average, NullValue::Integer(_)) => true,

		// Categorical accepts String
		(MeasurementKind::Categorical, NullValue::String(_)) => true,
		// Also allow Integer for categorical (e.g., status codes)
		(MeasurementKind::Categorical, NullValue::Integer(_)) => true,

		// Binary accepts Boolean or Integer (0/1)
		(MeasurementKind::Binary, NullValue::Boolean(_)) => true,
		(MeasurementKind::Binary, NullValue::Integer(_)) => true,

		_ => false,
	};

	if !is_valid {
		return Err(EtlError::Config(format!(
			"Measurement '{}': {} type {:?} is incompatible with MeasurementKind::{:?}. Expected: {}",
			measurement_name,
			field_name,
			null_value,
			kind,
			expected_null_value_types(kind)
		)));
	}

	Ok(())
}

/// Get a human-readable description of expected NullValue types for a MeasurementKind
fn expected_null_value_types(kind: MeasurementKind) -> &'static str {
	match kind {
		MeasurementKind::Count => "NullValue::Integer or NullValue::Float",
		MeasurementKind::Measure => "NullValue::Float or NullValue::Integer",
		MeasurementKind::Average => "NullValue::Float or NullValue::Integer",
		MeasurementKind::Categorical => "NullValue::String or NullValue::Integer",
		MeasurementKind::Binary => "NullValue::Boolean or NullValue::Integer",
	}
}

// ============================================================================
// Tests
// ============================================================================
#[cfg(test)]
mod tests {
	use super::*;
	use crate::{
		NullValue,
		unit::{Derivation, MeasurementKind, PointwiseExpr},
	};

	fn make_test_schema() -> EtlSchema {
		EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("sump_ft", MeasurementKind::Measure)
			.measurement_with_defaults("engine_1", MeasurementKind::Categorical)
			.measurement_with_defaults("engine_2", MeasurementKind::Categorical)
			.with_derivation(Derivation::pointwise(
				"any_engine",
				PointwiseExpr::any_on(vec!["engine_1", "engine_2"]),
			))
			.build()
			.unwrap()
	}

	#[test]
	fn test_basic_schema_build() {
		let schema = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("sump_ft", MeasurementKind::Measure)
			.build()
			.unwrap();

		assert_eq!(schema.name, "test");
		assert_eq!(schema.subject.as_str(), "station");
		assert_eq!(schema.time.as_str(), "timestamp");
		assert_eq!(schema.measurements.len(), 1);
		assert_eq!(schema.measurements[0].name, "sump_ft".into());
	}

	#[test]
	fn test_schema_with_quality() {
		let schema = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.quality("region")
			.measurement_with_defaults("value", MeasurementKind::Measure)
			.build()
			.unwrap();

		assert_eq!(schema.qualities.len(), 1);
		assert_eq!(schema.qualities[0].name, "region".into());
		assert_eq!(schema.qualities[0].subject, "station".into());
	}

	#[test]
	fn test_schema_with_derivation() {
		let schema = make_test_schema();

		assert!(schema.has_measurement("any_engine"));
		assert_eq!(schema.derivations.len(), 1);

		let derivation = schema.get_derivation("any_engine").unwrap();
		let inputs = derivation.input_columns();
		assert!(inputs.iter().any(|n| n.as_str() == "engine_1"));
		assert!(inputs.iter().any(|n| n.as_str() == "engine_2"));
	}

	#[test]
	fn test_measurement_with_component() {
		let schema = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("sales", MeasurementKind::Count)
			.with_component("color")
			.with_component("size")
			.build()
			.unwrap();

		let m = schema.get_measurement("sales").unwrap();
		assert_eq!(m.components.len(), 2);
		assert_eq!(m.components[0].as_str(), "color");
		assert_eq!(m.components[1].as_str(), "size");
	}

	#[test]
	fn test_measurement_with_hints_and_policy() {
		let schema = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("fuel", MeasurementKind::Measure)
			.with_measurement_chart_hints(ChartHints::measure().label("Fuel Level"))
			.with_policy(SignalPolicy::sliding(60u32, 3u32))
			.build()
			.unwrap();

		let m = schema.get_measurement("fuel").unwrap();
		assert!(m.chart_hints.is_some());
		assert!(m.signal_policy.is_some());
	}

	#[test]
	fn test_measurement_with_null_values() {
		let schema = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("temperature", MeasurementKind::Measure)
			.with_null_value(NullValue::float(-999.0))
			.with_null_value_extension(NullValue::float(0.0))
			.build()
			.unwrap();

		let m = schema.get_measurement("temperature").unwrap();
		assert!(m.null_value.is_some());
		assert!(m.null_value_extension.is_some());
	}

	#[test]
	fn test_null_value_type_validation_measure() {
		// Float is valid for Measure
		let result = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("temp", MeasurementKind::Measure)
			.with_null_value(NullValue::float(0.0))
			.build();
		assert!(result.is_ok());

		// String is invalid for Measure
		let result = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("temp", MeasurementKind::Measure)
			.with_null_value(NullValue::string("N/A"))
			.build();
		assert!(result.is_err());
		assert!(result.unwrap_err().to_string().contains("incompatible"));
	}

	#[test]
	fn test_null_value_type_validation_categorical() {
		// String is valid for Categorical
		let result = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("status", MeasurementKind::Categorical)
			.with_null_value(NullValue::string("unknown"))
			.build();
		assert!(result.is_ok());

		// Float is invalid for Categorical
		let result = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("status", MeasurementKind::Categorical)
			.with_null_value(NullValue::float(0.0))
			.build();
		assert!(result.is_err());
	}

	#[test]
	fn test_null_value_type_validation_binary() {
		// Boolean is valid for Binary
		let result = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("is_on", MeasurementKind::Binary)
			.with_null_value(NullValue::bool(false))
			.build();
		assert!(result.is_ok());

		// Integer is also valid for Binary (0/1)
		let result = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("is_on", MeasurementKind::Binary)
			.with_null_value(NullValue::int(0))
			.build();
		assert!(result.is_ok());
	}

	#[test]
	fn test_quality_with_null_values() {
		let schema = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.quality("region")
			.with_quality_null_value(NullValue::string("Unknown"))
			.with_quality_null_value_extension(NullValue::string("Unassigned"))
			.measurement_with_defaults("value", MeasurementKind::Measure)
			.build()
			.unwrap();

		let q = &schema.qualities[0];
		assert!(q.null_value.is_some());
		assert!(q.null_value_extension.is_some());
	}

	#[test]
	fn test_binary_measurement_with_string_truth_values() {
		let schema = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("engine_status", MeasurementKind::Binary)
			.with_true_values(["on", "running", "1"])
			.with_false_values(["off", "stopped", "0"])
			.build()
			.unwrap();

		let m = schema.get_measurement("engine_status").unwrap();
		assert!(m.is_binary());
		assert!(m.truth_mapping.is_some());

		let mapping = m.truth_mapping.as_ref().unwrap();
		assert_eq!(mapping.true_values.len(), 3);
		assert_eq!(mapping.false_values.as_ref().unwrap().len(), 3);
	}

	#[test]
	fn test_binary_measurement_with_truth_mapping() {
		let schema = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("pump_on", MeasurementKind::Binary)
			.with_truth_mapping(TruthMapping::numeric())
			.build()
			.unwrap();

		let m = schema.get_measurement("pump_on").unwrap();
		assert!(m.is_binary());
		assert!(m.truth_mapping.is_some());
	}

	#[test]
	fn test_binary_measurement_default_aggregation() {
		let schema = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("any_engine_on", MeasurementKind::Binary)
			.with_component("engine")
			.build()
			.unwrap();

		let m = schema.get_measurement("any_engine_on").unwrap();
		assert_eq!(m.kind.default_aggregation(), Aggregate::Any);
	}

	#[test]
	fn test_measurement_with_aggregation_override() {
		let schema = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("engine_status", MeasurementKind::Categorical)
			.with_component("engine")
			.with_aggregation(Aggregate::Any)
			.build()
			.unwrap();

		let m = schema.get_measurement("engine_status").unwrap();
		assert_eq!(m.signal_aggregation, Some(Aggregate::Any));
		assert_eq!(m.signal_aggregation(), Aggregate::Any);
	}

	#[test]
	fn test_nested_derivations() {
		let schema = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("engine_1", MeasurementKind::Categorical)
			.measurement_with_defaults("engine_2", MeasurementKind::Categorical)
			.measurement_with_defaults("engine_3", MeasurementKind::Categorical)
			.with_derivation(Derivation::pointwise(
				"any_engine_1_2",
				PointwiseExpr::any_on(vec!["engine_1", "engine_2"]),
			))
			.with_derivation(Derivation::pointwise(
				"any_engine_all",
				PointwiseExpr::any_on(vec!["any_engine_1_2", "engine_3"]),
			))
			.build()
			.unwrap();

		assert!(schema.has_measurement("any_engine_1_2"));
		assert!(schema.has_measurement("any_engine_all"));
	}

	#[test]
	fn test_missing_subject_error() {
		let result = EtlSchema::new("test")
			.time("timestamp")
			.measurement_with_defaults("value", MeasurementKind::Measure)
			.build();

		assert!(result.is_err());
		let err = result.unwrap_err().to_string();
		assert!(err.contains("Subject"));
	}

	#[test]
	fn test_missing_time_error() {
		let result = EtlSchema::new("test")
			.subject("station")
			.measurement_with_defaults("value", MeasurementKind::Measure)
			.build();

		assert!(result.is_err());
		let err = result.unwrap_err().to_string();
		assert!(err.contains("Time"));
	}

	#[test]
	fn test_derivation_unknown_source_error() {
		let result = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("engine_1", MeasurementKind::Categorical)
			.with_derivation(Derivation::pointwise(
				"any_engine",
				PointwiseExpr::any_on(vec!["engine_1", "engine_unknown"]),
			))
			.build();

		assert!(result.is_err());
		let err = result.unwrap_err().to_string();
		assert!(err.contains("engine_unknown"));
	}

	#[test]
	fn test_all_canonical_names() {
		let schema = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.quality("region")
			.measurement_with_defaults("temp", MeasurementKind::Measure)
			.with_component("sensor_type")
			.build()
			.unwrap();

		let names: Vec<&str> = schema
			.all_canonical_names()
			.iter()
			.map(|c| c.as_str())
			.collect();
		assert!(names.contains(&"station"));
		assert!(names.contains(&"timestamp"));
		assert!(names.contains(&"region"));
		assert!(names.contains(&"temp"));
		assert!(names.contains(&"sensor_type"));
	}
}
