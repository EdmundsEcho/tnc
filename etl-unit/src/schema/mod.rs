//! EtlSchema - Schema describing all EtlUnits in a DataFrame
//!
//! The schema defines the logical structure using canonical names only.
//! Physical column mappings are handled by `BoundSource`.

mod builder;
mod convert;
mod dto;

use std::{collections::HashSet, fs::File, io::BufReader, path::Path};

pub use builder::EtlSchemaBuilder;
pub use dto::SchemaDto;
use serde::{Deserialize, Serialize};

use crate::{
	ChartHints, Derivation, DomainSignature, EtlError, EtlResult, EtlUnitRef, MeasurementKind,
	MeasurementUnit, NullValue, QualityUnit, column::CanonicalColumnName,
};

/// Schema describing all EtlUnits using canonical names.
///
/// The schema is purely logical - it defines what units exist and their
/// canonical column names. The mapping from source DataFrame columns to
/// canonical names is handled separately by `BoundSource`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EtlSchema {
	/// Schema identifier
	pub name: String,

	/// Canonical subject column name (shared across all units)
	pub subject: CanonicalColumnName,

	/// Canonical time column name (shared across all measurement units)
	pub time: CanonicalColumnName,

	/// Quality units
	#[serde(default)]
	pub qualities: Vec<QualityUnit>,

	/// Measurement units
	#[serde(default)]
	pub measurements: Vec<MeasurementUnit>,

	/// Derivations (shape-preserving computations)
	#[serde(default)]
	pub derivations: Vec<Derivation>,
}

impl EtlSchema {
	// =========================================================================
	// Construction
	// =========================================================================

	#[allow(clippy::tabs_in_doc_comments)]
	/// Create a builder for constructing a schema
	///
	/// # Example
	///
	/// ```rust
	/// use synapse_etl_unit::{EtlSchema, MeasurementKind};
	///
	/// let schema = EtlSchema::new("pump_station")
	/// 	.subject("station")
	/// 	.time("timestamp")
	/// 	.measurement("sump", MeasurementKind::Measure)
	/// 	.build()
	/// 	.unwrap();
	/// ```
	#[allow(clippy::new_ret_no_self)]
	pub fn new(name: impl Into<String>) -> EtlSchemaBuilder {
		EtlSchemaBuilder::new(name)
	}

	/// Combine two schemas into one.
	///
	/// # Rules
	/// 1. Name becomes "name_self+name_other"
	/// 2. Subject and Time canonical names are taken from `self`
	/// 3. Units (Qualities, Measurements, Derivations) are unioned.
	///    If a unit exists in both (based on identity), `self`'s version is kept.
	///
	/// # Errors
	/// Returns `EtlError::CannotCombineSchema` if there is a naming collision
	/// (i.e., a unit with the same name exists in both schemas but has a different definition).
	pub fn combine(&self, other: &EtlSchema) -> EtlResult<Self> {
		let new_name = format!("{}+{}", self.name, other.name);

		// 1. Qualities
		let mut qualities = self.qualities.clone();
		for q in &other.qualities {
			// If self already has this EXACT quality, ignore other's (priority to self)
			if qualities.contains(q) {
				continue;
			}
			// Check for name collision (same name, different definition)
			if qualities.iter().any(|existing| existing.name == q.name) {
				return Err(EtlError::CannotCombineSchema(format!(
					"Quality unit name collision: '{}' defined differently in both schemas",
					q.name
				)));
			}
			qualities.push(q.clone());
		}

		// 2. Measurements
		let mut measurements = self.measurements.clone();
		for m in &other.measurements {
			if measurements.contains(m) {
				continue;
			}
			if measurements.iter().any(|existing| existing.name == m.name) {
				return Err(EtlError::CannotCombineSchema(format!(
					"Measurement unit name collision: '{}' defined differently in both schemas",
					m.name
				)));
			}
			measurements.push(m.clone());
		}

		// 3. Derivations
		let mut derivations = self.derivations.clone();
		for d in &other.derivations {
			if derivations.contains(d) {
				continue;
			}
			if derivations.iter().any(|existing| existing.name == d.name) {
				return Err(EtlError::CannotCombineSchema(format!(
					"Derivation name collision: '{}' defined differently in both schemas",
					d.name
				)));
			}
			derivations.push(d.clone());
		}

		Ok(Self {
			name: new_name,
			subject: self.subject.clone(),
			time: self.time.clone(),
			qualities,
			measurements,
			derivations,
		})
	}

	/// Load from a JSON file with detailed error reporting
	pub fn from_json_file<P: AsRef<Path>>(path: P) -> EtlResult<Self> {
		let path = path.as_ref();

		let file = File::open(path)
			.map_err(|e| EtlError::Config(format!("Failed to open schema file {:?}: {}", path, e)))?;
		let reader = BufReader::new(file);

		let deserializer = &mut serde_json::Deserializer::from_reader(reader);
		let schema_dto: SchemaDto = serde_path_to_error::deserialize(deserializer).map_err(|e| {
			let path_str = e.path().to_string();
			EtlError::Config(format!(
				"🚫 Failed to parse schema JSON in {:?} at '{}': {}",
				path, path_str, e
			))
		})?;

		let schema = schema_dto.into_schema()?;
		schema.validate()?;

		for m in &schema.measurements {
			if let Some(policy) = &m.signal_policy {
				policy.validate()?;
			}
		}

		// Validate NullValue types
		schema.validate_null_value_types()?;

		Ok(schema)
	}

	/// Load from a JSON string with detailed error reporting
	pub fn from_json_str(json: &str) -> EtlResult<Self> {
		let deserializer = &mut serde_json::Deserializer::from_str(json);
		let schema_dto: SchemaDto = serde_path_to_error::deserialize(deserializer).map_err(|e| {
			let path_str = e.path().to_string();
			EtlError::Config(format!("🚫 Failed to parse schema JSON at '{}': {}", path_str, e))
		})?;

		let schema = schema_dto.into_schema()?;
		schema.validate()?;

		for m in &schema.measurements {
			if let Some(policy) = &m.signal_policy {
				policy.validate()?;
			}
		}

		// Validate NullValue types
		schema.validate_null_value_types()?;

		Ok(schema)
	}

	// =========================================================================
	// Validation
	// =========================================================================

	/// Validate schema structure (no DataFrame required)
	///
	/// Checks:
	/// - Derivation sources exist in schema
	/// - Unpivot configurations are valid
	/// - No circular dependencies
	pub fn validate(&self) -> EtlResult<()> {
		// Check derivation sources exist
		for derivation in &self.derivations {
			for source in derivation.input_columns() {
				if !self.has_measurement(source) {
					return Err(EtlError::UnitNotFound(format!(
						"Derivation '{}' references unknown source '{}'",
						derivation.name, source
					)));
				}
			}
		}

		// Check for circular dependencies
		self.check_circular_dependencies()?;

		Ok(())
	}

	/// Validate that NullValue types are compatible with MeasurementKind
	pub fn validate_null_value_types(&self) -> EtlResult<()> {
		for m in &self.measurements {
			if let Some(ref null_val) = m.null_value {
				Self::validate_null_value_for_kind(&m.name, m.kind, null_val, "null_value")?;
			}
			if let Some(ref null_val) = m.null_value_extension {
				Self::validate_null_value_for_kind(&m.name, m.kind, null_val, "null_value_extension")?;
			}
		}
		Ok(())
	}

	/// Validate that a NullValue type is compatible with a MeasurementKind
	fn validate_null_value_for_kind(
		measurement_name: &CanonicalColumnName,
		kind: MeasurementKind,
		null_value: &NullValue,
		field_name: &str,
	) -> EtlResult<()> {
		if !kind.is_compatible_null_value(null_value) {
			return Err(EtlError::Config(format!(
				"Measurement '{}': {} type {:?} is incompatible with MeasurementKind::{:?}. \
				 Expected: {}",
				measurement_name,
				field_name,
				null_value,
				kind,
				kind.expected_null_value_types()
			)));
		}
		Ok(())
	}

	fn check_circular_dependencies(&self) -> EtlResult<()> {
		for derivation in &self.derivations {
			let mut visited = HashSet::new();
			self.check_circular_dfs(&derivation.name, &mut visited)?;
		}
		Ok(())
	}

	fn check_circular_dfs(&self, name: &str, visited: &mut HashSet<String>) -> EtlResult<()> {
		if visited.contains(name) {
			return Err(EtlError::CircularDependency(name.to_string()));
		}

		if let Some(derivation) = self.get_derivation(name) {
			visited.insert(name.to_string());
			for source in derivation.input_columns() {
				self.check_circular_dfs(source, visited)?;
			}
			visited.remove(name);
		}

		Ok(())
	}

	// =========================================================================
	// Lookups
	// =========================================================================

	/// Find a quality by name
	pub fn get_quality(&self, name: &str) -> Option<&QualityUnit> {
		self.qualities.iter().find(|q| q.name == name.into())
	}

	/// Find a base measurement by name
	pub fn get_measurement(&self, name: &str) -> Option<&MeasurementUnit> {
		self.measurements.iter().find(|m| m.name == name.into())
	}

	/// Find a derivation by name
	pub fn get_derivation(&self, name: &str) -> Option<&Derivation> {
		self.derivations.iter().find(|d| d.name == name.into())
	}

	/// Check if a measurement name exists (base, derivation, or unpivot)
	pub fn has_measurement(&self, name: &str) -> bool {
		self.get_measurement(name).is_some() || self.get_derivation(name).is_some()
	}

	/// Check if a quality name exists
	pub fn has_quality(&self, name: &str) -> bool {
		self.get_quality(name).is_some()
	}

	/// Resolve a typed unit reference against this schema.
	///
	/// Returns `true` if the ref exists in the schema and the kind matches.
	/// A `Measurement("sump")` resolves only if `sump` is a measurement (or derivation),
	/// not if it's a quality.
	pub fn resolve(&self, unit_ref: &EtlUnitRef) -> bool {
		match unit_ref {
			EtlUnitRef::Measurement(name) => self.has_measurement(name.as_str()),
			EtlUnitRef::Quality(name) => self.has_quality(name.as_str()),
			EtlUnitRef::Derivation(name) => self.get_derivation(name.as_str()).is_some(),
		}
	}

	/// Get all measurement names (base + derivations + unpivots)
	pub fn all_measurement_names(&self) -> Vec<&str> {
		self
			.measurements
			.iter()
			.map(|m| m.name.as_str())
			.chain(self.derivations.iter().map(|d| d.name.as_str()))
			.collect()
	}

	/// Get all quality names
	pub fn all_quality_names(&self) -> Vec<&str> {
		self.qualities.iter().map(|q| q.name.as_str()).collect()
	}

	// =========================================================================
	// Canonical Column Names
	// =========================================================================

	/// Get all canonical column names defined by this schema
	pub fn all_canonical_names(&self) -> Vec<&CanonicalColumnName> {
		let mut names = vec![&self.subject, &self.time];

		for q in &self.qualities {
			names.push(&q.value);
		}

		for m in &self.measurements {
			names.push(&m.value);
			for comp in &m.components {
				names.push(comp);
			}
		}

		names
	}

	/// Get the canonical subject name
	pub fn subject_canonical(&self) -> &CanonicalColumnName {
		&self.subject
	}

	/// Get the canonical time name
	pub fn time_canonical(&self) -> &CanonicalColumnName {
		&self.time
	}

	// =========================================================================
	// Domain Signatures
	// =========================================================================

	/// Get the domain signature for a unit by name
	pub fn get_domain_signature(&self, unit_name: &str) -> Option<DomainSignature> {
		if let Some(m) = self.get_measurement(unit_name) {
			Some(m.domain_signature())
		} else if let Some(q) = self.get_quality(unit_name) {
			Some(q.domain_signature())
		} else if let Some(d) = self.get_derivation(unit_name) {
			// Derivation inherits domain from first source
			d.input_columns()
				.first()
				.and_then(|s| self.get_domain_signature(s))
		} else {
			None
		}
	}

	// =========================================================================
	// Unit Metadata
	// =========================================================================

	/// Get the canonical value column name for a measurement
	pub fn get_measurement_value(&self, name: &str) -> Option<&CanonicalColumnName> {
		if let Some(m) = self.get_measurement(name) {
			Some(&m.value)
		} else {
			None
		}
	}

	/// Get the MeasurementKind for a measurement by name
	pub fn get_measurement_kind(&self, name: &str) -> Option<MeasurementKind> {
		self
			.get_measurement(name)
			.map(|m| m.kind)
			.or_else(|| self.get_derivation(name).map(|d| d.kind))
	}

	/// Get the ChartHints for a unit by name
	pub fn get_chart_hints(&self, name: &str) -> Option<ChartHints> {
		self
			.get_measurement(name)
			.map(|m| m.effective_chart_hints())
			.or_else(|| self.get_derivation(name).map(|d| d.effective_chart_hints()))
			.or_else(|| self.get_quality(name).map(|q| q.effective_chart_hints()))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::MeasurementKind;

	#[test]
	fn test_new_returns_builder() {
		let schema = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("sump", MeasurementKind::Measure)
			.build()
			.unwrap();

		assert_eq!(schema.name, "test");
		assert_eq!(schema.subject_canonical().as_str(), "station");
		assert_eq!(schema.time_canonical().as_str(), "timestamp");
		assert_eq!(schema.measurements.len(), 1);
	}

	#[test]
	fn test_has_measurement() {
		let schema = EtlSchema::new("test")
			.subject("id")
			.time("ts")
			.measurement_with_defaults("sump", MeasurementKind::Measure)
			.build()
			.unwrap();

		assert!(schema.has_measurement("sump"));
		assert!(!schema.has_measurement("unknown"));
	}

	#[test]
	fn test_all_measurement_names() {
		let schema = EtlSchema::new("test")
			.subject("id")
			.time("ts")
			.measurement_with_defaults("a", MeasurementKind::Measure)
			.measurement_with_defaults("b", MeasurementKind::Measure)
			.build()
			.unwrap();

		let names = schema.all_measurement_names();
		assert_eq!(names.len(), 2);
		assert!(names.contains(&"a"));
		assert!(names.contains(&"b"));
	}

	#[test]
	fn test_all_canonical_names() {
		let schema = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.quality("region")
			.measurement_with_defaults("temp", MeasurementKind::Measure)
			.build()
			.unwrap();

		let names = schema.all_canonical_names();
		// subject, time, region (quality), temp (measurement)
		assert!(names.len() >= 4);
	}

	#[test]
	fn test_null_value_validation() {
		// This should pass - Float is valid for Measure
		let schema = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("temp", MeasurementKind::Measure)
			.with_null_value(NullValue::float(0.0))
			.build();
		assert!(schema.is_ok());

		// This should fail - String is not valid for Measure
		let schema = EtlSchema::new("test")
			.subject("station")
			.time("timestamp")
			.measurement_with_defaults("temp", MeasurementKind::Measure)
			.with_null_value(NullValue::string("N/A"))
			.build();
		assert!(schema.is_err());
	}
}
