//! Quality unit: subject → value (constant over time)

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

use super::null_value::NullValue;
use crate::{
	chart_hints::ChartHints,
	column::{CanonicalColumnName, DomainSignature},
};

/// A Quality etl-unit: subject → value (constant over time)
///
/// Qualities are permanent attributes that describe a subject. They do not vary
/// with time. Examples: station_name, location, sensor_model.
///
/// All column references are canonical names. The mapping to source DataFrame columns
/// is handled by `BoundSource`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityUnit {
	/// Identifier for this unit (typically the canonical value name)
	pub name: CanonicalColumnName,
	/// The subject canonical name (copied from schema for domain_signature)
	pub subject: CanonicalColumnName,
	/// The quality value canonical name
	pub value: CanonicalColumnName,
	/// Value to substitute for nulls in source data
	pub null_value: Option<NullValue>,
	/// Value to substitute for nulls from joins (subject doesn't exist in source)
	pub null_value_extension: Option<NullValue>,
	/// Presentation hints for charting
	pub chart_hints: Option<ChartHints>,
}

// Implement equality based ONLY on subject and value
impl PartialEq for QualityUnit {
	fn eq(&self, other: &Self) -> bool {
		self.subject == other.subject && self.value == other.value
	}
}

impl Eq for QualityUnit {}

// Implement ordering based on subject then value
impl PartialOrd for QualityUnit {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for QualityUnit {
	fn cmp(&self, other: &Self) -> Ordering {
		match self.subject.as_str().cmp(other.subject.as_str()) {
			Ordering::Equal => self.value.as_str().cmp(other.value.as_str()),
			ord => ord,
		}
	}
}
impl QualityUnit {
	/// Create a quality unit
	///
	/// # Arguments
	/// * `subject` - The canonical subject name (from schema)
	/// * `value` - The canonical name for this quality's value
	pub fn new(subject: impl Into<String>, value: impl Into<String>) -> Self {
		let value = CanonicalColumnName::new(value);
		Self {
			name: value.clone(),
			subject: CanonicalColumnName::new(subject),
			value,
			null_value: None,
			null_value_extension: None,
			chart_hints: None,
		}
	}

	/// Set the null value for source data
	pub fn with_null_value(mut self, value: NullValue) -> Self {
		self.null_value = Some(value);
		self
	}

	/// Set the null value extension for joins
	pub fn with_null_extension(mut self, value: NullValue) -> Self {
		self.null_value_extension = Some(value);
		self
	}

	/// Set chart hints
	pub fn with_chart_hints(mut self, hints: ChartHints) -> Self {
		self.chart_hints = Some(hints);
		self
	}

	/// Get the domain signature for this unit
	pub fn domain_signature(&self) -> DomainSignature {
		DomainSignature::quality(self.subject.as_str())
	}

	/// Get the chart hints, using defaults if not set
	pub fn effective_chart_hints(&self) -> ChartHints {
		self.chart_hints.clone().unwrap_or_else(ChartHints::quality)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_simple_quality() {
		let q = QualityUnit::new("station_id", "name");
		assert_eq!(q.name, "name".into());
		assert_eq!(q.subject.as_str(), "station_id");
		assert_eq!(q.value.as_str(), "name");
	}

	#[test]
	fn test_quality_domain_signature() {
		let q = QualityUnit::new("station_id", "name");
		let sig = q.domain_signature();
		assert_eq!(sig.subject.as_str(), "station_id");
		assert!(sig.time.is_none());
		assert!(sig.components.is_empty());
	}

	#[test]
	fn test_null_values() {
		let q = QualityUnit::new("id", "status")
			.with_null_value(NullValue::string("unknown"))
			.with_null_extension(NullValue::string("N/A"));

		assert!(q.null_value.is_some());
		assert!(q.null_value_extension.is_some());
	}

	#[test]
	fn test_quality_chart_hints() {
		let q = QualityUnit::new("station_id", "region");
		let hints = q.effective_chart_hints();

		use crate::chart_hints::{ChartType, Index};
		assert_eq!(hints.chart_type, ChartType::Bar);
		assert_eq!(hints.index, Index::Subject);
	}
}
