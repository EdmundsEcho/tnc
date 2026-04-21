// subset/meta.rs
//! Track any qualifying information generated while building the subset
//! request from the universe of data.
use serde::{Deserialize, Serialize};

use crate::{chart_hints::ChartHints, unit::MeasurementKind};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubsetMeasurementInfo {
	pub etl_unit_name: String, // codomain
	pub kind:          MeasurementKind,
	pub chart_hints:   ChartHints,
	#[serde(default)]
	pub null_info:     Option<NullInfo>,
}

/// Information about null values in a subset measurement
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NullInfo {
	/// Number of null values in this measurement
	pub null_count: usize,

	/// Total number of values (for calculating percentage)
	pub total_count: usize,

	/// If a null_value was applied, what value was used
	pub null_value_applied: Option<serde_json::Value>,

	/// If null_value_expansion was used during joins
	pub null_value_expansion_applied: Option<serde_json::Value>,
}

impl NullInfo {
	pub fn new(null_count: usize, total_count: usize) -> Self {
		Self {
			null_count,
			total_count,
			null_value_applied: None,
			null_value_expansion_applied: None,
		}
	}

	/// Percentage of null values (0.0 to 100.0)
	pub fn null_percentage(&self) -> f64 {
		if self.total_count == 0 {
			0.0
		} else {
			(self.null_count as f64 / self.total_count as f64) * 100.0
		}
	}

	pub fn with_null_value(mut self, value: serde_json::Value) -> Self {
		self.null_value_applied = Some(value);
		self
	}

	pub fn with_null_value_expansion(mut self, value: serde_json::Value) -> Self {
		self.null_value_expansion_applied = Some(value);
		self
	}
}
