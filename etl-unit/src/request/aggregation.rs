use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::CanonicalColumnName;

/// Aggregation specification for overriding defaults
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AggregationSpec {
	/// Override aggregation for specific measurements (by name)
	pub overrides: HashMap<CanonicalColumnName, AggregationType>,
}

impl AggregationSpec {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn set(mut self, measurement: CanonicalColumnName, agg: AggregationType) -> Self {
		self.overrides.insert(measurement, agg);
		self
	}
}

/// Type of aggregation to apply
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregationType {
	Sum,
	Mean,
	Min,
	Max,
	First,
	Last,
	MostRecent,
	LeastRecent,
	Count,
}

impl AggregationType {
	pub fn as_str(&self) -> &'static str {
		match self {
			AggregationType::Sum => "sum",
			AggregationType::Mean => "mean",
			AggregationType::Min => "min",
			AggregationType::Max => "max",
			AggregationType::First => "first",
			AggregationType::Last => "last",
			AggregationType::MostRecent => "most_recent",
			AggregationType::LeastRecent => "least_recent",
			AggregationType::Count => "count",
		}
	}
}
