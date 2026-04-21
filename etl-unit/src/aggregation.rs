//! Aggregation: shape-reducing computations that collapse subjects.
//! See derivation for a shape-preserving computation.
//!
//! SyntheticSubject creates additional rows by aggregating across subjects.
//! The result is a "virtual subject" that coexists with real subjects in the data.
//!
//! Input:  n subjects × m time points
//! Output: (n + k) subjects × m time points (where k = number of synthetic subjects)
//!
//! NOTE:
//! # Aggregation System - comparison to `request::AggregationType`
//!
//! This module defines the types used for aggregating data across time or subjects.
//! It distinguishes between the simplified types exposed to the API and the robust
//! internal types used for execution.
//!
//! ## Type Comparison
//!
//! The system uses two distinct enums to handle aggregations:
//!
//! | Feature | [`request::AggregationType`] | [`aggregation::Aggregate`] |
//! |:--------|:-----------------------------|:---------------------------|
//! | **Role** | **User-facing API** | **Internal Semantics** |
//! | **Purpose** | Used for subset request overrides via `AggregationSpec`. | Used for full execution logic within the schema, signal policies, and `subset_executor`. |
//! | **Variants** | **Simplified set:**<br>- `Sum`, `Mean`, `Count`<br>- `Min`, `Max`<br>- `First`, `Last` | **Extended set:**<br>- *All `request` variants*<br>- `Any`, `All` (Boolean logic)<br>- `LinearTrend` (Complex logic)<br>- `Auto` (Inferred from `MeasurementKind`) |
//!
//! ## Usage Guide
//!
//! * **Input:** Users specify [`request::AggregationType`] when overriding defaults in a subset
//!   request.
//! * **Execution:** These are converted into [`aggregation::Aggregate`] internally to perform the
//!   actual Polars computations.
//! * **Defaults:** If no override is provided, the `MeasurementKind` determines the default
//!   `aggregation::Aggregate` (e.g., `Measure` → `Mean`, `Binary` → `Any`)./!

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// A synthetic subject created by aggregating across real subjects.
///
/// Example:
/// ```ignore
/// SyntheticSubject::mean_all("Fleet Average")
/// SyntheticSubject::new("Blue Shirts Total")
///     .rule("shirt_units", Aggregate::Sum)
///     .where_component("color", "blue")
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyntheticSubject {
	/// Name pattern for the synthetic subject.
	/// Can contain `{quality}` or `{component}` placeholders for grouping.
	pub name_pattern: String,

	/// Aggregation rule for each measurement
	pub rules: HashMap<String, Aggregate>,

	/// Default aggregation for measurements not in rules
	pub default_rule: Option<Aggregate>,

	/// Group by quality column (creates multiple synthetic subjects)
	pub group_by_quality: Option<String>,

	/// Group by component (creates multiple synthetic subjects)
	pub group_by_component: Option<String>,

	/// Filter to specific component values before aggregating
	pub component_filters: HashMap<String, String>,
}

impl SyntheticSubject {
	/// Create a new synthetic subject with custom rules
	pub fn new(name: impl Into<String>) -> Self {
		Self {
			name_pattern:       name.into(),
			rules:              HashMap::new(),
			default_rule:       None,
			group_by_quality:   None,
			group_by_component: None,
			component_filters:  HashMap::new(),
		}
	}

	/// Create a synthetic subject that averages all measurements
	pub fn mean_all(name: impl Into<String>) -> Self {
		Self {
			name_pattern:       name.into(),
			rules:              HashMap::new(),
			default_rule:       Some(Aggregate::Mean),
			group_by_quality:   None,
			group_by_component: None,
			component_filters:  HashMap::new(),
		}
	}

	/// Create a synthetic subject that sums all measurements
	pub fn sum_all(name: impl Into<String>) -> Self {
		Self {
			name_pattern:       name.into(),
			rules:              HashMap::new(),
			default_rule:       Some(Aggregate::Sum),
			group_by_quality:   None,
			group_by_component: None,
			component_filters:  HashMap::new(),
		}
	}

	/// Create a synthetic subject that takes the min of all measurements
	pub fn min_all(name: impl Into<String>) -> Self {
		Self {
			name_pattern:       name.into(),
			rules:              HashMap::new(),
			default_rule:       Some(Aggregate::Min),
			group_by_quality:   None,
			group_by_component: None,
			component_filters:  HashMap::new(),
		}
	}

	/// Create a synthetic subject that takes the max of all measurements
	pub fn max_all(name: impl Into<String>) -> Self {
		Self {
			name_pattern:       name.into(),
			rules:              HashMap::new(),
			default_rule:       Some(Aggregate::Max),
			group_by_quality:   None,
			group_by_component: None,
			component_filters:  HashMap::new(),
		}
	}

	/// Create a synthetic subject using automatic rules based on MeasurementKind:
	/// - Measure → Mean
	/// - Categorical → Any
	/// - Count → Sum
	/// - Average → Mean
	pub fn auto(name: impl Into<String>) -> Self {
		Self {
			name_pattern:       name.into(),
			rules:              HashMap::new(),
			default_rule:       Some(Aggregate::Auto),
			group_by_quality:   None,
			group_by_component: None,
			component_filters:  HashMap::new(),
		}
	}

	/// Set an aggregation rule for a specific measurement
	pub fn rule(mut self, measurement: impl Into<String>, aggregate: Aggregate) -> Self {
		self.rules.insert(measurement.into(), aggregate);
		self
	}

	/// Group by a quality column, creating multiple synthetic subjects.
	/// Use `{quality}` in name pattern to include the value.
	///
	/// Example:
	/// ```ignore
	/// SyntheticSubject::mean_all("{zone} Average")
	///     .group_by("zone")
	/// // Creates: "Zone A Average", "Zone B Average", etc.
	/// ```
	pub fn group_by(mut self, quality: impl Into<String>) -> Self {
		self.group_by_quality = Some(quality.into());
		self
	}

	/// Group by a component, creating multiple synthetic subjects.
	/// Use `{component}` in name pattern to include the value.
	///
	/// Example:
	/// ```ignore
	/// SyntheticSubject::sum_all("{color} Total")
	///     .group_by_component("color")
	/// // Creates: "blue Total", "red Total", etc.
	/// ```
	pub fn group_by_component(mut self, component: impl Into<String>) -> Self {
		self.group_by_component = Some(component.into());
		self
	}

	/// Filter to a specific component value before aggregating.
	///
	/// Example:
	/// ```ignore
	/// SyntheticSubject::sum_all("Blue Shirts Total")
	///     .where_component("color", "blue")
	/// ```
	pub fn where_component(
		mut self,
		component: impl Into<String>,
		value: impl Into<String>,
	) -> Self {
		self
			.component_filters
			.insert(component.into(), value.into());
		self
	}

	/// Get the aggregation for a measurement, falling back to default
	pub fn get_aggregate(&self, measurement: &str) -> Option<&Aggregate> {
		self.rules.get(measurement).or(self.default_rule.as_ref())
	}

	/// Check if this synthetic subject uses grouping
	pub fn is_grouped(&self) -> bool {
		self.group_by_quality.is_some() || self.group_by_component.is_some()
	}

	/// Check if this synthetic subject has component filters
	pub fn has_component_filters(&self) -> bool {
		!self.component_filters.is_empty()
	}

	/// Expand the name pattern with actual values
	pub fn expand_name(&self, quality_value: Option<&str>, component_value: Option<&str>) -> String {
		let mut name = self.name_pattern.clone();

		if let Some(qv) = quality_value {
			name = name.replace("{quality}", qv);
			// Also support named placeholders like {zone}
			if let Some(ref q) = self.group_by_quality {
				name = name.replace(&format!("{{{}}}", q), qv);
			}
		}

		if let Some(cv) = component_value {
			name = name.replace("{component}", cv);
			// Also support named placeholders like {color}
			if let Some(ref c) = self.group_by_component {
				name = name.replace(&format!("{{{}}}", c), cv);
			}
		}

		name
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Aggregate {
	/// Arithmetic mean
	Mean,
	/// Sum
	Sum,
	/// Minimum value
	Min,
	/// Maximum value
	Max,
	/// Boolean OR (any non-zero → true)
	Any,
	/// Boolean AND (all non-zero → true)
	All,
	/// Count of non-null values
	Count,
	/// First value
	First,
	/// Last value
	Last,
	/// Keep the row where the component column has the greatest value.
	/// For forecast data: keeps the most recent model run.
	/// Works with any orderable type (datetime, integer, string).
	MostRecent,
	/// Keep the row where the component column has the least value.
	/// Works with any orderable type (datetime, integer, string).
	LeastRecent,
	/// Linear regression trend line
	LinearTrend,
	/// Automatic based on MeasurementKind
	Auto,
}

impl Aggregate {
	/// Get the Polars aggregation name
	pub fn as_str(&self) -> &'static str {
		match self {
			Aggregate::Mean => "mean",
			Aggregate::Sum => "sum",
			Aggregate::Min => "min",
			Aggregate::Max => "max",
			Aggregate::Any => "max", // max of 0/1 = any
			Aggregate::All => "min", // min of 0/1 = all
			Aggregate::Count => "count",
			Aggregate::First => "first",
			Aggregate::Last => "last",
			Aggregate::MostRecent => "most_recent",
			Aggregate::LeastRecent => "least_recent",
			Aggregate::LinearTrend => "linear_trend",
			Aggregate::Auto => "auto",
		}
	}

	/// Resolve Auto to a concrete aggregation based on MeasurementKind
	pub fn resolve_auto(kind: crate::unit::MeasurementKind) -> Self {
		use crate::unit::MeasurementKind;
		match kind {
			MeasurementKind::Measure => Aggregate::Mean,
			MeasurementKind::Categorical => Aggregate::Last,
			MeasurementKind::Count => Aggregate::Sum,
			MeasurementKind::Average => Aggregate::Mean,
			MeasurementKind::Binary => Aggregate::Any,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_synthetic_subject_mean_all() {
		let s = SyntheticSubject::mean_all("Fleet Average");

		assert_eq!(s.name_pattern, "Fleet Average");
		assert_eq!(s.default_rule, Some(Aggregate::Mean));
		assert!(s.rules.is_empty());
	}

	#[test]
	fn test_synthetic_subject_with_rules() {
		let s = SyntheticSubject::new("Custom")
			.rule("fuel", Aggregate::Mean)
			.rule("units", Aggregate::Sum)
			.rule("any_engine", Aggregate::Any);

		assert_eq!(s.rules.len(), 3);
		assert_eq!(s.get_aggregate("fuel"), Some(&Aggregate::Mean));
		assert_eq!(s.get_aggregate("units"), Some(&Aggregate::Sum));
		assert_eq!(s.get_aggregate("unknown"), None);
	}

	#[test]
	fn test_synthetic_subject_grouped() {
		let s = SyntheticSubject::mean_all("{zone} Average").group_by("zone");

		assert!(s.is_grouped());
		assert_eq!(s.group_by_quality, Some("zone".to_string()));

		let name = s.expand_name(Some("North"), None);
		assert_eq!(name, "North Average");
	}

	#[test]
	fn test_synthetic_subject_component_filter() {
		let s = SyntheticSubject::sum_all("Blue Large Total")
			.where_component("color", "blue")
			.where_component("size", "L");

		assert!(s.has_component_filters());
		assert_eq!(s.component_filters.len(), 2);
	}

	#[test]
	fn test_synthetic_subject_grouped_by_component() {
		let s = SyntheticSubject::sum_all("{color} Total").group_by_component("color");

		assert!(s.is_grouped());

		let name = s.expand_name(None, Some("blue"));
		assert_eq!(name, "blue Total");
	}

	#[test]
	fn test_aggregate_auto_resolve() {
		use crate::unit::MeasurementKind;

		assert_eq!(Aggregate::resolve_auto(MeasurementKind::Measure), Aggregate::Mean);
		assert_eq!(Aggregate::resolve_auto(MeasurementKind::Categorical), Aggregate::Last);
		assert_eq!(Aggregate::resolve_auto(MeasurementKind::Count), Aggregate::Sum);
	}
}
