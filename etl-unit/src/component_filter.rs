//! Component filtering for subset requests.
//!
//! Controls which component columns appear in subset output and how their levels are filtered.
//!
//! # Behavior
//!
//! - `None` or empty `ComponentFilters` on a request → crush ALL components (aggregate them out using measurement's
//!   aggregation function)
//! - Components IN the filter → appear in output, others are crushed
//! - Each component's `LevelFilter` controls row filtering:
//!   - `All` → include all level values
//!   - `Include(vec)` → include only rows with these level values
//!   - `Exclude(vec)` → exclude rows with these level values
//!
//! # Examples
//!
//! ```rust,ignore
//! // Include "color" component, filter to only "red" and "blue" levels
//! // Crush all other components (e.g., "size", "region")
//! let filters = ComponentFilters::new()
//!     .include_levels("color", vec!["red".into(), "blue".into()]);
//!
//! // Include "color" with all levels
//! let filters = ComponentFilters::new()
//!     .include_all("color");
//!
//! // Include "color" but exclude the "unknown" level
//! let filters = ComponentFilters::new()
//!     .exclude_levels("color", vec!["unknown".into()]);
//!
//! // On a request:
//! let request = EtlUnitSubsetRequest::new()
//!     .measurements(vec!["temperature".into()])
//!     .filter_component("sensor_type", vec!["indoor".into()]);
//! ```

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Level filter mode for a component.
///
/// Controls which level values are included for a component column.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LevelFilter {
	/// Include all levels (no filtering)
	All,
	/// Include only these specific levels
	Include(Vec<serde_json::Value>),
	/// Exclude these specific levels (include all others)
	Exclude(Vec<serde_json::Value>),
}

impl Default for LevelFilter {
	fn default() -> Self {
		Self::All
	}
}

impl LevelFilter {
	/// Check if this filter includes all levels
	pub fn includes_all(&self) -> bool {
		matches!(self, Self::All)
	}

	/// Check if this filter has any level restrictions
	pub fn has_restrictions(&self) -> bool {
		match self {
			Self::All => false,
			Self::Include(v) | Self::Exclude(v) => !v.is_empty(),
		}
	}

	/// Get the levels if this is an Include filter
	pub fn included_levels(&self) -> Option<&Vec<serde_json::Value>> {
		match self {
			Self::Include(levels) => Some(levels),
			_ => None,
		}
	}

	/// Get the levels if this is an Exclude filter
	pub fn excluded_levels(&self) -> Option<&Vec<serde_json::Value>> {
		match self {
			Self::Exclude(levels) => Some(levels),
			_ => None,
		}
	}
}

/// Filter for a single component's levels.
///
/// Determines which level values to include or exclude for a specific component column.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ComponentFilter {
	/// How to filter levels for this component
	pub levels: LevelFilter,
}

impl ComponentFilter {
	/// Include all levels (no filtering)
	pub fn all() -> Self {
		Self {
			levels: LevelFilter::All,
		}
	}

	/// Include only specific levels
	pub fn include(levels: Vec<serde_json::Value>) -> Self {
		Self {
			levels: LevelFilter::Include(levels),
		}
	}

	/// Exclude specific levels (include all others)
	pub fn exclude(levels: Vec<serde_json::Value>) -> Self {
		Self {
			levels: LevelFilter::Exclude(levels),
		}
	}

	/// Check if this filter includes all levels
	pub fn includes_all(&self) -> bool {
		self.levels.includes_all()
	}

	/// Check if this filter has any level restrictions
	pub fn has_restrictions(&self) -> bool {
		self.levels.has_restrictions()
	}
}

/// Filters controlling which components appear in subset output.
///
/// # Crushing Behavior
///
/// Components not included in this filter are "crushed" - aggregated out using
/// the measurement's aggregation function (determined by `MeasurementKind`).
/// This reduces the dimensionality of the data.
///
/// For example, if data has (station, timestamp, value, color, size) and you only
/// include "color" in the filter, "size" will be crushed out by aggregating
/// values across all sizes for each (station, timestamp, color) combination.
///
/// # Level Filtering
///
/// For included components, you can further filter which level values to include:
/// - `LevelFilter::All` - keep all level values
/// - `LevelFilter::Include(vec)` - only keep rows where component value is in vec
/// - `LevelFilter::Exclude(vec)` - only keep rows where component value is NOT in vec
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ComponentFilters {
	/// Component canonical name → filter for that component
	filters: HashMap<String, ComponentFilter>,
}

impl ComponentFilters {
	/// Create empty component filters.
	///
	/// An empty filter means all components will be crushed.
	pub fn new() -> Self {
		Self::default()
	}

	/// Include a component with all its levels.
	///
	/// The component column will appear in output with all level values.
	pub fn include_all(mut self, component: impl Into<String>) -> Self {
		self.filters.insert(component.into(), ComponentFilter::all());
		self
	}

	/// Include a component filtered to specific levels only.
	///
	/// The component column will appear in output, but only rows with
	/// matching level values will be included.
	pub fn include_levels(mut self, component: impl Into<String>, levels: Vec<serde_json::Value>) -> Self {
		self.filters.insert(component.into(), ComponentFilter::include(levels));
		self
	}

	/// Include a component but exclude specific levels.
	///
	/// The component column will appear in output, but rows with
	/// matching level values will be excluded.
	pub fn exclude_levels(mut self, component: impl Into<String>, levels: Vec<serde_json::Value>) -> Self {
		self.filters.insert(component.into(), ComponentFilter::exclude(levels));
		self
	}

	/// Add a component with a custom filter.
	pub fn with_filter(mut self, component: impl Into<String>, filter: ComponentFilter) -> Self {
		self.filters.insert(component.into(), filter);
		self
	}

	/// Check if any components are included.
	///
	/// If empty, all components will be crushed.
	pub fn is_empty(&self) -> bool {
		self.filters.is_empty()
	}

	/// Get the number of included components.
	pub fn len(&self) -> usize {
		self.filters.len()
	}

	/// Check if a component is included.
	pub fn includes(&self, component: &str) -> bool {
		self.filters.contains_key(component)
	}

	/// Get the filter for a component.
	pub fn get(&self, component: &str) -> Option<&ComponentFilter> {
		self.filters.get(component)
	}

	/// Get the level filter for a component.
	pub fn get_level_filter(&self, component: &str) -> Option<&LevelFilter> {
		self.filters.get(component).map(|f| &f.levels)
	}

	/// Iterate over included component names.
	pub fn included_components(&self) -> impl Iterator<Item = &str> {
		self.filters.keys().map(|s| s.as_str())
	}

	/// Iterate over (component, filter) pairs.
	pub fn iter(&self) -> impl Iterator<Item = (&str, &ComponentFilter)> {
		self.filters.iter().map(|(k, v)| (k.as_str(), v))
	}

	/// Get components that have level restrictions (Include or Exclude).
	pub fn components_with_restrictions(&self) -> impl Iterator<Item = (&str, &LevelFilter)> {
		self.filters.iter().filter(|(_, f)| f.has_restrictions()).map(|(k, f)| (k.as_str(), &f.levels))
	}

	/// Get the underlying HashMap (for Polars operations).
	pub fn as_map(&self) -> &HashMap<String, ComponentFilter> {
		&self.filters
	}
}

#[cfg(test)]
mod tests {
	use serde_json::json;

	use super::*;

	#[test]
	fn test_level_filter_all() {
		let filter = LevelFilter::All;
		assert!(filter.includes_all());
		assert!(!filter.has_restrictions());
		assert!(filter.included_levels().is_none());
		assert!(filter.excluded_levels().is_none());
	}

	#[test]
	fn test_level_filter_include() {
		let filter = LevelFilter::Include(vec![json!("red"), json!("blue")]);
		assert!(!filter.includes_all());
		assert!(filter.has_restrictions());
		assert_eq!(filter.included_levels().unwrap().len(), 2);
		assert!(filter.excluded_levels().is_none());
	}

	#[test]
	fn test_level_filter_exclude() {
		let filter = LevelFilter::Exclude(vec![json!("unknown")]);
		assert!(!filter.includes_all());
		assert!(filter.has_restrictions());
		assert!(filter.included_levels().is_none());
		assert_eq!(filter.excluded_levels().unwrap().len(), 1);
	}

	#[test]
	fn test_component_filter_constructors() {
		let all = ComponentFilter::all();
		assert!(all.includes_all());

		let include = ComponentFilter::include(vec![json!("a"), json!("b")]);
		assert!(!include.includes_all());
		assert!(include.has_restrictions());

		let exclude = ComponentFilter::exclude(vec![json!("x")]);
		assert!(!exclude.includes_all());
		assert!(exclude.has_restrictions());
	}

	#[test]
	fn test_component_filters_empty() {
		let filters = ComponentFilters::new();
		assert!(filters.is_empty());
		assert_eq!(filters.len(), 0);
		assert!(!filters.includes("color"));
	}

	#[test]
	fn test_component_filters_include_all() {
		let filters = ComponentFilters::new().include_all("color").include_all("size");

		assert!(!filters.is_empty());
		assert_eq!(filters.len(), 2);
		assert!(filters.includes("color"));
		assert!(filters.includes("size"));
		assert!(!filters.includes("region"));

		let color_filter = filters.get("color").unwrap();
		assert!(color_filter.includes_all());
	}

	#[test]
	fn test_component_filters_include_levels() {
		let filters = ComponentFilters::new().include_levels("color", vec![json!("red"), json!("blue")]);

		assert!(filters.includes("color"));

		let level_filter = filters.get_level_filter("color").unwrap();
		assert!(matches!(level_filter, LevelFilter::Include(_)));

		let levels = level_filter.included_levels().unwrap();
		assert_eq!(levels.len(), 2);
	}

	#[test]
	fn test_component_filters_exclude_levels() {
		let filters = ComponentFilters::new().exclude_levels("color", vec![json!("unknown"), json!("invalid")]);

		assert!(filters.includes("color"));

		let level_filter = filters.get_level_filter("color").unwrap();
		assert!(matches!(level_filter, LevelFilter::Exclude(_)));

		let levels = level_filter.excluded_levels().unwrap();
		assert_eq!(levels.len(), 2);
	}

	#[test]
	fn test_component_filters_mixed() {
		let filters = ComponentFilters::new()
			.include_all("color")
			.include_levels("size", vec![json!("small"), json!("medium")])
			.exclude_levels("region", vec![json!("unknown")]);

		assert_eq!(filters.len(), 3);

		// color - all levels
		assert!(filters.get("color").unwrap().includes_all());

		// size - specific levels
		let size_filter = filters.get_level_filter("size").unwrap();
		assert!(matches!(size_filter, LevelFilter::Include(_)));

		// region - exclude specific
		let region_filter = filters.get_level_filter("region").unwrap();
		assert!(matches!(region_filter, LevelFilter::Exclude(_)));
	}

	#[test]
	fn test_components_with_restrictions() {
		let filters = ComponentFilters::new()
			.include_all("color")
			.include_levels("size", vec![json!("small")])
			.exclude_levels("region", vec![json!("unknown")]);

		let restricted: Vec<_> = filters.components_with_restrictions().collect();
		assert_eq!(restricted.len(), 2);

		let restricted_names: Vec<&str> = restricted.iter().map(|(name, _)| *name).collect();
		assert!(restricted_names.contains(&"size"));
		assert!(restricted_names.contains(&"region"));
		assert!(!restricted_names.contains(&"color"));
	}

	#[test]
	fn test_iteration() {
		let filters = ComponentFilters::new().include_all("a").include_all("b").include_all("c");

		let names: Vec<&str> = filters.included_components().collect();
		assert_eq!(names.len(), 3);

		let pairs: Vec<_> = filters.iter().collect();
		assert_eq!(pairs.len(), 3);
	}

	#[test]
	fn test_serialization() {
		let filters = ComponentFilters::new().include_levels("color", vec![json!("red"), json!("blue")]);

		let json = serde_json::to_string(&filters).unwrap();
		let deserialized: ComponentFilters = serde_json::from_str(&json).unwrap();

		assert_eq!(filters, deserialized);
	}
}
