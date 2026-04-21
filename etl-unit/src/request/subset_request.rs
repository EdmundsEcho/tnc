//! Subset request: specifies what slice of the universe to extract

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{aggregation::AggregationSpec, interval::Interval};
use crate::{
	CanonicalColumnName, aggregation::SyntheticSubject, component_filter::ComponentFilters,
};

/// Filter subjects by quality value (e.g., parish = "Lafourche")
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityFilter {
	pub quality: CanonicalColumnName,
	pub values: Vec<String>,
}

/// Request for a subset of the universe (from all etl-units)
///
/// Result shape:
/// - Rows: subjects × time points (+ synthetic subjects if requested)
/// - Columns: subject + time + measurements + qualities
///
/// # Component Filtering
///
/// The `component_filters` field controls which component columns appear in output:
/// - `None` or empty → crush ALL components (aggregate them out)
/// - Components in filter → appear in output, others are crushed
/// - Each component can have level filtering (Include/Exclude specific values)
///
/// See [`ComponentFilters`] for details.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EtlUnitSubsetRequest {
	/// Which measurements to include (by name). Empty = all base measurements + derivations.
	#[serde(default)]
	pub measurements: Vec<CanonicalColumnName>,

	/// Which qualities to include (by name). Empty = none.
	#[serde(default)]
	pub qualities: Vec<CanonicalColumnName>,

	/// Filter to specific subjects. None = all subjects.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub subject_filter: Option<SubjectFilter>,

	/// Filter subjects by quality value (e.g., only stations in parish "Lafourche").
	#[serde(skip_serializing_if = "Option::is_none")]
	pub quality_filter: Option<QualityFilter>,

	/// Filter to time range. None = all time.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub time_range: Option<TimeRange>,

	/// Component filters - controls which components appear in output and level filtering.
	/// None or empty = crush ALL components (aggregate them out).
	#[serde(skip_serializing_if = "Option::is_none")]
	pub component_filters: Option<ComponentFilters>,

	/// How to reduce/crush/aggregate when components are collapsed.
	/// Defaults to MeasurementKind's default reducer.
	#[deprecated(since = "0.1.0", note = "Please set at the measurement level")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub aggregation_override: Option<AggregationSpec>,

	/// Synthetic subjects to create by aggregating across real subjects
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub synthetic_subjects: Vec<SyntheticSubject>,

	/// Intervals to summarize data over time
	#[serde(skip_serializing_if = "Option::is_none")]
	pub interval: Option<Interval>,

	/// Report interval: aggregate data into coarse time buckets
	/// (monthly/weekly/daily/hourly/fixed) with per-measurement statistics
	/// (N, stderr, min, max) captured for each bucket.
	///
	/// Distinct from the simple `interval` field above, which controls
	/// master-grid spacing only. `report_interval` drives **bucketed
	/// aggregation** — each measurement's data in a bucket is collapsed
	/// to a single value via its configured or overridden aggregation,
	/// and LTTB decimation is bypassed (the bucketed output already fits
	/// any chart budget).
	///
	/// See [`crate::interval::ReportInterval`] for bucket types and
	/// rate-strategy options.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub report_interval: Option<crate::interval::ReportInterval>,
}

impl EtlUnitSubsetRequest {
	pub fn new() -> Self {
		Self::default()
	}

	/// Request specific measurements
	pub fn measurements(mut self, names: Vec<CanonicalColumnName>) -> Self {
		self.measurements = names;
		self
	}

	/// Request specific qualities
	pub fn qualities(mut self, names: Vec<CanonicalColumnName>) -> Self {
		self.qualities = names;
		self
	}

	/// Filter to specific subjects
	pub fn subject_filter(mut self, filter: SubjectFilter) -> Self {
		self.subject_filter = Some(filter);
		self
	}

	/// Filter to specific subjects by string IDs
	pub fn subjects(mut self, subjects: Vec<String>) -> Self {
		self.subject_filter = Some(SubjectFilter::include_strings(subjects));
		self
	}

	/// Filter subjects by quality value (e.g., only stations in parish "Lafourche")
	pub fn quality_filter(mut self, filter: QualityFilter) -> Self {
		self.quality_filter = Some(filter);
		self
	}

	/// Filter to a time range
	pub fn time_range(mut self, range: TimeRange) -> Self {
		self.time_range = Some(range);
		self
	}

	// =========================================================================
	// Component Filters
	// =========================================================================

	/// Set the component filters.
	///
	/// Components NOT in this filter will be crushed (aggregated out).
	/// Components IN this filter will appear in output with optional level filtering.
	pub fn with_component_filters(mut self, filters: ComponentFilters) -> Self {
		self.component_filters = Some(filters);
		self
	}

	/// Include a component with all its levels in output.
	///
	/// Other components not explicitly included will be crushed.
	pub fn include_component(mut self, component: impl Into<String>) -> Self {
		let filters = self
			.component_filters
			.get_or_insert_with(ComponentFilters::new);
		*filters = std::mem::take(filters).include_all(component);
		self
	}

	/// Include a component filtered to specific levels.
	///
	/// Only rows where the component matches one of the given levels will be included.
	/// Other components not explicitly included will be crushed.
	pub fn filter_component(
		mut self,
		component: impl Into<String>,
		levels: Vec<serde_json::Value>,
	) -> Self {
		let filters = self
			.component_filters
			.get_or_insert_with(ComponentFilters::new);
		*filters = std::mem::take(filters).include_levels(component, levels);
		self
	}

	/// Include a component but exclude specific levels.
	///
	/// Rows where the component matches one of the given levels will be excluded.
	/// Other components not explicitly included will be crushed.
	pub fn exclude_component_levels(
		mut self,
		component: impl Into<String>,
		levels: Vec<serde_json::Value>,
	) -> Self {
		let filters = self
			.component_filters
			.get_or_insert_with(ComponentFilters::new);
		*filters = std::mem::take(filters).exclude_levels(component, levels);
		self
	}

	/// Check if any component filters are defined.
	///
	/// If false, all components will be crushed.
	pub fn has_component_filters(&self) -> bool {
		self
			.component_filters
			.as_ref()
			.map(|f| !f.is_empty())
			.unwrap_or(false)
	}

	// =========================================================================
	// Aggregation and Synthetic Subjects
	// =========================================================================

	/// Override aggregation for specific measurements
	pub fn aggregation_override(mut self, spec: AggregationSpec) -> Self {
		self.aggregation_override = Some(spec);
		self
	}

	/// Add a synthetic subject
	pub fn with_synthetic_subject(mut self, synthetic: SyntheticSubject) -> Self {
		self.synthetic_subjects.push(synthetic);
		self
	}

	/// Check if the request includes synthetic subjects
	pub fn has_synthetic_subjects(&self) -> bool {
		!self.synthetic_subjects.is_empty()
	}

	/// Set the sampling interval for time bucketing.
	///
	/// Data will be aggregated into buckets of this size.
	/// Uses each measurement's default aggregation (Mean for Measure, Sum for Count, etc.)
	///
	/// # Example
	/// ```rust,ignore
	/// // Last 24 hours, sampled hourly (24 data points per measurement)
	/// let request = EtlUnitSubsetRequest::new()
	///     .measurements(vec!["sump".into()])
	///     .time_range(TimeRange::last_hours(24))
	///     .interval(Interval::hours(1));
	/// ```
	pub fn interval(mut self, interval: Interval) -> Self {
		self.interval = Some(interval);
		self
	}

	/// Set interval from a human-friendly string like "15m", "1h", "1d"
	///
	/// Returns error if the string cannot be parsed.
	pub fn interval_str(mut self, s: &str) -> Result<Self, crate::EtlError> {
		let interval = Interval::parse(s).map_err(|e| {
			crate::EtlError::Config(format!(
				"Invalid interval '{}': {}. Use formats like '15m', '1h', '6h', '1d'",
				s, e
			))
		})?;
		self.interval = Some(interval);
		Ok(self)
	}

	/// Check if this request requires time bucketing
	pub fn has_interval(&self) -> bool {
		self.interval.is_some()
	}

	/// Set the report interval. When set, subset execution aggregates
	/// each measurement's data into the configured buckets (monthly,
	/// weekly, daily, hourly, or fixed) and bypasses LTTB decimation.
	///
	/// See [`crate::interval::ReportInterval`].
	pub fn report_interval(mut self, interval: crate::interval::ReportInterval) -> Self {
		self.report_interval = Some(interval);
		self
	}

	/// Whether this request asks for interval-based bucketed aggregation.
	pub fn has_report_interval(&self) -> bool {
		self.report_interval.is_some()
	}

	/// Check if this request has components that will be crushed (not included in filter)
	pub fn has_components_to_crush(&self) -> bool {
		// If no component filters specified, all components get crushed
		// If component filters exist but don't cover all components, some get crushed
		// This is a simplified check - actual crushing logic is in subset_executor
		self.component_filters.as_ref().is_none_or(|f| f.is_empty())
	}

	/// Check if this request needs any aggregation (time bucketing or component crushing)
	pub fn needs_aggregation(&self) -> bool {
		self.interval.is_some() || self.has_components_to_crush()
	}
}

/// Filter subjects by inclusion or exclusion
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "values")]
pub enum SubjectFilter {
	/// Include only these specific subject values
	Include(Vec<serde_json::Value>),
	/// Exclude these subject values
	Exclude(Vec<serde_json::Value>),
}

impl SubjectFilter {
	pub fn include(values: Vec<serde_json::Value>) -> Self {
		Self::Include(values)
	}

	pub fn exclude(values: Vec<serde_json::Value>) -> Self {
		Self::Exclude(values)
	}

	/// Include a single string subject
	pub fn include_one(value: impl Into<String>) -> Self {
		Self::Include(vec![serde_json::Value::String(value.into())])
	}

	/// Include multiple string subjects
	pub fn include_strings(values: Vec<String>) -> Self {
		Self::Include(values.into_iter().map(serde_json::Value::String).collect())
	}
}

/// Time range filter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRange {
	/// Start of range (inclusive). None = no lower bound.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub start: Option<DateTime<Utc>>,

	/// End of range (exclusive). None = no upper bound.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub end: Option<DateTime<Utc>>,
}

impl TimeRange {
	pub fn new(start: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>) -> Self {
		Self {
			start,
			end,
		}
	}

	pub fn from(start: DateTime<Utc>) -> Self {
		Self {
			start: Some(start),
			end:   None,
		}
	}

	pub fn until(end: DateTime<Utc>) -> Self {
		Self {
			start: None,
			end:   Some(end),
		}
	}

	pub fn between(start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
		Self {
			start: Some(start),
			end:   Some(end),
		}
	}

	/// Last N hours from now
	pub fn last_hours(hours: i64) -> Self {
		let end = Utc::now();
		let start = end - chrono::Duration::hours(hours);
		Self::between(start, end)
	}

	/// Last N days from now
	pub fn last_days(days: i64) -> Self {
		let end = Utc::now();
		let start = end - chrono::Duration::days(days);
		Self::between(start, end)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::{
		aggregation::Aggregate, component_filter::LevelFilter, request::aggregation::AggregationType,
	};

	#[test]
	fn test_basic_request() {
		let req = EtlUnitSubsetRequest::new()
			.measurements(vec!["sump_ft".into(), "fuel_pct".into()])
			.qualities(vec!["station_name".into()])
			.time_range(TimeRange::last_hours(24));

		assert_eq!(req.measurements.len(), 2);
		assert_eq!(req.qualities.len(), 1);
		assert!(req.time_range.is_some());
	}

	#[test]
	fn test_subject_filter() {
		let filter = SubjectFilter::include_strings(vec!["station_1".into(), "station_2".into()]);

		if let SubjectFilter::Include(values) = filter {
			assert_eq!(values.len(), 2);
		} else {
			panic!("Expected Include variant");
		}
	}

	#[test]
	fn test_component_filters_include_all() {
		let req = EtlUnitSubsetRequest::new()
			.measurements(vec!["value".into()])
			.include_component("color")
			.include_component("size");

		assert!(req.has_component_filters());
		let filters = req.component_filters.unwrap();
		assert_eq!(filters.len(), 2);
		assert!(filters.includes("color"));
		assert!(filters.includes("size"));
		assert!(filters.get("color").unwrap().includes_all());
	}

	#[test]
	fn test_component_filters_include_levels() {
		let req = EtlUnitSubsetRequest::new()
			.measurements(vec!["value".into()])
			.filter_component("color", vec!["red".into(), "blue".into()]);

		assert!(req.has_component_filters());
		let filters = req.component_filters.unwrap();
		assert!(filters.includes("color"));

		let level_filter = filters.get("color").unwrap();
		assert!(!level_filter.includes_all());
		assert!(matches!(level_filter.levels, LevelFilter::Include(_)));
	}

	#[test]
	fn test_component_filters_exclude_levels() {
		let req = EtlUnitSubsetRequest::new()
			.measurements(vec!["value".into()])
			.exclude_component_levels("color", vec!["unknown".into()]);

		assert!(req.has_component_filters());
		let filters = req.component_filters.unwrap();
		assert!(filters.includes("color"));

		let level_filter = filters.get("color").unwrap();
		assert!(matches!(level_filter.levels, LevelFilter::Exclude(_)));
	}

	#[test]
	fn test_component_filters_with_builder() {
		let req = EtlUnitSubsetRequest::new()
			.measurements(vec!["value".into()])
			.with_component_filters(
				ComponentFilters::new()
					.include_all("color")
					.include_levels("size", vec!["small".into(), "medium".into()])
					.exclude_levels("region", vec!["unknown".into()]),
			);

		assert!(req.has_component_filters());
		let filters = req.component_filters.unwrap();
		assert_eq!(filters.len(), 3);
	}

	#[test]
	fn test_no_component_filters_means_crush_all() {
		let req = EtlUnitSubsetRequest::new().measurements(vec!["value".into()]);

		assert!(!req.has_component_filters());
		assert!(req.component_filters.is_none());
	}

	#[test]
	fn test_time_range_last_hours() {
		let range = TimeRange::last_hours(24);
		assert!(range.start.is_some());
		assert!(range.end.is_some());

		let duration = range.end.unwrap() - range.start.unwrap();
		assert_eq!(duration.num_hours(), 24);
	}

	#[test]
	fn test_aggregation_override() {
		let spec = AggregationSpec::new()
			.set("units_sold".into(), AggregationType::Sum)
			.set("price".into(), AggregationType::Mean);

		assert_eq!(spec.overrides.len(), 2);
		assert_eq!(spec.overrides.get(&("units_sold".into())), Some(&AggregationType::Sum));
	}

	#[test]
	fn test_request_with_synthetic_subject() {
		let req = EtlUnitSubsetRequest::new()
			.measurements(vec!["fuel".into(), "sump".into()])
			.with_synthetic_subject(SyntheticSubject::mean_all("Fleet Average"))
			.with_synthetic_subject(
				SyntheticSubject::new("Custom")
					.rule("fuel", Aggregate::Mean)
					.rule("sump", Aggregate::Max),
			);

		assert!(req.has_synthetic_subjects());
		assert_eq!(req.synthetic_subjects.len(), 2);
	}

	#[test]
	fn test_request_with_grouped_synthetic() {
		let req = EtlUnitSubsetRequest::new()
			.measurements(vec!["fuel".into()])
			.with_synthetic_subject(SyntheticSubject::mean_all("{zone} Average").group_by("zone"));

		assert_eq!(req.synthetic_subjects.len(), 1);
		assert!(req.synthetic_subjects[0].is_grouped());
	}

	#[test]
	fn test_subjects_shorthand() {
		let req = EtlUnitSubsetRequest::new().subjects(vec!["station_A".into(), "station_B".into()]);

		assert!(req.subject_filter.is_some());
		if let Some(SubjectFilter::Include(values)) = req.subject_filter {
			assert_eq!(values.len(), 2);
		} else {
			panic!("Expected Include variant");
		}
	}

	#[test]
	fn test_chained_component_filters() {
		// Test that we can chain multiple component filters
		let req = EtlUnitSubsetRequest::new()
			.measurements(vec!["value".into()])
			.include_component("color")
			.filter_component("size", vec!["small".into()])
			.exclude_component_levels("region", vec!["unknown".into()]);

		let filters = req.component_filters.unwrap();
		assert_eq!(filters.len(), 3);

		// color - all levels
		assert!(filters.get("color").unwrap().includes_all());

		// size - specific levels
		assert!(matches!(filters.get("size").unwrap().levels, LevelFilter::Include(_)));

		// region - exclude specific
		assert!(matches!(filters.get("region").unwrap().levels, LevelFilter::Exclude(_)));
	}
}
