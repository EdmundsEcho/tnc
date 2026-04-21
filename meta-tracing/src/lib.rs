//! Flexible Metadata Collection with Tracing Integration
//!
//! Provides a generic way to accumulate metadata during processing.
//! Any package can add serializable data to named sections without
//! coupling to other packages' types.
//!
//! All metadata additions are automatically traced, providing both
//! runtime observability and collected metadata for responses.
//!
//! # Usage
//!
//! ```rust,ignore
//! use synapse_meta::MetaCollector;
//! use serde::Serialize;
//!
//! #[derive(Serialize, Debug)]
//! struct MyStats {
//!     input_rows: usize,
//!     output_rows: usize,
//! }
//!
//! let mut collector = MetaCollector::new();
//! collector.add_section("stats", &MyStats { input_rows: 1000, output_rows: 500 });
//! collector.add_issue("[WARN] Some rows filtered");
//!
//! // Timed section with automatic span
//! {
//!     let section = collector.timed_section("expensive_work");
//!     // ... do work ...
//!     section.finish_with_data(&MyStats { input_rows: 500, output_rows: 250 });
//! }
//!
//! let meta = collector.build();
//! println!("{}", serde_json::to_string_pretty(&meta).unwrap());
//! ```

use std::{collections::HashMap, time::Instant};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{Span, debug, info, info_span, warn};

// =============================================================================
// CollectedMeta - Final Output
// =============================================================================

/// Accumulated metadata - the final serializable output
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CollectedMeta {
	/// Named sections from different packages
	#[serde(skip_serializing_if = "HashMap::is_empty")]
	pub sections: HashMap<String, Value>,

	/// Issues/warnings from any source
	#[serde(skip_serializing_if = "Vec::is_empty")]
	pub issues: Vec<String>,

	/// Row tracking
	#[serde(skip_serializing_if = "Option::is_none")]
	pub input_rows: Option<usize>,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub output_rows: Option<usize>,

	/// Processing time in milliseconds
	#[serde(skip_serializing_if = "Option::is_none")]
	pub processing_time_ms: Option<u64>,
}

impl CollectedMeta {
	/// Get a section as a specific type
	pub fn get_section<T: for<'de> Deserialize<'de>>(
		&self,
		name: &str,
	) -> Option<T> {
		self
			.sections
			.get(name)
			.and_then(|v| serde_json::from_value(v.clone()).ok())
	}

	/// Check if there are any issues
	pub fn has_issues(&self) -> bool {
		!self.issues.is_empty()
	}

	/// Count warnings
	pub fn warning_count(&self) -> usize {
		self.issues.iter().filter(|i| i.contains("[WARN]")).count()
	}

	/// Count errors
	pub fn error_count(&self) -> usize {
		self.issues.iter().filter(|i| i.contains("[ERROR]")).count()
	}
}

// =============================================================================
// TimedSection - Span-based Timing
// =============================================================================

/// A timed section that creates a tracing span and captures elapsed time.
///
/// Created via `MetaCollector::timed_section()`. When finished, the elapsed
/// time is recorded both to tracing and to the metadata.
#[derive(Debug)]
pub struct TimedSection<'a> {
	collector: &'a mut MetaCollector,
	name:      String,
	span:      Span,
	start:     Instant,
}

impl<'a> TimedSection<'a> {
	/// Finish the section without additional data
	pub fn finish(self) {
		let elapsed_ms = self.start.elapsed().as_millis() as u64;
		let _enter = self.span.enter();
		info!(elapsed_ms, "section complete");

		let timing = TimingMeta {
			elapsed_ms,
		};
		if let Ok(value) = serde_json::to_value(&timing) {
			self.collector.sections.insert(self.name, value);
		}
	}

	/// Finish the section with additional data
	pub fn finish_with_data<T: Serialize>(self, data: &T) {
		let elapsed_ms = self.start.elapsed().as_millis() as u64;
		let _enter = self.span.enter();
		info!(elapsed_ms, "section complete");

		let timed = TimedSectionMeta {
			elapsed_ms,
			data: serde_json::to_value(data).unwrap_or_default(),
		};
		if let Ok(value) = serde_json::to_value(&timed) {
			self.collector.sections.insert(self.name, value);
		}
	}

	/// Finish the section, recording an error
	pub fn finish_with_error(self, error: impl Into<String>) {
		let elapsed_ms = self.start.elapsed().as_millis() as u64;
		let error = error.into();
		let _enter = self.span.enter();
		tracing::error!(elapsed_ms, error = %error, "section failed");

		self
			.collector
			.add_issue(format!("[ERROR] {}: {}", self.name, error));

		let timing = TimingMeta {
			elapsed_ms,
		};
		if let Ok(value) = serde_json::to_value(&timing) {
			self.collector.sections.insert(self.name, value);
		}
	}
}

/// Timing metadata for a section
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimingMeta {
	pub elapsed_ms: u64,
}

/// Timed section metadata with data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimedSectionMeta {
	pub elapsed_ms: u64,
	pub data:       Value,
}

// =============================================================================
// MetaCollector
// =============================================================================

/// Accumulates metadata during processing with automatic tracing.
///
/// Every addition is traced at the appropriate level:
/// - `add_section`: DEBUG level
/// - `add_issue`: WARN level for warnings, ERROR for errors
/// - `timed_section`: INFO span with timing
#[derive(Debug)]
pub struct MetaCollector {
	start_time:  Instant,
	sections:    HashMap<String, Value>,
	issues:      Vec<String>,
	input_rows:  Option<usize>,
	output_rows: Option<usize>,
}

impl MetaCollector {
	/// Create a new MetaCollector
	pub fn new() -> Self {
		debug!("MetaCollector created");
		Self {
			start_time:  Instant::now(),
			sections:    HashMap::new(),
			issues:      Vec::new(),
			input_rows:  None,
			output_rows: None,
		}
	}

	// -------------------------------------------------------------------------
	// Section Management
	// -------------------------------------------------------------------------

	/// Add a named section with any serializable data.
	/// Automatically traces at DEBUG level.
	pub fn add_section<T: Serialize>(
		&mut self,
		name: impl Into<String>,
		data: &T,
	) {
		let name = name.into();
		if let Ok(value) = serde_json::to_value(data) {
			debug!(section = %name, "meta section added");
			self.sections.insert(name, value);
		}
	}

	/// Add or merge into a named section.
	/// If section exists and both are objects, merges keys.
	pub fn merge_section<T: Serialize>(
		&mut self,
		name: impl Into<String>,
		data: &T,
	) {
		let name = name.into();
		if let Ok(new_value) = serde_json::to_value(data) {
			if let Some(existing) = self.sections.get_mut(&name) &&
				let (Value::Object(existing_map), Value::Object(new_map)) =
					(existing, &new_value)
			{
				for (k, v) in new_map {
					existing_map.insert(k.clone(), v.clone());
				}
				debug!(section = %name, "meta section merged");
				return;
			}
			debug!(section = %name, "meta section added");
			self.sections.insert(name, new_value);
		}
	}

	/// Get a section by name
	pub fn get_section(&self, name: &str) -> Option<&Value> {
		self.sections.get(name)
	}

	/// Check if a section exists
	pub fn has_section(&self, name: &str) -> bool {
		self.sections.contains_key(name)
	}

	// -------------------------------------------------------------------------
	// Issues
	// -------------------------------------------------------------------------

	/// Add an issue. Automatically traces at WARN or ERROR level.
	pub fn add_issue(&mut self, issue: impl Into<String>) {
		let issue = issue.into();
		if issue.contains("[ERROR]") {
			tracing::error!(issue = %issue, "validation error");
		} else {
			warn!(issue = %issue, "validation issue");
		}
		self.issues.push(issue);
	}

	/// Add multiple issues
	pub fn add_issues(&mut self, issues: impl IntoIterator<Item = String>) {
		for issue in issues {
			self.add_issue(issue);
		}
	}

	/// Check if there are any issues
	pub fn has_issues(&self) -> bool {
		!self.issues.is_empty()
	}

	/// Get all issues
	pub fn issues(&self) -> &[String] {
		&self.issues
	}

	// -------------------------------------------------------------------------
	// Row Tracking
	// -------------------------------------------------------------------------

	/// Set input row count
	pub fn set_input_rows(&mut self, rows: usize) {
		debug!(input_rows = rows, "input rows recorded");
		self.input_rows = Some(rows);
	}

	/// Set output row count
	pub fn set_output_rows(&mut self, rows: usize) {
		debug!(output_rows = rows, "output rows recorded");
		self.output_rows = Some(rows);
	}

	/// Set both input and output rows
	pub fn set_rows(&mut self, input: usize, output: usize) {
		self.set_input_rows(input);
		self.set_output_rows(output);
	}

	/// Get input rows
	pub fn input_rows(&self) -> Option<usize> {
		self.input_rows
	}

	/// Get output rows
	pub fn output_rows(&self) -> Option<usize> {
		self.output_rows
	}

	// -------------------------------------------------------------------------
	// Timed Sections
	// -------------------------------------------------------------------------

	/// Start a timed section with a tracing span.
	///
	/// Returns a `TimedSection` that must be finished via `.finish()` or
	/// `.finish_with_data()`. The elapsed time is recorded both to tracing
	/// and to the metadata.
	pub fn timed_section(
		&mut self,
		name: impl Into<String>,
	) -> TimedSection<'_> {
		let name = name.into();
		let span = info_span!("meta_section", name = %name);
		span.in_scope(|| info!("section started"));

		TimedSection {
			collector: self,
			name,
			span,
			start: Instant::now(),
		}
	}

	// -------------------------------------------------------------------------
	// Timing
	// -------------------------------------------------------------------------

	/// Get elapsed time since collector creation
	pub fn elapsed_ms(&self) -> u64 {
		self.start_time.elapsed().as_millis() as u64
	}

	// -------------------------------------------------------------------------
	// Build
	// -------------------------------------------------------------------------

	/// Build the final CollectedMeta
	pub fn build(self) -> CollectedMeta {
		let elapsed = self.elapsed_ms();
		info!(
			elapsed_ms = elapsed,
			sections = self.sections.len(),
			issues = self.issues.len(),
			"MetaCollector finalized"
		);

		CollectedMeta {
			sections:           self.sections,
			issues:             self.issues,
			input_rows:         self.input_rows,
			output_rows:        self.output_rows,
			processing_time_ms: Some(elapsed),
		}
	}
}

impl Default for MetaCollector {
	fn default() -> Self {
		Self::new()
	}
}

// =============================================================================
// Convenience Functions for Option<&mut MetaCollector>
// =============================================================================

/// Record input rows if collector is present
pub fn record_input_rows(collector: Option<&mut MetaCollector>, rows: usize) {
	if let Some(c) = collector {
		c.set_input_rows(rows);
	}
}

/// Record output rows if collector is present
pub fn record_output_rows(collector: Option<&mut MetaCollector>, rows: usize) {
	if let Some(c) = collector {
		c.set_output_rows(rows);
	}
}

/// Record both input and output rows if collector is present
pub fn record_rows(
	collector: Option<&mut MetaCollector>,
	input: usize,
	output: usize,
) {
	if let Some(c) = collector {
		c.set_rows(input, output);
	}
}

/// Add an issue if collector is present
pub fn record_issue(
	collector: Option<&mut MetaCollector>,
	issue: impl Into<String>,
) {
	if let Some(c) = collector {
		c.add_issue(issue);
	}
}

/// Add multiple issues if collector is present
pub fn record_issues(
	collector: Option<&mut MetaCollector>,
	issues: impl IntoIterator<Item = String>,
) {
	if let Some(c) = collector {
		c.add_issues(issues);
	}
}

/// Add a section if collector is present
pub fn record_section<T: Serialize>(
	collector: Option<&mut MetaCollector>,
	name: impl Into<String>,
	data: &T,
) {
	if let Some(c) = collector {
		c.add_section(name, data);
	}
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
	use super::*;

	#[derive(Debug, Serialize, Deserialize, PartialEq)]
	struct TestStats {
		count: usize,
		label: String,
	}

	#[test]
	fn test_basic_collection() {
		let mut collector = MetaCollector::new();
		collector.add_section(
			"stats",
			&TestStats {
				count: 42,
				label: "test".into(),
			},
		);
		collector.set_rows(1000, 500);

		let meta = collector.build();

		assert_eq!(meta.input_rows, Some(1000));
		assert_eq!(meta.output_rows, Some(500));
		assert!(meta.processing_time_ms.is_some());

		let stats: TestStats = meta.get_section("stats").unwrap();
		assert_eq!(stats.count, 42);
	}

	#[test]
	fn test_issues() {
		let mut collector = MetaCollector::new();
		collector.add_issue("[WARN] Something might be wrong");
		collector.add_issue("[ERROR] Something is definitely wrong");
		collector.add_issue("[WARN] Another warning");

		assert!(collector.has_issues());
		assert_eq!(collector.issues().len(), 3);

		let meta = collector.build();
		assert_eq!(meta.warning_count(), 2);
		assert_eq!(meta.error_count(), 1);
	}

	#[test]
	fn test_merge_section() {
		let mut collector = MetaCollector::new();

		#[derive(Serialize)]
		struct Part1 {
			a: i32,
		}

		#[derive(Serialize)]
		struct Part2 {
			b: i32,
		}

		collector.add_section(
			"combined",
			&Part1 {
				a: 1,
			},
		);
		collector.merge_section(
			"combined",
			&Part2 {
				b: 2,
			},
		);

		let meta = collector.build();
		let section = meta.sections.get("combined").unwrap();

		assert_eq!(section.get("a").unwrap().as_i64(), Some(1));
		assert_eq!(section.get("b").unwrap().as_i64(), Some(2));
	}

	#[test]
	fn test_timed_section() {
		let mut collector = MetaCollector::new();

		{
			let section = collector.timed_section("work");
			std::thread::sleep(std::time::Duration::from_millis(10));
			section.finish();
		}

		let meta = collector.build();
		let timing: TimingMeta = meta.get_section("work").unwrap();
		assert!(timing.elapsed_ms >= 10);
	}

	#[test]
	fn test_timed_section_with_data() {
		let mut collector = MetaCollector::new();

		{
			let section = collector.timed_section("work");
			section.finish_with_data(&TestStats {
				count: 99,
				label: "done".into(),
			});
		}

		let meta = collector.build();
		let timed: TimedSectionMeta = meta.get_section("work").unwrap();

		let stats: TestStats = serde_json::from_value(timed.data).unwrap();
		assert_eq!(stats.count, 99);
	}

	#[test]
	fn test_convenience_functions_with_some() {
		let mut collector = MetaCollector::new();

		record_input_rows(Some(&mut collector), 1000);
		record_output_rows(Some(&mut collector), 500);
		record_issue(Some(&mut collector), "[WARN] test");
		record_section(Some(&mut collector), "test", &42);

		assert_eq!(collector.input_rows(), Some(1000));
		assert_eq!(collector.output_rows(), Some(500));
		assert!(collector.has_issues());
		assert!(collector.has_section("test"));
	}

	#[test]
	fn test_convenience_functions_with_none() {
		// Should not panic
		record_input_rows(None, 1000);
		record_output_rows(None, 500);
		record_issue(None, "[WARN] test");
		record_section(None, "test", &42);
	}

	#[test]
	fn test_elapsed_time() {
		let collector = MetaCollector::new();
		std::thread::sleep(std::time::Duration::from_millis(5));
		assert!(collector.elapsed_ms() >= 5);
	}
}
