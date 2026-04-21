// synapse-etl-unit/src/column.rs

use std::{fmt, hash::Hash, ops::Deref, sync::Arc};

use polars::prelude::{Expr, PlSmallStr, Selector, col};
use serde::{Deserialize, Serialize};

// ============================================================================
// Column Name Newtypes
// ============================================================================

/// The actual column name as it appears in a source DataFrame.
///
/// This is distinct from [`CanonicalColumnName`] which represents the
/// standardized name used for cross-source matching and output.
///
/// # Example
/// ```ignore
/// // A source might have column "pump_station_id"
/// let source_col = SourceColumnName::new("pump_station_id");
///
/// // Which maps to canonical name "station"
/// let canonical = CanonicalColumnName::new("station");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SourceColumnName(String);

impl SourceColumnName {
	pub fn new(name: impl Into<String>) -> Self {
		Self(name.into())
	}

	pub fn as_str(&self) -> &str {
		&self.0
	}

	pub fn into_inner(self) -> String {
		self.0
	}
}

impl Deref for SourceColumnName {
	type Target = str;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl AsRef<str> for SourceColumnName {
	fn as_ref(&self) -> &str {
		&self.0
	}
}

impl fmt::Display for SourceColumnName {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Source: {}", self.0)
	}
}

impl From<&str> for SourceColumnName {
	fn from(s: &str) -> Self {
		Self(s.to_string())
	}
}

impl From<String> for SourceColumnName {
	fn from(s: String) -> Self {
		Self(s)
	}
}

impl From<PlSmallStr> for SourceColumnName {
	fn from(s: PlSmallStr) -> Self {
		Self(s.to_string())
	}
}

// Allow col(&name) to work by converting &CanonicalColumnName -> PlSmallStr
// impl From<&SourceColumnName> for PlSmallStr {
// 	fn from(c: &SourceColumnName) -> Self {
// 		PlSmallStr::from(c.as_str())
// }
// }

// Allow col(&name) to work by converting &CanonicalColumnName -> PlSmallStr
impl From<SourceColumnName> for PlSmallStr {
	fn from(c: SourceColumnName) -> Self {
		PlSmallStr::from(c.as_str())
	}
}

// Great for: df.select(my_col.into()) using the selectors API
impl From<SourceColumnName> for Selector {
	fn from(c: SourceColumnName) -> Self {
		Selector::ByName {
			names: Arc::from(vec![PlSmallStr::from_string(c.0)]),
			strict: false,
		}
	}
}

impl From<&SourceColumnName> for Selector {
	fn from(c: &SourceColumnName) -> Self {
		Selector::ByName {
			names: Arc::from(vec![PlSmallStr::from(c.as_str())]),
			strict: false,
		}
	}
}

/// The canonical column name used for cross-source matching and output.
///
/// Canonical names provide a standardized interface regardless of how
/// different sources name their columns. For example, one source might
/// have "pump_station_id" while another has "station_name", but both
/// map to canonical name "station".
///
/// # Example
/// ```ignore
/// // Different sources use different column names
/// let source_a_col = SourceColumnName::new("pump_station_id");
/// let source_b_col = SourceColumnName::new("station_name");
///
/// // But they both map to the same canonical name
/// let canonical = CanonicalColumnName::new("station");
/// ```
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CanonicalColumnName(String);

impl CanonicalColumnName {
	pub fn new(name: impl Into<String>) -> Self {
		Self(name.into())
	}

	pub fn as_str(&self) -> &str {
		&self.0
	}

	pub fn into_inner(self) -> String {
		self.0
	}
}

impl Deref for CanonicalColumnName {
	type Target = str;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl AsRef<str> for CanonicalColumnName {
	fn as_ref(&self) -> &str {
		&self.0
	}
}

impl fmt::Display for CanonicalColumnName {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Canonical: {}", self.0)
	}
}

impl From<&str> for CanonicalColumnName {
	fn from(s: &str) -> Self {
		Self(s.to_string())
	}
}

impl From<String> for CanonicalColumnName {
	fn from(s: String) -> Self {
		Self(s)
	}
}

impl From<CanonicalColumnName> for String {
	fn from(c: CanonicalColumnName) -> Self {
		c.0
	}
}

impl From<PlSmallStr> for CanonicalColumnName {
	fn from(s: PlSmallStr) -> Self {
		Self(s.to_string())
	}
}

// Allow col(&name) to work by converting &CanonicalColumnName -> PlSmallStr
impl From<&CanonicalColumnName> for PlSmallStr {
	fn from(c: &CanonicalColumnName) -> Self {
		PlSmallStr::from(c.as_str())
	}
}

// Allow col(&name) to work by converting &CanonicalColumnName -> PlSmallStr
impl From<CanonicalColumnName> for PlSmallStr {
	fn from(c: CanonicalColumnName) -> Self {
		PlSmallStr::from(c.as_str())
	}
}

// Great for: df.select(my_col.into()) using the selectors API
impl From<CanonicalColumnName> for Selector {
	fn from(c: CanonicalColumnName) -> Self {
		Selector::ByName {
			names: Arc::from(vec![PlSmallStr::from_string(c.0)]),
			strict: false,
		}
	}
}

impl From<&CanonicalColumnName> for Selector {
	fn from(c: &CanonicalColumnName) -> Self {
		Selector::ByName {
			names: Arc::from(vec![PlSmallStr::from(c.as_str())]),
			strict: false,
		}
	}
}

// Great for: df.filter(col.into().eq(lit(1)))
impl From<CanonicalColumnName> for Expr {
	fn from(c: CanonicalColumnName) -> Self {
		col(&c.0)
	}
}

// Great for: df.select(cols.into())
impl From<CanonicalColumnName> for Vec<String> {
	fn from(c: CanonicalColumnName) -> Self {
		vec![c.0]
	}
}

// Great for: df.unique(col.into(), ...)
impl From<CanonicalColumnName> for Option<Vec<String>> {
	fn from(c: CanonicalColumnName) -> Self {
		Some(vec![c.0])
	}
}

// ============================================================================
// Domain Signature (for determining stack vs join)
// ============================================================================

/// The domain signature of an EtlUnit, used to determine composition strategy.
/// Units with matching signatures can be stacked (unioned).
/// Units with different signatures are joined on Subject.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct DomainSignature {
	/// Canonical subject column name
	pub subject: CanonicalColumnName,
	/// Canonical time column name (None for Quality units)
	pub time: Option<CanonicalColumnName>,
	/// Canonical component column names (sorted for comparison)
	pub components: Vec<CanonicalColumnName>,
}

impl DomainSignature {
	pub fn quality(subject: impl Into<String>) -> Self {
		Self {
			subject: CanonicalColumnName::new(subject),
			time: None,
			components: vec![],
		}
	}

	pub fn measurement(subject: impl Into<String>, time: impl Into<String>) -> Self {
		Self {
			subject: CanonicalColumnName::new(subject),
			time: Some(CanonicalColumnName::new(time)),
			components: vec![],
		}
	}

	pub fn with_components(mut self, mut components: Vec<String>) -> Self {
		components.sort();
		self.components = components
			.into_iter()
			.map(CanonicalColumnName::new)
			.collect();
		self
	}

	/// Check if two signatures can be stacked (must match exactly)
	pub fn can_stack_with(&self, other: &DomainSignature) -> bool {
		self == other
	}

	/// Check if two signatures can be joined (must share subject)
	pub fn can_join_with(&self, other: &DomainSignature) -> bool {
		self.subject == other.subject
	}

	/// Check if this signature has fewer components than another
	/// (would need to aggregate components when stacking)
	pub fn needs_component_reduction(&self, other: &DomainSignature) -> bool {
		self.subject == other.subject
			&& self.time == other.time
			&& self.components.len() < other.components.len()
	}
}

// ============================================================================
// Extension Trait for Explicit Column Name Construction
// ============================================================================

/// Extension trait for creating column names with explicit intent.
///
/// This makes it impossible to accidentally swap canonical and source columns:
/// ```rust ignore
/// # use synapse_etl_unit::column::ColumnNameExt;
/// // Clear and explicit - canonical on left, source on right
/// source.map("engine_status".canonical(), "status".source())
/// ```
pub trait ColumnNameExt {
	/// Create a canonical column name (the standardized schema name)
	fn canonical(self) -> CanonicalColumnName;

	/// Create a source column name (the actual DataFrame column name)
	fn source(self) -> SourceColumnName;
}

impl ColumnNameExt for &str {
	fn canonical(self) -> CanonicalColumnName {
		CanonicalColumnName::new(self)
	}

	fn source(self) -> SourceColumnName {
		SourceColumnName::new(self)
	}
}

impl ColumnNameExt for String {
	fn canonical(self) -> CanonicalColumnName {
		CanonicalColumnName::new(self)
	}

	fn source(self) -> SourceColumnName {
		SourceColumnName::new(self)
	}
}
// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_source_column_name() {
		let col = SourceColumnName::new("pump_station_id");
		assert_eq!(col.as_str(), "pump_station_id");
		assert_eq!(&*col, "pump_station_id"); // Deref
		assert_eq!(format!("{}", col), "Source: pump_station_id"); // Display
	}

	#[test]
	fn test_canonical_column_name() {
		let col = CanonicalColumnName::new("station");
		assert_eq!(col.as_str(), "station");
		assert_eq!(&*col, "station"); // Deref
		assert_eq!(format!("{}", col), "Canonical: station"); // Display
	}

	#[test]
	fn test_column_name_equality() {
		let a = SourceColumnName::new("col_a");
		let b = SourceColumnName::new("col_a");
		let c = SourceColumnName::new("col_c");

		assert_eq!(a, b);
		assert_ne!(a, c);
	}

	#[test]
	fn test_domain_signature_stack() {
		let sig1 =
			DomainSignature::measurement("station", "time").with_components(vec!["color".into()]);
		let sig2 =
			DomainSignature::measurement("station", "time").with_components(vec!["color".into()]);

		assert!(sig1.can_stack_with(&sig2));
	}

	#[test]
	fn test_domain_signature_join() {
		let sig1 = DomainSignature::measurement("station", "time");
		let sig2 = DomainSignature::measurement("station", "time")
			.with_components(vec!["sensor_type".into()]);

		assert!(!sig1.can_stack_with(&sig2)); // Different components
		assert!(sig1.can_join_with(&sig2)); // Same subject
		assert!(sig1.needs_component_reduction(&sig2));
	}

	#[test]
	fn test_domain_signature_quality_vs_measurement() {
		let quality_sig = DomainSignature::quality("station");
		let measurement_sig = DomainSignature::measurement("station", "time");

		assert!(!quality_sig.can_stack_with(&measurement_sig));
		assert!(quality_sig.can_join_with(&measurement_sig));
	}
}
