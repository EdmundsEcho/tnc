//! Subject dimension types.
//!
//! A Universe has exactly one subject dimension — declared in the schema.
//! All measurements and qualities are keyed by it. `SubjectType` represents
//! the dimension itself; `SubjectValue` represents a specific entity within it.
//!
//! Isomorphic qualities provide alternative keys that resolve to the same
//! subject values (e.g., location(lat, lon) → station_name).

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::column::CanonicalColumnName;

/// The subject dimension of a Universe.
///
/// One per pipeline. All measurements, qualities, and subset requests
/// are keyed by this column.
///
/// ```rust,ignore
/// let subject = SubjectType::new("station_name");
/// assert_eq!(subject.column().as_str(), "station_name");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SubjectType {
	column: CanonicalColumnName,
}

impl SubjectType {
	pub fn new(column: impl Into<CanonicalColumnName>) -> Self {
		Self { column: column.into() }
	}

	pub fn column(&self) -> &CanonicalColumnName {
		&self.column
	}

	pub fn as_str(&self) -> &str {
		self.column.as_str()
	}
}

impl fmt::Display for SubjectType {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.column.as_str())
	}
}

impl From<&str> for SubjectType {
	fn from(s: &str) -> Self {
		Self::new(s)
	}
}

/// A concrete subject value (e.g., "Coastal", "Parr", "USGS-02489500").
///
/// Newtype over String to prevent mixing with column names, measurement
/// names, or other string identifiers.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SubjectValue(String);

impl SubjectValue {
	pub fn new(value: impl Into<String>) -> Self {
		Self(value.into())
	}

	pub fn as_str(&self) -> &str {
		&self.0
	}

	pub fn into_inner(self) -> String {
		self.0
	}
}

impl fmt::Display for SubjectValue {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl From<&str> for SubjectValue {
	fn from(s: &str) -> Self {
		Self(s.to_string())
	}
}

impl From<String> for SubjectValue {
	fn from(s: String) -> Self {
		Self(s)
	}
}

impl AsRef<str> for SubjectValue {
	fn as_ref(&self) -> &str {
		&self.0
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_subject_type() {
		let st = SubjectType::new("station_name");
		assert_eq!(st.as_str(), "station_name");
		assert_eq!(format!("{}", st), "station_name");
	}

	#[test]
	fn test_subject_value() {
		let sv = SubjectValue::new("Coastal");
		assert_eq!(sv.as_str(), "Coastal");
		assert_eq!(format!("{}", sv), "Coastal");

		let sv2: SubjectValue = "Parr".into();
		assert_ne!(sv, sv2);
	}

	#[test]
	fn test_subject_value_ordering() {
		let mut vals = vec![
			SubjectValue::new("Coastal"),
			SubjectValue::new("Alpha"),
			SubjectValue::new("Parr"),
		];
		vals.sort();
		assert_eq!(vals[0].as_str(), "Alpha");
		assert_eq!(vals[1].as_str(), "Coastal");
		assert_eq!(vals[2].as_str(), "Parr");
	}
}
