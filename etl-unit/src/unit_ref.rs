//! Typed references to ETL units.
//!
//! `EtlUnitRef` carries both the canonical name and the kind of unit it
//! refers to (Measurement, Quality, or Derivation). This eliminates
//! stringly-typed programming and enables compile-time discrimination
//! between unit kinds.

use serde::{Deserialize, Serialize};

use crate::column::CanonicalColumnName;

/// A typed reference to an EtlUnit — carries both name and kind.
///
/// The variant discriminates what kind of unit this refers to.
/// No separate `MeasurementRef` / `QualityRef` newtypes are needed —
/// the enum variant IS the type discrimination.
///
/// # Examples
///
/// ```rust
/// use synapse_etl_unit::EtlUnitRef;
///
/// let sump = EtlUnitRef::measurement("sump");
/// let parish = EtlUnitRef::quality("parish");
/// let runtime = EtlUnitRef::derivation("total_runtime");
///
/// assert!(sump.is_measurement());
/// assert!(parish.is_quality());
/// assert!(runtime.is_derivation());
/// assert_eq!(sump.name().as_str(), "sump");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "name", rename_all = "snake_case")]
pub enum EtlUnitRef {
	Measurement(CanonicalColumnName),
	Quality(CanonicalColumnName),
	Derivation(CanonicalColumnName),
}

impl EtlUnitRef {
	// =========================================================================
	// Ergonomic Constructors
	// =========================================================================

	/// Create a reference to a measurement unit.
	pub fn measurement(name: impl Into<CanonicalColumnName>) -> Self {
		Self::Measurement(name.into())
	}

	/// Create a reference to a quality unit.
	pub fn quality(name: impl Into<CanonicalColumnName>) -> Self {
		Self::Quality(name.into())
	}

	/// Create a reference to a derivation unit.
	pub fn derivation(name: impl Into<CanonicalColumnName>) -> Self {
		Self::Derivation(name.into())
	}

	// =========================================================================
	// Accessors
	// =========================================================================

	/// Get the canonical name regardless of kind.
	pub fn name(&self) -> &CanonicalColumnName {
		match self {
			Self::Measurement(n) | Self::Quality(n) | Self::Derivation(n) => n,
		}
	}

	/// Get the canonical name as a string slice.
	pub fn as_str(&self) -> &str {
		self.name().as_str()
	}

	// =========================================================================
	// Kind Checks
	// =========================================================================

	pub fn is_measurement(&self) -> bool {
		matches!(self, Self::Measurement(_))
	}

	pub fn is_quality(&self) -> bool {
		matches!(self, Self::Quality(_))
	}

	pub fn is_derivation(&self) -> bool {
		matches!(self, Self::Derivation(_))
	}
}

impl std::fmt::Display for EtlUnitRef {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Measurement(n) => write!(f, "Measurement({})", n.as_str()),
			Self::Quality(n) => write!(f, "Quality({})", n.as_str()),
			Self::Derivation(n) => write!(f, "Derivation({})", n.as_str()),
		}
	}
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_measurement_ref() {
		let r = EtlUnitRef::measurement("sump");
		assert!(r.is_measurement());
		assert!(!r.is_quality());
		assert!(!r.is_derivation());
		assert_eq!(r.as_str(), "sump");
		assert_eq!(format!("{}", r), "Measurement(sump)");
	}

	#[test]
	fn test_quality_ref() {
		let r = EtlUnitRef::quality("parish");
		assert!(r.is_quality());
		assert_eq!(r.as_str(), "parish");
	}

	#[test]
	fn test_derivation_ref() {
		let r = EtlUnitRef::derivation("total_runtime");
		assert!(r.is_derivation());
		assert_eq!(r.as_str(), "total_runtime");
	}

	#[test]
	fn test_equality() {
		let a = EtlUnitRef::measurement("sump");
		let b = EtlUnitRef::measurement("sump");
		let c = EtlUnitRef::quality("sump"); // same name, different kind

		assert_eq!(a, b);
		assert_ne!(a, c);
	}

	#[test]
	fn test_serde_roundtrip() {
		let refs = vec![
			EtlUnitRef::measurement("sump"),
			EtlUnitRef::quality("parish"),
			EtlUnitRef::derivation("engine_any"),
		];

		let json = serde_json::to_string(&refs).unwrap();
		let deserialized: Vec<EtlUnitRef> = serde_json::from_str(&json).unwrap();
		assert_eq!(refs, deserialized);
	}
}
