//! Physical↔canonical column binding types used by the plan layer.
//!
//! Two shapes:
//!
//! - [`ColumnBinding`] — a key column (subject, time, component). Plain
//!   physical/canonical pair. Key columns are never null-filled, so this
//!   carries no null metadata.
//!
//! - [`CodomainBinding`] — a value column for a measurement or quality
//!   (a "codomain" of an etl-unit). Carries the two null-fill semantics
//!   (`null_value` for source-level fills and `null_value_extension` for
//!   join-induced fills) because only codomains have them.
//!
//! These are *resolved* bindings — the physical and canonical names are
//! both known and stored. Contrast with [`crate::BindingRule`], which is
//! the *recipe* for deriving a canonical column from a `BoundSource`
//! (either `Direct(SourceColumnName)` or `Computed(ColumnExpr)`). The
//! plan layer holds resolved bindings; the source layer holds rules.

use serde::{Deserialize, Serialize};

use crate::column::{CanonicalColumnName, SourceColumnName};
use crate::unit::NullValue;

/// A key column binding — subject, time, or a component dimension.
///
/// Key columns are never null-filled, so this type carries no null
/// metadata. Pairs the physical column name in the source DataFrame with
/// the canonical column name in the schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnBinding {
	pub physical:  SourceColumnName,
	pub canonical: CanonicalColumnName,
}

impl ColumnBinding {
	/// Construct a binding from a physical and canonical column name.
	pub fn new(
		physical: impl Into<SourceColumnName>,
		canonical: impl Into<CanonicalColumnName>,
	) -> Self {
		Self {
			physical:  physical.into(),
			canonical: canonical.into(),
		}
	}

	/// Construct an identity binding where the physical and canonical
	/// names are the same string.
	pub fn identity(name: impl Into<String>) -> Self {
		let s = name.into();
		Self {
			physical:  SourceColumnName::new(s.clone()),
			canonical: CanonicalColumnName::new(s),
		}
	}
}

/// A codomain column binding — the value column of a measurement or quality.
///
/// Carries the two null-fill semantics declared on the measurement/quality
/// configuration:
///
/// - [`source_null_fill`](Self::source_null_fill) corresponds to the
///   `null_value` config field. Applied during the **Filter** stage to
///   replace nulls in the raw source before any processing.
///
/// - [`join_null_fill`](Self::join_null_fill) corresponds to the
///   `null_value_extension` config field. Applied during the **Join**
///   stage to fill nulls created when a cross-source LEFT JOIN finds no
///   matching row on the right side.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CodomainBinding {
	pub physical:  SourceColumnName,
	pub canonical: CanonicalColumnName,
	/// `null_value` — fills nulls in the raw source before processing.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub source_null_fill: Option<NullValue>,
	/// `null_value_extension` — fills nulls produced by a cross-source join.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub join_null_fill: Option<NullValue>,
}

impl CodomainBinding {
	/// Construct a codomain binding with no null fills configured.
	pub fn new(
		physical: impl Into<SourceColumnName>,
		canonical: impl Into<CanonicalColumnName>,
	) -> Self {
		Self {
			physical:         physical.into(),
			canonical:        canonical.into(),
			source_null_fill: None,
			join_null_fill:   None,
		}
	}

	/// Attach a source-level null fill (`null_value`).
	pub fn with_source_null_fill(mut self, fill: NullValue) -> Self {
		self.source_null_fill = Some(fill);
		self
	}

	/// Attach a join-level null fill (`null_value_extension`).
	pub fn with_join_null_fill(mut self, fill: NullValue) -> Self {
		self.join_null_fill = Some(fill);
		self
	}

	/// Drop the null metadata and project to a plain key-style binding.
	/// Useful when a code path only needs the physical/canonical pair.
	pub fn as_column_binding(&self) -> ColumnBinding {
		ColumnBinding {
			physical:  self.physical.clone(),
			canonical: self.canonical.clone(),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn column_binding_identity() {
		let b = ColumnBinding::identity("station_name");
		assert_eq!(b.physical.as_str(), "station_name");
		assert_eq!(b.canonical.as_str(), "station_name");
	}

	#[test]
	fn column_binding_distinct_names() {
		let b = ColumnBinding::new("obs_time", "timestamp");
		assert_eq!(b.physical.as_str(), "obs_time");
		assert_eq!(b.canonical.as_str(), "timestamp");
	}

	#[test]
	fn codomain_binding_default_no_fills() {
		let b = CodomainBinding::new("value_mm", "historical_precip");
		assert!(b.source_null_fill.is_none());
		assert!(b.join_null_fill.is_none());
	}

	#[test]
	fn codomain_binding_with_fills() {
		let b = CodomainBinding::new("engine_count", "engines_on_count")
			.with_source_null_fill(NullValue::Integer(0))
			.with_join_null_fill(NullValue::Integer(0));
		assert!(b.source_null_fill.is_some());
		assert!(b.join_null_fill.is_some());
	}

	#[test]
	fn codomain_projects_to_column_binding() {
		let cd = CodomainBinding::new("v", "value")
			.with_source_null_fill(NullValue::Float(0.0));
		let cb = cd.as_column_binding();
		assert_eq!(cb.physical.as_str(), "v");
		assert_eq!(cb.canonical.as_str(), "value");
	}

	#[test]
	fn serde_roundtrip_column_binding() {
		let b = ColumnBinding::new("a", "b");
		let json = serde_json::to_string(&b).unwrap();
		let back: ColumnBinding = serde_json::from_str(&json).unwrap();
		assert_eq!(b, back);
	}

	#[test]
	fn serde_skips_none_null_fills() {
		let b = CodomainBinding::new("v", "value");
		let json = serde_json::to_string(&b).unwrap();
		// Should not contain the null-fill keys when None.
		assert!(!json.contains("source_null_fill"));
		assert!(!json.contains("join_null_fill"));
	}
}
