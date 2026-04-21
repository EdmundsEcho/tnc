//! Fragment data references for the ETL pipeline.
//!
//! # Type Hierarchy
//!
//! Data flows through these types as it is refined from raw source data
//! into measurements in the Universe:
//!
//! ```text
//! BoundSource (Arc<DataFrame>)
//!   The raw data from a source chain, with column mappings from
//!   physical names to canonical names. The Arc allows multiple
//!   measurements to share a reference to the same data.
//!
//!   → MeasurementFragment (FragmentRef)
//!       One source's contribution to one measurement. Holds a
//!       FragmentRef — typically a ColumnRef pointing back into the
//!       BoundSource's Arc<DataFrame>. No data is copied at this stage.
//!
//!     → ComposedMeasurement (FragmentRef or materialized)
//!         All sources' fragments for one measurement, composed.
//!         Single-source measurements pass through as ColumnRef.
//!         Multi-source measurements stack or materialize.
//!
//!       → MeasurementData (materializes on demand)
//!           The final measurement in the Universe. Materializes
//!           the FragmentRef into an owned DataFrame only when the
//!           subset path needs to read the data (signal policy,
//!           crush, resample, join).
//! ```
//!
//! # FragmentRef variants
//!
//! - **ColumnRef** — a column reference into a shared source DataFrame.
//!   Used for direct measurements (sump, discharge) from a single source.
//!   The `Arc<DataFrame>` is shared across all measurements extracted from
//!   the same source. No data is copied.
//!
//! - **Stacked** — multiple fragments from different sources, each holding
//!   a reference to its source DataFrame. Used when the same measurement
//!   comes from multiple sources with the same shape. Not materialized
//!   until the subset path needs a contiguous DataFrame.
//!
//! - **Materialized** — an owned DataFrame. Used when the shape changed
//!   from the source (unpivot, component crush, truth mapping) or when
//!   explicit concatenation was required.

use std::sync::Arc;

use memuse::DynamicUsage;
use polars::prelude::*;
use serde::Serialize;

use crate::column::{CanonicalColumnName, SourceColumnName};

// ============================================================================
// Deferred Transform
// ============================================================================

/// A deferred, shape-preserving transformation applied at materialization.
///
/// Wraps a closure that transforms a LazyFrame. Used for isomorphic
/// operations like null-value substitution and binary truth mapping
/// that don't change the row count or column structure.
///
/// Cloneable via Arc. Debug prints a label, not the closure body.
#[derive(Clone)]
pub struct DeferredTransform {
	label: String,
	apply: Arc<dyn Fn(LazyFrame) -> LazyFrame + Send + Sync>,
}

impl DeferredTransform {
	/// Create a new deferred transform with a descriptive label.
	pub fn new(
		label: impl Into<String>,
		apply: impl Fn(LazyFrame) -> LazyFrame + Send + Sync + 'static,
	) -> Self {
		Self {
			label: label.into(),
			apply: Arc::new(apply),
		}
	}

	/// Apply the transform to a LazyFrame.
	pub fn apply(&self, lf: LazyFrame) -> LazyFrame {
		(self.apply)(lf)
	}

	/// The label describing what this transform does.
	pub fn label(&self) -> &str {
		&self.label
	}
}

impl std::fmt::Debug for DeferredTransform {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "DeferredTransform({})", self.label)
	}
}

/// Identifier for a data source (e.g., "scada", "mrms").
/// Defined here in etl-unit so FragmentRef can use it
/// without depending on the data-pipeline crate.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct DataSourceName(String);

impl DataSourceName {
	pub fn new(name: impl Into<String>) -> Self { Self(name.into()) }
	pub fn as_str(&self) -> &str { &self.0 }
}

impl std::fmt::Display for DataSourceName {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl From<&str> for DataSourceName {
	fn from(s: &str) -> Self { Self(s.to_string()) }
}

impl From<String> for DataSourceName {
	fn from(s: String) -> Self { Self(s) }
}

/// How a measurement fragment references its data.
///
/// A `FragmentRef` is how a `MeasurementFragment` holds its data —
/// either as a cheap pointer into a shared source DataFrame, or as an
/// owned DataFrame when the data has been transformed.
///
/// See the [module-level docs](self) for where this fits in the type hierarchy.
#[derive(Debug, Clone)]
pub enum FragmentRef {
	/// Column reference into a shared source DataFrame.
	///
	/// The source DataFrame contains all columns from the source
	/// (subject, time, multiple measurements). This variant records
	/// which value column to extract and how to rename columns from
	/// source names to canonical names. No data is copied.
	ColumnRef(ColumnRefData),

	/// Multiple same-shape fragments from different sources.
	///
	/// Each fragment references its source DataFrame. Materialized
	/// only when `as_dataframe()` is called (e.g., during subset).
	Stacked(Vec<StackedFragment>),

	/// Owned DataFrame — shape differs from source.
	///
	/// Used for: unpivoted measurements, component-crushed measurements,
	/// truth-mapped binaries, or any case where the data couldn't be
	/// expressed as a column reference into the original source.
	Materialized(DataFrame),
}

/// A column reference into a shared source DataFrame.
#[derive(Debug, Clone)]
pub struct ColumnRefData {
	/// The shared source DataFrame (e.g., one BoundSource's data).
	pub source: Arc<DataFrame>,
	/// Source column name for this measurement's values.
	pub value_column: SourceColumnName,
	/// Canonical name for the measurement (used as alias in output).
	pub canonical_name: CanonicalColumnName,
	/// Which source this came from.
	pub source_name: DataSourceName,
	/// Column mappings for subject, time, and components.
	/// Each entry is (source_name_in_df, canonical_name_in_output).
	pub column_mappings: Vec<(SourceColumnName, CanonicalColumnName)>,
	/// Optional deferred transform applied at materialization.
	/// Shape-preserving: null fill, truth mapping, column arithmetic, etc.
	pub transform: Option<DeferredTransform>,
}

/// One fragment in a stacked measurement — references a source DataFrame.
#[derive(Debug, Clone)]
pub struct StackedFragment {
	/// The shared source DataFrame.
	pub source: Arc<DataFrame>,
	/// Source column name for this measurement's values.
	pub value_column: SourceColumnName,
	/// Canonical name for the measurement.
	pub canonical_name: CanonicalColumnName,
	/// Which source contributed this fragment.
	pub source_name: DataSourceName,
	/// Column mappings (same as ColumnRefData).
	pub column_mappings: Vec<(SourceColumnName, CanonicalColumnName)>,
	/// Optional deferred transform (same as ColumnRefData).
	pub transform: Option<DeferredTransform>,
}

impl FragmentRef {
	/// Resolve to a DataFrame.
	///
	/// - `ColumnRef`: selects, renames, and applies deferred transforms.
	/// - `Stacked`: selects + transforms + vertical stacks all fragments.
	/// - `Materialized`: returns a clone.
	pub fn as_dataframe(&self) -> Result<DataFrame, PolarsError> {
		self.as_lazy()?.collect()
	}

	/// Resolve to a LazyFrame for predicate pushdown.
	///
	/// Allows filters (time range, subject) to be pushed into the
	/// column selection plan alongside deferred transforms, so Polars
	/// can optimize them together.
	pub fn as_lazy(&self) -> Result<LazyFrame, PolarsError> {
		match self {
			FragmentRef::ColumnRef(ref_data) => {
				let exprs = build_select_exprs(
					&ref_data.column_mappings,
					&ref_data.value_column,
					&ref_data.canonical_name,
				);
				let mut lf = (*ref_data.source).clone().lazy().select(exprs);
				if let Some(ref transform) = ref_data.transform {
					lf = transform.apply(lf);
				}
				Ok(lf)
			}
			FragmentRef::Stacked(fragments) => {
				if fragments.is_empty() {
					return Ok(DataFrame::empty().lazy());
				}
				if fragments.len() == 1 {
					let f = &fragments[0];
					let exprs = build_select_exprs(
						&f.column_mappings,
						&f.value_column,
						&f.canonical_name,
					);
					let mut lf = (*f.source).clone().lazy().select(exprs);
					if let Some(ref transform) = f.transform {
						lf = transform.apply(lf);
					}
					return Ok(lf);
				}

				let mut lazy_frames = Vec::with_capacity(fragments.len());
				for frag in fragments {
					let exprs = build_select_exprs(
						&frag.column_mappings,
						&frag.value_column,
						&frag.canonical_name,
					);
					let mut lf = (*frag.source).clone().lazy().select(exprs);
					if let Some(ref transform) = frag.transform {
						lf = transform.apply(lf);
					}
					lazy_frames.push(lf);
				}

				concat(lazy_frames, UnionArgs::default())
			}
			FragmentRef::Materialized(df) => Ok(df.clone().lazy()),
		}
	}

	/// Row count.
	/// For ColumnRef, this is the source DataFrame's height (may include
	/// rows for other measurements). For Stacked, sum of all fragments.
	pub fn height(&self) -> usize {
		match self {
			FragmentRef::ColumnRef(r) => r.source.height(),
			FragmentRef::Stacked(frags) => frags.iter().map(|f| f.source.height()).sum(),
			FragmentRef::Materialized(df) => df.height(),
		}
	}

	/// Whether this is already materialized.
	pub fn is_materialized(&self) -> bool {
		matches!(self, FragmentRef::Materialized(_))
	}

	/// Estimated memory usage in bytes.
	///
	/// - `ColumnRef`: 0 — data lives in the shared source, not owned here.
	///   The source size is reported separately via `shared_source_bytes()`.
	/// - `Stacked`: 0 — same as ColumnRef, references only.
	/// - `Materialized`: the DataFrame's estimated heap size.
	pub fn owned_bytes(&self) -> usize {
		match self {
			FragmentRef::ColumnRef(_) => 0,
			FragmentRef::Stacked(_) => 0,
			FragmentRef::Materialized(df) => df.estimated_size(),
		}
	}

	/// Size of the shared source DataFrame(s) this fragment references.
	///
	/// For ColumnRef/Stacked, this is the source DataFrame size (shared
	/// with other measurements). For Materialized, returns 0 since there's
	/// no shared source.
	pub fn shared_source_bytes(&self) -> usize {
		match self {
			FragmentRef::ColumnRef(r) => r.source.estimated_size(),
			FragmentRef::Stacked(frags) => frags.iter()
				.map(|f| f.source.estimated_size())
				.sum(),
			FragmentRef::Materialized(_) => 0,
		}
	}

	/// Raw pointers to the underlying Arc<DataFrame>(s) for deduplication.
	///
	/// Two ColumnRefs from the same BoundSource return the same pointer.
	/// Used by `memory_summary()` to avoid double-counting shared memory.
	pub fn source_arc_ptrs(&self) -> Vec<usize> {
		match self {
			FragmentRef::ColumnRef(r) => vec![Arc::as_ptr(&r.source) as usize],
			FragmentRef::Stacked(frags) => frags.iter()
				.map(|f| Arc::as_ptr(&f.source) as usize)
				.collect(),
			FragmentRef::Materialized(_) => vec![],
		}
	}

	/// Shared source bytes for a specific Arc pointer.
	pub fn shared_source_bytes_for_ptr(&self, ptr: usize) -> usize {
		match self {
			FragmentRef::ColumnRef(r) => {
				if Arc::as_ptr(&r.source) as usize == ptr {
					r.source.estimated_size()
				} else { 0 }
			}
			FragmentRef::Stacked(frags) => {
				frags.iter()
					.find(|f| Arc::as_ptr(&f.source) as usize == ptr)
					.map(|f| f.source.estimated_size())
					.unwrap_or(0)
			}
			FragmentRef::Materialized(_) => 0,
		}
	}

	/// Get the raw source DataFrame without column selection or renaming.
	/// For ColumnRef: returns the full shared DataFrame (all columns from the source).
	/// For Materialized: returns the owned DataFrame (already has only its columns).
	/// For Stacked: returns None (multiple sources).
	pub fn raw_source_dataframe(&self) -> Option<&DataFrame> {
		match self {
			FragmentRef::ColumnRef(r) => Some(&*r.source),
			FragmentRef::Materialized(df) => Some(df),
			FragmentRef::Stacked(_) => None,
		}
	}

	/// The logical source name for this fragment, if any.
	///
	/// - `ColumnRef`: the source the column points into.
	/// - `Stacked`: the *first* fragment's source name. (Stacked fragments
	///   may span multiple sources; the plan layer treats the first one as
	///   the primary identity, consistent with `source_arc_ptrs().first()`.)
	/// - `Materialized`: `None` — no upstream source identity preserved.
	pub fn source_name(&self) -> Option<&DataSourceName> {
		match self {
			FragmentRef::ColumnRef(r) => Some(&r.source_name),
			FragmentRef::Stacked(frags) => frags.first().map(|f| &f.source_name),
			FragmentRef::Materialized(_) => None,
		}
	}

	/// Get the physical (source) column name for a canonical column name.
	/// Checks both the column_mappings (subject, time, components) and the
	/// value_column/canonical_name pair.
	/// Returns None if not a ColumnRef or the canonical name isn't mapped.
	/// For Materialized: physical = canonical (already renamed).
	pub fn physical_column_name(&self, canonical: &str) -> Option<String> {
		match self {
			FragmentRef::ColumnRef(r) => {
				// Check value column
				if r.canonical_name.as_str() == canonical {
					return Some(r.value_column.as_str().to_string());
				}
				// Check column mappings (subject, time, components)
				r.column_mappings.iter()
					.find(|(_, canon)| canon.as_str() == canonical)
					.map(|(src, _)| src.as_str().to_string())
			}
			_ => Some(canonical.to_string()),
		}
	}

	/// Whether this fragment has a deferred transform.
	pub fn has_transform(&self) -> bool {
		match self {
			FragmentRef::ColumnRef(r) => r.transform.is_some(),
			FragmentRef::Stacked(frags) => frags.iter().any(|f| f.transform.is_some()),
			FragmentRef::Materialized(_) => false,
		}
	}

	/// Diagnostic description.
	pub fn storage_description(&self) -> StorageDescription {
		match self {
			FragmentRef::ColumnRef(r) => StorageDescription {
				kind: "column_ref".to_string(),
				source_count: 1,
				sources: vec![r.source_name.to_string()],
				rows: r.source.height(),
			},
			FragmentRef::Stacked(frags) => StorageDescription {
				kind: "stacked".to_string(),
				source_count: frags.len(),
				sources: frags.iter().map(|f| f.source_name.to_string()).collect(),
				rows: frags.iter().map(|f| f.source.height()).sum(),
			},
			FragmentRef::Materialized(df) => StorageDescription {
				kind: "materialized".to_string(),
				source_count: 1,
				sources: vec![],
				rows: df.height(),
			},
		}
	}
}

// ============================================================================
// Memory Measurement (memuse)
// ============================================================================

impl DynamicUsage for FragmentRef {
	fn dynamic_usage(&self) -> usize {
		match self {
			// ColumnRef: the fragment itself owns no data — just an Arc pointer
			// and some small vecs for column mappings. The source DataFrame
			// is shared and counted separately.
			FragmentRef::ColumnRef(r) => {
				// Column mappings vec overhead
				r.column_mappings.len() * std::mem::size_of::<(SourceColumnName, CanonicalColumnName)>()
				+ r.value_column.as_str().len()
				+ r.canonical_name.as_str().len()
				+ r.source_name.as_str().len()
			}
			FragmentRef::Stacked(frags) => {
				frags.iter().map(|f| {
					f.column_mappings.len() * std::mem::size_of::<(SourceColumnName, CanonicalColumnName)>()
					+ f.value_column.as_str().len()
					+ f.canonical_name.as_str().len()
					+ f.source_name.as_str().len()
				}).sum()
			}
			// Materialized: owns the DataFrame data on the heap.
			FragmentRef::Materialized(df) => df.estimated_size(),
		}
	}

	fn dynamic_usage_bounds(&self) -> (usize, Option<usize>) {
		let usage = self.dynamic_usage();
		(usage, Some(usage))
	}
}

impl DynamicUsage for ColumnRefData {
	fn dynamic_usage(&self) -> usize {
		// Only counts the metadata — the Arc<DataFrame> is shared
		self.column_mappings.len() * std::mem::size_of::<(SourceColumnName, CanonicalColumnName)>()
		+ self.value_column.as_str().len()
		+ self.canonical_name.as_str().len()
		+ self.source_name.as_str().len()
	}

	fn dynamic_usage_bounds(&self) -> (usize, Option<usize>) {
		let usage = self.dynamic_usage();
		(usage, Some(usage))
	}
}

/// Diagnostic info about how a fragment's data is stored.
#[derive(Debug, Clone, Serialize)]
pub struct StorageDescription {
	pub kind: String,
	pub source_count: usize,
	pub sources: Vec<String>,
	pub rows: usize,
}

/// Build select expressions with source→canonical renaming.
fn build_select_exprs(
	column_mappings: &[(SourceColumnName, CanonicalColumnName)],
	value_source: &SourceColumnName,
	value_canonical: &CanonicalColumnName,
) -> Vec<Expr> {
	let mut exprs: Vec<Expr> = column_mappings.iter()
		.map(|(src, canon)| col(src.as_str()).alias(canon.as_str()))
		.collect();
	exprs.push(col(value_source.as_str()).alias(value_canonical.as_str()));
	exprs
}

#[cfg(test)]
mod tests {
	use super::*;

	fn make_source_df() -> DataFrame {
		DataFrame::new(vec![
			Column::new("station_id".into(), &["A", "A", "B", "B"]),
			Column::new("obs_time".into(), &[1i64, 2, 1, 2]),
			Column::new("sump_reading".into(), &[1.0f64, 2.0, 3.0, 4.0]),
			Column::new("discharge_reading".into(), &[10.0f64, 20.0, 30.0, 40.0]),
		]).unwrap()
	}

	fn standard_mappings() -> Vec<(SourceColumnName, CanonicalColumnName)> {
		vec![
			(SourceColumnName::new("station_id"), CanonicalColumnName::new("station_name")),
			(SourceColumnName::new("obs_time"), CanonicalColumnName::new("timestamp")),
		]
	}

	#[test]
	fn test_column_ref_selects_and_renames() {
		let source = Arc::new(make_source_df());
		let storage = FragmentRef::ColumnRef(ColumnRefData {
			source: source.clone(),
			value_column: SourceColumnName::new("sump_reading"),
			canonical_name: CanonicalColumnName::new("sump"),
			source_name: DataSourceName::new("scada"),
			column_mappings: standard_mappings(),
			transform: None,
		});

		let df = storage.as_dataframe().unwrap();
		assert_eq!(df.width(), 3); // station_name, timestamp, sump
		assert_eq!(df.height(), 4);
		assert!(df.column("sump").is_ok());
		assert!(df.column("station_name").is_ok());
		assert!(df.column("timestamp").is_ok());
		// Source names should not be in output
		assert!(df.column("station_id").is_err());
		assert!(df.column("sump_reading").is_err());
	}

	#[test]
	fn test_stacked_combines_fragments() {
		let source_a = Arc::new(DataFrame::new(vec![
			Column::new("station_id".into(), &["A", "A"]),
			Column::new("obs_time".into(), &[1i64, 2]),
			Column::new("sump_reading".into(), &[1.0f64, 2.0]),
		]).unwrap());

		let source_b = Arc::new(DataFrame::new(vec![
			Column::new("station_id".into(), &["B", "B"]),
			Column::new("obs_time".into(), &[1i64, 2]),
			Column::new("sump_reading".into(), &[3.0f64, 4.0]),
		]).unwrap());

		let mappings = standard_mappings();
		let storage = FragmentRef::Stacked(vec![
			StackedFragment {
				source: source_a,
				value_column: SourceColumnName::new("sump_reading"),
				canonical_name: CanonicalColumnName::new("sump"),
				source_name: DataSourceName::new("store"),
				column_mappings: mappings.clone(),
				transform: None,
			},
			StackedFragment {
				source: source_b,
				value_column: SourceColumnName::new("sump_reading"),
				canonical_name: CanonicalColumnName::new("sump"),
				source_name: DataSourceName::new("adhoc"),
				column_mappings: mappings,
				transform: None,
			},
		]);

		let df = storage.as_dataframe().unwrap();
		assert_eq!(df.height(), 4);
		assert_eq!(df.width(), 3);
		assert!(df.column("sump").is_ok());
		assert!(df.column("station_name").is_ok());
	}

	#[test]
	fn test_materialized_returns_as_is() {
		let df = DataFrame::new(vec![
			Column::new("station_name".into(), &["A"]),
			Column::new("timestamp".into(), &[1i64]),
			Column::new("engine".into(), &["1"]),
			Column::new("engines_on_count".into(), &[1.0f64]),
		]).unwrap();

		let storage = FragmentRef::Materialized(df.clone());
		let result = storage.as_dataframe().unwrap();
		assert_eq!(result.height(), 1);
		assert_eq!(result.width(), 4);
	}

	#[test]
	fn test_column_ref_shared_across_measurements() {
		let source = Arc::new(make_source_df());
		let mappings = standard_mappings();

		let sump_ref = FragmentRef::ColumnRef(ColumnRefData {
			source: source.clone(),
			value_column: SourceColumnName::new("sump_reading"),
			canonical_name: CanonicalColumnName::new("sump"),
			source_name: DataSourceName::new("scada"),
			column_mappings: mappings.clone(),
			transform: None,
		});

		let discharge_ref = FragmentRef::ColumnRef(ColumnRefData {
			source: source.clone(),
			value_column: SourceColumnName::new("discharge_reading"),
			canonical_name: CanonicalColumnName::new("discharge"),
			source_name: DataSourceName::new("scada"),
			column_mappings: mappings,
			transform: None,
		});

		// Both reference the same source — Arc refcount is 3
		assert_eq!(Arc::strong_count(&source), 3);

		let sump_df = sump_ref.as_dataframe().unwrap();
		let discharge_df = discharge_ref.as_dataframe().unwrap();

		assert!(sump_df.column("sump").is_ok());
		assert!(discharge_df.column("discharge").is_ok());
		// Each has its own measurement column
		assert!(sump_df.column("discharge").is_err());
		assert!(discharge_df.column("sump").is_err());
	}

	#[test]
	fn test_lazy_produces_same_result() {
		let source = Arc::new(make_source_df());
		let storage = FragmentRef::ColumnRef(ColumnRefData {
			source,
			value_column: SourceColumnName::new("sump_reading"),
			canonical_name: CanonicalColumnName::new("sump"),
			source_name: DataSourceName::new("scada"),
			column_mappings: standard_mappings(),
			transform: None,
		});

		let eager = storage.as_dataframe().unwrap();
		let lazy = storage.as_lazy().unwrap().collect().unwrap();

		assert_eq!(eager.height(), lazy.height());
		assert_eq!(eager.width(), lazy.width());
		assert_eq!(eager.get_column_names(), lazy.get_column_names());
	}
}
