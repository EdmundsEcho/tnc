//! Derivation: Shape-preserving computations on EtlUnits.
//! This is a canonical-level derivation (derivations do not happen at the source-level).
//!
//! A [`Derivation`] defines how to construct a new `EtlUnit` (Measurement) by transforming
//! or combining existing `EtlUnit`s.
//!
//! # Concepts
//!
//! - **Reference by Name**: Derivations do not store data; they store instructions. They refer to
//!   their input dependencies by the unique `name` of the input `EtlUnit`.
//! - **Shape Preservation**: Unlike aggregations (which reduce rows), derivations maintain the
//!   exact same dimensionality (N subjects × M time points) as their inputs.
//!
//! # Three Axes of Computation
//!
//! 1. **Over Time (`TimeExpr`)**: Operations typically used for signal processing.
//!     * *Context:* A single subject's entire history.
//!     * *Examples:* `Derivative`, `RollingMean`, `CumSum`.
//!
//! 2. **Over Subjects (`OverSubjectExpr`)**: Operations for "Natural Transformations" or
//!    benchmarking.
//!     * *Context:* A single time point across all subjects.
//!     * *Examples:* `Rank`, `Quantile` (Deciles), `ZScore`.
//!
//! 3. **Pointwise (`PointwiseExpr`)**: Operations for combining multiple units.
//!     * *Context:* A single row (same subject, same time).
//!     * *Examples:* `AnyOn` (logical OR), `Sum` (stacking values), `Ratio`.

use polars::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{CanonicalColumnName, MeasurementKind, chart_hints::ChartHints};

/// A definition for a derived measurement.
///
/// This struct holds the "recipe" for creating a new column in the resulting DataFrame.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Derivation {
	/// The unique name of this new derived unit.
	/// This name can be referenced by *other* derivations (allowing chains).
	pub name: CanonicalColumnName,

	/// The computational logic to perform.
	pub computation: Computation,

	/// The semantic kind of the resulting measurement.
	pub kind: MeasurementKind,

	/// Visualization hints for the UI.
	pub chart_hints: Option<ChartHints>,
}

impl PartialEq for Derivation {
	fn eq(&self, other: &Self) -> bool {
		self.name == other.name && self.computation == other.computation && self.kind == other.kind
	}
}

impl Eq for Derivation {}
impl Derivation {
	// =========================================================================
	// Builders / Constructors
	// =========================================================================

	/// Create a derivation that operates along the time axis (per subject).
	///
	/// # Arguments
	/// * `name` - The name of the new unit being created.
	/// * `expr` - The time-based expression (e.g., `TimeExpr::derivative("fuel_level")`).
	pub fn over_time(name: impl Into<CanonicalColumnName>, expr: TimeExpr) -> Self {
		Self {
			name: name.into(),
			computation: Computation::OverTime(expr),
			kind: MeasurementKind::Measure, // Default, can be overridden
			chart_hints: None,
		}
	}

	/// Create a derivation that operates across subjects (per time point).
	///
	/// Useful for creating "Natural Transformations" like Deciles or Ranks.
	///
	/// # Arguments
	/// * `name` - The name of the new unit.
	/// * `expr` - The subject-based expression (e.g., `OverSubjectExpr::decile("sales")`).
	pub fn over_subjects(name: impl Into<CanonicalColumnName>, expr: OverSubjectExpr) -> Self {
		// Auto-detect Kind: Ranks/Buckets are usually Categorical/Ordinal
		let kind = match expr {
			OverSubjectExpr::Rank {
				..
			}
			| OverSubjectExpr::Quantile {
				..
			}
			| OverSubjectExpr::Bucket {
				..
			} => MeasurementKind::Categorical,
			_ => MeasurementKind::Measure,
		};

		Self {
			name: name.into(),
			computation: Computation::OverSubjects(expr),
			kind,
			chart_hints: None,
		}
	}

	/// Create a derivation that combines multiple units in the same row.
	///
	/// # Arguments
	/// * `name` - The name of the new unit.
	/// * `expr` - The pointwise expression (e.g., `PointwiseExpr::any_on(vec!["e1", "e2"])`).
	pub fn pointwise(name: impl Into<CanonicalColumnName>, expr: PointwiseExpr) -> Self {
		Self {
			name: name.into(),
			kind: expr.result_kind(), // Auto-detect based on op (Any -> Bool/Cat)
			computation: Computation::Pointwise(expr),
			chart_hints: None,
		}
	}

	// =========================================================================
	// Fluent Modifiers
	// =========================================================================

	/// Override the default MeasurementKind.
	pub fn with_kind(mut self, kind: MeasurementKind) -> Self {
		self.kind = kind;
		self
	}

	/// Attach chart hints for visualization.
	pub fn with_chart_hints(mut self, hints: ChartHints) -> Self {
		self.chart_hints = Some(hints);
		self
	}

	/// Set the name of the derivation (useful if renaming after construction).
	pub fn named(mut self, name: impl Into<CanonicalColumnName>) -> Self {
		self.name = name.into();
		self
	}

	// =========================================================================
	// Accessors
	// =========================================================================

	/// Get the effective chart hints (using defaults if none provided).
	pub fn effective_chart_hints(&self) -> ChartHints {
		self.chart_hints.clone().unwrap_or_else(|| match self.kind {
			MeasurementKind::Categorical => ChartHints::categorical(),
			_ => ChartHints::measure(),
		})
	}

	/// Get a list of **EtlUnit names** that this derivation depends on.
	///
	/// The Executor uses this to ensure dependencies are calculated first.
	pub fn input_columns(&self) -> Vec<&CanonicalColumnName> {
		match &self.computation {
			Computation::OverTime(expr) => expr.source_columns(),
			Computation::OverSubjects(expr) => expr.source_columns(),
			Computation::Pointwise(expr) => expr.source_columns(),
		}
	}
}

/// The specific strategy used for computation.
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Computation {
	OverTime(TimeExpr),
	OverSubjects(OverSubjectExpr),
	Pointwise(PointwiseExpr),
}

// =============================================================================
// 1. Time-Axis Expressions (Temporal)
// =============================================================================

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TimeExpr {
	/// Calculate rate of change per unit of time.
	Derivative {
		input: CanonicalColumnName,
		time_unit: TimeUnit,
	},
	/// Moving average for smoothing.
	RollingMean {
		input: CanonicalColumnName,
		window: usize,
	},
	/// Moving sum.
	RollingSum {
		input: CanonicalColumnName,
		window: usize,
	},
	/// Value from N periods ago.
	Lag {
		input: CanonicalColumnName,
		periods: usize,
	},
	/// Value from N periods ahead.
	Lead {
		input: CanonicalColumnName,
		periods: usize,
	},
	/// Running total.
	CumSum {
		input: CanonicalColumnName,
	},
	/// Simple difference: x(t) - x(t-1).
	Diff {
		input: CanonicalColumnName,
		periods: usize,
	},
}

impl TimeExpr {
	pub fn derivative(input: impl Into<CanonicalColumnName>) -> Self {
		Self::Derivative {
			input: input.into(),
			time_unit: TimeUnit::Second,
		}
	}

	pub fn rolling_mean(input: impl Into<CanonicalColumnName>, window: usize) -> Self {
		Self::RollingMean {
			input: input.into(),
			window,
		}
	}

	pub fn rolling_sum(input: impl Into<CanonicalColumnName>, window: usize) -> Self {
		Self::RollingSum {
			input: input.into(),
			window,
		}
	}

	pub fn lag(input: impl Into<CanonicalColumnName>, periods: usize) -> Self {
		Self::Lag {
			input: input.into(),
			periods,
		}
	}

	pub fn lead(input: impl Into<CanonicalColumnName>, periods: usize) -> Self {
		Self::Lead {
			input: input.into(),
			periods,
		}
	}

	pub fn cum_sum(input: impl Into<CanonicalColumnName>) -> Self {
		Self::CumSum {
			input: input.into(),
		}
	}

	pub fn diff(input: impl Into<CanonicalColumnName>, periods: usize) -> Self {
		Self::Diff {
			input: input.into(),
			periods,
		}
	}

	/// Fluent setter for Derivative time unit
	pub fn per_hour(mut self) -> Self {
		if let Self::Derivative {
			time_unit,
			..
		} = &mut self
		{
			*time_unit = TimeUnit::Hour;
		}
		self
	}

	pub fn source_columns(&self) -> Vec<&CanonicalColumnName> {
		match self {
			Self::Derivative {
				input,
				..
			}
			| Self::RollingMean {
				input,
				..
			}
			| Self::RollingSum {
				input,
				..
			}
			| Self::Lag {
				input,
				..
			}
			| Self::Lead {
				input,
				..
			}
			| Self::CumSum {
				input,
			}
			| Self::Diff {
				input,
				..
			} => vec![input],
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeUnit {
	Microseconds,
	Second,
	Minute,
	Hour,
	Day,
}

impl TimeUnit {
	/// Get multiplier to convert from microseconds (Polars default time unit) to target unit.
	///
	/// Polars uses microseconds for its `Datetime` type. To get rate per second/hour/etc,
	/// we calculate `value_diff / time_diff_micros * conversion_factor`.
	pub fn from_microseconds(&self) -> f64 {
		match self {
			TimeUnit::Microseconds => 1.0,
			TimeUnit::Second => 1_000_000.0,
			TimeUnit::Minute => 60_000_000.0,
			TimeUnit::Hour => 3_600_000_000.0,
			TimeUnit::Day => 86_400_000_000.0,
		}
	}
}

// =============================================================================
// 2. Subject-Axis Expressions (Cross-Sectional / Natural Transformations)
// =============================================================================

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OverSubjectExpr {
	/// Share of total: `value / sum(value)`.
	PercentOf {
		input: CanonicalColumnName,
	},

	/// Ordinal position: 1, 2, 3...
	Rank {
		input: CanonicalColumnName,
		descending: bool,
	},

	/// Statistical normalization: `(x - mean) / std_dev`.
	ZScore {
		input: CanonicalColumnName,
	},

	/// Simple deviation: `x - mean`.
	DeviationFromMean {
		input: CanonicalColumnName,
	},

	/// **Natural Transformation**: Assigns subjects to N buckets (quantiles).
	/// e.g. Deciles (10), Quartiles (4).
	Quantile {
		input: CanonicalColumnName,
		quantiles: u32,
	},

	/// **Natural Transformation**: Assigns subjects to fixed buckets defined by cut points.
	Bucket {
		input: CanonicalColumnName,
		breaks: Vec<i64>,
	},
}

impl OverSubjectExpr {
	pub fn percent_of(input: impl Into<CanonicalColumnName>) -> Self {
		Self::PercentOf {
			input: input.into(),
		}
	}

	pub fn rank(input: impl Into<CanonicalColumnName>) -> Self {
		Self::Rank {
			input: input.into(),
			descending: true,
		}
	}

	pub fn z_score(input: impl Into<CanonicalColumnName>) -> Self {
		Self::ZScore {
			input: input.into(),
		}
	}

	pub fn deviation_from_mean(input: impl Into<CanonicalColumnName>) -> Self {
		Self::DeviationFromMean {
			input: input.into(),
		}
	}

	/// Create a decile analysis (splits subjects into 10 groups by magnitude).
	pub fn decile(input: impl Into<CanonicalColumnName>) -> Self {
		Self::Quantile {
			input: input.into(),
			quantiles: 10,
		}
	}

	/// Create a quartile analysis (splits subjects into 4 groups).
	pub fn quartile(input: impl Into<CanonicalColumnName>) -> Self {
		Self::Quantile {
			input: input.into(),
			quantiles: 4,
		}
	}

	pub fn quantile(input: impl Into<CanonicalColumnName>, quantiles: u32) -> Self {
		Self::Quantile {
			input: input.into(),
			quantiles,
		}
	}

	pub fn bucket(input: impl Into<CanonicalColumnName>, breaks: Vec<i64>) -> Self {
		Self::Bucket {
			input: input.into(),
			breaks,
		}
	}

	pub fn source_columns(&self) -> Vec<&CanonicalColumnName> {
		match self {
			Self::PercentOf {
				input,
			}
			| Self::Rank {
				input,
				..
			}
			| Self::ZScore {
				input,
			}
			| Self::DeviationFromMean {
				input,
			}
			| Self::Quantile {
				input,
				..
			}
			| Self::Bucket {
				input,
				..
			} => vec![input],
		}
	}
}

// =============================================================================
// 3. Pointwise Expressions (Combining Units)
// =============================================================================

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PointwiseExpr {
	/// Logical OR: true if *any* input > 0.
	AnyOn {
		inputs: Vec<CanonicalColumnName>,
	},
	/// Logical AND: true if *all* sources > 0.
	AllOn {
		inputs: Vec<CanonicalColumnName>,
	},
	/// Integer count of how many sources > 0.
	CountNonZero {
		inputs: Vec<CanonicalColumnName>,
	},
	/// Sum of values.
	Sum {
		inputs: Vec<CanonicalColumnName>,
	},
	/// Average of values.
	Mean {
		inputs: Vec<CanonicalColumnName>,
	},
	Max {
		inputs: Vec<CanonicalColumnName>,
	},
	Min {
		inputs: Vec<CanonicalColumnName>,
	},
	Difference {
		a: CanonicalColumnName,
		b: CanonicalColumnName,
	},
	Ratio {
		numerator: CanonicalColumnName,
		denominator: CanonicalColumnName,
	},
}

impl PointwiseExpr {
	/// Combine multiple units with Logical OR.
	pub fn any_on(inputs: impl IntoCanonicalVec) -> Self {
		Self::AnyOn {
			inputs: inputs.into_canonical_vec(),
		}
	}

	/// Combine multiple units with Logical AND.
	pub fn all_on(inputs: impl IntoCanonicalVec) -> Self {
		Self::AllOn {
			inputs: inputs.into_canonical_vec(),
		}
	}

	/// Count non-zero values.
	pub fn count_non_zero(inputs: impl IntoCanonicalVec) -> Self {
		Self::CountNonZero {
			inputs: inputs.into_canonical_vec(),
		}
	}

	/// Combine multiple units by summing them.
	pub fn sum(inputs: impl IntoCanonicalVec) -> Self {
		Self::Sum {
			inputs: inputs.into_canonical_vec(),
		}
	}

	/// Combine multiple units by averaging them.
	pub fn mean(inputs: impl IntoCanonicalVec) -> Self {
		Self::Mean {
			inputs: inputs.into_canonical_vec(),
		}
	}

	/// Take max across multiple units.
	pub fn max(inputs: impl IntoCanonicalVec) -> Self {
		Self::Max {
			inputs: inputs.into_canonical_vec(),
		}
	}

	/// Take min across multiple units.
	pub fn min(inputs: impl IntoCanonicalVec) -> Self {
		Self::Min {
			inputs: inputs.into_canonical_vec(),
		}
	}

	/// Calculate difference (a - b).
	pub fn difference(a: impl Into<CanonicalColumnName>, b: impl Into<CanonicalColumnName>) -> Self {
		Self::Difference {
			a: a.into(),
			b: b.into(),
		}
	}

	/// Calculate ratio (numerator / denominator).
	pub fn ratio(
		numerator: impl Into<CanonicalColumnName>,
		denominator: impl Into<CanonicalColumnName>,
	) -> Self {
		Self::Ratio {
			numerator: numerator.into(),
			denominator: denominator.into(),
		}
	}

	/// Determines the semantic kind of the output.
	pub fn result_kind(&self) -> MeasurementKind {
		match self {
			Self::AnyOn {
				..
			}
			| Self::AllOn {
				..
			} => MeasurementKind::Categorical,
			Self::CountNonZero {
				..
			} => MeasurementKind::Count,
			_ => MeasurementKind::Measure,
		}
	}

	pub fn source_columns(&self) -> Vec<&CanonicalColumnName> {
		match self {
			Self::AnyOn {
				inputs,
			}
			| Self::AllOn {
				inputs,
			}
			| Self::CountNonZero {
				inputs,
			}
			| Self::Sum {
				inputs,
			}
			| Self::Mean {
				inputs,
			}
			| Self::Max {
				inputs,
			}
			| Self::Min {
				inputs,
			} => inputs.iter().collect(),
			Self::Difference {
				a,
				b,
			} => vec![a, b],
			Self::Ratio {
				numerator,
				denominator,
			} => vec![numerator, denominator],
		}
	}

	/// Generate Polars expression for this pointwise operation.
	pub fn to_polars_expr(&self, output_name: &str) -> PolarsResult<Expr> {
		use polars::lazy::dsl::{max_horizontal, mean_horizontal, min_horizontal, sum_horizontal};

		let expr = match self {
			Self::AnyOn {
				inputs,
			} => {
				let cols: Vec<Expr> = inputs.iter().map(|c| col(c.as_str())).collect();
				max_horizontal(&cols)?.gt(lit(0)).cast(DataType::Int32)
			}
			Self::AllOn {
				inputs,
			} => {
				let cols: Vec<Expr> = inputs.iter().map(|c| col(c.as_str())).collect();
				min_horizontal(&cols)?.gt(lit(0)).cast(DataType::Int32)
			}
			Self::CountNonZero {
				inputs,
			} => {
				let non_zero: Vec<Expr> = inputs
					.iter()
					.map(|c| col(c.as_str()).neq(lit(0)).cast(DataType::UInt32))
					.collect();
				sum_horizontal(&non_zero, true)?
			}
			Self::Sum {
				inputs,
			} => {
				let cols: Vec<Expr> = inputs.iter().map(|c| col(c.as_str())).collect();
				sum_horizontal(&cols, true)?
			}
			Self::Mean {
				inputs,
			} => {
				let cols: Vec<Expr> = inputs.iter().map(|c| col(c.as_str())).collect();
				mean_horizontal(&cols, true)?
			}
			Self::Max {
				inputs,
			} => {
				let cols: Vec<Expr> = inputs.iter().map(|c| col(c.as_str())).collect();
				max_horizontal(&cols)?
			}
			Self::Min {
				inputs,
			} => {
				let cols: Vec<Expr> = inputs.iter().map(|c| col(c.as_str())).collect();
				min_horizontal(&cols)?
			}
			Self::Difference {
				a,
				b,
			} => col(a.as_str()) - col(b.as_str()),
			Self::Ratio {
				numerator,
				denominator,
			} => col(numerator.as_str()) / col(denominator.as_str()),
		};

		Ok(expr.alias(output_name))
	}
}

// =============================================================================
// Helper Trait for Ergonomic Vec Construction
// =============================================================================

/// Trait to allow passing various types that can become `Vec<CanonicalColumnName>`.
pub trait IntoCanonicalVec {
	fn into_canonical_vec(self) -> Vec<CanonicalColumnName>;
}

impl IntoCanonicalVec for Vec<CanonicalColumnName> {
	fn into_canonical_vec(self) -> Vec<CanonicalColumnName> {
		self
	}
}

impl IntoCanonicalVec for Vec<&str> {
	fn into_canonical_vec(self) -> Vec<CanonicalColumnName> {
		self.into_iter().map(CanonicalColumnName::from).collect()
	}
}

impl IntoCanonicalVec for Vec<String> {
	fn into_canonical_vec(self) -> Vec<CanonicalColumnName> {
		self.into_iter().map(CanonicalColumnName::from).collect()
	}
}

impl<const N: usize> IntoCanonicalVec for [&str; N] {
	fn into_canonical_vec(self) -> Vec<CanonicalColumnName> {
		self.into_iter().map(CanonicalColumnName::from).collect()
	}
}

impl<const N: usize> IntoCanonicalVec for [String; N] {
	fn into_canonical_vec(self) -> Vec<CanonicalColumnName> {
		self.into_iter().map(CanonicalColumnName::from).collect()
	}
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_combine_units_with_pointwise() {
		let derivation = Derivation::pointwise(
			"any_engine_running",
			PointwiseExpr::any_on(["engine_1", "engine_2", "engine_3"]),
		);

		assert_eq!(derivation.name, CanonicalColumnName::from("any_engine_running"));
		assert_eq!(derivation.kind, MeasurementKind::Categorical);

		let inputs = derivation.input_columns();
		assert!(inputs.contains(&&CanonicalColumnName::from("engine_1")));
		assert!(inputs.contains(&&CanonicalColumnName::from("engine_2")));
		assert!(inputs.contains(&&CanonicalColumnName::from("engine_3")));
	}

	#[test]
	fn test_natural_transformation_deciles() {
		let derivation = Derivation::over_subjects("sales_decile", OverSubjectExpr::decile("sales"));

		assert_eq!(derivation.kind, MeasurementKind::Categorical);
		assert_eq!(derivation.input_columns(), vec![&CanonicalColumnName::from("sales")]);
	}

	#[test]
	fn test_fluent_building() {
		let derivation = Derivation::over_time("fuel_rate", TimeExpr::derivative("fuel"))
			.with_kind(MeasurementKind::Measure)
			.named("fuel_consumption_rate")
			.with_chart_hints(ChartHints::measure().label("Fuel Rate (L/hr)"));

		assert_eq!(derivation.name, CanonicalColumnName::from("fuel_consumption_rate"));
		assert!(derivation.chart_hints.is_some());
	}
}
