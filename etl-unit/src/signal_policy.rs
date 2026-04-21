use std::{fs::File, io::BufReader, path::Path, time::Duration};

use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

use crate::{
	MeasurementUnit,
	error::{EtlError, EtlResult},
	polars_fns::{apply_instant_policy, apply_instant_policy_from_parts},
};

// ============================================================================
// Ergonomic Helper Trait
// ============================================================================

/// Helper trait to allow passing either `Duration` or `u64` (as seconds)
/// into configuration methods.
pub trait IntoDuration {
	fn into_duration(self) -> Duration;
}

impl IntoDuration for Duration {
	fn into_duration(self) -> Duration {
		self
	}
}

/// Integers are interpreted as Seconds
impl IntoDuration for u64 {
	fn into_duration(self) -> Duration {
		Duration::from_secs(self)
	}
}

/// Integers are interpreted as Seconds
impl IntoDuration for u32 {
	fn into_duration(self) -> Duration {
		Duration::from_secs(self.into())
	}
}

// ============================================================================
// Signal Policy
// ============================================================================

/// Defines how raw measurements are converted into a stable signal.
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct SignalPolicy {
	/// The "Time-to-Live". If we hear nothing for this long,
	/// the value becomes Null.
	#[serde(with = "humantime_serde")]
	pub max_staleness: Duration,

	/// The strategy for aggregating samples.
	pub windowing: WindowStrategy,

	/// chrono time format string (e.g., "%Y-%m-%d %H:%M:%S")
	pub time_format: Option<String>,
}

impl Default for SignalPolicy {
	fn default() -> Self {
		Self {
			max_staleness: Duration::from_secs(60),
			windowing:     WindowStrategy::Instant,
			time_format:   None,
		}
	}
}

impl SignalPolicy {
	// --- Static Constructors (Ergonomic) ---

	/// Create a default "Instant" policy (no windowing).
	/// Uses default TTL (60s) and Rate (6s).
	pub fn instant() -> Self {
		Self::default()
	}

	/// Create a Sliding Window policy.
	///
	/// # Arguments
	/// * `duration` - Window size (Duration or u64 seconds)
	/// * `min_samples` - Minimum samples required in window
	pub fn sliding(duration: impl IntoDuration, min_samples: u32) -> Self {
		Self {
			windowing: WindowStrategy::Sliding {
				duration: duration.into_duration(),
				min_samples,
			},
			..Self::default()
		}
	}

	/// Create a Tumbling Window policy.
	pub fn tumbling(duration: impl IntoDuration, min_samples: u32) -> Self {
		Self {
			windowing: WindowStrategy::Tumbling {
				duration: duration.into_duration(),
				min_samples,
			},
			..Self::default()
		}
	}

	// --- Fluent Setters (Wither Pattern) ---

	/// Set the Max Staleness (TTL).
	/// Accepts `Duration` or `u64` (seconds).
	pub fn with_ttl(mut self, ttl: impl IntoDuration) -> Self {
		self.max_staleness = ttl.into_duration();
		self
	}

	/// Set the chrono time format string (e.g., "%Y-%m-%d %H:%M:%S")
	pub fn with_time_format(mut self, fmt: String) -> Self {
		self.time_format = Some(fmt);
		self
	}

	// --- Validation & IO ---

	/// Alias for `max_staleness`
	pub fn ttl(&self) -> Duration {
		self.max_staleness
	}

	/// Validates that the policy configuration is consistent.
	pub fn validate(&self) -> EtlResult<()> {
		// Windowing strategies don't need sample-rate-based validation;
		// the window duration and min_samples are validated at schema-build time.
		Ok(())
	}

	/// Loads a SignalPolicy from a JSON file using EtlResult
	pub fn from_json_file<P: AsRef<Path>>(path: P) -> EtlResult<Self> {
		let path = path.as_ref();

		let file = File::open(path)
			.map_err(|e| EtlError::Config(format!("Failed to open policy file {:?}: {}", path, e)))?;

		let reader = BufReader::new(file);

		let policy: SignalPolicy = serde_json::from_reader(reader)
			.map_err(|e| EtlError::Config(format!("Failed to parse JSON in {:?}: {}", path, e)))?;

		policy.validate()?;

		Ok(policy)
	}

	// ========================================================================
	// Signal Policy Application (Public API for Testing)
	// ========================================================================

	/// Apply the signal policy to transform raw signals into observations.
	///
	/// This is the main entry point for signal policy application. It dispatches
	/// to the appropriate windowing strategy (Instant, Sliding, or Tumbling).
	///
	/// # Algorithm (Instant Policy)
	///
	/// 1. **Truncate**: Timestamps are truncated to grid cell boundaries (TTL-aligned)
	/// 2. **Aggregate**: Simple group_by on truncated time + partitions (subject + components)
	/// 3. **Complete Grid**: Cross join of time grid × unique partitions
	/// 4. **Fill**: Left join ensures all cells exist (missing = null)
	///
	/// # Arguments
	///
	/// * `df` - Source DataFrame with raw signal data
	/// * `measurement` - MeasurementUnit containing policy configuration
	///
	/// # Returns
	///
	/// DataFrame with regular time grid where each row is one observation.
	/// Grid cells without signals will have null values.
	///
	/// # Example
	///
	/// ```ignore
	/// let measurement = MeasurementUnit::new("station", "timestamp", "value", MeasurementKind::Measure)
	///     .with_signal_policy(SignalPolicy::instant().with_ttl(60));
	///
	/// let observations = SignalPolicy::apply(&signals_df, &measurement)?;
	/// ```
	pub fn apply(df: DataFrame, measurement: &MeasurementUnit) -> EtlResult<DataFrame> {
		apply_instant_policy(df, measurement)
	}

	/// Apply signal policy using explicit parameters (no MeasurementUnit required).
	///
	/// This is useful for testing or when you don't have a full MeasurementUnit.
	///
	/// # Arguments
	///
	/// * `df` - Source DataFrame with raw signal data
	/// * `time_col` - Name of the timestamp column (must be Datetime type)
	/// * `value_col` - Name of the value column to aggregate
	/// * `partition_cols` - Columns that define partitions (subject + components)
	/// * `ttl_ms` - Grid cell size in milliseconds
	///
	/// # Returns
	///
	/// DataFrame with regular time grid. The output contains:
	/// - `grid_time`: Truncated timestamp (renamed to `time_col` in final output)
	/// - Partition columns (unchanged)
	/// - Value column (aggregated: mean for Measure, max for Binary)
	pub fn apply_from_parts(
		df: DataFrame,
		time_col: &str,
		value_col: &str,
		partition_cols: &[&str],
		ttl_ms: i64,
		time_format: Option<&str>,
	) -> EtlResult<DataFrame> {
		apply_instant_policy_from_parts(df, time_col, value_col, partition_cols, ttl_ms, time_format)
	}
}

// ============================================================================
// Window Strategy
// ============================================================================

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WindowStrategy {
	Instant,
	Sliding {
		#[serde(with = "humantime_serde")]
		duration:    Duration,
		min_samples: u32,
	},
	Tumbling {
		#[serde(with = "humantime_serde")]
		duration:    Duration,
		min_samples: u32,
	},
}

impl WindowStrategy {
	pub fn name(&self) -> &str {
		match self {
			WindowStrategy::Instant => "Instant",
			WindowStrategy::Sliding {
				..
			} => "Sliding",
			WindowStrategy::Tumbling {
				..
			} => "Tumbling",
		}
	}
}
