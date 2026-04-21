//! Alignment specification for cross-measurement sample rate unification.
//!
//! `AlignmentSpec` is the single source of truth for how measurements are
//! aligned to a common sample rate. Computed once from measurement configs,
//! it drives both the processing pipeline and diagnostics.
//!
//! # Type-driven design
//!
//! The spec is a value type — computed, stored, serialized. The processing
//! code reads from it instead of recomputing rates. The diagnostics serialize
//! it directly instead of building parallel descriptions. This ensures the
//! diagnostic output always matches what the code actually did.

use serde::Serialize;

use crate::column::CanonicalColumnName;
use crate::ResampleStrategy;

/// What action to take for a measurement during alignment.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AlignAction {
	/// Apply signal policy at native rate. No resampling needed —
	/// the measurement's native rate equals the unified rate.
	SignalOnly,
	/// Upsample from native rate to the unified rate (faster).
	Upsample { strategy: ResampleStrategy },
	/// Downsample from native rate to the unified rate (slower).
	Downsample { strategy: ResampleStrategy },
	/// No signal policy and no resampling. Data passes through as-is.
	PassThrough,
}

/// Per-measurement alignment decision.
#[derive(Debug, Clone, Serialize)]
pub struct MeasurementAlignment {
	/// Canonical measurement name.
	pub name: CanonicalColumnName,
	/// Declared native sample rate (ms). None if not declared.
	pub native_rate_ms: Option<i64>,
	/// Signal policy TTL (ms). None if no signal policy.
	pub ttl_ms: Option<u64>,
	/// Declared upsample strategy (if any).
	pub upsample_strategy: Option<ResampleStrategy>,
	/// Declared downsample strategy (if any).
	pub downsample_strategy: Option<ResampleStrategy>,
	/// What action the alignment pipeline will take.
	pub action: AlignAction,
	/// The rate this measurement will be at after alignment (ms).
	pub effective_rate_ms: i64,
}

/// The alignment specification for a set of measurements.
///
/// Computed once from measurement configs. Stored on the Universe.
/// Drives `ensure_aligned()`, the subset grid interval, and diagnostics.
#[derive(Debug, Clone, Serialize)]
pub struct AlignmentSpec {
	/// The common sample rate all measurements align to (ms).
	pub unified_rate_ms: i64,
	/// Per-measurement alignment decisions.
	pub measurements: Vec<MeasurementAlignment>,
}

impl AlignmentSpec {
	/// Compute the alignment spec from a set of measurement units.
	///
	/// The unified rate is the **slowest effective rate** across all
	/// measurements. A measurement's effective rate is:
	/// - Its native `sample_rate_ms` if no upsampling is declared
	/// - The fastest rate in the group if upsample IS declared
	///   (meaning it can reach the fastest rate)
	///
	/// If all slow measurements declare upsample, the unified rate
	/// equals the fastest native rate. Otherwise, it equals the slowest.
	pub fn compute(
		measurements: &std::collections::HashMap<CanonicalColumnName, super::universe_of_etlunits::MeasurementData>,
		requested: &[CanonicalColumnName],
	) -> Option<Self> {
		let mut infos: Vec<(CanonicalColumnName, Option<i64>, Option<u64>, Option<ResampleStrategy>, Option<ResampleStrategy>)> = Vec::new();
		let mut declared_rates: Vec<i64> = Vec::new();

		for name in requested {
			if let Some(md) = measurements.get(name) {
				let native = md.unit.sample_rate_ms;
				let ttl = md.unit.signal_policy.as_ref().map(|p| p.ttl().as_millis() as u64);
				let up = md.unit.upsample_strategy;
				let down = md.unit.downsample_strategy;

				if let Some(rate) = native {
					declared_rates.push(rate);
				}
				infos.push((name.clone(), native, ttl, up, down));
			}
		}

		if declared_rates.is_empty() {
			return None;
		}

		let fastest = *declared_rates.iter().min().unwrap();
		let slowest = *declared_rates.iter().max().unwrap();

		// Determine unified rate.
		let unified_rate_ms = if fastest == slowest {
			// All same rate — no alignment needed
			fastest
		} else {
			// Check if ALL slow measurements declare upsample
			let all_slow_have_upsample = infos.iter()
				.filter(|(_, native, _, _, _)| native.unwrap_or(0) > fastest)
				.all(|(_, _, _, up, _)| up.is_some());

			if all_slow_have_upsample {
				fastest
			} else {
				slowest
			}
		};

		// Build per-measurement alignment decisions
		let mut alignments = Vec::new();
		for (name, native, ttl, up, down) in infos {
			let native_ms = native.unwrap_or(unified_rate_ms);

			let action = if native.is_none() {
				// No declared rate — pass through or signal only
				if measurements.get(&name).map(|md| md.unit.signal_policy.is_some()).unwrap_or(false) {
					AlignAction::SignalOnly
				} else {
					AlignAction::PassThrough
				}
			} else if unified_rate_ms < native_ms {
				// Unified is faster than native — need to upsample
				match up {
					Some(strategy) => AlignAction::Upsample { strategy },
					None => AlignAction::SignalOnly, // can't upsample, stay at native
				}
			} else if unified_rate_ms > native_ms {
				// Unified is slower than native — need to downsample
				match down {
					Some(strategy) => AlignAction::Downsample { strategy },
					None => AlignAction::SignalOnly, // no downsample declared, signal only
				}
			} else {
				// Same rate — signal policy only
				AlignAction::SignalOnly
			};

			let effective_rate_ms = match &action {
				AlignAction::Upsample { .. } | AlignAction::Downsample { .. } => unified_rate_ms,
				_ => native_ms,
			};

			alignments.push(MeasurementAlignment {
				name,
				native_rate_ms: native,
				ttl_ms: ttl,
				upsample_strategy: up,
				downsample_strategy: down,
				action,
				effective_rate_ms,
			});
		}

		Some(AlignmentSpec {
			unified_rate_ms,
			measurements: alignments,
		})
	}

	/// Get the alignment action for a specific measurement.
	pub fn action_for(&self, name: &CanonicalColumnName) -> Option<&AlignAction> {
		self.measurements.iter()
			.find(|m| &m.name == name)
			.map(|m| &m.action)
	}

	/// Get the full alignment info for a specific measurement.
	pub fn info_for(&self, name: &CanonicalColumnName) -> Option<&MeasurementAlignment> {
		self.measurements.iter()
			.find(|m| &m.name == name)
	}
}
