use std::fmt;

use serde::{Deserialize, Serialize};

use crate::CanonicalColumnName;

/// Statistics from signal policy application
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalPolicyStats {
	/// Measurement name
	pub measurement:        CanonicalColumnName,
	/// Source name
	pub source:             String,
	/// Policy type applied
	pub policy_type:        String,
	/// Raw data points before policy
	pub input_points:       usize,
	/// Grid points generated (observations after policy)
	pub grid_points:        usize,
	/// Observations with valid data (non-null)
	pub valid_observations: usize,
	/// Observations where signal was stale/expired (null)
	pub null_observations:  usize,
	/// Fill rate: valid / grid_points (0.0 - 1.0)
	pub fill_rate:          f64,
	/// TTL/max staleness used
	pub ttl_ms:             u64,
	/// Time span of the input data in milliseconds
	#[serde(default)]
	pub time_span_ms:       u64,
}

impl fmt::Display for SignalPolicyStats {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let expected = self.expected_grid_points();

		writeln!(f, "--- SignalPolicyStats: {} ---", self.measurement)?;
		writeln!(f, "  source:           {}", self.source)?;
		writeln!(f, "  policy_type:      {}", self.policy_type)?;
		writeln!(f, "  ttl_ms:           {}", self.ttl_ms)?;
		writeln!(
			f,
			"  time_span:        {} ms ({:.1}s)",
			self.time_span_ms,
			self.time_span_ms as f64 / 1000.0
		)?;
		writeln!(f, "  input_points:     {}", self.input_points)?;
		writeln!(
			f,
			"  expected_grid:    ceil({}/{}) + 1 = {}",
			self.time_span_ms, self.ttl_ms, expected
		)?;

		if self.grid_ok() {
			writeln!(f, "  grid_points:      {}", self.grid_points)?;
		} else {
			writeln!(f, "  grid_points:      {} 🚩 expected {}", self.grid_points, expected)?;
		}

		if !self.compression_ok() {
			writeln!(
				f,
				"  compression:      🚩 output > input ({} > {})",
				self.grid_points, self.input_points
			)?;
		}

		writeln!(f, "  valid:            {}", self.valid_observations)?;
		writeln!(f, "  null:             {}", self.null_observations)?;
		writeln!(f, "  fill_rate:        {:.1}%", self.fill_rate * 100.0)?;

		if self.is_valid() {
			write!(f, "  ✅ PASS")
		} else {
			write!(f, "  🚫 FAIL")
		}
	}
}
impl SignalPolicyStats {
	pub fn new(
		measurement: CanonicalColumnName,
		source: &str,
		policy_type: &str,
		input_points: usize,
		grid_points: usize,
		null_observations: usize,
		ttl_ms: u64,
		time_span_ms: u64,
	) -> Self {
		let valid_observations = grid_points.saturating_sub(null_observations);
		let fill_rate = if grid_points > 0 {
			valid_observations as f64 / grid_points as f64
		} else {
			0.0
		};

		Self {
			measurement,
			source: source.to_string(),
			policy_type: policy_type.to_string(),
			input_points,
			grid_points,
			valid_observations,
			null_observations,
			fill_rate,
			ttl_ms,
			time_span_ms,
		}
	}

	/// Calculate expected grid points from time span and TTL
	pub fn expected_grid_points(&self) -> usize {
		if self.ttl_ms > 0 {
			(self.time_span_ms as f64 / self.ttl_ms as f64).ceil() as usize + 1
		} else {
			0
		}
	}

	/// Check if grid points match expected
	pub fn grid_ok(&self) -> bool {
		self.grid_points == self.expected_grid_points()
	}

	/// Check if compression occurred (output <= input)
	pub fn compression_ok(&self) -> bool {
		self.grid_points <= self.input_points
	}

	/// Overall validation pass
	pub fn is_valid(&self) -> bool {
		self.grid_ok() && self.compression_ok()
	}
}
