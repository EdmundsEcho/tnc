//! Time interval for sampling/bucketing.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Time interval for sampling/bucketing.
///
/// Wraps a Duration with human-friendly parsing via humantime.
/// Used with Polars `dt().truncate()` for time bucketing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Interval(#[serde(with = "humantime_serde")] Duration);

impl Interval {
	pub fn new(duration: Duration) -> Self {
		Self(duration)
	}

	/// Parse from human-friendly string like "15m", "1h", "24h", "1d"
	pub fn parse(s: &str) -> Result<Self, humantime::DurationError> {
		let duration = humantime::parse_duration(s)?;
		Ok(Self(duration))
	}

	/// Get the underlying Duration
	pub fn duration(&self) -> Duration {
		self.0
	}

	/// Convert to Polars truncate string format
	/// Polars uses: "1h", "30m", "1d", "1w", etc.
	pub fn to_polars_truncate(&self) -> String {
		let secs = self.0.as_secs();
		let nanos = self.0.subsec_nanos();

		if nanos > 0 {
			// Sub-second precision
			format!("{}us", self.0.as_micros())
		} else {
			// Units in descending order: (divisor, label)
			let units = [
				(86400, "d"), // Days
				(3600, "h"),  // Hours
				(60, "m"),    // Minutes
			];

			// Find the largest unit that cleanly divides the seconds
			for (divisor, label) in units {
				if secs.is_multiple_of(divisor) {
					return format!("{}{}", secs / divisor, label);
				}
			}

			// Default to seconds
			format!("{}s", secs)
		}
	}

	// Convenience constructors
	pub fn minutes(n: u64) -> Self {
		Self(Duration::from_secs(n * 60))
	}

	pub fn hours(n: u64) -> Self {
		Self(Duration::from_secs(n * 3600))
	}

	pub fn days(n: u64) -> Self {
		Self(Duration::from_secs(n * 86400))
	}
}

impl TryFrom<&str> for Interval {
	type Error = humantime::DurationError;

	fn try_from(s: &str) -> Result<Self, Self::Error> {
		Self::parse(s)
	}
}

impl std::fmt::Display for Interval {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.to_polars_truncate())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_interval_to_polars() {
		assert_eq!(Interval::minutes(15).to_polars_truncate(), "15m");
		assert_eq!(Interval::hours(1).to_polars_truncate(), "1h");
		assert_eq!(Interval::hours(6).to_polars_truncate(), "6h");
		assert_eq!(Interval::days(1).to_polars_truncate(), "1d");
	}

	#[test]
	fn test_interval_parse() {
		let i = Interval::parse("30m").unwrap();
		assert_eq!(i.duration(), Duration::from_secs(30 * 60));

		let i = Interval::parse("2h").unwrap();
		assert_eq!(i.duration(), Duration::from_secs(2 * 3600));

		let i = Interval::parse("1d").unwrap();
		assert_eq!(i.duration(), Duration::from_secs(86400));
	}
}
