//! Chart presentation hints for EtlUnits
//!
//! ChartHints provide guidance for how to render measurements and qualities
//! on charts. They support various chart types, axis configurations, and
//! indexing strategies.

use serde::{Deserialize, Serialize};

/// Which y-axis to use for charting
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, Hash)]
#[serde(rename_all = "lowercase")]
pub enum AxisId {
	#[default]
	/// First y-axis
	Y,
	/// Second y-axis shared with Y
	Y1,
	/// Secondary y-axis (secondary to Y)
	Y2,
}

impl AxisId {
	pub fn as_str(&self) -> &'static str {
		match self {
			AxisId::Y => "y",
			AxisId::Y1 => "y1",
			AxisId::Y2 => "y2",
		}
	}
}

/// Type of chart to render
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChartType {
	/// Line chart (default for time series)
	#[default]
	Line,
	/// Bar chart
	Bar,
	/// Scatter plot
	Scatter,
	/// Bubble chart with size dimension
	Bubble {
		/// Column name for bubble size
		size: String,
	},
}

impl ChartType {
	pub fn line() -> Self {
		Self::Line
	}

	pub fn bar() -> Self {
		Self::Bar
	}

	pub fn scatter() -> Self {
		Self::Scatter
	}

	pub fn bubble(size: impl Into<String>) -> Self {
		Self::Bubble {
			size: size.into(),
		}
	}
}

/// What to use as the X-axis index
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Index {
	/// Time axis (default for measurements) - plot values over time
	#[default]
	Time,
	/// Subject axis - compare subjects (default for qualities)
	Subject,
	/// Quality column as categorical axis - compare across quality values
	Quality(String),
	/// Component axis - compare component values
	Component(String),
}

impl Index {
	pub fn time() -> Self {
		Self::Time
	}

	pub fn subject() -> Self {
		Self::Subject
	}

	pub fn quality(column: impl Into<String>) -> Self {
		Self::Quality(column.into())
	}

	pub fn component(name: impl Into<String>) -> Self {
		Self::Component(name.into())
	}
}

/// What creates separate series in the chart
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChartSeries {
	/// One series per subject (default)
	#[default]
	Subject,
	/// One series per quality value
	Quality(String),
	/// One series per component value
	Component(String),
	/// One series per subject × component combination
	SubjectAndComponent(String),
}

impl ChartSeries {
	pub fn subject() -> Self {
		Self::Subject
	}

	pub fn quality(name: impl Into<String>) -> Self {
		Self::Quality(name.into())
	}

	pub fn component(name: impl Into<String>) -> Self {
		Self::Component(name.into())
	}

	pub fn subject_and_component(name: impl Into<String>) -> Self {
		Self::SubjectAndComponent(name.into())
	}
}

/// Hints for how to present an EtlUnit on a chart
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChartHints {
	/// Which y-axis (Y, Y1, Y2)
	#[serde(default)]
	pub axis: AxisId,

	/// Render as stepped line (for categorical/binary data)
	#[serde(default)]
	pub stepped: bool,

	/// Line tension (0 = straight, 0.4 = smooth). None uses chart default.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub tension: Option<f32>,

	/// Legend label override (defaults to unit name)
	#[serde(skip_serializing_if = "Option::is_none")]
	pub label: Option<String>,

	/// Display unit for the values (e.g., "ft", "%", "mm"). Used by
	/// the response formatters to label axes / table columns.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub unit: Option<String>,

	/// Explicit color override
	#[serde(skip_serializing_if = "Option::is_none")]
	pub color: Option<String>,

	/// Chart type (line, bar, scatter, bubble)
	#[serde(default, skip_serializing_if = "is_default_chart_type")]
	pub chart_type: ChartType,

	/// What to use as X-axis
	#[serde(default, skip_serializing_if = "is_default_index")]
	pub index: Index,

	/// What creates separate series
	#[serde(default, skip_serializing_if = "is_default_series")]
	pub series: ChartSeries,
}

fn is_default_chart_type(ct: &ChartType) -> bool {
	*ct == ChartType::default()
}

fn is_default_index(idx: &Index) -> bool {
	*idx == Index::default()
}

fn is_default_series(s: &ChartSeries) -> bool {
	*s == ChartSeries::default()
}

impl ChartHints {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn axis(mut self, axis: AxisId) -> Self {
		self.axis = axis;
		self
	}

	pub fn stepped(mut self) -> Self {
		self.stepped = true;
		self
	}

	pub fn tension(mut self, t: f32) -> Self {
		self.tension = Some(t);
		self
	}

	pub fn label(mut self, label: impl Into<String>) -> Self {
		self.label = Some(label.into());
		self
	}

	pub fn unit(mut self, unit: impl Into<String>) -> Self {
		self.unit = Some(unit.into());
		self
	}

	pub fn color(mut self, color: impl Into<String>) -> Self {
		self.color = Some(color.into());
		self
	}

	pub fn chart_type(mut self, ct: ChartType) -> Self {
		self.chart_type = ct;
		self
	}

	pub fn index(mut self, idx: Index) -> Self {
		self.index = idx;
		self
	}

	pub fn series_by(mut self, s: ChartSeries) -> Self {
		self.series = s;
		self
	}

	/// Create hints for a typical measure (smooth line on primary axis, time index)
	pub fn measure() -> Self {
		Self {
			axis: AxisId::Y,
			stepped: false,
			tension: Some(0.25),
			index: Index::Time,
			..Default::default()
		}
	}

	/// Create hints for a categorical measurement (stepped line on secondary axis)
	pub fn categorical() -> Self {
		Self {
			axis: AxisId::Y2,
			stepped: true,
			tension: None,
			index: Index::Time,
			..Default::default()
		}
	}

	/// Create hints for a quality (bar chart, subject index)
	pub fn quality() -> Self {
		Self {
			axis: AxisId::Y,
			stepped: false,
			tension: None,
			chart_type: ChartType::Bar,
			index: Index::Subject,
			..Default::default()
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_measure_hints() {
		let hints = ChartHints::measure();
		assert!(!hints.stepped);
		assert_eq!(hints.axis, AxisId::Y);
		assert_eq!(hints.index, Index::Time);
	}

	#[test]
	fn test_categorical_hints() {
		let hints = ChartHints::categorical();
		assert!(hints.stepped);
		assert_eq!(hints.axis, AxisId::Y2);
	}

	#[test]
	fn test_quality_hints() {
		let hints = ChartHints::quality();
		assert_eq!(hints.chart_type, ChartType::Bar);
		assert_eq!(hints.index, Index::Subject);
	}

	#[test]
	fn test_chart_hints_builder() {
		let hints = ChartHints::new()
			.chart_type(ChartType::Bar)
			.index(Index::quality("zone"))
			.series_by(ChartSeries::subject())
			.color("#ff0000");

		assert_eq!(hints.chart_type, ChartType::Bar);
		assert_eq!(hints.index, Index::Quality("zone".to_string()));
		assert_eq!(hints.color, Some("#ff0000".to_string()));
	}

	#[test]
	fn test_bubble_chart() {
		let hints = ChartHints::new().chart_type(ChartType::bubble("fuel_rate"));

		if let ChartType::Bubble {
			size,
		} = hints.chart_type
		{
			assert_eq!(size, "fuel_rate");
		} else {
			panic!("Expected Bubble chart type");
		}
	}
}
