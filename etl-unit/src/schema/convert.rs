//! Conversion from DTOs to runtime types
//!
//! This module handles the transformation of JSON-deserialized DTOs
//! into the runtime `EtlSchema` type.

use super::{
	EtlSchema,
	dto::{
		ChartHintsDto, ChartTypeDto, DerivationDto, IndexDto, MeasurementDto, QualityDto, SchemaDto,
		SeriesDto, SignalPolicyDto, WindowStrategyDto,
	},
};
use crate::{
	Derivation, EtlResult, MeasurementUnit, QualityUnit, SignalPolicy,
	chart_hints::{AxisId, ChartHints, ChartSeries, ChartType, Index},
	column::CanonicalColumnName,
	signal_policy::WindowStrategy,
};

impl SchemaDto {
	/// Convert the DTO into an EtlSchema
	pub fn into_schema(self) -> EtlResult<EtlSchema> {
		let subject = CanonicalColumnName::new(&self.subject);
		let time = CanonicalColumnName::new(&self.time);

		// Build qualities
		let qualities: Vec<QualityUnit> = self
			.qualities
			.into_iter()
			.map(|q| convert_quality(&subject, q))
			.collect();

		// Build measurements
		let measurements: Vec<MeasurementUnit> = self
			.measurements
			.into_iter()
			.map(|m| convert_measurement(&subject, &time, m))
			.collect::<EtlResult<Vec<_>>>()?;

		// Build derivations
		let derivations: Vec<Derivation> = self
			.derivations
			.into_iter()
			.map(convert_derivation)
			.collect();

		// Construct the schema
		let schema = EtlSchema {
			name: self.name,
			subject,
			time,
			qualities,
			measurements,
			derivations,
		};

		Ok(schema)
	}
}

/// Convert a QualityDto to QualityUnit
fn convert_quality(subject: &CanonicalColumnName, dto: QualityDto) -> QualityUnit {
	let mut quality = QualityUnit::new(subject.as_str(), &dto.name);

	if let Some(hints_dto) = dto.chart_hints {
		quality.chart_hints = Some(convert_chart_hints(hints_dto));
	}

	quality
}

/// Convert a MeasurementDto to MeasurementUnit
fn convert_measurement(
	subject: &CanonicalColumnName,
	time: &CanonicalColumnName,
	dto: MeasurementDto,
) -> EtlResult<MeasurementUnit> {
	let mut measurement = MeasurementUnit::new(
		subject.clone(), // k
		time.clone(),
		CanonicalColumnName::new(&dto.name),
		dto.kind,
	);

	// Add components
	for component in dto.components {
		measurement
			.components
			.push(CanonicalColumnName::new(component));
	}

	// Convert signal policy if present
	if let Some(policy_dto) = dto.signal_policy {
		measurement.signal_policy = Some(convert_signal_policy(policy_dto)?);
	}

	// Convert chart hints if present
	if let Some(hints_dto) = dto.chart_hints {
		measurement.chart_hints = Some(convert_chart_hints(hints_dto));
	}

	Ok(measurement)
}

/// Convert a SignalPolicyDto to SignalPolicy
fn convert_signal_policy(dto: SignalPolicyDto) -> EtlResult<SignalPolicy> {
	let windowing = match dto.windowing {
		WindowStrategyDto::Instant => WindowStrategy::Instant,
		WindowStrategyDto::Sliding {
			duration,
			min_samples,
		} => {
			WindowStrategy::Sliding {
				duration,
				min_samples,
			}
		}
	};

	Ok(SignalPolicy {
		max_staleness: dto.max_staleness,
		windowing,
		time_format: dto.time_format,
	})
}

/// Convert a ChartHintsDto to ChartHints
fn convert_chart_hints(dto: ChartHintsDto) -> ChartHints {
	let mut hints = ChartHints::new();

	if let Some(label) = dto.label {
		hints = hints.label(label);
	}
	if let Some(color) = dto.color {
		hints = hints.color(color);
	}
	if let Some(true) = dto.stepped {
		hints = hints.stepped();
	}
	if let Some(t) = dto.tension {
		hints = hints.tension(t);
	}
	if let Some(axis_str) = dto.axis {
		hints.axis = match axis_str.as_str() {
			"y1" => AxisId::Y1,
			"y2" => AxisId::Y2,
			_ => AxisId::Y,
		};
	}
	if let Some(chart_type_dto) = dto.chart_type {
		hints.chart_type = convert_chart_type(chart_type_dto);
	}
	if let Some(index_dto) = dto.index {
		hints.index = convert_index(index_dto);
	}
	if let Some(series_dto) = dto.series {
		hints.series = convert_series(series_dto);
	}

	hints
}

fn convert_chart_type(dto: ChartTypeDto) -> ChartType {
	match dto {
		ChartTypeDto::Line => ChartType::Line,
		ChartTypeDto::Bar => ChartType::Bar,
		ChartTypeDto::Scatter => ChartType::Scatter,
		ChartTypeDto::Bubble {
			size,
		} => {
			ChartType::Bubble {
				size,
			}
		}
	}
}

fn convert_index(dto: IndexDto) -> Index {
	match dto {
		IndexDto::Time => Index::Time,
		IndexDto::Subject => Index::Subject,
		IndexDto::Quality(col) => Index::Quality(col),
		IndexDto::Component(col) => Index::Component(col),
	}
}

fn convert_series(dto: SeriesDto) -> ChartSeries {
	match dto {
		SeriesDto::Subject => ChartSeries::Subject,
		SeriesDto::Quality(col) => ChartSeries::Quality(col),
		SeriesDto::Component(col) => ChartSeries::Component(col),
		SeriesDto::SubjectAndComponent(col) => ChartSeries::SubjectAndComponent(col),
	}
}

/// Convert a DerivationDto to Derivation
fn convert_derivation(dto: DerivationDto) -> Derivation {
	let mut derivation = Derivation::pointwise(
		dto.name, // k
		dto.computation.pointwise,
	)
	.with_kind(dto.kind);

	if let Some(hints_dto) = dto.chart_hints {
		derivation.chart_hints = Some(convert_chart_hints(hints_dto));
	}

	derivation
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_simple_schema_conversion() {
		let dto = SchemaDto {
			name:         "test".into(),
			subject:      "station".into(),
			time:         "timestamp".into(),
			qualities:    vec![],
			measurements: vec![MeasurementDto {
				name:          "water_level".into(),
				kind:          crate::MeasurementKind::Measure,
				components:    vec![],
				signal_policy: None,
				chart_hints:   None,
			}],
			derivations:  vec![],
		};

		let schema = dto.into_schema().unwrap();
		assert_eq!(schema.name, "test");
		assert_eq!(schema.subject.as_str(), "station");
		assert_eq!(schema.time.as_str(), "timestamp");
		assert_eq!(schema.measurements.len(), 1);
		assert_eq!(schema.measurements[0].name, "water_level".into());
	}

	#[test]
	fn test_measurement_with_components() {
		let dto = SchemaDto {
			name:         "test".into(),
			subject:      "station".into(),
			time:         "timestamp".into(),
			qualities:    vec![],
			measurements: vec![MeasurementDto {
				name:          "sales".into(),
				kind:          crate::MeasurementKind::Count,
				components:    vec!["color".into(), "size".into()],
				signal_policy: None,
				chart_hints:   None,
			}],
			derivations:  vec![],
		};

		let schema = dto.into_schema().unwrap();
		let m = &schema.measurements[0];
		assert_eq!(m.components.len(), 2);
		assert_eq!(m.components[0].as_str(), "color");
		assert_eq!(m.components[1].as_str(), "size");
	}

	#[test]
	fn test_quality_conversion() {
		let dto = SchemaDto {
			name:         "test".into(),
			subject:      "station".into(),
			time:         "timestamp".into(),
			qualities:    vec![QualityDto {
				name:        "region".into(),
				chart_hints: None,
			}],
			measurements: vec![],
			derivations:  vec![],
		};

		let schema = dto.into_schema().unwrap();
		assert_eq!(schema.qualities.len(), 1);
		assert_eq!(schema.qualities[0].name, "region".into());
		assert_eq!(schema.qualities[0].subject.as_str(), "station");
		assert_eq!(schema.qualities[0].value.as_str(), "region");
	}

	#[test]
	fn test_chart_hints_conversion() {
		use crate::chart_hints::{ChartType, Index};

		let hints_dto = ChartHintsDto {
			label:      Some("Test Label".into()),
			color:      Some("#ff0000".into()),
			stepped:    Some(true),
			tension:    Some(0.3),
			axis:       Some("y2".into()),
			chart_type: Some(ChartTypeDto::Bar),
			index:      Some(IndexDto::Subject),
			series:     Some(SeriesDto::Component("engine_id".into())),
		};

		let hints = convert_chart_hints(hints_dto);
		assert_eq!(hints.label, Some("Test Label".into()));
		assert_eq!(hints.color, Some("#ff0000".into()));
		assert!(hints.stepped);
		assert_eq!(hints.tension, Some(0.3));
		assert_eq!(hints.axis, AxisId::Y2);
		assert_eq!(hints.chart_type, ChartType::Bar);
		assert_eq!(hints.index, Index::Subject);
	}
}
