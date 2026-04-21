use polars::{
	prelude::{
		DataFrame, DataType, IntoLazy, RollingOptionsFixedWindow, SortMultipleOptions, col, lit,
	},
	series::ops::NullBehavior,
};

use crate::{EtlResult, EtlSchema, TimeExpr, TimeUnit};

/// Compute an over_time derivation.
pub(crate) fn compute_over_time(
	df: DataFrame,
	output_name: &str,
	expr: &TimeExpr,
	schema: &EtlSchema,
) -> EtlResult<DataFrame> {
	let subject_col = schema.subject.as_str();
	let result = match expr {
		TimeExpr::Derivative {
			input,
			time_unit,
		} => compute_derivative(&df, output_name, input, schema, time_unit)?,
		TimeExpr::RollingMean {
			input,
			window,
		} => {
			df.lazy()
				.with_column(
					col(input)
						.rolling_mean(RollingOptionsFixedWindow {
							window_size: *window,
							min_periods: 1,
							..Default::default()
						})
						.over([col(subject_col)])
						.alias(output_name),
				)
				.collect()?
		}
		TimeExpr::RollingSum {
			input,
			window,
		} => {
			df.lazy()
				.with_column(
					col(input)
						.rolling_sum(RollingOptionsFixedWindow {
							window_size: *window,
							min_periods: 1,
							..Default::default()
						})
						.over([col(subject_col)])
						.alias(output_name),
				)
				.collect()?
		}
		TimeExpr::Lag {
			input,
			periods,
		} => {
			df.lazy()
				.with_column(
					col(input)
						.shift(lit(*periods as i64))
						.over([col(subject_col)])
						.alias(output_name),
				)
				.collect()?
		}
		TimeExpr::Lead {
			input,
			periods,
		} => {
			df.lazy()
				.with_column(
					col(input)
						.shift(lit(-(*periods as i64)))
						.over([col(subject_col)])
						.alias(output_name),
				)
				.collect()?
		}
		TimeExpr::CumSum {
			input,
		} => {
			df.lazy()
				.with_column(
					col(input)
						.cum_sum(false)
						.over([col(subject_col)])
						.alias(output_name),
				)
				.collect()?
		}
		TimeExpr::Diff {
			input,
			periods,
		} => {
			df.lazy()
				.with_column(
					col(input)
						.diff(lit(*periods as i64), NullBehavior::Ignore)
						.over([col(subject_col)])
						.alias(output_name),
				)
				.collect()?
		}
	};

	Ok(result)
}

/// Compute derivative (rate of change) per subject over time.
pub(crate) fn compute_derivative(
	df: &DataFrame,
	output_name: &str,
	input: &str,
	schema: &EtlSchema,
	time_unit: &TimeUnit,
) -> EtlResult<DataFrame> {
	let subject_col = schema.subject.as_str();
	let time_col = schema.time.as_str();
	let time_divisor = time_unit.from_microseconds();
	let original_cols: Vec<String> = df
		.get_column_names()
		.iter()
		.map(|s| s.to_string())
		.collect();

	let result = df
		.clone()
		.lazy()
		.sort([subject_col, time_col], SortMultipleOptions::default())
		.with_columns([
			col(input)
				.diff(lit(1), NullBehavior::Ignore)
				.over([col(subject_col)])
				.alias("_dv"),
			col(time_col)
				.diff(lit(1), NullBehavior::Ignore)
				.over([col(subject_col)])
				.dt()
				.total_microseconds()
				.alias("_dt_us"),
		])
		.with_column(
			(col("_dv") / (col("_dt_us").cast(DataType::Float64) / lit(time_divisor)))
				.alias(output_name),
		)
		.select(
			original_cols
				.iter()
				.map(|c| col(c.as_str()))
				.chain(std::iter::once(col(output_name)))
				.collect::<Vec<_>>(),
		)
		.collect()?;

	Ok(result)
}
