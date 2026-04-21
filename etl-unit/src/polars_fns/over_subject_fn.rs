use polars::prelude::{
	DataFrame, DataType, Expr, IntoLazy, RankMethod, RankOptions, col, lit, when,
};

use crate::{EtlResult, EtlSchema, unit::OverSubjectExpr};

/// Compute an over_subjects derivation.
pub(crate) fn compute_over_subjects(
	df: DataFrame,
	output_name: &str,
	expr: &OverSubjectExpr,
	schema: &EtlSchema,
) -> EtlResult<DataFrame> {
	let time_col = schema.time.as_str();
	let result = match expr {
		OverSubjectExpr::PercentOf {
			input,
		} => df
			.lazy()
			.with_column(
				(col(input) / col(input).sum().over([col(time_col)]) * lit(100.0)).alias(output_name),
			)
			.collect()?,
		OverSubjectExpr::Rank {
			input,
			descending,
		} => {
			let rank_opts = RankOptions {
				method: RankMethod::Ordinal,
				descending: *descending,
			};
			df.lazy()
				.with_column(
					col(input)
						.rank(rank_opts, None)
						.over([col(time_col)])
						.alias(output_name),
				)
				.collect()?
		}
		OverSubjectExpr::ZScore {
			input,
		} => df
			.lazy()
			.with_column(
				((col(input) - col(input).mean().over([col(time_col)]))
					/ col(input).std(1).over([col(time_col)]))
				.alias(output_name),
			)
			.collect()?,
		OverSubjectExpr::DeviationFromMean {
			input,
		} => df
			.lazy()
			.with_column((col(input) - col(input).mean().over([col(time_col)])).alias(output_name))
			.collect()?,
		OverSubjectExpr::Quantile {
			input,
			quantiles,
		} => {
			let rank_opts = RankOptions {
				method: RankMethod::Average,
				descending: false,
			};
			let q = *quantiles as f64;
			df.lazy()
				.with_column({
					let rank_frac = col(input).rank(rank_opts, None).over([col(time_col)])
						/ col(input).count().over([col(time_col)]);
					let scaled = rank_frac * lit(q);
					let truncated = scaled.clone().cast(DataType::Int64);
					let ceiled = truncated.clone()
						+ scaled
							.neq(truncated.clone().cast(DataType::Float64))
							.cast(DataType::Int64);
					ceiled.cast(DataType::UInt32).alias(output_name)
				})
				.collect()?
		}
		OverSubjectExpr::Bucket {
			input,
			breaks,
		} => {
			let bucket_expr = build_bucket_expr(input, breaks, output_name);
			df.lazy().with_column(bucket_expr).collect()?
		}
	};

	Ok(result)
}

/// Build a bucket expression using when/then/otherwise chains.
fn build_bucket_expr(input: &str, breaks: &[i64], output_name: &str) -> Expr {
	if breaks.is_empty() {
		return lit(0i32).alias(output_name);
	}

	let mut expr = lit(breaks.len() as i32);

	for (i, &threshold) in breaks.iter().enumerate().rev() {
		expr = when(col(input).lt_eq(lit(threshold)))
			.then(lit(i as i32))
			.otherwise(expr);
	}

	expr.alias(output_name)
}
