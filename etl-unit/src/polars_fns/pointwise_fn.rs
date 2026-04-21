use polars::prelude::{DataFrame, IntoLazy};

use crate::{EtlResult, unit::PointwiseExpr};

/// Compute a pointwise derivation.
/// TODO: Canonical name
pub(crate) fn compute_pointwise(df: DataFrame, output_name: &str, expr: &PointwiseExpr) -> EtlResult<DataFrame> {
	let polars_expr = expr.to_polars_expr(output_name)?;
	let result = df.lazy().with_column(polars_expr).collect()?;
	Ok(result)
}
