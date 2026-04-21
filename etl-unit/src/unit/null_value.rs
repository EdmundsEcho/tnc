use polars::prelude::{AnyValue, DataType, Expr, LiteralValue, Scalar, lit};
use serde::{Deserialize, Serialize};

/// A value that can be used to fill nulls (supports multiple types)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum NullValue {
	String(String),
	Integer(i64),
	Float(f64),
	Boolean(bool),
}

// Allow converting NullValue -> Polars Expr (for .fill_null())
impl From<&NullValue> for Expr {
	fn from(val: &NullValue) -> Self {
		match val {
			NullValue::String(s) => lit(s.as_str()),
			NullValue::Integer(i) => lit(*i),
			NullValue::Float(f) => lit(*f),
			NullValue::Boolean(b) => lit(*b),
		}
	}
}

impl From<NullValue> for Expr {
	fn from(val: NullValue) -> Self {
		match val {
			NullValue::String(s) => lit(s),
			NullValue::Integer(i) => lit(i),
			NullValue::Float(f) => lit(f),
			NullValue::Boolean(b) => lit(b),
		}
	}
}
impl From<LiteralValue> for NullValue {
	fn from(val: LiteralValue) -> Self {
		match val {
			LiteralValue::Scalar(scalar) => {
				match scalar.as_any_value() {
					AnyValue::Boolean(b) => NullValue::Boolean(b),
					AnyValue::String(s) => NullValue::String(s.to_string()),
					AnyValue::StringOwned(s) => NullValue::String(s.to_string()),
					AnyValue::Int64(i) => NullValue::Integer(i),
					AnyValue::Int32(i) => NullValue::Integer(i as i64),
					AnyValue::Float64(f) => NullValue::Float(f),
					AnyValue::Float32(f) => NullValue::Float(f as f64),
					v => panic!("Unsupported Scalar value for NullValue: {:?}", v),
				}
			}
			_ => panic!("Unsupported LiteralValue variant for NullValue: {:?}", val),
		}
	}
}

// Conversion required for .into_lit() to work
impl From<NullValue> for LiteralValue {
	fn from(val: NullValue) -> Self {
		let (dt, av) = match val {
			NullValue::Boolean(b) => (DataType::Boolean, AnyValue::Boolean(b)),
			NullValue::String(s) => (DataType::String, AnyValue::StringOwned(s.into())),
			NullValue::Integer(i) => (DataType::Int64, AnyValue::Int64(i)),
			NullValue::Float(f) => (DataType::Float64, AnyValue::Float64(f)),
		};
		LiteralValue::Scalar(Scalar::new(dt, av))
	}
}

impl NullValue {
	pub fn string(s: impl Into<String>) -> Self {
		Self::String(s.into())
	}

	pub fn int(i: i64) -> Self {
		Self::Integer(i)
	}

	pub fn float(f: f64) -> Self {
		Self::Float(f)
	}

	pub fn bool(b: bool) -> Self {
		Self::Boolean(b)
	}

	pub fn into_lit(&self) -> LiteralValue {
		self.clone().into()
	}

	pub fn into_expr(&self) -> Expr {
		self.clone().into()
	}
}
