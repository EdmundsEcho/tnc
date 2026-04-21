//! Semantic data model for ETL units.

pub mod aggregation;
pub mod binder;
pub mod chart_hints;
pub mod column;
pub mod component_filter;
pub mod composition;
pub mod error;
pub mod expr;
pub mod interval;
pub mod pipeline;
pub mod plan;
pub(crate) mod polars_fns;
pub mod request;
pub mod schema;
pub mod signal_policy;
pub mod source;
pub mod subject;
pub mod subset;
pub mod time_range;
pub mod unit;
pub mod unit_ref;
pub mod universe;
pub mod unpivot;

// Re-export primary types
pub use aggregation::{Aggregate, SyntheticSubject};
pub use chart_hints::{AxisId, ChartHints, ChartSeries, ChartType, Index};
pub use column::{CanonicalColumnName, ColumnNameExt, DomainSignature, SourceColumnName};
pub use component_filter::*;
pub use composition::{ComponentReduction, CompositionPlan, CompositionStrategy};
pub use error::{EtlError, EtlResult};
pub use expr::{ColumnExpr, EpochUnit};
pub use request::{EtlUnitSubsetRequest, Interval, QualityFilter, SubjectFilter, TimeRange};
pub use schema::{EtlSchema, EtlSchemaBuilder};
pub use signal_policy::{SignalPolicy, WindowStrategy};
pub use source::{BindingRule, BoundSource, DedupStrategy, EtlUniverseBuildPlan, SignalPolicyMode, StackConfig};
// Plan layer — primitives, stage plans, and top-level plans.
pub use plan::{
	BuildPlan, BuildSink, CodomainBinding, ColumnBinding, DerivationPlan, ExtractionCore,
	ProcessingPlan, RuntimePlan, SourceContext, SourceKey, SourceMember,
};
pub use subset::{MeasurementMeta, QualityMeta, SubsetExecutor, SubsetInfo, SubsetUniverse};
pub use time_range::EtlTimeRange;
// Re-export derivation types
pub use unit::{Computation, Derivation, OverSubjectExpr, PointwiseExpr, TimeExpr, TimeUnit};
// Re-export unit types
pub use unit::{
	DataTemporality, MeasurementKind, MeasurementUnit, NullValue, QualityUnit,
	ResampleStrategy, TruthMapping,
};
pub use binder::SourceBinder;
pub use subject::{SubjectType, SubjectValue};
pub use unit_ref::EtlUnitRef;
pub use universe::{
	AlignAction, AlignmentSpec, CompositionSummary, MeasurementAlignment, MemorySummary,
	Universe, UniverseBuildInfo, UniverseBuilder, extract_all_fragments,
};
// Re-export unpivot types
pub use unpivot::UnpivotConfig;
