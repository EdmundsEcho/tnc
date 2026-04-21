//! The plan layer for the ETL pipeline.
//!
//! Plans express *what* the runtime is going to do — they're typed,
//! structurally validated descriptions of the work, decoupled from the
//! executor that walks them. The plan layer is the substrate shared by
//! the runtime query path and the store-builder path.
//!
//! # Architecture
//!
//! ```text
//! Runtime path:
//!   FilterPlan → CrushPlan(Components) → SignalPolicy → JoinPlan → Derivations
//!
//! Store-builder path:
//!   FilterPlan → CrushPlan(Time)                                  → Parquet sink
//! ```
//!
//! The shared `ExtractionCore { sources, filter, crush }` is consumed by
//! both `RuntimePlan` and `BuildPlan`. Type-safe dispatch — `RuntimePlan`
//! cannot be passed to a build executor and vice versa.
//!
//! # Module layout
//!
//! - [`bindings`] — `ColumnBinding` (key columns) and `CodomainBinding`
//!   (value columns with null-fill metadata).
//! - [`source_context`] — `SourceKey`, `SourceContext`, `SourceMember`.
//!   The shared per-source invariants every plan node references.
//! - [`filter`] — `FilterPlan`, `SourceFilter`, `NullFill`.
//! - [`crush`] — `CrushPlan`, `Crush` (variants `Components` and `Time`),
//!   `CrushMember`.
//! - [`join`] — `JoinPlan`, `SourceJoin`, `JoinColumn`, `JoinKeys`.
//! - [`core`] — `ExtractionCore`, the shared filter+crush substrate.
//! - [`runtime`] — `RuntimePlan`, `DerivationPlan`. Composes
//!   `ExtractionCore` with the runtime-only join + derivation phases.
//! - [`build`] — `BuildPlan`, `BuildSink`. Composes `ExtractionCore`
//!   with a sink for the store-builder pipeline.
//! - [`processing_plan`] — `ProcessingPlan`, the additive top-level
//!   wrapper enum over `RuntimePlan` and `BuildPlan`. Used at boundaries
//!   where the caller wants one type to mean "any pipeline plan."
//!
//! # Relationship to `BindingRule`
//!
//! [`crate::BindingRule`] (in `source.rs`) is the *recipe* for deriving
//! a canonical column from a `BoundSource` — `Direct(SourceColumnName)`
//! or `Computed(ColumnExpr)`. The plan layer's [`ColumnBinding`] /
//! [`CodomainBinding`] are *resolved* facts: physical and canonical
//! names are both known and pinned. Rules live on bindings during
//! extraction; resolved bindings live on plans during execution.

pub mod bindings;
pub mod build;
pub mod builder;
pub mod core;
pub mod crush;
pub mod filter;
pub mod join;
pub mod processing_plan;
pub mod runtime;
pub mod source_context;

pub use bindings::{CodomainBinding, ColumnBinding};
pub use build::{BuildPlan, BuildSink};
pub use builder::build_runtime_plan;
pub use core::ExtractionCore;
pub use crush::{Crush, CrushMember, CrushPlan};
pub use filter::{FilterPlan, NullFill, SourceFilter};
pub use join::{GroupSignalConfig, JoinColumn, JoinKeys, JoinPlan, SourceJoin};
pub use processing_plan::ProcessingPlan;
pub use runtime::{DerivationPlan, RuntimePlan};
pub use source_context::{SourceContext, SourceKey, SourceMember};
