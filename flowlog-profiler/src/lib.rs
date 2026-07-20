//! FlowLog's profiling model: the plan graph written at compile time, and
//! the reader that joins a profiled run's metrics back onto it.
//!
//! - [`PlanGraph`]: the shared model. flowlog-build records it through
//!   [`with_plan_graph`] / [`try_with_plan_graph`]; every consumer
//!   deserializes it and reads its nodes and rules to interpret metrics
//!   and derive its own view.
//! - [`metrics`]: [`metrics::read()`] joins a run's logs onto a plan and
//!   returns per-transaction [`metrics::Snapshot`]s -- per-node and
//!   per-operator *measured facts* keyed to plan ids, with no plan
//!   structure re-shipped.
//!
//! The split is deliberate: the profiler exposes facts keyed to the plan;
//! a consumer holds the [`PlanGraph`] and joins by node id, deriving
//! roots, trees, and badges itself. There is no rendering "report" type
//! here -- that belongs to whatever renders.

mod addr;
mod error;
pub mod metrics;
mod plan;

// Crate-internal spellings; external consumers reach these types only
// through the fields and returns of the public API below.
pub(crate) use addr::Addr;
pub(crate) use error::ProfilerError;
pub use metrics::Stats;
pub use plan::PlanGraph;
// The record API offered to the producing crate (flowlog-build); the
// fallible variant serves recording steps whose failure must surface
// (e.g. an unbalanced scope exit).
pub use plan::graph::try_with_plan_graph;
pub use plan::graph::with_plan_graph;
