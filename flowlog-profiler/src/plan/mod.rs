//! The compile-time plan graph: the model `ops.json` serializes.
//!
//! While lowering a program to a dataflow, the compiler records what it
//! built here. Each logical step becomes a `node::Node` (say "load
//! `edge`", "join `reach` with `edge`", "dedup", or "print size"), tagged
//! with the `block::Block` it runs in and the run of consecutive timely
//! operator addresses it is predicted to occupy. Those addresses are the
//! hinge the profiler later uses to bind a measured metric back to the
//! step that produced it.
//!
//! So the recursive rule
//!
//! ```text
//! reach(x, y) :- reach(x, z), edge(z, y).
//! ```
//!
//! lands under `stratum 0` as a short chain of nodes (arrange each side,
//! join, map to the head, dedup), beside a `rule::Rule` carrying the
//! rule's text and the transformation tree the join came from.
//!
//! The pieces:
//!
//! - `graph`: [`crate::PlanGraph`], the aggregate everything records into
//! - `node`: one logical step and its predicted operator range
//! - `rule`: rule text and its transformation DAG
//! - `block`: which dataflow region a node runs in
//! - `builder`: the recording methods the compiler calls while lowering
//! - `manager`: the node-id and operator-address allocator behind them
//! - `steps`: how many timely operators a step expands to, per mode

pub(crate) mod block;
pub(crate) mod builder;
pub(crate) mod graph;
pub(crate) mod manager;
pub(crate) mod node;
pub(crate) mod rule;
mod steps;

// The plan is the shared read model: consumers deserialize a [`PlanGraph`]
// and read its [`Node`]s and [`Rule`]s to interpret metrics. The recording
// machinery (`builder`, `manager`, `steps`) stays crate-internal.
pub use graph::PlanGraph;
pub use node::Node;
