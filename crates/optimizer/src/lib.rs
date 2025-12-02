//! FlowLog Optimizer Library
//! This crate provides two handlers:
//! * Reader handler: load / update base (EDB) relation cardinalities.
//! * Planner handler: given a stratum catalog collection, build `PlanTree`s per rule.
//!   (Currently left‑to‑right only; future phases may add cost-based ordering.)
//!
//! All interfaces are intentionally lightweight to allow later extension.

pub mod optimizer;
pub mod plan_tree;

pub use optimizer::Optimizer;
pub use plan_tree::PlanTree;
