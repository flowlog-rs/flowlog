//! FlowLog Optimizer Library
//! This crate provides two handlers:
//! * Reader handler: load / update base (EDB) relation cardinalities.
//! * Planner handler: given a stratum catalog collection, build `PlanTree`s per rule.
//!   (Currently left‑to‑right only; future phases may add cost-based ordering.)
//!
//! All interfaces are intentionally lightweight to allow later extension.

pub(crate) mod canonical;
pub(crate) mod cardinality;
pub(crate) mod core;
pub(crate) mod error;
pub(crate) mod feedback;
pub(crate) mod hog;
pub(crate) mod tree;

pub use self::core::Optimizer;
