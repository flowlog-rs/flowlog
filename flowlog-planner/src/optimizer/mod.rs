//! Join and aggregate order for a rule body.
//!
//! - `core`: the optimizer and the decision it returns for a rule's next
//!   step.
//! - `join_order`: the next join, avoiding cross products and, on an
//!   acyclic body, joins between atoms that are not join-tree neighbors.

pub(crate) mod core;
pub(crate) mod join_order;

pub use self::core::Optimizer;
pub(crate) use self::core::PlanDecision;
