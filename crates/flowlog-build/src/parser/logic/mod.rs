//! Logic components for FlowLog Datalog programs.
//!
//! This module exposes the core logic layer used by the parser and planner:
//! - [`rule`][]: rules (head + body)
//! - [`head`]: rule heads and head arguments
//! - [`predicate`]: predicates appearing in rule bodies
//! - [`atom`][]: atoms (relation + arguments)
//! - [`arithmetic`]: arithmetic expressions and factors
//! - [`comparison`]: comparison expressions and operators
//! - [`aggregation`]: aggregation operators and wrappers

mod aggregation;
mod arithmetic;
mod atom;
mod builtin;
mod comparison;
mod fn_call;
mod head;
mod loop_block;
mod predicate;
mod rule;

// Re-exports for a flat intra-crate path; the outer parser/mod.rs
// re-re-exports the externally-needed ones at `parser::*`.
//
// Externally-visible: widened to `pub` so parser/mod.rs can re-export them.
pub use aggregation::AggregationOperator;
pub use arithmetic::ArithmeticOperator;
pub use builtin::BuiltinOperator;
pub use comparison::ComparisonOperator;
pub use rule::FlowLogRule;

// Intra-crate only.
pub(crate) use aggregation::Aggregation;
pub(crate) use arithmetic::{Arithmetic, Factor};
pub(crate) use atom::{Atom, AtomArg};
pub(crate) use builtin::BuiltinCall;
pub(crate) use comparison::ComparisonExpr;
pub(crate) use fn_call::FnCall;
pub(crate) use head::{Head, HeadArg};
pub(crate) use loop_block::{IterativeDirective, LoopBlock, LoopCondition, LoopConnective};
pub(crate) use predicate::Predicate;
pub(crate) use rule::{apply_indices_to_rule, consume_plan_directive, parse_plan_indices};
