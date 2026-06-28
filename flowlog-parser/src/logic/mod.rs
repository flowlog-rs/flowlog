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
mod tuple;

// Operator enums — re-exported from the crate root; they leak through
// downstream public types (e.g. `TypeCheckError` variants, `Features`).
pub use aggregation::AggregationOperator;
pub use arithmetic::ArithmeticOperator;
pub use builtin::BuiltinOperator;
pub use comparison::ComparisonOperator;
pub use rule::FlowLogRule;

// Re-exported from the crate root for downstream pipeline crates.
pub use aggregation::Aggregation;
pub use arithmetic::{Arithmetic, Factor};
pub use atom::{Atom, AtomArg};
pub use builtin::BuiltinCall;
pub use comparison::ComparisonExpr;
pub use fn_call::FnCall;
pub use head::HeadArg;
pub use loop_block::{IterativeDirective, LoopCondition, LoopConnective};
pub use predicate::Predicate;
pub use tuple::{TupleElem, TupleLit};

// Intra-crate only.
pub(crate) use head::Head;
pub(crate) use loop_block::LoopBlock;
pub(crate) use rule::{apply_indices_to_rule, consume_plan_directive, parse_plan_indices};
