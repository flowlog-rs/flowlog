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
// Re-exported from the crate root for downstream pipeline crates.
pub use aggregation::Aggregation;
pub use aggregation::AggregationOperator;
pub use arithmetic::Arithmetic;
pub use arithmetic::ArithmeticOperator;
pub use arithmetic::Cast;
pub use arithmetic::Factor;
pub use atom::Atom;
pub use atom::AtomArg;
pub use builtin::BuiltinCall;
pub use builtin::BuiltinOperator;
pub use comparison::ComparisonExpr;
pub use comparison::ComparisonOperator;
pub use fn_call::FnCall;
pub use head::Head;
pub use head::HeadArg;
pub use loop_block::IterativeDirective;
pub use loop_block::LoopBlock;
pub use loop_block::LoopCondition;
pub use loop_block::LoopConnective;
pub use loop_block::StopGroup;
pub use loop_block::StopRelation;
pub use predicate::Predicate;
pub use rule::FlowLogRule;
// Intra-crate only.
pub(crate) use rule::{apply_indices_to_rule, consume_plan_directive, parse_plan_indices};
pub use tuple::TupleElem;
pub use tuple::TupleLit;
