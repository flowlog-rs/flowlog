//! Rule AST: node types in containment order (defining file in parentheses).
//!
//! ```text
//! FlowLogRule (rule)
//! |-- Head (head)
//! |   `-- HeadArg: Var(Variable) (variable) | Arith(Arithmetic)
//! `-- body: Predicate list (predicate)
//!     |-- PositiveAtom / NegativeAtom: Atom (atom)
//!     |   `-- AtomArg: Var(Variable) | Const(Constant) (constant)
//!     |               | Placeholder
//!     |-- Compare: ComparisonExpr (comparison)
//!     |   `-- Arithmetic (arithmetic)
//!     |       `-- Factor: Var(Variable) | Const(Constant) | FnCall (fn_call)
//!     |               | Builtin (builtin) | Cast (cast) | Group
//!     |               | Tuple | Placeholder | TupleProj
//!     |               | Aggregate(AggregateExpr)
//!     `-- Aggregate: AggregateExpr (aggregation), binding its var
//!         |-- target: Arithmetic
//!         `-- body: Predicate list
//! ```
//!
//! `Factor::Aggregate` and `Factor::Placeholder` are surface syntax: rule
//! construction hoists each aggregate into a `Predicate::Aggregate`
//! binding and names each `_` as an anonymous variable.
//!
//! `plan` holds the `.plan` directive machinery (no AST node of its own).

mod aggregation;
mod arithmetic;
mod atom;
mod builtin;
mod cast;
mod comparison;
mod constant;
mod fn_call;
mod head;
mod predicate;
mod rule;
mod variable;

// Re-exported at the crate root for the downstream pipeline crates.
pub use aggregation::AggregateExpr;
pub use aggregation::AggregationOperator;
pub use arithmetic::Arithmetic;
pub use arithmetic::ArithmeticOperator;
pub use arithmetic::Factor;
pub use atom::Atom;
pub use atom::AtomArg;
pub use builtin::BuiltinCall;
pub use builtin::BuiltinOperator;
pub use cast::Cast;
pub use comparison::ComparisonExpr;
pub use comparison::ComparisonOperator;
pub use constant::Constant;
pub use fn_call::FnCall;
pub use head::Head;
pub use head::HeadArg;
pub use predicate::Predicate;
pub use rule::FlowLogRule;
pub use variable::Variable;
