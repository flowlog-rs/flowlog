//! Rule AST: node types in containment order (defining file in parentheses).
//!
//! ```text
//! FlowLogRule (rule)
//! |-- Head (head)
//! |   `-- HeadArg: Var | Arith(Arithmetic) | Aggregation (aggregation)
//! `-- body: Predicate list (predicate)
//!     |-- PositiveAtom / NegativeAtom: Atom (atom)
//!     |   `-- AtomArg: Var | Const(Constant) (constant) | Placeholder
//!     `-- Compare: ComparisonExpr (comparison)
//!         `-- Arithmetic (arithmetic)
//!             `-- Factor: Var | Const(Constant) | FnCall (fn_call) | Builtin (builtin) | Cast (cast) | Group | Tuple (tuple) | TupleProj
//! ```
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
mod tuple;

// Re-exported at the crate root for the downstream pipeline crates.
pub use aggregation::Aggregation;
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
pub use tuple::TupleElem;
pub use tuple::TupleLit;
