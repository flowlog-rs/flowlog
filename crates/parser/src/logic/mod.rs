//! Logic components for Macaron Datalog programs.
//!
//! This module exposes the core logic layer used by the parser and planner:
//! - [`rule`]: rules (head + body)
//! - [`head`]: rule heads and head arguments
//! - [`predicate`]: predicates appearing in rule bodies
//! - [`atom`]: atoms (relation + arguments)
//! - [`arithmetic`]: arithmetic expressions and factors
//! - [`comparison`]: comparison expressions and operators
//! - [`aggregation`]: aggregation operators and wrappers
//!
//! # Example
//! ```rust
//! use parser::logic::{MacaronRule, Head, HeadArg, Predicate};
//!
//! // result(X) :- input(X).
//! let head = Head::new("result".to_string(), vec![HeadArg::Var("X".to_string())]);
//! let body = vec![Predicate::BoolPredicate(true)]; // adjust to your real body/predicate
//! let rule = MacaronRule::new(head, body, false);
//! ```

pub mod aggregation;
pub mod arithmetic;
pub mod atom;
pub mod comparison;
pub mod head;
pub mod predicate;
pub mod rule;

// Re-exports for a convenient public surface.
pub use aggregation::{Aggregation, AggregationOperator};
pub use arithmetic::{Arithmetic, ArithmeticOperator, Factor};
pub use atom::{Atom, AtomArg};
pub use comparison::{ComparisonExpr, ComparisonOperator};
pub use head::{Head, HeadArg};
pub use predicate::Predicate;
pub use rule::MacaronRule;
