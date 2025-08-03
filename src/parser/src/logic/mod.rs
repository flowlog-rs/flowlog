//! Logic components for FlowLog programs.
//!
//! This module contains the core logic structures used in FlowLog programs,
//! including rules, predicates, arithmetic expressions, and atoms.
//!
//! # Components
//!
//! - [`rule`]: FlowLog rules with heads and bodies
//! - [`head`]: Rule heads with arguments
//! - [`predicate`]: Predicates used in rule bodies
//! - [`atom`]: Atom expressions used in predicates
//! - [`arithmetic`]: Arithmetic expressions and operations used in predicates
//! - [`comparison`]: Comparison expressions and operators used in predicates
//!
//! # Examples
//!
//! ```rust
//! use parser::logic::{FLRule, Head, HeadArg, Predicate};
//!
//! // Create a simple rule: result(X) :- input(X).
//! let head = Head::new("result".to_string(), vec![HeadArg::Var("X".to_string())]);
//! let body = vec![Predicate::BoolPredicate(true)];
//! let rule = FLRule::new(head, body, false);
//! ```

pub mod arithmetic;
pub mod atom;
pub mod comparison;
pub mod head;
pub mod predicate;
pub mod rule;

// Re-export main types for easier access
pub use arithmetic::{Arithmetic, ArithmeticOperator, Factor};
pub use atom::{Atom, AtomArg};
pub use comparison::{ComparisonExpr, ComparisonOperator};
pub use head::{Head, HeadArg};
pub use predicate::Predicate;
pub use rule::FLRule;
