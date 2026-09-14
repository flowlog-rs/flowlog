//! Per-rule metadata for planning.
//!
//! A [`Catalog`] digests one rule into compact signatures for atoms and
//! arguments, local filters, and variable-occurrence facts that the
//! planner and optimizer query while ordering joins.
//!
//! # Layout
//!
//! - `rule`: the [`Catalog`] itself, its population and rewrites.
//! - `atom`, `arithmetic`, `compare`: positional signatures for atom
//!   arguments, arithmetic expressions, and comparisons.
//! - `predicate`: join and key-value predicate bundles for the planner.
//! - `filter`: one rule's constant, equality, and placeholder filters.
//! - `error`: user diagnostics and catalog ICEs.

mod arithmetic;
mod atom;
mod compare;
mod error;
mod filter;
mod predicate;
mod rule;

pub(crate) use arithmetic::ArithmeticPos;
pub(crate) use arithmetic::FactorPos;
pub(crate) use atom::AtomArgumentSignature;
pub(crate) use atom::AtomSignature;
pub(crate) use compare::ComparisonExprPos;
pub(crate) use error::CatalogError;
pub(crate) use error::UnsafePredicateKind;
pub(crate) use filter::Filters;
pub(crate) use predicate::JoinPredicates;
pub(crate) use predicate::KvPredicates;
pub(crate) use rule::Catalog;
