//! FlowLog Catalog Library
//!
//! This crate provides per-rule metadata and utilities used by the planner.
//! It builds compact signatures for atoms/arguments, detects core
//! atoms, records local filters, and exposes helpers to reason about variable
//! occurrence and comparisons.

mod arithmetic;
mod atom;
mod compare;
mod error;
mod filter;
mod predicate;
mod rule;

// External API — used by integration tests.
// Intra-crate shortcuts.
pub(crate) use arithmetic::ArithmeticPos;
pub(crate) use arithmetic::FactorPos;
pub(crate) use atom::AtomArgumentSignature;
pub(crate) use atom::AtomSignature;
pub(crate) use compare::ComparisonExprPos;
pub use error::CatalogError;
pub use error::UnsafePredicateKind;
pub(crate) use filter::Filters;
pub(crate) use predicate::JoinPredicates;
pub(crate) use predicate::KvPredicates;
pub use rule::Catalog;
