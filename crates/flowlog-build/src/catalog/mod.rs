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
mod fn_call;
mod predicate;
mod rule;

// External API — used by integration tests.
pub use error::{CatalogError, UnsafePredicateKind};
pub use rule::Catalog;

// Intra-crate shortcuts.
pub(crate) use arithmetic::{ArithmeticPos, FactorPos};
pub(crate) use atom::{AtomArgumentSignature, AtomSignature};
pub(crate) use compare::ComparisonExprPos;
pub(crate) use filter::Filters;
pub(crate) use fn_call::FnCallPredicatePos;
pub(crate) use predicate::{JoinPredicates, KvPredicates};
