//! Macaron Catalog Library
//!
//! This crate provides per-rule metadata and utilities used by the planner.
//! It builds compact signatures for atoms/arguments, detects core
//! atoms, records local filters, and exposes helpers to reason about variable
//! occurrence and comparisons.

/// Arithmetic utilities for tracking positions inside head expressions.
pub mod arithmetic;
/// Atom and argument signature types used to uniquely identify positions in rules.
pub mod atom;
/// Comparison helpers for tracking variable sets of comparison predicates.
pub mod compare;
/// Base constraint filters (var==var, var==const, placeholders).
pub mod filter;
/// Per-rule catalog with precomputed metadata and signatures.
pub mod rule;

/// Re-exported.
pub use arithmetic::{ArithmeticPos, FactorPos};
pub use atom::{AtomArgumentSignature, AtomSignature};
pub use compare::ComparisonExprPos;
pub use filter::Filters;
