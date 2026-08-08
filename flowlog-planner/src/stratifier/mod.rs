//! Partitions a parsed program into dependency-ordered evaluation strata.
//!
//! # Evaluation model
//!
//! A *stratum* is a group of rules evaluated as a unit. Its rules may
//! depend on rules in the same stratum or an earlier one, but never on a
//! later stratum. A dependency cycle makes a stratum *recursive*: it
//! repeats to a fixpoint instead of running once.
//!
//! ```text
//! Reach(x, y) :- Edge(x, y).
//! Reach(x, z) :- Edge(x, y), Reach(y, z).
//! ```
//!
//! # Layout
//!
//! - `dependency_graph`: rule dependency edges between rules.
//! - `scc`: dependency cycles, found and ordered for evaluation.
//! - `core`: the [`Stratifier`] driver and relation metadata.
//! - `error`: user diagnostics for structurally invalid programs.

mod core;
mod dependency_graph;
mod error;
mod scc;

pub(crate) use self::core::Stratifier;
pub(crate) use self::core::Stratum;
