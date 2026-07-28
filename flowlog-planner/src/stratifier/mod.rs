//! Partitions a parsed program into dependency-ordered evaluation strata.
//!
//! # Evaluation model
//!
//! A *stratum* is a group of rules evaluated as a unit. Its rules may
//! depend on rules in the same stratum or an earlier one, but never on a
//! later stratum. A dependency cycle makes a stratum *recursive*: it
//! repeats to a fixpoint instead of running once.
//!
//! Explicit `loop` and `fixpoint` blocks are indivisible evaluation
//! barriers: all of a block's rules iterate together under its
//! [`LoopCondition`](flowlog_parser::LoopCondition), and no rule moves
//! across the block boundary.
//!
//! ```text
//! fixpoint {
//!     Reach(x, y) :- Edge(x, y).
//!     Reach(x, z) :- Edge(x, y), Reach(y, z).
//! }
//! ```
//!
//! The `extend-batch` and `extend-inc` modes require recursive rules to
//! appear inside one of these blocks; the Datalog modes also allow
//! recursion in plain rules.
//!
//! # Layout
//!
//! - `dependency_graph`: rule dependency edges within a segment.
//! - `scc`: dependency cycles, found and ordered for evaluation.
//! - `core`: the [`Stratifier`] driver; explicit loops and relation
//!   metadata.
//! - `error`: user diagnostics for structurally invalid programs.

mod core;
mod dependency_graph;
mod error;
mod scc;

pub(crate) use self::core::Stratifier;
pub(crate) use self::core::Stratum;
