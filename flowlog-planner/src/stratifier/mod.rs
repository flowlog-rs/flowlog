//! Partitions a parsed program into dependency-ordered evaluation strata.
//!
//! # Evaluation model
//!
//! A stratum is a group of rules evaluated as a unit. Its rules may depend
//! on rules in the same stratum or an earlier one, but never on a later
//! stratum. A dependency cycle makes a stratum recursive: it repeats to a
//! fixpoint instead of running once.
//!
//! Explicit `loop` and `fixpoint` blocks remain indivisible evaluation
//! barriers. All rules in the block iterate together under its
//! [`LoopCondition`](flowlog_parser::LoopCondition). The `extend-batch` and
//! `extend-inc` modes require recursive rules to appear inside one of these
//! blocks; the Datalog modes also allow recursion in plain rules.
//!
//! `dependency_graph` builds rule edges, `scc` orders plain-rule components,
//! `core` handles explicit loops and relation metadata, and `error` renders
//! user diagnostics.

mod core;
mod dependency_graph;
mod error;
mod scc;

pub(crate) use self::core::Stratifier;
pub(crate) use self::core::Stratum;
