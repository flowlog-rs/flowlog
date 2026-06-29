//! Stratification for FlowLog Datalog programs.
//!
//! This crate analyses a parsed [`Program`] and determines the order in which
//! rule groups (strata) must be evaluated so that every rule's dependencies
//! are fully computed before it fires.
//!
//! # Concepts
//!
//! ## Strata
//!
//! A *stratum* is a set of rules that can be evaluated as a unit.  Rules within
//! the same stratum may depend on each other (forming a strongly-connected
//! component, SCC); rules in stratum *i* may only depend on relations produced
//! by strata *0 … i-1*.
//!
//! ## Recursive vs. Non-recursive strata
//!
//! A stratum is *recursive* when it contains a cycle — either a multi-rule SCC
//! or a single rule that references its own head in its body.  Recursive strata
//! are evaluated recursively until a fixpoint is reached; non-recursive strata
//! are evaluated in a single pass.
//!
//! ## Loop blocks and Extended Datalog mode
//!
//! FlowLog supports an **Extended Datalog** mode (enabled with
//! `--mode extend-batch` or `--mode extend-inc`) where recursion
//! must be written explicitly using `fixpoint` or `loop` blocks:
//!
//! ```text
//! fixpoint {
//!     Reach(x, y) :- Edge(x, y).
//!     Reach(x, z) :- Edge(x, y), Reach(y, z).
//! }
//! ```
//!
//! Each `fixpoint`/`loop` block maps to **exactly one recursive stratum**,
//! regardless of its internal rule structure — all rules inside iterate
//! together under the block's [`LoopCondition`].  In Extended Datalog mode,
//! any recursive dependency found outside a block is a hard error.
//!
//! In datalog modes (`datalog-batch` / `datalog-inc`), recursion in
//! plain rules is allowed and handled implicitly via SCC detection, matching
//! classic stratified-Datalog semantics.

mod core;
mod dependency_graph;
mod error;

pub use error::StratifyError;

pub use self::core::Stratifier;
