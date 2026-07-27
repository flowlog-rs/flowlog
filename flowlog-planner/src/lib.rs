//! Semantic analysis and query planning: the pipeline stages between the
//! parsed AST and code generation (`flowlog-build`).
//!
//! - [`catalog`]: per-rule metadata (signatures, pushdown filters, range
//!   checks) consumed by the planner.
//! - [`stratifier`]: groups rules into strata so every rule's dependencies
//!   are computed before it fires.
//! - [`planner`]: lowers each stratum's rules into transformation plans.
//! - [`optimizer`]: cardinality-based join ordering over catalog data.

pub mod catalog;
pub mod optimizer;
pub mod planner;
pub mod stratifier;
