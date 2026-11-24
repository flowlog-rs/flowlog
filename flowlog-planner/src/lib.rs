//! Semantic analysis and query planning: the pipeline stages between the
//! parsed AST and code generation (`flowlog-codegen`).
//!
//! - `catalog`: per-rule metadata (signatures, pushdown filters, range checks).
//! - `stratifier`: groups rules into strata so every rule's dependencies
//!   are computed before it fires.
//! - [`planner`]: lowers each stratum's rules into transformation plans.
//! - [`optimizer`]: join and aggregate order for a rule body.

pub(crate) mod catalog;
pub mod optimizer;
pub mod planner;
pub(crate) mod stratifier;
#[cfg(test)]
mod test_harness;
