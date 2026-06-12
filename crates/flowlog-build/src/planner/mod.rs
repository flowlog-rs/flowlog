//! Query planning: lower stratified rules into a shared dataflow plan.
//!
//! A [`ProgramPlanner`] turns a stratified program into one [`StratumPlanner`]
//! per stratum. Within a stratum, each rule is planned independently into a
//! pipeline of [`Transformation`]s (prepare → SIP → core join → fuse → post),
//! and then the per-rule pipelines are **shared** so identical work is emitted
//! once.
//!
//! # Arrangement / transformation sharing
//!
//! Sharing is driven entirely by a **content-canonical fingerprint** computed
//! when each rule's plan is materialized, then exploited by three passes that
//! all key on that fingerprint:
//!
//! 1. **Materialize** ([`RulePlanner::materialize`] → [`Transformation::from_info`]):
//!    rewrite each operation's *lineage* fingerprint (which embeds rule-local
//!    atom positions and so never matches across rules) into a content fp =
//!    `hash(variant tag, resolved input fps, flow)`. Identical operations from
//!    different rules now carry the same fingerprint.
//! 2. **Within-stratum dedup** ([`StratumPlanner`]): keep the first occurrence
//!    of each content fingerprint, in topological order.
//! 3. **Recursion factoring** ([`StratumPlanner`]): split a recursive stratum's
//!    transformations into *static* (EDB / earlier-stratum inputs — built once
//!    and `.enter()`-ed into the loop) and *dynamic* (IDB-dependent — rebuilt
//!    each round), so loop-invariant arrangements are not rebuilt per round.
//! 4. **Cross-stratum prune** ([`ProgramPlanner`]): drop a non-recursive
//!    transformation whose content fingerprint was already emitted by an
//!    earlier stratum, unless an intervening stratum rewrote an IDB it depends
//!    on (the extended-mode soundness gate).
//!
//! Because every pass keys on the same content fingerprint, two rules that
//! reach the same arrangement via different shapes still share it, and a
//! relation read after all its producers is always read in its complete form.


mod argument;
mod arithmetic;
mod collection;
mod compare;
mod constraint;
mod error;
mod fn_call;
mod program_planner;
mod rule_planner;
mod stratum_planner;
mod transformation;

// External API — used by flowlog-compiler and integration tests.
pub use error::PlanError;
pub use program_planner::ProgramPlanner;
pub use stratum_planner::StratumPlanner;

// Intra-crate shortcuts.
pub(crate) use argument::TransformationArgument;
pub(crate) use arithmetic::{ArithmeticArgument, FactorArgument};
pub(crate) use collection::Collection;
pub(crate) use compare::ComparisonExprArgument;
pub(crate) use constraint::Constraints;
pub(crate) use fn_call::FnCallPredicateArgument;
pub(crate) use rule_planner::RulePlanner;
pub(crate) use transformation::{
    KeyValueLayout, Transformation, TransformationFlow, TransformationInfo,
};
