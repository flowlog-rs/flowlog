//! The timely-operator count each codegen pattern expands to, per execution
//! mode. Address prediction multiplies these counts into the operator
//! ranges [`crate::plan::node::Node::operators`] records.
//!
//! The counts are facts about the pinned dependencies, verified against
//! differential-dataflow 0.25 / timely 0.31. A dependency bump that changes
//! an expansion shifts every later address and silently misattributes
//! metrics, so re-verify this table whenever those crates move; an
//! end-to-end profiled run of `example/graph_analysis/reach.dl` shows drift
//! immediately.
//!
//! Every count below composes these primitives, one per DD combinator, each
//! listing the operators it adds. The functions further down sum them:
//!
//! - `stream.as_collection()` (0 ops)
//!   - nothing (just wraps)
//! - `arrangement.as_collection(|k,v| ...)` (1 op)
//!   - AsCollection
//! - `.inner.map(...).as_collection()` (1 op)
//!   - Map
//! - `.consolidate()` (3 ops)
//!   - FlatMap + Consolidate + AsCollection
//! - `.threshold(...)` (4 ops)
//!   - FlatMap + Arrange:Threshold + Threshold + AsCollection
//! - `.threshold_semigroup(...)`, `.threshold_total(...)` (3 ops)
//!   - FlatMap + Arrange:ThresholdTotal + ThresholdTotal

use flowlog_common::ExecutionMode;

/// Operators from arranging a collection, by arrangement kind (the
/// `only_key` split in `register_arrangement`):
///
/// - key-only `arrange_by_self()` (2): ArrangeBySelf + AsCollection
/// - key-value `arrange_by_key()` (1): ArrangeByKey
pub(crate) fn arrange(is_key_only: bool) -> u32 {
    if is_key_only { 2 } else { 1 }
}

/// Operators from `general_aggregate`, the group-by reduce pipeline
/// (`reduce_core` batch / `reduce_abelian` incremental), 4 either way:
///
/// - Map (row chop) + ArrangeByKey + Reduce + AsCollection (merge)
pub(crate) const GENERAL_AGGREGATE: u32 = 4;

/// Operators from `opt_aggregate`, the batch monoid fast path via
/// `threshold_semigroup` (skips the second arrange `reduce` would add). The
/// two Maps are the pre/post `.inner.map().as_collection()`:
///
/// - (5): Map + `.threshold_semigroup()` (3) + Map
pub(crate) const OPT_AGGREGATE: u32 = 5;

/// Operators from the post-leave opt-aggregate step, merging semiring diffs
/// collapsed across iterations (`.consolidate()` then convert back):
///
/// - (4): `.consolidate()` (3) + Map
pub(crate) const POST_LEAVE_OPT_AGGREGATE: u32 = 4;

/// Operators from `flowlog_dedup` at an outer scope (EDBs, rule outputs,
/// SIP projections). Three whichever diff is ambient, differing in which:
///
/// - batch `.consolidate()` (3)
/// - i32 `.threshold_total()` (3)
pub(crate) const DEDUP_NONRECURSIVE: u32 = 3;

/// Operators from a dedup inside `iterate`, whether the retained
/// `flowlog_dedup_retained` on feedback or `flowlog_dedup` on a SIP
/// projection:
///
/// - batch `.threshold_semigroup(...)` / `.consolidate()` (3)
/// - incremental `.threshold(...)` (4): `Product<u32, u16>` is not
///   totally ordered
pub(crate) fn dedup_recursive(mode: ExecutionMode) -> u32 {
    match mode {
        ExecutionMode::Batch => 3,
        ExecutionMode::Inc => 4,
    }
}

/// Operators in `flowlog_antijoin` (excluding arrangement), by DD
/// operator. The deref and projection steps are `.flat_map`s (hence
/// FlatMap), as is the weight adjust on the `Present` path, while the
/// `i32` path negates in place (MapInPlace); `join_core` is Join, `.concat`
/// is Concatenate. `dedup` is the dedup expansion, 3 via `threshold_total` or
/// 4 via `threshold` (see [`DEDUP_NONRECURSIVE`] / [`dedup_recursive`]).
///
/// - Batch (9): FlatMap (deref) + FlatMap (pos weight) + Join
///   + FlatMap (neg weight) + Concatenate + FlatMap (project) + dedup (3)
/// - Inc, recursive scope (17): FlatMap + dedup (4) + Join + dedup (4)
///   + FlatMap + Concatenate + FlatMap + dedup (4)
/// - Inc, outer scope (14): FlatMap + dedup (3) + Join + dedup (3)
///   + FlatMap + Concatenate + FlatMap + dedup (3)
pub(crate) fn anti_join(mode: ExecutionMode, recursive: bool) -> u32 {
    match mode {
        ExecutionMode::Batch => 9,
        ExecutionMode::Inc => {
            if recursive {
                17
            } else {
                14
            }
        }
    }
}

/// Operators in `gen_size_inspector`, nine whichever diff is ambient: the
/// two modes spend their FlatMap and Probe differently.
///
/// - Batch (9): `flowlog_dedup` (3) + FlatMap (lift to `i32`)
///   + FlatMap (collapse onto one key) + `.consolidate()` (3) + InspectBatch
/// - Others (9): `flowlog_dedup` (3) + FlatMap (collapse onto one key)
///   + `.consolidate()` (3) + InspectBatch + Probe
pub(crate) const INSPECT_SIZE: u32 = 9;

/// Operators in content inspectors (terminal/file). Not exercised by the
/// reach fixture, so the names are from codegen, not a run.
///
/// - Batch (1): InspectBatch
/// - Inc (5): `.consolidate()` (3) + InspectBatch + Probe
pub(crate) fn inspect_content(mode: ExecutionMode) -> u32 {
    match mode {
        ExecutionMode::Inc => 5,
        ExecutionMode::Batch => 1,
    }
}
