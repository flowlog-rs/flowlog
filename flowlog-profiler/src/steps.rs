//! Mode-dependent operator step counts for profiler address tracking.
//!
//! **WARNING**: The step counts below have only been verified for `DatalogBatch`
//! and `DatalogInc` modes. Extended semantics (`ExtendBatch`, `ExtendInc`) may
//! generate additional operators (e.g. loop conditions, UDF pipelines) that are
//! not yet tracked by the profiler. Using `-P` with extended modes may produce
//! incorrect address mappings.
//!
//! Each count reflects the number of timely operators that DD
//! internally creates for the corresponding codegen pattern.
//!
//! Key facts:
//! - `stream.as_collection()`              = 0 ops (just wraps)
//! - `arrangement.as_collection(|k,v|...)` = 1 op  (AsCollection)
//! - `.inner.map(...).as_collection()`     = 1 op  (only the map)
//! - `.consolidate()`                      = 3 ops (FlatMap + Consolidate + AsCollection)
//! - `.threshold(...)`                     = 4 ops (FlatMap + Arrange:Threshold + Threshold + AsCollection)
//! - `.threshold_semigroup(...)`           = 3 ops (FlatMap + Arrange:ThresholdTotal + ThresholdTotal)
//! - `.threshold_total(...)`               = 3 ops (FlatMap + Arrange:ThresholdTotal + ThresholdTotal)

use flowlog_common::ExecutionMode;

use crate::Profiler;

impl Profiler {
    /// Operators created by `dedup_nonrecursive()` — the trace-free outer-scope
    /// dedup for EDBs and rule outputs. Mode-independent: `.consolidate()`
    /// (batch) and `.threshold_total()` (i32) each build 3 timely operators.
    pub(crate) fn dedup_nonrecursive_steps(&self) -> u32 {
        3
    }

    /// Operators created by `dedup_recursive()` — the in-loop dedup.
    ///
    /// - DatalogBatch `.threshold_semigroup(...)` → 3 (FlatMap + Arrange + ThresholdTotal)
    /// - Others `.threshold(...)` → 4 (FlatMap + Arrange + Threshold + AsCollection)
    pub(crate) fn dedup_recursive_steps(&self) -> u32 {
        if self.mode == ExecutionMode::DatalogBatch {
            3
        } else {
            4
        }
    }

    /// Operators in the anti-join pipeline (excluding arrangement).
    ///
    /// - DatalogBatch (9): flat_map_ref(1) + pos_weight(1) + join(1)
    ///     + neg_weight(1) + concat(1) + flat_map(1) + final_normalize(3)
    /// - Others, recursive scope (17): flat_map_ref(1) + inter_dedup(4) + join(1)
    ///     + inter_dedup(4) + neg_weight(1) + concat(1) + flat_map(1) + final_normalize(4)
    /// - Others, outer scope (14): same shape, but all three dedups are
    ///   `threshold_total` (3 each): 1 + 3 + 1 + 3 + 1 + 1 + 1 + 3
    pub(crate) fn anti_join_steps(&self, recursive: bool) -> u32 {
        if self.mode == ExecutionMode::DatalogBatch {
            9
        } else if recursive {
            17
        } else {
            14
        }
    }

    /// Operators in `gen_size_inspector`.
    ///
    /// - Batch (9): consolidate(3) + flat_map(1) + map(1) + consolidate(3) + inspect(1)
    /// - Inc (11): threshold(4) + flat_map(1) + map(1) + consolidate(3) + inspect(1) + probe(1)
    pub(crate) fn inspect_size_steps(&self) -> u32 {
        if self.mode == ExecutionMode::DatalogBatch {
            9
        } else {
            11
        }
    }

    /// Operators in content inspectors (terminal/file).
    ///
    /// - Batch (1): inspect(1)
    /// - Inc (5): consolidate(3) + inspect(1) + probe(1)
    pub(crate) fn inspect_content_steps(&self) -> u32 {
        match self.mode {
            ExecutionMode::DatalogInc | ExecutionMode::ExtendInc => 5,
            _ => 1,
        }
    }
}
