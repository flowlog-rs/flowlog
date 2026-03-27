//! Min-semiring optimized aggregation code generation.
//!
//! Encodes `min` aggregation in the diff position using `MinI32`/`MinI64` semigroups,
//! replacing expensive reduce arrangements with lightweight `threshold_semigroup` or
//! `consolidate` pipelines.

use parser::DataType;
use proc_macro2::TokenStream;

use super::common::{
    aggregation_optimize_pipeline, aggregation_pre_leave_pipeline, result_from_key, row_pattern,
    semiring_new, ThresholdCmp,
};
use crate::import::SemiringKind;

/// Generates the Min-semiring optimized aggregation pipeline.
pub fn aggregation_min_optimize(arity: usize, agg_pos: usize, agg_type: DataType) -> TokenStream {
    aggregation_optimize_pipeline(
        arity,
        agg_pos,
        row_pattern(arity),
        semiring_new(SemiringKind::Min, agg_pos, agg_type),
        ThresholdCmp::Lt,
        result_from_key(arity, agg_pos),
    )
}

/// Generates the pre-leave conversion for min-aggregated recursive relations.
pub fn aggregation_min_pre_leave(arity: usize, agg_pos: usize, agg_type: DataType) -> TokenStream {
    aggregation_pre_leave_pipeline(
        arity,
        agg_pos,
        row_pattern(arity),
        semiring_new(SemiringKind::Min, agg_pos, agg_type),
    )
}
