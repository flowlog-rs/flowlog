//! Max-semiring optimized aggregation code generation.
//!
//! Encodes `max` aggregation in the diff position using `MaxI32`/`MaxI64` semigroups,
//! replacing expensive reduce arrangements with lightweight `threshold_semigroup` or
//! `consolidate` pipelines.
//!
//! Mirror of the min-semiring pipeline with `>` instead of `<`.

use parser::DataType;
use proc_macro2::TokenStream;

use super::common::{
    aggregation_optimize_pipeline, aggregation_pre_leave_pipeline, result_from_key, row_pattern,
    semiring_new, ThresholdCmp,
};
use crate::import::SemiringKind;

/// Generates the Max-semiring optimized aggregation pipeline.
pub fn aggregation_max_optimize(arity: usize, agg_pos: usize, agg_type: DataType) -> TokenStream {
    aggregation_optimize_pipeline(
        arity,
        agg_pos,
        row_pattern(arity),
        semiring_new(SemiringKind::Max, agg_pos, agg_type),
        ThresholdCmp::Gt,
        result_from_key(arity, agg_pos),
    )
}

/// Generates the pre-leave conversion for max-aggregated recursive relations.
pub fn aggregation_max_pre_leave(arity: usize, agg_pos: usize, agg_type: DataType) -> TokenStream {
    aggregation_pre_leave_pipeline(
        arity,
        agg_pos,
        row_pattern(arity),
        semiring_new(SemiringKind::Max, agg_pos, agg_type),
    )
}
