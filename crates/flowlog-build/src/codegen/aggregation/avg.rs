//! Avg-semiring optimized aggregation code generation.
//!
//! Encodes `avg` aggregation in the diff position using `AvgI32`/`AvgI64`
//! semigroups.  Each input tuple maps to `Avg::new(value)` which stores
//! `(sum: value, count: 1)`.  Consolidation accumulates both components,
//! and the final average is computed as `sum / count` in Phase 3.

use crate::parser::{AggregationOperator, DataType};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use super::common::{
    ThresholdCmp, agg_semiring_new, aggregation_optimize_pipeline, aggregation_pre_leave_pipeline,
    key_pattern, row_pattern, tuple,
};

/// Full row reconstruction from `(key, agg_val)` where the averaged value is
/// computed as `agg_val.avg()` (= sum / count) at position `agg_pos`.
fn avg_result_from_key(arity: usize, agg_pos: usize) -> TokenStream {
    let key_arity = arity - 1;
    let mut parts = Vec::with_capacity(arity);
    let mut ki = 0usize;
    for i in 0..arity {
        if i == agg_pos {
            parts.push(quote! { agg_val.avg() });
        } else {
            let kf = format_ident!("k{}", ki);
            parts.push(quote! { #kf });
            ki += 1;
        }
    }
    debug_assert_eq!(ki, key_arity);
    tuple(&parts)
}

/// Generates the Avg-semiring optimized aggregation pipeline.
pub(crate) fn aggregation_avg_optimize(
    arity: usize,
    agg_pos: usize,
    agg_type: DataType,
) -> TokenStream {
    aggregation_optimize_pipeline(
        arity,
        agg_pos,
        row_pattern(arity),
        agg_semiring_new(AggregationOperator::Avg, agg_pos, agg_type),
        ThresholdCmp::Ne,
        avg_result_from_key(arity, agg_pos),
    )
}

/// Generates the pre-leave conversion for avg-aggregated recursive relations.
pub(crate) fn aggregation_avg_pre_leave(
    arity: usize,
    agg_pos: usize,
    agg_type: DataType,
) -> TokenStream {
    aggregation_pre_leave_pipeline(
        arity,
        agg_pos,
        row_pattern(arity),
        agg_semiring_new(AggregationOperator::Avg, agg_pos, agg_type),
    )
}

/// Post-leave conversion for avg-aggregated recursive relations.
///
/// Same as `aggregation_opt_post_leave` but uses `agg_val.avg()` (sum/count)
/// instead of `agg_val.value`.
pub(crate) fn aggregation_avg_post_leave(arity: usize, agg_pos: usize) -> TokenStream {
    let key_pat = key_pattern(arity);
    let result = avg_result_from_key(arity, agg_pos);

    quote! {
        .consolidate()
        .inner
        .map(move |(#key_pat, t, agg_val)| {
            let row = #result;
            (row, t, SEMIRING_ONE)
        })
        .as_collection()
    }
}
