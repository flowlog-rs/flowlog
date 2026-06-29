//! Sum-semiring optimized aggregation code generation.
//!
//! Encodes `sum` aggregation in the diff position using `SumI32`/`SumI64` semigroups,
//! replacing expensive reduce arrangements with lightweight `threshold_semigroup` or
//! `consolidate` pipelines.
//!
//! Unlike min/max, sum uses real `Multiply` (`value * rhs`) because sum is not
//! idempotent.  The threshold emits whenever the accumulated sum changes.

use flowlog_parser::AggregationOperator;
use flowlog_parser::DataType;
use proc_macro2::TokenStream;

use super::common::ThresholdCmp;
use super::common::agg_semiring_new;
use super::common::aggregation_optimize_pipeline;
use super::common::aggregation_pre_leave_pipeline;
use super::common::result_from_key;
use super::common::row_pattern;

/// Generates the Sum-semiring optimized aggregation pipeline.
pub(crate) fn aggregation_sum_optimize(
    arity: usize,
    agg_pos: usize,
    agg_type: DataType,
) -> TokenStream {
    aggregation_optimize_pipeline(
        arity,
        agg_pos,
        row_pattern(arity),
        agg_semiring_new(AggregationOperator::Sum, agg_pos, agg_type),
        ThresholdCmp::Ne,
        result_from_key(arity, agg_pos),
    )
}

/// Generates the pre-leave conversion for sum-aggregated recursive relations.
pub(crate) fn aggregation_sum_pre_leave(
    arity: usize,
    agg_pos: usize,
    agg_type: DataType,
) -> TokenStream {
    aggregation_pre_leave_pipeline(
        arity,
        agg_pos,
        row_pattern(arity),
        agg_semiring_new(AggregationOperator::Sum, agg_pos, agg_type),
    )
}
