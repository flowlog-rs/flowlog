//! Count-semiring optimized aggregation code generation.
//!
//! Encodes `count` aggregation in the diff position by reusing the Sum semiring
//! (`SumI32`/`SumI64`).  Each input tuple maps to `SumI64::new(1)`, so
//! consolidation computes the count via addition.  The threshold emits whenever
//! the accumulated count changes.

use parser::DataType;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use super::common::{
    aggregation_optimize_pipeline, aggregation_pre_leave_pipeline, result_from_key, semiring_one,
    ThresholdCmp,
};

/// Row destructuring pattern with `_` at the aggregated position (count ignores the value).
fn count_row_pattern(arity: usize, agg_pos: usize) -> TokenStream {
    let fields: Vec<_> = (0..arity)
        .map(|i| {
            if i == agg_pos {
                quote! { _ }
            } else {
                let id = format_ident!("x{}", i);
                quote! { #id }
            }
        })
        .collect();
    if arity == 1 {
        let field0 = &fields[0];
        quote! { ( #field0 , ) }
    } else {
        quote! { ( #(#fields),* ) }
    }
}

/// Generates the Count-semiring optimized aggregation pipeline.
pub fn aggregation_count_optimize(arity: usize, agg_pos: usize, agg_type: DataType) -> TokenStream {
    aggregation_optimize_pipeline(
        arity,
        agg_pos,
        count_row_pattern(arity, agg_pos),
        semiring_one("Sum", agg_type),
        ThresholdCmp::Ne,
        result_from_key(arity, agg_pos),
    )
}

/// Generates the pre-leave conversion for count-aggregated recursive relations.
pub fn aggregation_count_pre_leave(
    arity: usize,
    agg_pos: usize,
    agg_type: DataType,
) -> TokenStream {
    aggregation_pre_leave_pipeline(
        arity,
        agg_pos,
        count_row_pattern(arity, agg_pos),
        semiring_one("Sum", agg_type),
    )
}
