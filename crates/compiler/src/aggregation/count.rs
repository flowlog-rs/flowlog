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
    aggregation_optimize_pipeline, aggregation_pre_leave_pipeline, result_from_key, ThresholdCmp,
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

/// `Sum{I32,I64}::new(1)` expression — every tuple contributes 1 to the count.
fn count_one(agg_type: DataType) -> TokenStream {
    match agg_type {
        DataType::Int8 => quote! { SumI8::new(1) },
        DataType::Int16 => quote! { SumI16::new(1) },
        DataType::Int32 => quote! { SumI32::new(1) },
        DataType::Int64 => quote! { SumI64::new(1) },
        DataType::UInt8 => quote! { SumU8::new(1) },
        DataType::UInt16 => quote! { SumU16::new(1) },
        DataType::UInt32 => quote! { SumU32::new(1) },
        DataType::UInt64 => quote! { SumU64::new(1) },
        DataType::Float32 => quote! { SumF32::new(OrderedFloat(1.0)) },
        DataType::Float64 => quote! { SumF64::new(OrderedFloat(1.0)) },
        _ => unreachable!("Compiler error: count aggregation on non-numeric type"),
    }
}

/// Generates the Count-semiring optimized aggregation pipeline.
pub fn aggregation_count_optimize(arity: usize, agg_pos: usize, agg_type: DataType) -> TokenStream {
    aggregation_optimize_pipeline(
        arity,
        agg_pos,
        count_row_pattern(arity, agg_pos),
        count_one(agg_type),
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
        count_one(agg_type),
    )
}
