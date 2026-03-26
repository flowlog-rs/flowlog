//! Sum-semiring optimized aggregation code generation.
//!
//! Encodes `sum` aggregation in the diff position using `SumI32`/`SumI64` semigroups,
//! replacing expensive reduce arrangements with lightweight `threshold_semigroup` or
//! `consolidate` pipelines.
//!
//! Unlike min/max, sum uses real `Multiply` (`value * rhs`) because sum is not
//! idempotent.  The threshold emits whenever the accumulated sum changes.

use parser::DataType;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use super::common::{
    aggregation_optimize_pipeline, aggregation_pre_leave_pipeline, result_from_key, row_pattern,
    ThresholdCmp,
};

/// `Sum{I32,I64}::new(x<agg_pos>)` expression.
fn sum_new(agg_pos: usize, agg_type: DataType) -> TokenStream {
    let field = format_ident!("x{}", agg_pos);
    match agg_type {
        DataType::Int8 => quote! { SumI8::new(#field) },
        DataType::Int16 => quote! { SumI16::new(#field) },
        DataType::Int32 => quote! { SumI32::new(#field) },
        DataType::Int64 => quote! { SumI64::new(#field) },
        DataType::UInt8 => quote! { SumU8::new(#field) },
        DataType::UInt16 => quote! { SumU16::new(#field) },
        DataType::UInt32 => quote! { SumU32::new(#field) },
        DataType::UInt64 => quote! { SumU64::new(#field) },
        DataType::Float32 => quote! { SumF32::new(#field) },
        DataType::Float64 => quote! { SumF64::new(#field) },
        _ => unreachable!("Compiler error: sum aggregation on non-numeric type"),
    }
}

/// Generates the Sum-semiring optimized aggregation pipeline.
pub fn aggregation_sum_optimize(arity: usize, agg_pos: usize, agg_type: DataType) -> TokenStream {
    aggregation_optimize_pipeline(
        arity,
        agg_pos,
        row_pattern(arity),
        sum_new(agg_pos, agg_type),
        ThresholdCmp::Ne,
        result_from_key(arity, agg_pos),
    )
}

/// Generates the pre-leave conversion for sum-aggregated recursive relations.
pub fn aggregation_sum_pre_leave(arity: usize, agg_pos: usize, agg_type: DataType) -> TokenStream {
    aggregation_pre_leave_pipeline(arity, agg_pos, row_pattern(arity), sum_new(agg_pos, agg_type))
}
