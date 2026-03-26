//! Min-semiring optimized aggregation code generation.
//!
//! Encodes `min` aggregation in the diff position using `MinI32`/`MinI64` semigroups,
//! replacing expensive reduce arrangements with lightweight `threshold_semigroup` or
//! `consolidate` pipelines.

use parser::DataType;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use super::common::{
    aggregation_optimize_pipeline, aggregation_pre_leave_pipeline, result_from_key, row_pattern,
    ThresholdCmp,
};

/// `Min{I32,I64}::new(x<agg_pos>)` expression.
fn min_new(agg_pos: usize, agg_type: DataType) -> TokenStream {
    let field = format_ident!("x{}", agg_pos);
    match agg_type {
        DataType::Int8 => quote! { MinI8::new(#field) },
        DataType::Int16 => quote! { MinI16::new(#field) },
        DataType::Int32 => quote! { MinI32::new(#field) },
        DataType::Int64 => quote! { MinI64::new(#field) },
        DataType::UInt8 => quote! { MinU8::new(#field) },
        DataType::UInt16 => quote! { MinU16::new(#field) },
        DataType::UInt32 => quote! { MinU32::new(#field) },
        DataType::UInt64 => quote! { MinU64::new(#field) },
        DataType::Float32 => quote! { MinF32::new(#field) },
        DataType::Float64 => quote! { MinF64::new(#field) },
        _ => unreachable!("Compiler error: min aggregation on non-numeric type"),
    }
}

/// Generates the Min-semiring optimized aggregation pipeline.
pub fn aggregation_min_optimize(arity: usize, agg_pos: usize, agg_type: DataType) -> TokenStream {
    aggregation_optimize_pipeline(
        arity,
        agg_pos,
        row_pattern(arity),
        min_new(agg_pos, agg_type),
        ThresholdCmp::Lt,
        result_from_key(arity, agg_pos),
    )
}

/// Generates the pre-leave conversion for min-aggregated recursive relations.
pub fn aggregation_min_pre_leave(arity: usize, agg_pos: usize, agg_type: DataType) -> TokenStream {
    aggregation_pre_leave_pipeline(arity, agg_pos, row_pattern(arity), min_new(agg_pos, agg_type))
}
