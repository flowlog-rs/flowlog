//! Count-semiring optimized aggregation code generation.
//!
//! Encodes `count` aggregation in the diff position by reusing the Sum semiring
//! (`SumI32`/`SumI64`).  Each input tuple maps to `SumI64::new(1)`, so
//! consolidation computes the count via addition.  The threshold emits whenever
//! the accumulated count changes.

use parser::DataType;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use super::common::{key_from_row, key_pattern, result_from_key};

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
        _ => unreachable!("Compiler error: count aggregation on non-integer type"),
    }
}

/// Generates the Count-semiring optimized aggregation pipeline.
///
/// Same structure as sum, but Phase 1 maps every tuple to `Sum::new(1)` instead
/// of extracting a field value.
pub fn aggregation_count_optimize(arity: usize, agg_pos: usize, agg_type: DataType) -> TokenStream {
    let row_pat = count_row_pattern(arity, agg_pos);
    let key_expr = key_from_row(arity, agg_pos);
    let one_expr = count_one(agg_type);
    let key_pat = key_pattern(arity);
    let result = result_from_key(arity, agg_pos);

    quote! {
        // Phase 1: (row, time, _) → (key, time, Sum::new(1))
        .inner
        .map(move |(#row_pat, t, _)| {
            let key = #key_expr;
            (key, t, #one_expr)
        })
        .as_collection()

        // Phase 2: threshold_semigroup — emits when count changes
        .threshold_semigroup(|_k, &new_count, current_count| {
            match current_count {
                Some(current) if new_count != *current => Some(new_count),
                Some(_) => None,
                None if !new_count.is_zero() => Some(new_count),
                None => None,
            }
        })

        // Phase 3: (key, time, Sum{value}) → (full_row, time, SEMIRING_ONE)
        .inner
        .map(move |(#key_pat, t, agg_val)| {
            let row = #result;
            (row, t, SEMIRING_ONE)
        })
        .as_collection()
    }
}

/// Generates the pre-leave conversion for count-aggregated recursive relations.
///
/// Converts `(row, time, Present)` → `((key,), time, Sum::new(1))` so that
/// `leave()` carries count diffs, enabling cross-iteration count via consolidation.
pub fn aggregation_count_pre_leave(
    arity: usize,
    agg_pos: usize,
    agg_type: DataType,
) -> TokenStream {
    let row_pat = count_row_pattern(arity, agg_pos);
    let key_expr = key_from_row(arity, agg_pos);
    let one_expr = count_one(agg_type);

    quote! {
        .inner
        .map(move |(#row_pat, t, _)| ((#key_expr), t, #one_expr))
        .as_collection()
    }
}
