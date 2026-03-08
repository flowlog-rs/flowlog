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

use super::common::{key_from_row, key_pattern, result_from_key, row_pattern};

/// `Sum{I32,I64}::new(x<agg_pos>)` expression.
fn sum_new(agg_pos: usize, agg_type: DataType) -> TokenStream {
    let field = format_ident!("x{}", agg_pos);
    match agg_type {
        DataType::Int8 => quote! { SumI8::new(#field) },
        DataType::Int16 => quote! { SumI16::new(#field) },
        DataType::Int32 => quote! { SumI32::new(#field) },
        DataType::Int64 => quote! { SumI64::new(#field) },
        _ => unreachable!("Compiler error: sum aggregation on non-integer type"),
    }
}

/// Generates the Sum-semiring optimized aggregation pipeline.
///
/// Identical structure to `aggregation_min_optimize` / `aggregation_max_optimize`
/// but the threshold emits whenever the accumulated sum changes (any non-equal update).
pub fn aggregation_sum_optimize(arity: usize, agg_pos: usize, agg_type: DataType) -> TokenStream {
    let row_pat = row_pattern(arity);
    let key_expr = key_from_row(arity, agg_pos);
    let sum_expr = sum_new(agg_pos, agg_type);
    let key_pat = key_pattern(arity);
    let result = result_from_key(arity, agg_pos);

    quote! {
        // Phase 1: (row, time, _) → (key, time, Sum::new(value))
        .inner
        .map(move |(#row_pat, t, _)| {
            let key = #key_expr;
            (key, t, #sum_expr)
        })
        .as_collection()

        // Phase 2: threshold_semigroup with Sum semiring
        .threshold_semigroup(|_k, &new_sum, current_sum| {
            match current_sum {
                Some(current) if new_sum != *current => Some(new_sum),
                Some(_) => None,
                None if !new_sum.is_zero() => Some(new_sum),
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

/// Generates the pre-leave conversion for sum-aggregated recursive relations.
///
/// Converts `(row, time, Present)` → `((key,), time, Sum::new(value))` so that
/// `leave()` carries SumI32/SumI64 diffs, enabling cross-iteration sum via consolidation.
pub fn aggregation_sum_pre_leave(arity: usize, agg_pos: usize, agg_type: DataType) -> TokenStream {
    let row_pat = row_pattern(arity);
    let key_expr = key_from_row(arity, agg_pos);
    let sum_expr = sum_new(agg_pos, agg_type);

    quote! {
        .inner
        .map(move |(#row_pat, t, _)| ((#key_expr), t, #sum_expr))
        .as_collection()
    }
}
