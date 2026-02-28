//! Max-semiring optimized aggregation code generation.
//!
//! Encodes `max` aggregation in the diff position using `MaxI32`/`MaxI64` semigroups,
//! replacing expensive reduce arrangements with lightweight `threshold_semigroup` or
//! `consolidate` pipelines.
//!
//! Mirror of the min-semiring pipeline with `>` instead of `<`.

use parser::DataType;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use super::common::{key_from_row, key_pattern, result_from_key, row_pattern};

/// `Max{I32,I64}::new(x<agg_pos>)` expression.
fn max_new(agg_pos: usize, agg_type: DataType) -> TokenStream {
    let field = format_ident!("x{}", agg_pos);
    match agg_type {
        DataType::Int32 => quote! { MaxI32::new(#field) },
        DataType::Int64 => quote! { MaxI64::new(#field) },
        _ => unreachable!("Compiler error: max aggregation on non-integer type"),
    }
}

/// Generates the Max-semiring optimized aggregation pipeline.
///
/// Identical structure to `aggregation_min_optimize` but the threshold compares
/// with `>` (emits when the running maximum increases).
pub fn aggregation_max_optimize(arity: usize, agg_pos: usize, agg_type: DataType) -> TokenStream {
    let row_pat = row_pattern(arity);
    let key_expr = key_from_row(arity, agg_pos);
    let max_expr = max_new(agg_pos, agg_type);
    let key_pat = key_pattern(arity);
    let result = result_from_key(arity, agg_pos);

    quote! {
        // Phase 1: (row, time, _) → (key, time, Max::new(value))
        .inner
        .map(move |(#row_pat, t, _)| {
            let key = #key_expr;
            (key, t, #max_expr)
        })
        .as_collection()

        // Phase 2: threshold_semigroup with Max semiring
        .threshold_semigroup(|_k, &new_max, current_max| {
            match current_max {
                Some(current) if new_max > *current => Some(new_max),
                Some(_) => None,
                None if !new_max.is_zero() => Some(new_max),
                None => None,
            }
        })

        // Phase 3: (key, time, Max{value}) → (full_row, time, SEMIRING_ONE)
        .inner
        .map(move |(#key_pat, t, agg_val)| {
            let row = #result;
            (row, t, SEMIRING_ONE)
        })
        .as_collection()
    }
}

/// Generates the pre-leave conversion for max-aggregated recursive relations.
///
/// Converts `(row, time, Present)` → `((key,), time, Max::new(value))` so that
/// `leave()` carries MaxI32 diffs, enabling cross-iteration max via consolidation.
pub fn aggregation_max_pre_leave(arity: usize, agg_pos: usize, agg_type: DataType) -> TokenStream {
    let row_pat = row_pattern(arity);
    let key_expr = key_from_row(arity, agg_pos);
    let max_expr = max_new(agg_pos, agg_type);

    quote! {
        .inner
        .map(move |(#row_pat, t, _)| ((#key_expr), t, #max_expr))
        .as_collection()
    }
}
