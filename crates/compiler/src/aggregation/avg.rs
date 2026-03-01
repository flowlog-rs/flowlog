//! Avg-semiring optimized aggregation code generation.
//!
//! Encodes `avg` aggregation in the diff position using `AvgI32`/`AvgI64`
//! semigroups.  Each input tuple maps to `Avg::new(value)` which stores
//! `(sum: value, count: 1)`.  Consolidation accumulates both components,
//! and the final average is computed as `sum / count` in Phase 3.

use parser::DataType;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use super::common::{key_from_row, key_pattern, row_pattern, tuple};

/// `Avg{I32,I64}::new(x<agg_pos>)` expression.
fn avg_new(agg_pos: usize, agg_type: DataType) -> TokenStream {
    let field = format_ident!("x{}", agg_pos);
    match agg_type {
        DataType::Int32 => quote! { AvgI32::new(#field) },
        DataType::Int64 => quote! { AvgI64::new(#field) },
        _ => unreachable!("Compiler error: avg aggregation on non-integer type"),
    }
}

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
pub fn aggregation_avg_optimize(arity: usize, agg_pos: usize, agg_type: DataType) -> TokenStream {
    let row_pat = row_pattern(arity);
    let key_expr = key_from_row(arity, agg_pos);
    let avg_expr = avg_new(agg_pos, agg_type);
    let key_pat = key_pattern(arity);
    let result = avg_result_from_key(arity, agg_pos);

    quote! {
        // Phase 1: (row, time, _) → (key, time, Avg::new(value))
        .inner
        .map(move |(#row_pat, t, _)| {
            let key = #key_expr;
            (key, t, #avg_expr)
        })
        .as_collection()

        // Phase 2: threshold_semigroup with Avg semiring
        .threshold_semigroup(|_k, &new_avg, current_avg| {
            match current_avg {
                Some(current) if new_avg != *current => Some(new_avg),
                Some(_) => None,
                None if !new_avg.is_zero() => Some(new_avg),
                None => None,
            }
        })

        // Phase 3: (key, time, Avg{sum, count}) → (full_row, time, SEMIRING_ONE)
        .inner
        .map(move |(#key_pat, t, agg_val)| {
            let row = #result;
            (row, t, SEMIRING_ONE)
        })
        .as_collection()
    }
}

/// Generates the pre-leave conversion for avg-aggregated recursive relations.
pub fn aggregation_avg_pre_leave(arity: usize, agg_pos: usize, agg_type: DataType) -> TokenStream {
    let row_pat = row_pattern(arity);
    let key_expr = key_from_row(arity, agg_pos);
    let avg_expr = avg_new(agg_pos, agg_type);

    quote! {
        .inner
        .map(move |(#row_pat, t, _)| ((#key_expr), t, #avg_expr))
        .as_collection()
    }
}

/// Post-leave conversion for avg-aggregated recursive relations.
///
/// Same as `aggregation_opt_post_leave` but uses `agg_val.avg()` (sum/count)
/// instead of `agg_val.value`.
pub fn aggregation_avg_post_leave(arity: usize, agg_pos: usize) -> TokenStream {
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
