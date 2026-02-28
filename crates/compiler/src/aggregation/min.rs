//! Min-semiring optimized aggregation code generation.
//!
//! Encodes `min` aggregation in the diff position using `MinI32`/`MinI64` semigroups,
//! replacing expensive reduce arrangements with lightweight `threshold_semigroup` or
//! `consolidate` pipelines.

use parser::DataType;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use super::common::{key_from_row, key_pattern, result_from_key, row_pattern};

/// `Min{I32,I64}::new(x<agg_pos>)` expression.
fn min_new(agg_pos: usize, agg_type: DataType) -> TokenStream {
    let field = format_ident!("x{}", agg_pos);
    match agg_type {
        DataType::Int32 => quote! { MinI32::new(#field) },
        DataType::Int64 => quote! { MinI64::new(#field) },
        _ => unreachable!("Compiler error: min aggregation on non-integer type"),
    }
}

/// Generates the Min-semiring optimized aggregation pipeline as a single `TokenStream`.
///
/// Instead of `map(row_chop) → reduce_core(min) → as_collection(merge_kv)`,
/// this produces a 3-phase pipeline that encodes `min` in the diff position:
///
/// 1. **Phase 1** – Rewrite each `(row, time, _diff)` into `(key, time, Min::new(value))`
/// 2. **Phase 2** – `threshold_semigroup` with the `Min` semigroup; consolidation
///    computes the running minimum, and the threshold only emits when it decreases.
/// 3. **Phase 3** – Convert back: `(key, time, Min{value})` → `(full_row, time, SEMIRING_ONE)`
///
/// This replaces two arrangements (threshold + reduce_core) with one (threshold only).
pub fn aggregation_min_optimize(arity: usize, agg_pos: usize, agg_type: DataType) -> TokenStream {
    let row_pat = row_pattern(arity);
    let key_expr = key_from_row(arity, agg_pos);
    let min_expr = min_new(agg_pos, agg_type);
    let key_pat = key_pattern(arity);
    let result = result_from_key(arity, agg_pos);

    quote! {
        // Phase 1: (row, time, _) → (key, time, Min::new(value))
        .inner
        .map(move |(#row_pat, t, _)| {
            let key = #key_expr;
            (key, t, #min_expr)
        })
        .as_collection()

        // Phase 2: threshold_semigroup with Min semiring
        .threshold_semigroup(|_k, &new_min, current_min| {
            match current_min {
                Some(current) if new_min < *current => Some(new_min),
                Some(_) => None,
                None if !new_min.is_zero() => Some(new_min),
                None => None,
            }
        })

        // Phase 3: (key, time, Min{value}) → (full_row, time, SEMIRING_ONE)
        .inner
        .map(move |(#key_pat, t, agg_val)| {
            let row = #result;
            (row, t, SEMIRING_ONE)
        })
        .as_collection()
    }
}

/// Generates the pre-leave conversion for min-aggregated recursive relations.
///
/// Converts `(row, time, Present)` → `((key,), time, Min::new(value))` so that
/// `leave()` carries MinI32 diffs, enabling cross-iteration min via consolidation.
pub fn aggregation_min_pre_leave(arity: usize, agg_pos: usize, agg_type: DataType) -> TokenStream {
    let row_pat = row_pattern(arity);
    let key_expr = key_from_row(arity, agg_pos);
    let min_expr = min_new(agg_pos, agg_type);

    quote! {
        .inner
        .map(move |(#row_pat, t, _)| ((#key_expr), t, #min_expr))
        .as_collection()
    }
}
