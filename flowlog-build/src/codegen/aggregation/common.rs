//! Aggregation codegen helpers shared across operators.
//!
//! Two paths share the structural primitives (row / key / result patterns):
//! the semiring-optimised pipeline (`DatalogBatch` fast path) and the
//! generic `reduce_core` / `reduce_abelian` pipeline (batch + incremental
//! fallback).

use flowlog_parser::AggregationOperator;
use flowlog_parser::DataType;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::codegen::CodegenError;
use crate::codegen::tuple_tokens;

// ==================================================
// Semiring constructor helpers
// ==================================================

/// Semiring type ident, e.g. `MinI32`, `SumF64`.
fn agg_semiring_type_ident(op: AggregationOperator, dt: &DataType) -> proc_macro2::Ident {
    format_ident!("{}{}", op.semiring_prefix(), dt.semiring_suffix())
}

/// `Kind{Suffix}::new(x<agg_pos>)` expression for a given aggregation operator.
pub(super) fn agg_semiring_new(
    op: AggregationOperator,
    agg_pos: usize,
    agg_type: DataType,
) -> TokenStream {
    let field = format_ident!("x{}", agg_pos);
    let ty = agg_semiring_type_ident(op, &agg_type);
    quote! { #ty::new(#field) }
}

/// `Kind{Suffix}::new(1)` expression — every tuple contributes 1 to the count.
///
/// Float types use `OrderedFloat(1.0)` as the argument.
pub(super) fn agg_semiring_unit(op: AggregationOperator, agg_type: DataType) -> TokenStream {
    let ty = agg_semiring_type_ident(op, &agg_type);
    if agg_type.is_float() {
        quote! { #ty::new(OrderedFloat(1.0)) }
    } else {
        quote! { #ty::new(1) }
    }
}

// ==================================================
// Structural helpers
// ==================================================

/// Local shorthand for [`tuple_tokens`].
pub(super) fn tuple(fields: &[TokenStream]) -> TokenStream {
    tuple_tokens(fields.iter().cloned())
}

/// `(x0, x1, …, x<n-1>)` field tokens.
fn indexed_fields(prefix: &str, indices: impl Iterator<Item = usize>) -> Vec<TokenStream> {
    indices
        .map(|i| {
            let id = format_ident!("{prefix}{i}");
            quote! { #id }
        })
        .collect()
}

/// Row destructuring pattern: `(x0,)` (arity 1) or `(x0, x1, …)`.
pub(super) fn row_pattern(arity: usize) -> TokenStream {
    tuple(&indexed_fields("x", 0..arity))
}

/// Key construction from row fields, excluding the aggregated position.
pub(super) fn key_from_row(arity: usize, agg_pos: usize) -> TokenStream {
    tuple(&indexed_fields("x", (0..arity).filter(|&i| i != agg_pos)))
}

/// Key destructuring pattern `(k0, k1, …)`, or `_key` when the key is empty.
pub(super) fn key_pattern(arity: usize) -> TokenStream {
    let key_arity = arity - 1;
    if key_arity == 0 {
        return quote! { _key };
    }
    tuple(&indexed_fields("k", 0..key_arity))
}

/// Full row reconstruction from `(key, agg_val)` where the aggregated value is
/// accessed as `agg_val.value` (semiring wrapper) at position `agg_pos`.
pub(super) fn result_from_key(arity: usize, agg_pos: usize) -> TokenStream {
    row_with_agg_at(arity, agg_pos, quote! { agg_val.value })
}

/// Interleave `agg_token` at `agg_pos` with `k0, k1, …` at the other
/// positions to reconstruct a full output row of `arity` slots.
fn row_with_agg_at(arity: usize, agg_pos: usize, agg_token: TokenStream) -> TokenStream {
    let mut parts = Vec::with_capacity(arity);
    let mut ki = 0usize;
    for i in 0..arity {
        if i == agg_pos {
            parts.push(agg_token.clone());
        } else {
            let kf = format_ident!("k{}", ki);
            parts.push(quote! { #kf });
            ki += 1;
        }
    }
    debug_assert_eq!(ki, arity - 1);
    tuple(&parts)
}

// ==================================================
// Shared semiring-optimised pipeline helpers
// ==================================================

/// Threshold comparison strategy — when a consolidated semiring value
/// is re-emitted.
pub(super) enum ThresholdCmp {
    /// `min`: emit when strictly less than current.
    Lt,
    /// `max`: emit when strictly greater than current.
    Gt,
    /// `sum` / `count` / `avg`: emit when different from current.
    Ne,
}

/// Three-phase semiring-optimised aggregation pipeline:
///
/// 1. Rewrite each `(row, time, _diff)` into `(key, time, Semiring::new(value))`.
/// 2. `threshold_semigroup` with the semiring; only re-emits when the
///    consolidated value satisfies the comparison predicate.
/// 3. Convert back: `(key, time, SemiringVal)` → `(full_row, time, SEMIRING_ONE)`.
///
/// `phase3_result` controls how the aggregated value is extracted —
/// `result_from_key` (`.value`) for min/max/sum/count, or
/// `avg_result_from_key` (`.avg()`) for avg.
pub(super) fn aggregation_optimize_pipeline(
    arity: usize,
    agg_pos: usize,
    row_pat: TokenStream,
    semiring_expr: TokenStream,
    cmp: ThresholdCmp,
    phase3_result: TokenStream,
) -> TokenStream {
    let key_expr = key_from_row(arity, agg_pos);
    let key_pat = key_pattern(arity);

    let cmp_op = match cmp {
        ThresholdCmp::Lt => quote! { < },
        ThresholdCmp::Gt => quote! { > },
        ThresholdCmp::Ne => quote! { != },
    };
    let threshold = quote! {
        .threshold_semigroup(|_k, &new_val, current_val| {
            match current_val {
                Some(current) if new_val #cmp_op *current => Some(new_val),
                Some(_) => None,
                None if !new_val.is_zero() => Some(new_val),
                None => None,
            }
        })
    };

    quote! {
        .inner
        .map(move |(#row_pat, t, _)| {
            let key = #key_expr;
            (key, t, #semiring_expr)
        })
        .as_collection()

        #threshold

        .inner
        .map(move |(#key_pat, t, agg_val)| {
            let row = #phase3_result;
            (row, t, SEMIRING_ONE)
        })
        .as_collection()
    }
}

/// Pre-leave conversion for semiring-optimised recursive relations:
/// `(row, time, Present)` → `((key,), time, Semiring::new(value))`, so
/// `leave()` carries semiring diffs across iterations.
pub(super) fn aggregation_pre_leave_pipeline(
    arity: usize,
    agg_pos: usize,
    row_pat: TokenStream,
    semiring_expr: TokenStream,
) -> TokenStream {
    let key_expr = key_from_row(arity, agg_pos);

    quote! {
        .inner
        .map(move |(#row_pat, t, _)| ((#key_expr), t, #semiring_expr))
        .as_collection()
    }
}

// ==================================================
// Semiring-optimised post-leave (shared by min / max / sum / avg)
// ==================================================

/// Post-leave conversion for semiring-optimised recursive relations.
///
/// After `leave()`, the collection carries semiring diffs from multiple
/// iteration timestamps collapsed to the same outer time. `consolidate()`
/// triggers `plus_equals` to merge them, then we convert back to
/// `(row, Present)`. Aggregation-type agnostic.
pub(crate) fn aggregation_opt_post_leave(arity: usize, agg_pos: usize) -> TokenStream {
    let key_pat = key_pattern(arity);
    let result = result_from_key(arity, agg_pos);

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

// ==================================================
// Normal aggregation pipelines (row chop → reduce → merge_kv)
// ==================================================

/// Closure splitting a row into `(group-by key, aggregation value)`.
pub(crate) fn aggregation_row_chop(arity: usize, agg_pos: usize) -> TokenStream {
    let pat = row_pattern(arity);
    let key = key_from_row(arity, agg_pos);
    let agg_field = format_ident!("x{}", agg_pos);

    quote! {
        |#pat| (#key, #agg_field)
    }
}

fn aggregation_result_expr(
    op: &AggregationOperator,
    agg_type: DataType,
) -> Result<TokenStream, CodegenError> {
    let non_numeric = |op: &AggregationOperator| {
        CodegenError::internal(format!(
            "{op:?} aggregation reached codegen with non-numeric column `{agg_type:?}` \
             — upstream `check_aggregation_type` should have rejected this"
        ))
    };
    Ok(match op {
        AggregationOperator::Count => match agg_type {
            DataType::Int8 => quote! { (!input.is_empty()).then_some(input.len() as i8) },
            DataType::Int16 => quote! { (!input.is_empty()).then_some(input.len() as i16) },
            DataType::Int32 => quote! { (!input.is_empty()).then_some(input.len() as i32) },
            DataType::Int64 => quote! { (!input.is_empty()).then_some(input.len() as i64) },
            DataType::UInt8 => quote! { (!input.is_empty()).then_some(input.len() as u8) },
            DataType::UInt16 => quote! { (!input.is_empty()).then_some(input.len() as u16) },
            DataType::UInt32 => quote! { (!input.is_empty()).then_some(input.len() as u32) },
            DataType::UInt64 => quote! { (!input.is_empty()).then_some(input.len() as u64) },
            DataType::Float32 => {
                quote! { (!input.is_empty()).then_some(OrderedFloat(input.len() as f32)) }
            }
            DataType::Float64 => {
                quote! { (!input.is_empty()).then_some(OrderedFloat(input.len() as f64)) }
            }
            _ => return Err(non_numeric(op)),
        },
        AggregationOperator::Sum => match agg_type {
            DataType::Int8 => {
                quote! { (!input.is_empty()).then_some(input.iter().map(|(v, _)| *v).sum::<i8>()) }
            }
            DataType::Int16 => {
                quote! { (!input.is_empty()).then_some(input.iter().map(|(v, _)| *v).sum::<i16>()) }
            }
            DataType::Int32 => {
                quote! { (!input.is_empty()).then_some(input.iter().map(|(v, _)| *v).sum::<i32>()) }
            }
            DataType::Int64 => {
                quote! { (!input.is_empty()).then_some(input.iter().map(|(v, _)| *v).sum::<i64>()) }
            }
            DataType::UInt8 => {
                quote! { (!input.is_empty()).then_some(input.iter().map(|(v, _)| *v).sum::<u8>()) }
            }
            DataType::UInt16 => {
                quote! { (!input.is_empty()).then_some(input.iter().map(|(v, _)| *v).sum::<u16>()) }
            }
            DataType::UInt32 => {
                quote! { (!input.is_empty()).then_some(input.iter().map(|(v, _)| *v).sum::<u32>()) }
            }
            DataType::UInt64 => {
                quote! { (!input.is_empty()).then_some(input.iter().map(|(v, _)| *v).sum::<u64>()) }
            }
            DataType::Float32 => quote! {
                (!input.is_empty()).then_some(
                    input.iter()
                        .map(|(v, _)| *v)
                        .fold(OrderedFloat(0.0f32), |a, b| a + b)
                )
            },
            DataType::Float64 => quote! {
                (!input.is_empty()).then_some(
                    input.iter()
                        .map(|(v, _)| *v)
                        .fold(OrderedFloat(0.0f64), |a, b| a + b)
                )
            },
            _ => return Err(non_numeric(op)),
        },
        AggregationOperator::Min => match agg_type {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64 => quote! { input.iter().map(|(v, _)| *v).min().copied() },
            _ => return Err(non_numeric(op)),
        },
        AggregationOperator::Max => match agg_type {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64 => quote! { input.iter().map(|(v, _)| *v).max().copied() },
            _ => return Err(non_numeric(op)),
        },
        AggregationOperator::Avg => match agg_type {
            DataType::Int8 => quote! {
                if input.is_empty() {
                    None
                } else {
                    let sum = input.iter().map(|(v, _)| *v as i64).sum::<i64>();
                    let count = input.len() as i64;
                    Some((sum / count) as i8)
                }
            },
            DataType::Int16 => quote! {
                if input.is_empty() {
                    None
                } else {
                    let sum = input.iter().map(|(v, _)| *v as i64).sum::<i64>();
                    let count = input.len() as i64;
                    Some((sum / count) as i16)
                }
            },
            DataType::Int32 => quote! {
                if input.is_empty() {
                    None
                } else {
                    let sum = input.iter().map(|(v, _)| *v as i64).sum::<i64>();
                    let count = input.len() as i64;
                    Some((sum / count) as i32)
                }
            },
            DataType::Int64 => quote! {
                if input.is_empty() {
                    None
                } else {
                    let sum = input.iter().map(|(v, _)| *v).sum::<i64>();
                    let count = input.len() as i64;
                    Some(sum / count)
                }
            },
            DataType::UInt8 => quote! {
                if input.is_empty() {
                    None
                } else {
                    let sum = input.iter().map(|(v, _)| *v as u64).sum::<u64>();
                    let count = input.len() as u64;
                    Some((sum / count) as u8)
                }
            },
            DataType::UInt16 => quote! {
                if input.is_empty() {
                    None
                } else {
                    let sum = input.iter().map(|(v, _)| *v as u64).sum::<u64>();
                    let count = input.len() as u64;
                    Some((sum / count) as u16)
                }
            },
            DataType::UInt32 => quote! {
                if input.is_empty() {
                    None
                } else {
                    let sum = input.iter().map(|(v, _)| *v as u64).sum::<u64>();
                    let count = input.len() as u64;
                    Some((sum / count) as u32)
                }
            },
            DataType::UInt64 => quote! {
                if input.is_empty() {
                    None
                } else {
                    let sum = input.iter().map(|(v, _)| *v).sum::<u64>();
                    let count = input.len() as u64;
                    Some(sum / count)
                }
            },
            DataType::Float32 => quote! {
                if input.is_empty() {
                    None
                } else {
                    let sum: f32 = input.iter().map(|(v, _)| v.into_inner()).sum::<f32>();
                    let count = input.len() as f32;
                    Some(OrderedFloat(sum / count))
                }
            },
            DataType::Float64 => quote! {
                if input.is_empty() {
                    None
                } else {
                    let sum: f64 = input.iter().map(|(v, _)| v.into_inner()).sum::<f64>();
                    let count = input.len() as f64;
                    Some(OrderedFloat(sum / count))
                }
            },
            _ => return Err(non_numeric(op)),
        },
    })
}

/// Reduce closure for `reduce_core` (batch); emits a single update when
/// the aggregation yields a value.
fn aggregation_reduce(
    op: &AggregationOperator,
    agg_type: DataType,
) -> Result<TokenStream, CodegenError> {
    let result_expr = aggregation_result_expr(op, agg_type)?;
    Ok(quote! {
        |_, input, _output, updates| {
            if let Some(result) = { #result_expr } {
                updates.push((result, SEMIRING_ONE));
            }
        }
    })
}

/// Reduce closure for `reduce_abelian` (incremental); same logic but with
/// the arity expected by the abelian variant, which auto-retracts previous
/// outputs on replacement.
fn aggregation_reduce_abelian(
    op: &AggregationOperator,
    agg_type: DataType,
) -> Result<TokenStream, CodegenError> {
    let result_expr = aggregation_result_expr(op, agg_type)?;
    Ok(quote! {
        |_, input, updates| {
            if let Some(result) = { #result_expr } {
                updates.push((result, SEMIRING_ONE));
            }
        }
    })
}

/// Reduction operator for the generic aggregation pipeline — picks
/// `reduce_core` (batch) or `reduce_abelian` (incremental) so aggregate
/// replacements emit the required retractions automatically.
pub(crate) fn aggregation_reduce_stmt(
    is_incremental: bool,
    op: &AggregationOperator,
    agg_type: DataType,
) -> Result<TokenStream, CodegenError> {
    let (combinator, reduce_logic) = if is_incremental {
        (
            quote! { reduce_abelian },
            aggregation_reduce_abelian(op, agg_type)?,
        )
    } else {
        (quote! { reduce_core }, aggregation_reduce(op, agg_type)?)
    };
    Ok(quote! {
        .#combinator::<_,ValBuilder<_,_,_,_>,ValSpine<_,_,_,_>,_>(
            "aggregation",
            #reduce_logic,
            |vec, key, upds| {
                vec.clear();
                vec.extend(upds.drain(..).map(|(v, t, r)| ((key.clone(), v), t, r)));
            },
        )
    })
}

/// Closure merging `(group-by key, aggregation value)` into a full output row.
pub(crate) fn aggregation_merge_kv(arity: usize, agg_pos: usize) -> TokenStream {
    let pattern = key_pattern(arity);
    let result_tuple = row_with_agg_at(arity, agg_pos, quote! { v });
    quote! { |&#pattern, &v| #result_tuple }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `row_with_agg_at(arity, agg_pos, agg_token)` drives two counters:
    /// `i` walks every output slot, `ki` only advances through the
    /// non-agg positions. Off-by-one in either — advancing `ki` at
    /// `i == agg_pos`, or using `i` for the key fields — puts the
    /// aggregated value in the wrong slot or duplicates a `kN`.
    #[test]
    fn row_with_agg_at_interleaves_agg_at_correct_position() {
        // arity=4, agg_pos=2 → positions [k0, k1, AGG_SLOT, k2].
        let output = row_with_agg_at(4, 2, quote! { AGG_SLOT }).to_string();
        // Normalize to collapse whitespace variants proc-macro2 may emit.
        let normalized: String = output.split_whitespace().collect::<Vec<_>>().join(" ");
        assert_eq!(
            normalized, "(k0 , k1 , AGG_SLOT , k2)",
            "agg must be at slot 2; k-fields at 0, 1, 3"
        );

        // Boundary case: agg at slot 0.
        let at_zero = row_with_agg_at(3, 0, quote! { AGG_SLOT }).to_string();
        let at_zero_norm: String = at_zero.split_whitespace().collect::<Vec<_>>().join(" ");
        assert_eq!(at_zero_norm, "(AGG_SLOT , k0 , k1)");

        // Boundary case: agg at slot len-1.
        let at_last = row_with_agg_at(3, 2, quote! { AGG_SLOT }).to_string();
        let at_last_norm: String = at_last.split_whitespace().collect::<Vec<_>>().join(" ");
        assert_eq!(at_last_norm, "(k0 , k1 , AGG_SLOT)");
    }

    /// `key_pattern(arity=1)` means the key is empty (the one slot is
    /// the agg). The function must return the `_key` wildcard pattern,
    /// not a `()` destructuring — downstream closures destructure the
    /// key positionally and `()` would silently match nothing.
    #[test]
    fn key_pattern_arity_one_returns_wildcard() {
        let single = key_pattern(1).to_string();
        assert_eq!(single, "_key", "arity-1 (empty key) must yield `_key`");

        // Spot-check the non-special branch: arity=3 → two k-fields.
        let multi = key_pattern(3).to_string();
        let multi_norm: String = multi.split_whitespace().collect::<Vec<_>>().join(" ");
        assert_eq!(multi_norm, "(k0 , k1)");
    }
}
