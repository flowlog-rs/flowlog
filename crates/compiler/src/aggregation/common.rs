//! Common aggregation code generation utilities.
//!
//! Provides shared structural helpers (arity-based tuple patterns, key/row
//! construction) used by all aggregation optimisation pipelines, plus the
//! generic reduce-based codegen helpers used in batch and incremental modes.

use parser::{AggregationOperator, DataType};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::import::SemiringKind;

// =============================================================================
// Semiring constructor helpers
// =============================================================================

/// `Kind{Suffix}::new(x<agg_pos>)` expression for a given semiring kind.
///
/// Used by min, max, sum, and avg aggregation pipelines to construct the
/// initial semiring value from the aggregated column.
pub(super) fn semiring_new(kind: SemiringKind, agg_pos: usize, agg_type: DataType) -> TokenStream {
    let field = format_ident!("x{}", agg_pos);
    let ty = format_ident!("{}{}", kind.prefix(), agg_type.semiring_suffix());
    quote! { #ty::new(#field) }
}

/// `Kind{Suffix}::new(1)` expression — every tuple contributes 1 to the count.
///
/// Float types use `OrderedFloat(1.0)` as the argument.
pub(super) fn semiring_one(kind: SemiringKind, agg_type: DataType) -> TokenStream {
    let ty = format_ident!("{}{}", kind.prefix(), agg_type.semiring_suffix());
    if agg_type.is_float() {
        quote! { #ty::new(OrderedFloat(1.0)) }
    } else {
        quote! { #ty::new(1) }
    }
}

// =============================================================================
// Structural helpers
// =============================================================================

/// Wraps a list of token-streams in a Rust tuple literal, using trailing-comma
/// for single-element tuples.
pub(super) fn tuple(fields: &[TokenStream]) -> TokenStream {
    match fields.len() {
        0 => quote! { () },
        1 => {
            let f = &fields[0];
            quote! { ( #f ,) }
        }
        _ => quote! { ( #(#fields),* ) },
    }
}

/// Row destructuring pattern: `(x0,)` (arity 1) or `(x0, x1, …)`.
pub(super) fn row_pattern(arity: usize) -> TokenStream {
    let fields: Vec<_> = (0..arity)
        .map(|i| {
            let id = format_ident!("x{}", i);
            quote! { #id }
        })
        .collect();
    tuple(&fields)
}

/// Key construction from row fields, excluding the aggregated position: `(x0,)`.
pub(super) fn key_from_row(arity: usize, agg_pos: usize) -> TokenStream {
    let fields: Vec<_> = (0..arity)
        .filter(|&i| i != agg_pos)
        .map(|i| {
            let id = format_ident!("x{}", i);
            quote! { #id }
        })
        .collect();
    tuple(&fields)
}

/// Key destructuring pattern for post-conversion: `(k0, k1, …)` or `_key`.
pub(super) fn key_pattern(arity: usize) -> TokenStream {
    let key_arity = arity - 1;
    if key_arity == 0 {
        return quote! { _key };
    }
    let fields: Vec<_> = (0..key_arity)
        .map(|i| {
            let id = format_ident!("k{}", i);
            quote! { #id }
        })
        .collect();
    tuple(&fields)
}

/// Full row reconstruction from `(key, agg_val)` where the aggregated value is
/// accessed as `agg_val.value` (semiring wrapper) at position `agg_pos`.
pub(super) fn result_from_key(arity: usize, agg_pos: usize) -> TokenStream {
    let key_arity = arity - 1;
    let mut parts = Vec::with_capacity(arity);
    let mut ki = 0usize;
    for i in 0..arity {
        if i == agg_pos {
            parts.push(quote! { agg_val.value });
        } else {
            let kf = format_ident!("k{}", ki);
            parts.push(quote! { #kf });
            ki += 1;
        }
    }
    debug_assert_eq!(ki, key_arity);
    tuple(&parts)
}

// =============================================================================
// Shared semiring-optimised pipeline helpers
// =============================================================================

/// Threshold comparison strategy for the semiring optimisation pipeline.
///
/// Determines when a consolidated semiring value is re-emitted.
pub(super) enum ThresholdCmp {
    /// Emit when the new value is strictly less than the current (`min`).
    Lt,
    /// Emit when the new value is strictly greater than the current (`max`).
    Gt,
    /// Emit when the new value differs from the current (`sum`, `count`, `avg`).
    Ne,
}

/// Generates the 3-phase semiring-optimised aggregation pipeline.
///
/// 1. **Phase 1** – Rewrite each `(row, time, _diff)` into `(key, time, Semiring::new(value))`
/// 2. **Phase 2** – `threshold_semigroup` with the semiring; emits only when the
///    consolidated value satisfies the comparison predicate.
/// 3. **Phase 3** – Convert back: `(key, time, SemiringVal)` → `(full_row, time, SEMIRING_ONE)`
///
/// `phase3_result` controls how the aggregated value is extracted:
/// use `result_from_key` (`.value`) for min/max/sum/count, or
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

    let threshold = match cmp {
        ThresholdCmp::Lt => quote! {
            .threshold_semigroup(|_k, &new_val, current_val| {
                match current_val {
                    Some(current) if new_val < *current => Some(new_val),
                    Some(_) => None,
                    None if !new_val.is_zero() => Some(new_val),
                    None => None,
                }
            })
        },
        ThresholdCmp::Gt => quote! {
            .threshold_semigroup(|_k, &new_val, current_val| {
                match current_val {
                    Some(current) if new_val > *current => Some(new_val),
                    Some(_) => None,
                    None if !new_val.is_zero() => Some(new_val),
                    None => None,
                }
            })
        },
        ThresholdCmp::Ne => quote! {
            .threshold_semigroup(|_k, &new_val, current_val| {
                match current_val {
                    Some(current) if new_val != *current => Some(new_val),
                    Some(_) => None,
                    None if !new_val.is_zero() => Some(new_val),
                    None => None,
                }
            })
        },
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

/// Generates the pre-leave conversion for semiring-optimised recursive relations.
///
/// Converts `(row, time, Present)` → `((key,), time, Semiring::new(value))` so
/// that `leave()` carries semiring diffs across iterations.
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

// =============================================================================
// Semiring-optimised post-leave (shared by min / max / sum / avg)
// =============================================================================

/// Post-leave conversion for semiring-optimised recursive relations.
///
/// After `leave()`, the collection carries semiring diffs from multiple
/// iteration timestamps collapsed to the same outer time.  `consolidate()`
/// triggers `plus_equals` to merge them, then we convert back to
/// `(row, Present)`.  This is aggregation-type agnostic.
pub fn aggregation_opt_post_leave(arity: usize, agg_pos: usize) -> TokenStream {
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

// =============================================================================
// Normal aggregation pipelines (row chop → reduce → merge_kv)
// =============================================================================

/// Generates a closure to split a row into `(group-by key, aggregation value)`.
pub fn aggregation_row_chop(arity: usize, agg_pos: usize) -> TokenStream {
    let pat = row_pattern(arity);
    let key = key_from_row(arity, agg_pos);
    let agg_field = format_ident!("x{}", agg_pos);

    quote! {
        |#pat| (#key, #agg_field)
    }
}

fn aggregation_result_expr(op: &AggregationOperator, agg_type: DataType) -> TokenStream {
    match op {
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
            _ => {
                panic!("Compiler error: count aggregation result should be numeric type")
            }
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
            _ => panic!("Compiler error: sum aggregation result should be numeric type"),
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
            _ => {
                panic!("Compiler error: min aggregation is not supported on non-numeric type")
            }
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
            _ => {
                panic!("Compiler error: max aggregation is not supported on non-numeric type")
            }
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
            _ => panic!("Compiler error: avg aggregation result should be numeric type"),
        },
    }
}

/// Generates the aggregation logic closure for differential dataflow's
/// `reduce_core` operator.
fn aggregation_reduce(op: &AggregationOperator, agg_type: DataType) -> TokenStream {
    let result_expr = aggregation_result_expr(op, agg_type);

    quote! {
        |_, input, _output, updates| {
            if let Some(result) = { #result_expr } {
                updates.push((result, SEMIRING_ONE));
            }
        }
    }
}

/// Generates the aggregation logic closure for differential dataflow's
/// `reduce_abelian` operator, which automatically retracts prior outputs.
fn aggregation_reduce_abelian(op: &AggregationOperator, agg_type: DataType) -> TokenStream {
    let result_expr = aggregation_result_expr(op, agg_type);

    quote! {
        |_, input, updates| {
            if let Some(result) = { #result_expr } {
                updates.push((result, SEMIRING_ONE));
            }
        }
    }
}

/// Generates the reduction operator call for normal aggregation pipelines.
///
/// Batch mode uses `reduce_core`, while incremental mode uses `reduce_abelian`
/// so aggregate replacements emit the needed retractions automatically.
pub fn aggregation_reduce_stmt(
    is_incremental: bool,
    op: &AggregationOperator,
    agg_type: DataType,
) -> TokenStream {
    if is_incremental {
        let reduce_logic = aggregation_reduce_abelian(op, agg_type);
        quote! {
            .reduce_abelian::<_,ValBuilder<_,_,_,_>,ValSpine<_,_,_,_>>(
                "aggregation",
                #reduce_logic
            )
        }
    } else {
        let reduce_logic = aggregation_reduce(op, agg_type);
        quote! {
            .reduce_core::<_,ValBuilder<_,_,_,_>,ValSpine<_,_,_,_>>(
                "aggregation",
                #reduce_logic
            )
        }
    }
}

/// Generates a closure to merge `(group-by key, aggregation value)` into a full output row.
pub fn aggregation_merge_kv(arity: usize, agg_pos: usize) -> TokenStream {
    let pattern = key_pattern(arity);
    let key_arity = arity - 1;

    let mut result_parts = Vec::with_capacity(arity);
    let mut ki = 0usize;
    for i in 0..arity {
        if i == agg_pos {
            result_parts.push(quote! { v });
        } else {
            let kf = format_ident!("k{}", ki);
            result_parts.push(quote! { #kf });
            ki += 1;
        }
    }
    debug_assert_eq!(ki, key_arity);
    let result_tuple = tuple(&result_parts);

    quote! {
        |&#pattern, &v| #result_tuple
    }
}
