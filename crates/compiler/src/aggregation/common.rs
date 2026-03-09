//! Common aggregation code generation utilities.
//!
//! Provides shared structural helpers (arity-based tuple patterns, key/row
//! construction) used by all aggregation optimisation pipelines, plus the
//! generic reduce-based aggregation code-gen functions.

use parser::{AggregationOperator, DataType};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

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

/// Generates the aggregation logic closure for differential dataflow's `reduce` operator.
pub fn aggregation_reduce(op: &AggregationOperator, agg_type: DataType) -> TokenStream {
    match op {
        AggregationOperator::Count => match agg_type {
            DataType::Int8 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let count = input.len() as i8;
                    updates.push((count, SEMIRING_ONE));
                }
            },
            DataType::Int16 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let count = input.len() as i16;
                    updates.push((count, SEMIRING_ONE));
                }
            },
            DataType::Int32 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let count = input.len() as i32;
                    updates.push((count, SEMIRING_ONE));
                }
            },
            DataType::Int64 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let count = input.len() as i64;
                    updates.push((count, SEMIRING_ONE));
                }
            },
            DataType::UInt8 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let count = input.len() as u8;
                    updates.push((count, SEMIRING_ONE));
                }
            },
            DataType::UInt16 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let count = input.len() as u16;
                    updates.push((count, SEMIRING_ONE));
                }
            },
            DataType::UInt32 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let count = input.len() as u32;
                    updates.push((count, SEMIRING_ONE));
                }
            },
            DataType::UInt64 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let count = input.len() as u64;
                    updates.push((count, SEMIRING_ONE));
                }
            },
            DataType::Float32 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let count = OrderedFloat(input.len() as f32);
                    updates.push((count, SEMIRING_ONE));
                }
            },
            DataType::Float64 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let count = OrderedFloat(input.len() as f64);
                    updates.push((count, SEMIRING_ONE));
                }
            },
            _ => {
                panic!("Compiler error: count aggregation result should be numeric type")
            }
        },
        AggregationOperator::Sum => match agg_type {
            DataType::Int8 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let sum = input.iter().map(|(v, _)| *v).sum::<i8>();
                    updates.push((sum, SEMIRING_ONE));
                }
            },
            DataType::Int16 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let sum = input.iter().map(|(v, _)| *v).sum::<i16>();
                    updates.push((sum, SEMIRING_ONE));
                }
            },
            DataType::Int32 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let sum = input.iter().map(|(v, _)| *v).sum::<i32>();
                    updates.push((sum, SEMIRING_ONE));
                }
            },
            DataType::Int64 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let sum = input.iter().map(|(v, _)| *v).sum::<i64>();
                    updates.push((sum, SEMIRING_ONE));
                }
            },
            DataType::UInt8 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let sum = input.iter().map(|(v, _)| *v).sum::<u8>();
                    updates.push((sum, SEMIRING_ONE));
                }
            },
            DataType::UInt16 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let sum = input.iter().map(|(v, _)| *v).sum::<u16>();
                    updates.push((sum, SEMIRING_ONE));
                }
            },
            DataType::UInt32 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let sum = input.iter().map(|(v, _)| *v).sum::<u32>();
                    updates.push((sum, SEMIRING_ONE));
                }
            },
            DataType::UInt64 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let sum = input.iter().map(|(v, _)| *v).sum::<u64>();
                    updates.push((sum, SEMIRING_ONE));
                }
            },
            DataType::Float32 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let sum: OrderedFloat<f32> = input.iter().map(|(v, _)| *v).fold(OrderedFloat(0.0f32), |a, b| a + b);
                    updates.push((sum, SEMIRING_ONE));
                }
            },
            DataType::Float64 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let sum: OrderedFloat<f64> = input.iter().map(|(v, _)| *v).fold(OrderedFloat(0.0f64), |a, b| a + b);
                    updates.push((sum, SEMIRING_ONE));
                }
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
            | DataType::Float64 => quote! {
                |_, input, _output, updates| {
                    if let Some(min) = input.iter().map(|(v, _)| *v).min() {
                        updates.push((*min, SEMIRING_ONE));
                    }
                }
            },
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
            | DataType::Float64 => quote! {
                |_, input, _output, updates| {
                    if let Some(max) = input.iter().map(|(v, _)| *v).max() {
                        updates.push((*max, SEMIRING_ONE));
                    }
                }
            },
            _ => {
                panic!("Compiler error: max aggregation is not supported on non-numeric type")
            }
        },
        AggregationOperator::Avg => match agg_type {
            DataType::Int8 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let sum = input.iter().map(|(v, _)| *v as i64).sum::<i64>();
                    let count = input.len() as i64;
                    let avg = sum / count;
                    updates.push((avg as i8, SEMIRING_ONE));
                }
            },
            DataType::Int16 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let sum = input.iter().map(|(v, _)| *v as i64).sum::<i64>();
                    let count = input.len() as i64;
                    let avg = sum / count;
                    updates.push((avg as i16, SEMIRING_ONE));
                }
            },
            DataType::Int32 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let sum = input.iter().map(|(v, _)| *v as i64).sum::<i64>();
                    let count = input.len() as i64;
                    let avg = sum / count;
                    updates.push((avg as i32, SEMIRING_ONE));
                }
            },
            DataType::Int64 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let sum = input.iter().map(|(v, _)| *v).sum::<i64>();
                    let count = input.len() as i64;
                    let avg = sum / count;
                    updates.push((avg, SEMIRING_ONE));
                }
            },
            DataType::UInt8 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let sum = input.iter().map(|(v, _)| *v as u64).sum::<u64>();
                    let count = input.len() as u64;
                    let avg = sum / count;
                    updates.push((avg as u8, SEMIRING_ONE));
                }
            },
            DataType::UInt16 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let sum = input.iter().map(|(v, _)| *v as u64).sum::<u64>();
                    let count = input.len() as u64;
                    let avg = sum / count;
                    updates.push((avg as u16, SEMIRING_ONE));
                }
            },
            DataType::UInt32 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let sum = input.iter().map(|(v, _)| *v as u64).sum::<u64>();
                    let count = input.len() as u64;
                    let avg = sum / count;
                    updates.push((avg as u32, SEMIRING_ONE));
                }
            },
            DataType::UInt64 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let sum = input.iter().map(|(v, _)| *v).sum::<u64>();
                    let count = input.len() as u64;
                    let avg = sum / count;
                    updates.push((avg, SEMIRING_ONE));
                }
            },
            DataType::Float32 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let sum: f32 = input.iter().map(|(v, _)| v.into_inner()).sum::<f32>();
                    let count = input.len() as f32;
                    let avg = OrderedFloat(sum / count);
                    updates.push((avg, SEMIRING_ONE));
                }
            },
            DataType::Float64 => quote! {
                |_, input, _output, updates| {
                    if input.is_empty() {
                        return;
                    }
                    let sum: f64 = input.iter().map(|(v, _)| v.into_inner()).sum::<f64>();
                    let count = input.len() as f64;
                    let avg = OrderedFloat(sum / count);
                    updates.push((avg, SEMIRING_ONE));
                }
            },
            _ => panic!("Compiler error: avg aggregation result should be numeric type"),
        },
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
