//! Aggregation code generation utilities for FlowLog compiler.
//!
//! This module provides helper functions to generate Rust code for aggregation operations
//! in the codegen pipeline. It supports row chopping, aggregation reduction logic,
//! and merging key-value pairs back into output rows. All functions return `TokenStream`
//! fragments suitable for use in procedural macro code generation.
//!
//! # Functions
//! - [`aggregation_row_chop`]: Generates a closure to split a row into (group-by key, aggregation value).
//! - [`aggregation_reduce`]: Generates the aggregation logic closure for differential dataflow reduce.
//! - [`aggregation_merge_kv`]: Generates a closure to merge (key, value) pairs into output rows.

use parser::{AggregationOperator, DataType};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

/// Generates a closure to split a row into `(group-by key, aggregation value)`.
pub fn aggregation_row_chop(arity: usize, agg_pos: usize) -> TokenStream {
    let mut fields: Vec<_> = (0..arity).map(|i| format_ident!("x{}", i)).collect();
    let mut key_fields: Vec<_> = (0..arity)
        .filter(|&i| i != agg_pos)
        .map(|i| format_ident!("x{}", i))
        .collect();
    let agg_field = format_ident!("x{}", agg_pos);

    let pat = match fields.len() {
        0 => quote! { () },
        1 => {
            let f = fields.remove(0);
            quote! { #f }
        }
        _ => quote! { ( #(#fields),* ) },
    };

    let key_tuple = match key_fields.len() {
        0 => quote! { () },
        1 => {
            let k = key_fields.remove(0);
            quote! { ( #k ,) }
        }
        _ => quote! { ( #(#key_fields),* ) },
    };

    quote! {
        |#pat| (#key_tuple, #agg_field)
    }
}

/// Generates the aggregation logic closure for differential dataflow's `reduce` operator.
pub fn aggregation_reduce(op: &AggregationOperator, agg_type: DataType) -> TokenStream {
    match op {
        AggregationOperator::Count => match agg_type {
            DataType::Int32 => quote! {
                |_, input, _output, updates| {
                    let count = input.len() as i32;
                    updates.push(((count,), SEMIRING_ONE));
                }
            },
            DataType::Int64 => quote! {
                |_, input, _output, updates| {
                    let count = input.len() as i64;
                    updates.push(((count,), SEMIRING_ONE));
                }
            },
            DataType::String => {
                panic!("Compiler error: count aggregation result should be integer type")
            }
        },
        AggregationOperator::Sum => match agg_type {
            DataType::Int32 => quote! {
                |_, input, _output, updates| {
                    let sum = input.iter().map(|(v, _)| *v).sum::<i32>();
                    updates.push((*sum, SEMIRING_ONE));
                }
            },
            DataType::Int64 => quote! {
                |_, input, _output, updates| {
                    let sum = input.iter().map(|(v, _)| *v).sum::<i64>();
                    updates.push((*sum, SEMIRING_ONE));
                }
            },
            _ => panic!("Compiler error: sum aggregation result should be integer type"),
        },
        AggregationOperator::Min => match agg_type {
            DataType::Int32 | DataType::Int64 => quote! {
                |_, input, _output, updates| {
                    if let Some(min) = input.iter().map(|(v, _)| *v).min() {
                        updates.push((*min, SEMIRING_ONE));
                    }
                }
            },
            DataType::String => {
                panic!("Compiler error: min aggregation is not supported on string type")
            }
        },
        AggregationOperator::Max => match agg_type {
            DataType::Int32 | DataType::Int64 => quote! {
                |_, input, _output, updates| {
                    if let Some(max) = input.iter().map(|(v, _)| *v).max() {
                        updates.push((*max, SEMIRING_ONE));
                    }
                }
            },
            DataType::String => {
                panic!("Compiler error: max aggregation is not supported on string type")
            }
        },
    }
}

/// Generates a closure to merge `(group-by key, aggregation value)` into a full output row.
pub fn aggregation_merge_kv(arity: usize, agg_pos: usize) -> TokenStream {
    let mut pattern_fields = Vec::new();
    for i in 0..arity {
        if i != agg_pos {
            pattern_fields.push(format_ident!("k{}", i));
        }
    }

    let pattern = match pattern_fields.len() {
        0 => quote! { () },
        1 => {
            let f = &pattern_fields[0];
            quote! { ( #f ,) }
        }
        _ => quote! { ( #(#pattern_fields),* ) },
    };

    let mut result_fields = Vec::new();
    let mut key_iter = pattern_fields.iter();
    for i in 0..arity {
        if i == agg_pos {
            result_fields.push(quote! { v });
        } else {
            let k = key_iter.next().unwrap();
            result_fields.push(quote! { #k });
        }
    }

    let result_tuple = match result_fields.len() {
        0 => quote! { () },
        1 => {
            let f = result_fields.remove(0);
            quote! { ( #f ,) }
        }
        _ => quote! { ( #(#result_fields),* ) },
    };

    quote! {
        |&#pattern, &v| #result_tuple
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
    let key_arity = arity - 1;

    // Phase 1: row destructuring and key/value extraction
    let row_fields: Vec<_> = (0..arity).map(|i| format_ident!("x{}", i)).collect();
    let row_pat = match arity {
        0 => quote! { () },
        1 => {
            let f = &row_fields[0];
            quote! { #f }
        }
        _ => quote! { ( #(#row_fields),* ) },
    };

    let key_idents: Vec<_> = (0..arity)
        .filter(|&i| i != agg_pos)
        .map(|i| format_ident!("x{}", i))
        .collect();
    let key_construct = match key_idents.len() {
        0 => quote! { () },
        1 => {
            let k = &key_idents[0];
            quote! { ( #k ,) }
        }
        _ => quote! { ( #(#key_idents),* ) },
    };

    let agg_field = format_ident!("x{}", agg_pos);

    // Choose the right Min type based on the aggregated column's data type
    let (min_new, min_extract) = match agg_type {
        DataType::Int32 => (quote! { MinI32::new(#agg_field) }, quote! { min_val.value }),
        DataType::Int64 => (quote! { MinI64::new(#agg_field) }, quote! { min_val.value }),
        _ => unreachable!("min aggregation on non-integer type"),
    };

    // Phase 3: reconstruct full row from (key, Min diff)
    let key_fields_p3: Vec<_> = (0..key_arity).map(|i| format_ident!("k{}", i)).collect();
    let key_pat_p3 = match key_arity {
        0 => quote! { _key },
        1 => {
            let f = &key_fields_p3[0];
            quote! { ( #f ,) }
        }
        _ => quote! { ( #(#key_fields_p3),* ) },
    };

    let mut result_parts = Vec::new();
    let mut ki = 0usize;
    for i in 0..arity {
        if i == agg_pos {
            result_parts.push(min_extract.clone());
        } else {
            let kf = &key_fields_p3[ki];
            result_parts.push(quote! { #kf });
            ki += 1;
        }
    }
    let result_construct = match result_parts.len() {
        0 => quote! { () },
        1 => {
            let f = &result_parts[0];
            quote! { ( #f ,) }
        }
        _ => quote! { ( #(#result_parts),* ) },
    };

    quote! {
        // Phase 1: (row, time, _) → (key, time, Min::new(value))
        .inner
        .map(move |(#row_pat, t, _)| {
            let key = #key_construct;
            (key, t, #min_new)
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
        .map(move |(#key_pat_p3, t, min_val)| {
            let row = #result_construct;
            (row, t, SEMIRING_ONE)
        })
        .as_collection()
    }
}
