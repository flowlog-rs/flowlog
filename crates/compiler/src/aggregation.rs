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

use parser::AggregationOperator;
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
pub fn aggregation_reduce(op: &AggregationOperator) -> TokenStream {
    match op {
        AggregationOperator::Count => quote! {
            |_, input, _output, updates| {
                let count = input.len() as i32;
                updates.push(((count,), SEMIRING_ONE));
            }
        },
        AggregationOperator::Sum => quote! {
            |_, input, _output, updates| {
                let sum = input.iter().map(|(v, _)| *v).sum::<i32>();
                updates.push((*sum, SEMIRING_ONE));
            }
        },
        AggregationOperator::Min => quote! {
            |_, input, _output, updates| {
                if let Some(min) = input.iter().map(|(v, _)| *v).min() {
                    updates.push((*min, SEMIRING_ONE));
                }
            }
        },
        AggregationOperator::Max => quote! {
            |_, input, _output, updates| {
                if let Some(max) = input.iter().map(|(v, _)| *v).max() {
                    updates.push((*max, SEMIRING_ONE));
                }
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
