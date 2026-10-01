//! Aggregation arguments, built from the head's arity and the position of
//! its aggregated column: [`aggregation_kind`] names the runtime
//! aggregation, [`aggregation_split`] and [`aggregation_merge`] take the
//! aggregated column out of a row and put the result back, and
//! [`aggregation_empty_key`] names the group that exists without input.

use flowlog_parser::AggregationOperator;
use flowlog_parser::DataType;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::tuple_tokens;
use crate::ty::data::internal_column_tokens;

/// Returns the runtime type that implements `op`.
pub(crate) fn aggregation_kind(op: AggregationOperator) -> TokenStream {
    let name = format_ident!(
        "{}",
        match op {
            AggregationOperator::Min => "Min",
            AggregationOperator::Max => "Max",
            AggregationOperator::Sum => "Sum",
            AggregationOperator::Avg => "Avg",
            AggregationOperator::Count => "Count",
        }
    );
    quote! { ::flowlog_runtime::operators::#name }
}

/// Returns the group key that exists without any input row: `Some(())` when
/// the aggregate is the head's only column, so the key is empty, and `None`
/// otherwise, since every other key comes from input. The runtime decides
/// whether an aggregation defines a result for an empty group.
pub(crate) fn aggregation_empty_key(arity: usize) -> TokenStream {
    if arity == 1 {
        quote! { Some(()) }
    } else {
        quote! { None }
    }
}

/// Returns the closure that splits a row into `(key, aggregated column)`,
/// the key holding every other column in order.
///
/// `count` ignores the column's value but splits the same way, so two rows
/// that differ only in that column stay two rows.
pub(crate) fn aggregation_split(arity: usize, agg_pos: usize) -> TokenStream {
    let pattern = indexed("x", 0..arity);
    let key = indexed("x", (0..arity).filter(|&i| i != agg_pos));
    let value = format_ident!("x{}", agg_pos);
    quote! { |#pattern| (#key, #value) }
}

/// Returns the closure that rebuilds a row from a key and the group's
/// aggregate, putting the aggregate back at `agg_pos`.
///
/// The aggregate's type `agg_type` is written out because `count` reports a
/// number unrelated to the column it read, so nothing else in the call fixes
/// it. Writing it for every operator keeps one closure shape.
pub(crate) fn aggregation_merge(arity: usize, agg_pos: usize, agg_type: &DataType) -> TokenStream {
    let pattern = indexed("k", 0..arity - 1);
    let row = row_with_agg_at(arity, agg_pos);
    let reported = internal_column_tokens(agg_type, false);
    quote! { |#pattern, v: #reported| #row }
}

// =========================================================================
// Row shapes
// =========================================================================

/// Returns the tuple of `prefix` names over `indices`, such as
/// `(x0, x2)`.
fn indexed(prefix: &str, indices: impl Iterator<Item = usize>) -> TokenStream {
    tuple_tokens(indices.map(|i| {
        let field = format_ident!("{prefix}{i}");
        quote! { #field }
    }))
}

/// Returns the output row with `v` at `agg_pos` and `k0, k1, ...` elsewhere.
fn row_with_agg_at(arity: usize, agg_pos: usize) -> TokenStream {
    let mut key_field = 0usize;
    let fields: Vec<TokenStream> = (0..arity)
        .map(|i| {
            if i == agg_pos {
                quote! { v }
            } else {
                let field = format_ident!("k{}", key_field);
                key_field += 1;
                quote! { #field }
            }
        })
        .collect();
    debug_assert_eq!(key_field, arity - 1);
    tuple_tokens(fields)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn normalized(tokens: TokenStream) -> String {
        tokens.to_string().split_whitespace().collect()
    }

    // Cases: operator, runtime type.
    #[rstest]
    #[case(AggregationOperator::Min, "Min")]
    #[case(AggregationOperator::Max, "Max")]
    #[case(AggregationOperator::Sum, "Sum")]
    #[case(AggregationOperator::Avg, "Avg")]
    #[case(AggregationOperator::Count, "Count")]
    fn kind_names_the_runtime_aggregation(#[case] op: AggregationOperator, #[case] expected: &str) {
        assert_eq!(
            normalized(aggregation_kind(op)),
            format!("::flowlog_runtime::operators::{expected}")
        );
    }

    // Cases: arity, empty key.
    #[rstest]
    #[case(1, "Some(())")]
    #[case(2, "None")]
    fn only_an_aggregate_only_head_has_a_group_without_input(
        #[case] arity: usize,
        #[case] expected: &str,
    ) {
        assert_eq!(normalized(aggregation_empty_key(arity)), expected);
    }

    // Cases: arity, aggregated position, split closure.
    #[rstest]
    #[case(3, 2, "|(x0,x1,x2)|((x0,x1),x2)")]
    #[case(3, 0, "|(x0,x1,x2)|((x1,x2),x0)")]
    #[case(1, 0, "|(x0,)|((),x0)")]
    fn split_takes_the_aggregated_column_out(
        #[case] arity: usize,
        #[case] agg_pos: usize,
        #[case] expected: &str,
    ) {
        assert_eq!(normalized(aggregation_split(arity, agg_pos)), expected);
    }

    /// The aggregate goes back to the position split took its column from.
    // Cases: arity, aggregated position, merge closure.
    #[rstest]
    #[case(4, 2, "|(k0,k1,k2),v:i64|(k0,k1,v,k2)")]
    #[case(3, 0, "|(k0,k1),v:i64|(v,k0,k1)")]
    #[case(3, 2, "|(k0,k1),v:i64|(k0,k1,v)")]
    #[case(1, 0, "|(),v:i64|(v,)")]
    fn merge_puts_the_aggregate_back(
        #[case] arity: usize,
        #[case] agg_pos: usize,
        #[case] expected: &str,
    ) {
        assert_eq!(
            normalized(aggregation_merge(arity, agg_pos, &DataType::Int64)),
            expected
        );
    }

    /// A float column reports a wrapped float, so the ascription is the
    /// internal lowering rather than the surface type.
    #[test]
    fn merge_ascribes_the_internal_column_type() {
        assert_eq!(
            normalized(aggregation_merge(2, 1, &DataType::Float32)),
            "|(k0,),v:::flowlog_runtime::ordered_float::OrderedFloat<f32>|(k0,v)"
        );
    }
}
