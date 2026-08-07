//! The pieces generated code passes to the runtime's reduce.
//!
//! An aggregation reaches codegen already reduced to "accumulate the column
//! at this position": the rule's arithmetic was materialized by the
//! preceding flat map, so `sum(a + b)` arrives as a column holding `a + b`.
//! What codegen contributes is the part it alone knows -- the row shape --
//! as two closures, plus a name for the aggregation. Which semiring
//! accumulates, and whether the aggregate rides in the difference position
//! or through an arrangement, are the runtime's to decide.
//!
//! [`aggregation_split`] and [`aggregation_merge`] are inverses around the
//! aggregated column: split takes it out, merge puts the result back.

use flowlog_parser::AggregationOperator;
use flowlog_parser::DataType;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::codegen::tuple_tokens;
use crate::codegen::ty::data::internal_column_tokens;

/// The runtime type naming the aggregation.
pub(super) fn aggregation_kind(op: AggregationOperator) -> TokenStream {
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

/// Closure cutting a row into `(group-by key, aggregated column)`.
///
/// `count` splits the same way as the rest: it ignores the column's value,
/// but both strategies still need it present to tell otherwise-identical
/// rows apart.
pub(super) fn aggregation_split(arity: usize, agg_pos: usize) -> TokenStream {
    let pattern = row_pattern(arity);
    let key = key_from_row(arity, agg_pos);
    let value = format_ident!("x{}", agg_pos);
    quote! { |#pattern| (#key, #value) }
}

/// Closure rebuilding an output row from a key and the group's aggregate.
///
/// The aggregate's type is written out because `count` reports a number
/// unrelated to the column it read, so nothing else in the call fixes it.
/// Spelling it on every operator keeps one shape for all five.
pub(super) fn aggregation_merge(arity: usize, agg_pos: usize, agg_type: &DataType) -> TokenStream {
    let pattern = key_pattern(arity);
    let row = row_with_agg_at(arity, agg_pos);
    let reported = internal_column_tokens(agg_type, false);
    quote! { |#pattern, v: #reported| #row }
}

// =========================================================================
// Row shapes
// =========================================================================

/// `(p0, p1, ...)` over the given indices.
fn indexed(prefix: &str, indices: impl Iterator<Item = usize>) -> TokenStream {
    tuple_tokens(indices.map(|i| {
        let field = format_ident!("{prefix}{i}");
        quote! { #field }
    }))
}

/// Row destructuring pattern: `(x0,)` at arity 1, `(x0, x1, ...)` above.
fn row_pattern(arity: usize) -> TokenStream {
    indexed("x", 0..arity)
}

/// Key built from every row field but the aggregated one.
fn key_from_row(arity: usize, agg_pos: usize) -> TokenStream {
    indexed("x", (0..arity).filter(|&i| i != agg_pos))
}

/// Key destructuring pattern, or `_key` when the key is empty -- `()` would
/// destructure nothing, leaving the closure's parameter unbound.
fn key_pattern(arity: usize) -> TokenStream {
    if arity == 1 {
        return quote! { _key };
    }
    indexed("k", 0..arity - 1)
}

/// Output row with the aggregate at `agg_pos` and `k0, k1, ...` elsewhere.
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

    /// `count` and `sum` share the split: `count` ignores the value, but the
    /// column has to stay so two rows differing only there stay distinct.
    #[test]
    fn split_takes_the_aggregated_column_out() {
        assert_eq!(
            normalized(aggregation_split(3, 2)),
            "|(x0,x1,x2)|((x0,x1),x2)"
        );
        assert_eq!(
            normalized(aggregation_split(3, 0)),
            "|(x0,x1,x2)|((x1,x2),x0)"
        );
    }

    /// Split and merge are inverses: whatever position the column came out
    /// of, the aggregate goes back into.
    #[rstest]
    #[case(4, 2, "|(k0,k1,k2),v:i64|(k0,k1,v,k2)")]
    #[case(3, 0, "|(k0,k1),v:i64|(v,k0,k1)")]
    #[case(3, 2, "|(k0,k1),v:i64|(k0,k1,v)")]
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

    /// An arity-1 relation aggregates its only column, leaving an empty key.
    #[test]
    fn empty_key_uses_a_wildcard() {
        assert_eq!(normalized(aggregation_split(1, 0)), "|(x0,)|((),x0)");
        assert_eq!(
            normalized(aggregation_merge(1, 0, &DataType::Int64)),
            "|_key,v:i64|(v,)"
        );
    }

    /// Every operator is a bare name; the reported type rides on the merge
    /// closure instead, since only `count`'s differs from its column.
    #[rstest]
    #[case(AggregationOperator::Min, "Min")]
    #[case(AggregationOperator::Count, "Count")]
    #[case(AggregationOperator::Avg, "Avg")]
    fn kind_names_the_aggregation(#[case] op: AggregationOperator, #[case] expected: &str) {
        assert_eq!(
            normalized(aggregation_kind(op)),
            format!("::flowlog_runtime::operators::{expected}")
        );
    }

    /// A float column reports a wrapped float, so the ascription has to be
    /// the internal lowering rather than the surface type.
    #[test]
    fn merge_ascribes_the_internal_column_type() {
        assert_eq!(
            normalized(aggregation_merge(2, 1, &DataType::Float32)),
            "|(k0,),v:OrderedFloat<f32>|(k0,v)"
        );
    }
}
