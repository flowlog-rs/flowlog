//! Rule heads: the steps a stratum chains to bind a relation from the heads
//! it produces for it. [`Codegen::gen_union_dedup`] unions and dedups them
//! with the relation's earlier binding, [`Codegen::gen_aggregate`] reduces
//! an aggregated relation's result, and
//! [`Codegen::gen_aggregate_leave`] is the reduce a static aggregate folds
//! through when it leaves a loop. Each step binds the ident its caller
//! names and records itself in the plan graph; the strata own the names and
//! the order.
//!
//! DD's `concatenate` needs every part at one weight, so a relation unions
//! at its most mutable part's weight, and each part below that weight is
//! lifted to it first.

use flowlog_parser::AggregationOperator;
use flowlog_parser::Mutability;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;
use crate::expr::aggregation::aggregation_empty_key;
use crate::expr::aggregation::aggregation_kind;
use crate::expr::aggregation::aggregation_merge;
use crate::expr::aggregation::aggregation_split;
use crate::ident::intermediate_ident;
use crate::ty::diff::weight_tokens;

// =============================================================================
// Union and dedup
// =============================================================================

impl Codegen {
    /// Returns `let output = flowlog_dedup(<union>);`, the dedup of relation
    /// `idb_fp`'s union of its `earlier` binding, when it has one, with the
    /// head collections `head_fps`, and the relation's weight: the most
    /// mutable part's. Records the step in the plan graph, where `recursive`
    /// says it sits inside a loop.
    ///
    /// A presence part may announce a row more than once, each announcement
    /// lifting to its own count, but the dedup clamps every count to one. A
    /// relation with no part is a planner bug, reported as an internal
    /// error.
    pub(crate) fn gen_union_dedup(
        &self,
        idb_fp: u64,
        head_fps: &[u64],
        earlier: Option<Ident>,
        output: &Ident,
        recursive: bool,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<(TokenStream, Mutability), CodegenError> {
        let name = self.display_name(idb_fp);
        let earlier = earlier.map(|binding| Ok((binding, self.mutability(idb_fp)?)));
        let parts: Vec<(Ident, Mutability)> = earlier
            .into_iter()
            .chain(
                head_fps
                    .iter()
                    .map(|fp| Ok((intermediate_ident(*fp), self.mutability(*fp)?))),
            )
            .collect::<Result<_, CodegenError>>()?;
        let Some(((first, first_mutability), rest)) = parts.split_first() else {
            return Err(CodegenError::internal(format!(
                "relation `{name}` has no collections to union"
            )));
        };
        let mutability = rest
            .iter()
            .map(|(_, part)| *part)
            .fold(*first_mutability, Mutability::max);
        // The lift names its target weight: an arrangement of the union
        // dispatches on the weight before the concatenation would fix it.
        let lift_name = format!("Lift: {name}");
        let weight = weight_tokens(mutability);
        let lift = |collection: &Ident, part: Mutability| {
            if part < mutability {
                quote! {
                    ::flowlog_runtime::operators::flowlog_lift::<#weight, _, _, _>(
                        #collection.clone(), #lift_name,
                    )
                }
            } else {
                quote! { #collection.clone() }
            }
        };
        let head = lift(first, *first_mutability);
        let tail: Vec<TokenStream> = rest.iter().map(|(c, part)| lift(c, *part)).collect();
        let union = if tail.is_empty() {
            head
        } else {
            quote! { #head.concatenate([ #( #tail ),* ]) }
        };

        with_plan_graph(plan_graph, |plan_graph| {
            // A part less mutable than the relation is lifted.
            let lifts: u32 = parts
                .iter()
                .map(|(_, part)| u32::from(*part < mutability))
                .sum();
            plan_graph.concat_dedup_operator(
                name.to_string(),
                parts.iter().map(|(id, _)| id.to_string()).collect(),
                output.to_string(),
                lifts,
                u32::from(!tail.is_empty()),
                mutability,
                recursive,
            );
        });

        Ok((
            quote! { let #output = ::flowlog_runtime::operators::flowlog_dedup(#union); },
            mutability,
        ))
    }
}

// =============================================================================
// Aggregation
// =============================================================================

impl Codegen {
    /// Returns `let output = <reduce>(input, ...)`, the aggregation
    /// `(operator, position, arity)` of relation `idb_fp` over its deduped
    /// `input` at weight `mutability`, and records the step in the plan
    /// graph. Append rows reduce through `flowlog_reduce_append`, whose
    /// answers are signed; the other weights through `flowlog_reduce`.
    pub(crate) fn gen_aggregate(
        &self,
        idb_fp: u64,
        (agg_op, agg_pos, agg_arity): (AggregationOperator, usize, usize),
        input: &Ident,
        output: &Ident,
        mutability: Mutability,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let name = self.display_name(idb_fp);
        let agg_type = self.agg_column_type(idb_fp, agg_pos)?;
        let kind = aggregation_kind(agg_op);
        let empty_key = aggregation_empty_key(agg_arity);
        let split = aggregation_split(agg_arity, agg_pos);
        let merge = aggregation_merge(agg_arity, agg_pos, &agg_type);
        let op_name = format!("Reduce: {name}");

        // The runtime builds a different operator for each weight, and seeds
        // the empty group of a `count` or `sum` over an empty key with its
        // zero; the plan graph predicts both so it counts the same operators.
        let seeded = agg_arity == 1
            && match agg_op {
                AggregationOperator::Count | AggregationOperator::Sum => true,
                AggregationOperator::Min | AggregationOperator::Max | AggregationOperator::Avg => {
                    false
                }
            };
        // The append reduce mirrors the mutable one operator for operator.
        with_plan_graph(plan_graph, |plan_graph| match mutability {
            Mutability::Static => {
                plan_graph.static_aggregate_operator(name, input.to_string(), output.to_string());
            }
            Mutability::Append | Mutability::Mutable => {
                plan_graph.mutable_aggregate_operator(
                    name,
                    input.to_string(),
                    output.to_string(),
                    seeded,
                );
            }
        });

        let reduce = match mutability {
            Mutability::Static | Mutability::Mutable => quote! { flowlog_reduce },
            Mutability::Append => quote! { flowlog_reduce_append },
        };
        Ok(quote! {
            let #output = ::flowlog_runtime::operators::#reduce(
                #input.clone(), #op_name, #kind, #empty_key, #split, #merge,
            );
        })
    }

    /// Returns the `flowlog_reduce_leave(...)` expression through which
    /// relation `idb_fp`, aggregated by `(operator, position, arity)`, leaves
    /// a static loop: it lifts the loop's contributions into the semiring
    /// diff, leaves, and folds every iteration once at the outer timestamp,
    /// since a static aggregate cannot retract an earlier answer. Records
    /// the fold's inner-scope side in the plan graph.
    ///
    /// `deduped` is the relation's union inside the loop and `aggregated`
    /// its reduce. Min and max fold their improving bounds, which leaves the
    /// final extreme unchanged and sends fewer rows across the boundary.
    /// Count, sum, and avg need the original contributions: summing the
    /// running answers 2 and 5 would give 7, although the inputs 2 and 3 sum
    /// to 5.
    pub(crate) fn gen_aggregate_leave(
        &self,
        idb_fp: u64,
        (agg_op, agg_pos, agg_arity): (AggregationOperator, usize, usize),
        deduped: &Ident,
        aggregated: &Ident,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let name = self.display_name(idb_fp);
        let agg_type = self.agg_column_type(idb_fp, agg_pos)?;
        let kind = aggregation_kind(agg_op);
        let empty_key = aggregation_empty_key(agg_arity);
        let split = aggregation_split(agg_arity, agg_pos);
        let merge = aggregation_merge(agg_arity, agg_pos, &agg_type);
        let input = match agg_op {
            AggregationOperator::Min | AggregationOperator::Max => aggregated,
            AggregationOperator::Count | AggregationOperator::Sum | AggregationOperator::Avg => {
                deduped
            }
        };

        with_plan_graph(plan_graph, |plan_graph| {
            plan_graph.recursive_pre_leave_static_aggregate_operator(
                name.clone(),
                input.to_string(),
                aggregated.to_string(),
            );
        });

        let op_name = format!("ReduceLeave: {name}");
        Ok(quote! {
            ::flowlog_runtime::operators::flowlog_reduce_leave(
                #input, scope, #op_name, #kind, #empty_key, #split, #merge,
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use proc_macro2::Span;
    use rstest::rstest;

    use super::*;
    use crate::test_harness::codegen;

    /// A static input `R` and a static output `S` derived from it.
    const PROGRAM: &str =
        ".decl R(a: int32)\n.input R\n.decl S(a: int32)\nS(a) :- R(a).\n.output S\n";

    fn ident(name: &str) -> Ident {
        Ident::new(name, Span::call_site())
    }

    /// `S` unions its earlier binding first, then its heads, at its most
    /// mutable part's weight, lifting only the static parts of a mutable
    /// relation.
    // Cases: heads with their mutability, earlier binding, expected, weight.
    #[rstest]
    #[case::one_static_head(
        vec![(0x1, Mutability::Static)],
        None,
        quote! { let r = ::flowlog_runtime::operators::flowlog_dedup(t_1.clone()); },
        Mutability::Static
    )]
    #[case::static_heads(
        vec![(0x1, Mutability::Static), (0x2, Mutability::Static)],
        None,
        quote! { let r = ::flowlog_runtime::operators::flowlog_dedup(t_1.clone().concatenate([t_2.clone()])); },
        Mutability::Static
    )]
    #[case::mixed_heads(
        vec![(0x1, Mutability::Static), (0x2, Mutability::Mutable)],
        None,
        quote! {
            let r = ::flowlog_runtime::operators::flowlog_dedup(
                ::flowlog_runtime::operators::flowlog_lift::<::flowlog_runtime::diff::Mutable, _, _, _>(
                    t_1.clone(), "Lift: S",
                )
                .concatenate([t_2.clone()])
            );
        },
        Mutability::Mutable
    )]
    #[case::earlier_binding_first(
        vec![(0x1, Mutability::Static)],
        Some("earlier"),
        quote! { let r = ::flowlog_runtime::operators::flowlog_dedup(earlier.clone().concatenate([t_1.clone()])); },
        Mutability::Static
    )]
    fn a_relation_unions_its_parts_at_the_most_mutable_weight(
        #[case] heads: Vec<(u64, Mutability)>,
        #[case] earlier: Option<&str>,
        #[case] expected: TokenStream,
        #[case] mutability: Mutability,
    ) {
        let mut codegen = codegen(PROGRAM);
        let s = codegen.program.idbs()[0].fingerprint();
        // Each head collection's mutability is what its rule body recorded.
        codegen
            .global_fp_to_mutability
            .extend(heads.iter().copied());
        let head_fps: Vec<u64> = heads.iter().map(|(fp, _)| *fp).collect();
        let (deduped, found) = codegen
            .gen_union_dedup(
                s,
                &head_fps,
                earlier.map(ident),
                &ident("r"),
                false,
                &mut None,
            )
            .expect("parts to union");
        assert_eq!(
            (deduped.to_string(), found),
            (expected.to_string(), mutability)
        );
    }

    /// A relation with no head and no earlier binding has nothing to union:
    /// a planner bug, not an empty collection.
    #[test]
    fn a_relation_with_no_part_is_an_internal_error() {
        let codegen = codegen(PROGRAM);
        let s = codegen.program.idbs()[0].fingerprint();
        let error = codegen
            .gen_union_dedup(s, &[], None, &ident("r"), false, &mut None)
            .expect_err("nothing to union");
        assert!(matches!(error, CodegenError::Internal(_)));
    }

    /// Append rows reduce through the signed append reduce; static and
    /// mutable rows through the reduce their own weight selects.
    // Cases: input weight, reduce.
    #[rstest]
    #[case(Mutability::Static, "flowlog_reduce (")]
    #[case(Mutability::Append, "flowlog_reduce_append (")]
    #[case(Mutability::Mutable, "flowlog_reduce (")]
    fn an_aggregate_reduces_through_its_weights_operator(
        #[case] mutability: Mutability,
        #[case] reduce: &str,
    ) {
        let codegen = codegen(PROGRAM);
        let s = codegen.program.idbs()[0].fingerprint();
        let aggregate = codegen
            .gen_aggregate(
                s,
                (AggregationOperator::Count, 0, 1),
                &ident("deduped"),
                &ident("aggregated"),
                mutability,
                &mut None,
            )
            .expect("aggregation over the declared column");
        let expected = format!("let aggregated = :: flowlog_runtime :: operators :: {reduce}");
        assert!(aggregate.to_string().contains(&expected), "{aggregate}");
    }

    /// The boundary fold reads the original contributions, except that an
    /// extreme can fold its own improving answers.
    // Cases: operator, input.
    #[rstest]
    #[case(AggregationOperator::Min, "aggregated")]
    #[case(AggregationOperator::Max, "aggregated")]
    #[case(AggregationOperator::Count, "deduped")]
    #[case(AggregationOperator::Sum, "deduped")]
    #[case(AggregationOperator::Avg, "deduped")]
    fn a_boundary_fold_reads_contributions_except_for_extremes(
        #[case] agg_op: AggregationOperator,
        #[case] input: &str,
    ) {
        let codegen = codegen(PROGRAM);
        let s = codegen.program.idbs()[0].fingerprint();
        let leave = codegen
            .gen_aggregate_leave(
                s,
                (agg_op, 0, 1),
                &ident("deduped"),
                &ident("aggregated"),
                &mut None,
            )
            .expect("aggregation over the declared column");
        let expected = format!("flowlog_reduce_leave ({input} , scope ,");
        assert!(leave.to_string().contains(&expected), "{leave}");
    }
}
