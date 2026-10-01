//! Rule heads: a relation's union of the heads a stratum produces for it,
//! the dedup that follows, and its aggregation. [`CodeGen::gen_head`]
//! returns the step for one relation, in a non-recursive stratum or inside a
//! recursive stratum's loop.
//!
//! DD's `concatenate` needs every part at one weight, so a relation unions
//! at its most mutable part's weight, and each static part of a mutable
//! relation is lifted to signed counts first.

use flowlog_parser::AggregationOperator;
use flowlog_parser::Mutability;
use flowlog_planner::planner::StratumPlanner;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::CodeGen;
use crate::CodegenError;
use crate::expr::aggregation::aggregation_empty_key;
use crate::expr::aggregation::aggregation_kind;
use crate::expr::aggregation::aggregation_merge;
use crate::expr::aggregation::aggregation_split;
use crate::ident::intermediate_ident;

// =============================================================================
// Head step
// =============================================================================

impl CodeGen {
    /// Returns relation `idb_fp`'s head step in `stratum`, the binding the
    /// relation ends in, and its weight. The step unions the relation's
    /// `earlier` binding, when it has one, with its `head_fps`, dedups the
    /// union, and aggregates it when the relation has an aggregation.
    ///
    /// A non-recursive step rebinds the relation's global binding. A
    /// `recursive` one, inside the loop, binds `next_<fp>`, and
    /// `aggregated_<fp>` for an aggregation.
    pub(crate) fn gen_head(
        &self,
        stratum: &StratumPlanner,
        idb_fp: u64,
        head_fps: &[u64],
        earlier: Option<Ident>,
        recursive: bool,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<(TokenStream, Ident, Mutability), CodegenError> {
        let name = self.display_name(idb_fp);
        let (deduped, aggregated) = if recursive {
            // Keep both streams: feedback needs the current answers, but a
            // static loop's boundary fold needs the original contributions.
            // In particular, a seeded count result of 0 is an answer, not an
            // input row that should be counted again at leave.
            (
                format_ident!("next_{idb_fp}"),
                format_ident!("aggregated_{idb_fp}"),
            )
        } else {
            let binding = self.find_global_ident(idb_fp);
            (binding.clone(), binding)
        };

        let mut parts: Vec<(Ident, Mutability)> = head_fps
            .iter()
            .map(|fp| Ok((intermediate_ident(*fp), self.mutability(*fp)?)))
            .collect::<Result<_, CodegenError>>()?;
        if let Some(earlier) = earlier {
            parts.insert(0, (earlier, self.mutability(idb_fp)?));
        }
        let (union, mutability) = gen_union_dedup(&parts, &name, &deduped, recursive, plan_graph)?;
        // The stratifier assigns the relation the same value on its own; the
        // two derivations agree by Lemma 4 of `docs/design/mutability.md`.
        debug_assert_eq!(
            stratum.mutability(idb_fp),
            Some(mutability),
            "`{name}` unions at a weight the stratifier does not give it",
        );

        let mut code = quote! { let #deduped = #union; };
        let binding = match stratum.idb_to_aggregation_map().get(&idb_fp) {
            Some(aggregation) => {
                let aggregate = self.gen_aggregate(
                    idb_fp,
                    *aggregation,
                    &deduped,
                    &aggregated,
                    mutability,
                    plan_graph,
                )?;
                code = quote! { #code #aggregate };
                aggregated
            }
            None => deduped,
        };
        Ok((code, binding, mutability))
    }
}

// =============================================================================
// Union and aggregation
// =============================================================================

/// Returns the dedup of relation `name`'s union of `parts`, each a
/// collection and its own mutability, and the relation's weight: the most
/// mutable part's. Records the step, bound as `output`, in the plan graph.
///
/// A static part may announce a row more than once, each announcement
/// lifting to its own count, but the dedup clamps every count to one. A
/// relation with no part is a planner bug, reported as an internal error.
fn gen_union_dedup(
    parts: &[(Ident, Mutability)],
    name: &str,
    output: &Ident,
    recursive: bool,
    plan_graph: &mut Option<PlanGraph>,
) -> Result<(TokenStream, Mutability), CodegenError> {
    let Some(((first, first_mutability), rest)) = parts.split_first() else {
        return Err(CodegenError::internal(format!(
            "relation `{name}` has no collections to union"
        )));
    };
    let mutability = rest
        .iter()
        .map(|(_, part)| *part)
        .fold(*first_mutability, Mutability::max);
    let lift_name = format!("Lift: {name}");
    let lift = |collection: &Ident, part: Mutability| match (part, mutability) {
        (Mutability::Static, Mutability::Mutable) => quote! {
            ::flowlog_runtime::operators::flowlog_lift(#collection.clone(), #lift_name)
        },
        // `mutability` is the most mutable part's, so no part is more
        // mutable than it: a mutable part of a static relation never
        // arises.
        (Mutability::Static, Mutability::Static)
        | (Mutability::Mutable, Mutability::Mutable)
        | (Mutability::Mutable, Mutability::Static) => quote! { #collection.clone() },
    };
    let head = lift(first, *first_mutability);
    let tail: Vec<TokenStream> = rest.iter().map(|(c, part)| lift(c, *part)).collect();
    let union = if tail.is_empty() {
        head
    } else {
        quote! { #head.concatenate([ #( #tail ),* ]) }
    };

    with_plan_graph(plan_graph, |plan_graph| {
        // A part less mutable than the relation is a lifted static part.
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
        quote! { ::flowlog_runtime::operators::flowlog_dedup(#union) },
        mutability,
    ))
}

impl CodeGen {
    /// Returns `let output = flowlog_reduce(input, ...)`, the aggregation
    /// `(operator, position, arity)` of relation `idb_fp` over its deduped
    /// `input` at weight `mutability`, and records the step in the plan
    /// graph.
    fn gen_aggregate(
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
        let split = aggregation_split(agg_arity, agg_pos);
        let merge = aggregation_merge(agg_arity, agg_pos, &agg_type);
        let empty_key = aggregation_empty_key(agg_arity);
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
        with_plan_graph(plan_graph, |plan_graph| match mutability {
            Mutability::Static => {
                plan_graph.static_aggregate_operator(name, input.to_string(), output.to_string());
            }
            Mutability::Mutable => {
                plan_graph.mutable_aggregate_operator(
                    name,
                    input.to_string(),
                    output.to_string(),
                    seeded,
                );
            }
        });

        Ok(quote! {
            let #output = ::flowlog_runtime::operators::flowlog_reduce(
                #input.clone(), #op_name, #kind, #empty_key, #split, #merge,
            );
        })
    }
}

#[cfg(test)]
mod tests {
    use proc_macro2::Span;
    use rstest::rstest;

    use super::*;

    fn part(name: &str, mutability: Mutability) -> (Ident, Mutability) {
        (Ident::new(name, Span::call_site()), mutability)
    }

    /// A relation unions at its most mutable part's weight, lifting only
    /// the static parts of a mutable relation.
    #[rstest]
    #[case::one_static_part(
        vec![part("a", Mutability::Static)],
        quote! { ::flowlog_runtime::operators::flowlog_dedup(a.clone()) },
        Mutability::Static
    )]
    #[case::static_parts(
        vec![part("a", Mutability::Static), part("b", Mutability::Static)],
        quote! { ::flowlog_runtime::operators::flowlog_dedup(a.clone().concatenate([b.clone()])) },
        Mutability::Static
    )]
    #[case::mixed_parts(
        vec![part("a", Mutability::Static), part("b", Mutability::Mutable)],
        quote! {
            ::flowlog_runtime::operators::flowlog_dedup(
                ::flowlog_runtime::operators::flowlog_lift(a.clone(), "Lift: R")
                    .concatenate([b.clone()])
            )
        },
        Mutability::Mutable
    )]
    fn a_relation_unions_at_its_most_mutable_parts_weight(
        #[case] parts: Vec<(Ident, Mutability)>,
        #[case] expected: TokenStream,
        #[case] mutability: Mutability,
    ) {
        let output = Ident::new("r", Span::call_site());
        let (deduped, found) =
            gen_union_dedup(&parts, "R", &output, false, &mut None).expect("parts to union");
        assert_eq!(
            (deduped.to_string(), found),
            (expected.to_string(), mutability)
        );
    }
}
