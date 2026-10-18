//! Aggregates: the operator reducing the matches of an aggregate's braces
//! to one answer per group, then mapping each answer row to the step's
//! output. The matches come arranged by the group key, one row per match;
//! the groups, when the step has them, come arranged by themselves. The
//! runtime reduces the matches' arrangement and answers the groups it is
//! given as the aggregation allows, so nothing here depends on the
//! operator.

use std::collections::HashMap;

use flowlog_planner::planner::Collection;
use flowlog_planner::planner::TransformationArgument;
use flowlog_planner::planner::TransformationFlow;
use flowlog_planner::planner::aggregate_mutability;
use flowlog_profiler::Groups;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;
use syn::Index;

use super::Operator;
use super::arranged_input;
use super::keyed_output;
use super::map::flat_map_body_tokens;
use super::register_arrangement;
use crate::Codegen;
use crate::CodegenError;
use crate::expr::aggregation::aggregation_kind;
use crate::expr::param::kv_params;
use crate::ident::find_local_ident;
use crate::ty::data::KvTypes;
use crate::ty::data::internal_column_tokens;
use crate::ty::diff::weight_tokens;

impl Codegen {
    /// Returns the reduce of the arrangement of `matches` under `flow`'s
    /// aggregation, answering the arrangement of `groups` or, without
    /// groups, the empty key when the matches have no key, its answer
    /// rows mapped into `output` by `flow`, as rows or keyed as
    /// `operator` says, and the arrangement of a keyed output.
    ///
    /// A filter on the answer row needs its own map after the reduce;
    /// without one the projection runs inside the reduce's merge.
    pub(super) fn gen_aggregate(
        &mut self,
        operator: Operator<'_>,
        (matches, groups): (&Collection, Option<&Collection>),
        output: &Collection,
        flow: &TransformationFlow,
        arranged_map: &mut HashMap<u64, Ident>,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let TransformationFlow::Aggregate {
            operator: op,
            target,
            ty,
            key,
            value,
            compares,
        } = flow
        else {
            return Err(CodegenError::internal(format!(
                "aggregate {output} has the flow of a map or a join"
            )));
        };
        let rows = arranged_input(operator.local_fp_to_ident, arranged_map, matches)?;
        let groups = groups
            .map(|groups| arranged_input(operator.local_fp_to_ident, arranged_map, groups))
            .transpose()?;
        let out = find_local_ident(operator.local_fp_to_ident, output.fingerprint());
        let Operator {
            name,
            literal,
            arrange,
            recursive,
            si,
            ..
        } = operator;

        let matches_mutability = matches.mutability();
        debug_assert_eq!(
            output.mutability(),
            aggregate_mutability(matches_mutability),
            "aggregate {output} answers at a weight the planner does not give it",
        );
        // The groups the runtime answers as the aggregation allows: the
        // arrangement the step has, or the empty key when the matches
        // have no key, so the one group is answered whether or not any
        // match exists; a keyed aggregate without groups answers only the
        // keys its matches hold. Groups without an arrangement take the
        // matches' weight, which nothing else fixes.
        let weight = weight_tokens(matches_mutability);
        let (groups_kind, seeds) = match &groups {
            Some(groups) => (
                Groups::Arranged,
                quote! { ::flowlog_runtime::operators::Groups::Arranged(#groups.clone()) },
            ),
            None if matches.arity().0 == 0 => (
                Groups::Key,
                quote! { ::flowlog_runtime::operators::Groups::<_, _, #weight>::Key(()) },
            ),
            None => (
                Groups::None,
                quote! { ::flowlog_runtime::operators::Groups::<_, _, #weight>::None },
            ),
        };

        let matches_type = self.find_global_type(matches.fingerprint())?.clone();
        let answer_row: KvTypes = (matches_type.0, vec![ty.clone()]);
        let answer_ty = internal_column_tokens(ty, si);
        let (k, v) = kv_params(key, value, compares, None);
        let value_expr = self.kv_projection(value, si, &answer_row)?;
        let out_expr = if arrange.is_some() {
            keyed_output(
                output,
                self.kv_projection(key, si, &answer_row)?,
                value_expr,
            )
        } else {
            value_expr
        };
        let pred = self.kv_compare_predicate(compares, si, &answer_row)?;

        with_plan_graph(plan_graph, |plan_graph| {
            let inputs = std::iter::once(&rows)
                .chain(groups.as_ref())
                .map(Ident::to_string)
                .collect();
            let (output_name, arrange) = match arrange.as_deref() {
                Some(_) => (format!("{out}_arr"), Some(output.is_k_only())),
                None => (out.to_string(), None),
            };
            plan_graph.aggregate_operator(
                name,
                inputs,
                output_name,
                output.fingerprint(),
                *op,
                matches_mutability,
                groups_kind,
                recursive,
                pred.is_some(),
                arrange,
            );
        });

        let kind = aggregation_kind(*op);
        let target = target_tokens(*target)?;
        // The merge sees the answer as a scalar; the projection reads it
        // as the answer row's one value column.
        let rebind = (v.to_string() == "v").then(|| quote! { let v = (v,); });
        let arrange_stmt = register_arrangement(arranged_map, output, &out, arrange.as_deref());
        let step = match pred {
            None => quote! {
                let #out = ::flowlog_runtime::operators::flowlog_aggregate(
                    #rows.clone(), #seeds, #literal, #kind, #target,
                    |#k, #v: #answer_ty| { #rebind #out_expr },
                );
            },
            Some(pred) => {
                let answers = format_ident!("{out}_answers");
                let body = flat_map_body_tokens(Some(pred), out_expr);
                quote! {
                    let #answers = ::flowlog_runtime::operators::flowlog_aggregate(
                        #rows.clone(), #seeds, #literal, #kind, #target,
                        |k, v: #answer_ty| (k, (v,)),
                    );
                    let #out = ::flowlog_runtime::operators::flowlog_map(
                        #answers,
                        #literal,
                        |(#k, #v), t, d| { #body },
                    );
                }
            }
        };
        Ok(quote! {
            #step
            #arrange_stmt
        })
    }
}

/// Returns the closure reading the aggregated column out of a match's
/// value: nothing for `count`, which has no target.
///
/// # Errors
///
/// Returns an internal error if the target is not a value column, which
/// the planner's target column rules out.
fn target_tokens(target: Option<TransformationArgument>) -> Result<TokenStream, CodegenError> {
    match target {
        None => Ok(quote! { |_| () }),
        Some(TransformationArgument::KV((false, idx))) => {
            let i = Index::from(idx);
            Ok(quote! { |v| v.#i })
        }
        Some(target) => Err(CodegenError::internal(format!(
            "aggregate target {target:?} is not a value column of the matches"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn normalized(tokens: TokenStream) -> String {
        tokens.to_string().split_whitespace().collect()
    }

    /// The target reads its value column; `count` reads nothing.
    // Cases: target, closure.
    #[rstest]
    #[case(None, "|_|()")]
    #[case(Some(TransformationArgument::KV((false, 2))), "|v|v.2")]
    fn the_target_reads_its_value_column(
        #[case] target: Option<TransformationArgument>,
        #[case] expected: &str,
    ) {
        let tokens = target_tokens(target).expect("a value column");
        assert_eq!(normalized(tokens), expected);
    }

    /// A target in the key is a planner bug.
    #[test]
    fn a_target_in_the_key_is_an_internal_error() {
        let error =
            target_tokens(Some(TransformationArgument::KV((true, 0)))).expect_err("a key column");
        assert!(matches!(error, CodegenError::Internal(_)));
    }
}
