//! Joins: the operators over two arrangements, a join on their keys or an
//! antijoin keeping what the filter side lacks, and the pieces only they
//! assemble. A join's output is rows or keyed as its [`Operator`] says; a
//! keyed output is arranged for the joins that read it.

use std::collections::HashMap;

use flowlog_planner::planner::Collection;
use flowlog_planner::planner::TransformationFlow;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;

use super::Operator;
use super::arranged_input;
use super::keyed_output;
use super::register_arrangement;
use crate::Codegen;
use crate::CodegenError;
use crate::expr::param::join_params;
use crate::expr::param::kv_params;
use crate::ident::arrangement_ident;
use crate::ident::find_local_ident;
use crate::ty::time::time_order;

impl Codegen {
    /// Returns the join of the arrangements of `left` and `right` on their
    /// keys into `output` by `flow`, as rows or keyed as `operator` says,
    /// and the arrangement of a keyed output.
    pub(super) fn gen_join(
        &mut self,
        operator: Operator<'_>,
        (left, right): (&Collection, &Collection),
        output: &Collection,
        flow: &TransformationFlow,
        arranged_map: &mut HashMap<u64, Ident>,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let (l, r) = arranged_inputs(operator.local_fp_to_ident, arranged_map, left, right)?;
        let out = find_local_ident(operator.local_fp_to_ident, output.fingerprint());
        let Operator {
            name,
            literal,
            arrange,
            si,
            ..
        } = operator;

        with_plan_graph(plan_graph, |plan_graph| {
            let inputs = vec![l.to_string(), r.to_string()];
            let fp = output.fingerprint();
            if arrange.is_some() {
                let arr = arrangement_ident(&out).to_string();
                plan_graph.map_join_arrange_operator(name, inputs, arr, fp, output.is_k_only());
            } else {
                plan_graph.map_join_operator(name, inputs, out.to_string(), fp);
            }
        });

        let (jn_k, jn_lv, jn_rv) = join_params(flow.key(), flow.value(), flow.compares());
        let left_type = self.find_global_type(left.fingerprint())?.clone();
        let right_type = self.find_global_type(right.fingerprint())?.clone();
        let value = self.join_projection(flow.value(), si, &left_type, &right_type)?;
        let out_expr = if arrange.is_some() {
            let key = self.join_projection(flow.key(), si, &left_type, &right_type)?;
            keyed_output(output, key, value)
        } else {
            value
        };
        let pred = self.join_compare_predicate(flow.compares(), si, &left_type, &right_type)?;
        let body = join_body_tokens(pred, out_expr);

        let arrange_stmt = register_arrangement(arranged_map, output, &out, arrange.as_deref());
        Ok(quote! {
            let #out = ::flowlog_runtime::operators::join(
                #l.clone(),
                #r.clone(),
                #literal,
                |#jn_k, #jn_lv, #jn_rv| { #body },
            );
            #arrange_stmt
        })
    }

    /// Returns the antijoin keeping the pairs of the `right` arrangement
    /// whose key the key-only `left` arrangement lacks, into `output` by
    /// `flow`, as rows or keyed as `operator` says, and the arrangement of
    /// a keyed output.
    pub(super) fn gen_antijoin(
        &mut self,
        operator: Operator<'_>,
        (left, right): (&Collection, &Collection),
        output: &Collection,
        flow: &TransformationFlow,
        arranged_map: &mut HashMap<u64, Ident>,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let (l, r) = arranged_inputs(operator.local_fp_to_ident, arranged_map, left, right)?;
        let out = find_local_ident(operator.local_fp_to_ident, output.fingerprint());
        let Operator {
            name,
            literal,
            arrange,
            loop_time,
            si,
            ..
        } = operator;
        let order = time_order(loop_time);

        with_plan_graph(plan_graph, |plan_graph| {
            let inputs = vec![l.to_string(), r.to_string()];
            let fp = output.fingerprint();
            let filter = left.mutability();
            if arrange.is_some() {
                let arr = arrangement_ident(&out).to_string();
                let k_only = output.is_k_only();
                plan_graph.anti_join_arrange_operator(name, inputs, arr, fp, k_only, filter, order);
            } else {
                plan_graph.anti_join_operator(name, inputs, out.to_string(), fp, filter, order);
            }
        });

        // The closure sees the surviving right side's `(k, v)`; the left
        // side only filters by key.
        let (anti_k, anti_v) = kv_params(flow.key(), flow.value(), flow.compares(), None);
        let input_type = self.find_global_type(right.fingerprint())?.clone();
        let value = self.kv_projection(flow.value(), si, &input_type)?;
        let out_expr = if arrange.is_some() {
            keyed_output(
                output,
                self.kv_projection(flow.key(), si, &input_type)?,
                value,
            )
        } else {
            value
        };

        let arrange_stmt = register_arrangement(arranged_map, output, &out, arrange.as_deref());
        Ok(quote! {
            let #out = ::flowlog_runtime::operators::antijoin(
                #l.clone(),
                #r.clone(),
                #literal,
                |( #anti_k, #anti_v )| #out_expr,
            );
            #arrange_stmt
        })
    }
}

/// Returns the arrangements of a join's `left` and `right` inputs, or an
/// internal error when either is not arranged yet.
fn arranged_inputs(
    local_fp_to_ident: &HashMap<u64, Ident>,
    arranged_map: &HashMap<u64, Ident>,
    left: &Collection,
    right: &Collection,
) -> Result<(Ident, Ident), CodegenError> {
    Ok((
        arranged_input(local_fp_to_ident, arranged_map, left)?,
        arranged_input(local_fp_to_ident, arranged_map, right)?,
    ))
}

/// Returns the body of a join closure that yields `out`, only when `pred`
/// holds if there is one. With no predicate the body is a bare `Some`, as
/// the join operator expects an `Option`, not an iterator.
fn join_body_tokens(pred: Option<TokenStream>, out: TokenStream) -> TokenStream {
    match pred {
        Some(pred) => quote! { if #pred { Some( #out ) } else { None } },
        None => quote! { Some( #out ) },
    }
}

#[cfg(test)]
mod tests {
    use flowlog_planner::planner::ProgramPlanner;
    use flowlog_profiler::PlanGraph;
    use rstest::rstest;

    use crate::test_harness::codegen;

    /// Declares a mutable graph and a mutable set of blocked nodes, and
    /// reaches every edge's target; each case adds one more rule, which
    /// loops if it reads `Reach`.
    const PROGRAM: &str = ".decl Edge(x: int32, y: int32) mutable\n\
                           .input Edge(IO=\"command\")\n\
                           .decl Blocked(x: int32) mutable\n\
                           .input Blocked(IO=\"command\")\n\
                           .decl Reach(x: int32)\n.output Reach\n\
                           Reach(y) :- Edge(x, y).\n";

    /// An antijoin whose filter grows dedups what it keeps at its time's
    /// order: in a mutable loop the order is partial, which costs one more
    /// operator than the total order outside a loop. An antijoin whose
    /// output a later join reads also arranges it, here key-only, for two
    /// more.
    // Cases: rule, operators of its only antijoin.
    #[rstest]
    #[case::in_a_mutable_loop("Reach(y) :- Reach(x), Edge(x, y), !Blocked(y).", 10)]
    #[case::outside_a_loop("Reach(y) :- Edge(x, y), !Blocked(y).", 9)]
    #[case::arranged_in_a_mutable_loop(
        "Reach(z) :- Reach(y), Edge(x, y), !Blocked(x), Edge(y, z).",
        12
    )]
    #[case::arranged_outside_a_loop("Reach(z) :- Edge(x, y), !Blocked(x), Edge(y, z).", 11)]
    fn an_antijoin_dedups_at_its_times_order(#[case] rule: &str, #[case] expected: usize) {
        let mut codegen = codegen(&format!("{PROGRAM}{rule}\n"));
        let mut plan_graph = Some(PlanGraph::new(/* incremental */ true));
        let planner =
            ProgramPlanner::from_program(&codegen.program, &mut plan_graph).expect("plans");
        codegen
            .generate(&planner, &mut plan_graph)
            .expect("generates");
        let antijoins: Vec<usize> = plan_graph
            .expect("recorded")
            .nodes()
            .iter()
            .filter(|node| node.name.starts_with("AntiJoin"))
            .map(|node| node.operators.len())
            .collect();

        assert_eq!(antijoins, [expected]);
    }
}
