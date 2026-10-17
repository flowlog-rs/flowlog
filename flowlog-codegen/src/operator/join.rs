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
use crate::ident::find_local_ident;

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
                let arr = format!("{}_arr", out);
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
            let #out = ::flowlog_runtime::operators::flowlog_join(
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
            recursive,
            si,
            ..
        } = operator;

        with_plan_graph(plan_graph, |plan_graph| {
            let inputs = vec![l.to_string(), r.to_string()];
            let fp = output.fingerprint();
            let (lm, rm) = (left.mutability(), right.mutability());
            if arrange.is_some() {
                let arr = format!("{}_arr", out);
                let k_only = output.is_k_only();
                plan_graph
                    .anti_join_arrange_operator(name, inputs, arr, fp, k_only, lm, rm, recursive);
            } else {
                plan_graph.anti_join_operator(name, inputs, out.to_string(), fp, lm, rm, recursive);
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
            let #out = ::flowlog_runtime::operators::flowlog_antijoin(
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
