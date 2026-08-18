//! Non-recursive flow codegen — two entry points per stratum:
//!
//! - **Core flows.** The stratum's non-recursive transformations; a
//!   recursive stratum can also carry these when the planner factors
//!   non-recursive work out of a fixpoint.
//! - **Post flows.** Final output processing after core flows: union the
//!   heads producing each IDB, dedup, and apply aggregation. Emitted only
//!   for non-recursive strata.

use std::collections::HashSet;
use std::mem;

use flowlog_planner::planner::StratumPlanner;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;
use tracing::trace;

use crate::codegen::CodeGen;
use crate::codegen::CodegenError;
use crate::codegen::aggregation::aggregation_kind;
use crate::codegen::aggregation::aggregation_merge;
use crate::codegen::aggregation::aggregation_split;

// =========================================================================
// Non-Recursive Flow Generation
// =========================================================================
impl CodeGen {
    /// Emit the stratum's non-recursive transformation pipelines into the
    /// program-wide outer-scope arrangement cache (`self.outer_arranged`).
    pub(crate) fn gen_non_recursive_core_flows(
        &mut self,
        stratum: &StratumPlanner,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Vec<TokenStream>, CodegenError> {
        let mut flows = Vec::new();
        let global_fp_to_ident = self.global_fp_to_ident.clone();
        let mut outer_arranged = mem::take(&mut self.outer_arranged);

        for transformation in stratum.non_recursive_transformations() {
            flows.push(self.gen_transformation(
                &global_fp_to_ident,
                transformation,
                &mut outer_arranged,
                stratum,
                plan_graph,
            )?);
        }

        self.outer_arranged = outer_arranged;

        trace!("Generated static flows:\n{}\n", quote! { #(#flows)* });
        Ok(flows)
    }

    /// Emit per-IDB post-processing: union the contributing heads, dedup,
    /// and apply aggregation.
    pub(crate) fn gen_non_recursive_post_flows(
        &mut self,
        bound_fps: &HashSet<u64>,
        stratum: &StratumPlanner,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Vec<TokenStream>, CodegenError> {
        let mut flows = Vec::new();

        for (idb_fp, head_fps) in stratum.idb_to_heads_map() {
            let output = self.find_global_ident(*idb_fp);
            let outs: Vec<Ident> = head_fps
                .iter()
                .map(|fp| format_ident!("t_{}", fp))
                .collect();

            // Union the per-head collections.
            let head = &outs[0];
            let tail = &outs[1..];

            // Fold into the existing binding rather than shadowing it.
            let already_bound = bound_fps.contains(idb_fp);
            let (concat_expr, concat_count) = if already_bound {
                (quote! { #output.concatenate([ #( #outs.clone() ),* ]) }, 1)
            } else if tail.is_empty() {
                (quote! { #head.clone() }, 0)
            } else {
                (
                    quote! { #head.clone().concatenate([ #( #tail.clone() ),* ]) },
                    1,
                )
            };

            with_plan_graph(plan_graph, |plan_graph| {
                plan_graph.concat_dedup_operator(
                    self.display_name(*idb_fp),
                    outs.iter().map(|id| id.to_string()).collect(),
                    output.to_string(),
                    concat_count,
                    false,
                );
            });

            let mut block = quote! {
                let #output = ::flowlog_runtime::operators::flowlog_dedup(#concat_expr);
            };

            if let Some((agg_op, agg_pos, agg_arity)) = stratum.idb_to_aggregation_map().get(idb_fp)
            {
                let agg_type = self.agg_column_type(*idb_fp, *agg_pos)?;
                let kind = aggregation_kind(*agg_op);
                let split = aggregation_split(*agg_arity, *agg_pos);
                let merge = aggregation_merge(*agg_arity, *agg_pos, &agg_type);
                let op_name = format!("Reduce: {}", self.display_name(*idb_fp));
                block = quote! {
                    #block
                    let #output = ::flowlog_runtime::operators::flowlog_reduce(
                        #output, #op_name, #kind, #split, #merge,
                    );
                };

                // The runtime picks its strategy from the ambient difference,
                // and the two build different operators, so the plan graph
                // has to predict the same way.
                let name = self.display_name(*idb_fp);
                let binding = output.to_string();
                with_plan_graph(plan_graph, |plan_graph| {
                    if self.config.is_datalog_batch() {
                        plan_graph.opt_aggregate_operator(name, binding.clone(), binding);
                    } else {
                        plan_graph.general_aggregate_operator(name, binding.clone(), binding);
                    }
                });
            }

            flows.push(block);
        }

        trace!(
            "Generated post-processing flows:\n{}\n",
            quote! { #(#flows)* }
        );
        Ok(flows)
    }
}
