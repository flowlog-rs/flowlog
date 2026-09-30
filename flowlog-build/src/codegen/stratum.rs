//! Strata in evaluation order. Each stratum emits its prelude, the planned
//! rule bodies that run outside any loop, then its [`non_recursive`] heads
//! or its [`recursive`] loop; the rules' pieces come from
//! [`rule`](crate::codegen::rule).

mod non_recursive;
mod recursive;

use std::collections::HashSet;
use std::mem;

use flowlog_planner::planner::StratumPlanner;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::TokenStream;
use quote::quote;
use tracing::trace;

use crate::codegen::CodeGen;
use crate::codegen::CodegenError;

impl CodeGen {
    /// Emits every stratum of the plan, in evaluation order.
    pub(crate) fn gen_strata(
        &mut self,
        strata: &[StratumPlanner],
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Vec<TokenStream>, CodegenError> {
        let mut flows = Vec::new();
        // Relations whose outer ident is already bound, by an input or a
        // prior stratum. Without the inputs, a rule for an EDB relation
        // would shadow the EDB binding and drop its tuples.
        let mut bound_fps: HashSet<u64> = self.program.edb_fingerprints();

        for (idx, stratum) in strata.iter().enumerate() {
            with_plan_graph(plan_graph, |plan_graph| {
                plan_graph.update_stratum_block(idx);
            });

            flows.extend(self.gen_prelude(stratum, plan_graph)?);
            if stratum.is_recursive() {
                let outer_snapshot = self.outer_arranged.clone();
                flows.push(self.gen_recursive(&outer_snapshot, stratum, plan_graph)?);
            } else {
                flows.extend(self.gen_non_recursive(stratum, &bound_fps, plan_graph)?);
            }

            bound_fps.extend(stratum.output_relations());
        }
        Ok(flows)
    }

    /// Emits the stratum's prelude: its planned steps outside any loop, into
    /// the program-wide outer-scope arrangement cache (`self.outer_arranged`).
    /// A recursive stratum has one too when the planner factors work that
    /// reads no feedback out of its loop.
    fn gen_prelude(
        &mut self,
        stratum: &StratumPlanner,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Vec<TokenStream>, CodegenError> {
        let global_fp_to_ident = self.global_fp_to_ident.clone();
        let mut outer_arranged = mem::take(&mut self.outer_arranged);
        let flows = stratum
            .non_recursive_transformations()
            .iter()
            .map(|transformation| {
                self.gen_transformation(
                    &global_fp_to_ident,
                    transformation,
                    &mut outer_arranged,
                    stratum,
                    plan_graph,
                )
            })
            .collect::<Result<Vec<_>, _>>();
        self.outer_arranged = outer_arranged;
        let flows = flows?;
        trace!("Generated prelude:\n{}\n", quote! { #(#flows)* });
        Ok(flows)
    }
}
