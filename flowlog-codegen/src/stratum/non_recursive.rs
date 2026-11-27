//! A non-recursive stratum, its static part and its non-static part
//! apart: a part's prelude, then each of its relations' bindings, the
//! union of the relation's rule results with its earlier binding, merged
//! by its lattice column or its choice domain when it has one, outside any
//! loop, and mapped into the relation's own layout right where it
//! collapses.

use std::sync::Arc;

use flowlog_parser::Mutability;
use flowlog_planner::planner::RelationBinding;
use flowlog_planner::planner::StratumPlanner;
use flowlog_profiler::PlanGraph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;

impl Codegen {
    /// Returns the static part of the non-recursive `stratum`: its static
    /// prelude, then the binding of every relation it leaves static.
    pub(super) fn gen_static_non_recursive(
        &mut self,
        stratum: &StratumPlanner,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let steps = stratum.non_recursive_static_transformations();
        let prelude = self.gen_prelude(stratum, steps, plan_graph)?;
        let bindings = self.gen_bindings(&stratum.static_relation_bindings(), plan_graph)?;
        Ok(quote! { #(#prelude)* #(#bindings)* })
    }

    /// Returns the non-static part of the non-recursive `stratum`: its
    /// non-static prelude, then the binding of every relation it leaves
    /// non-static.
    pub(super) fn gen_non_static_non_recursive(
        &mut self,
        stratum: &StratumPlanner,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let steps = stratum.non_recursive_non_static_transformations();
        let prelude = self.gen_prelude(stratum, steps, plan_graph)?;
        let bindings = self.gen_bindings(&stratum.non_static_relation_bindings(), plan_graph)?;
        Ok(quote! { #(#prelude)* #(#bindings)* })
    }

    /// Returns each of `bindings` outside any loop. The plan selects a
    /// binding's earlier seed and operation; each input keeps its recorded
    /// weight until the union lifts it.
    fn gen_bindings(
        &mut self,
        bindings: &[&RelationBinding],
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Vec<TokenStream>, CodegenError> {
        let mut flows = Vec::new();
        for binding in bindings {
            // Rebinding must retain earlier inputs and results, at their
            // original weight until the union lifts them.
            let parts: Vec<(Ident, Mutability)> = binding
                .input_seed()
                .into_iter()
                .chain(binding.input_contributors().iter().map(Arc::as_ref))
                .map(|input| {
                    (
                        self.find_global_ident(input.fingerprint()),
                        input.mutability(),
                    )
                })
                .collect();
            let collapse_ident = self.find_global_ident(binding.collapse().fingerprint());
            let rows = self.gen_relation_binding(
                &parts,
                binding,
                &collapse_ident,
                /*loop_time*/ None,
                plan_graph,
            )?;
            let export = self.gen_binding_export(binding, plan_graph)?;
            let output = binding.output();
            self.global_fp_to_mutability
                .insert(output.fingerprint(), output.mutability());
            flows.push(quote! {
                let #collapse_ident = #rows;
                #export
            });
        }
        Ok(flows)
    }
}
