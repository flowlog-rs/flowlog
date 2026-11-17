//! A non-recursive stratum: its prelude, then each relation's binding,
//! the union of its rule results with its earlier binding, merged by its
//! lattice column when it has one, outside any loop, and mapped into the
//! relation's own layout right where it collapses.

use std::sync::Arc;

use flowlog_parser::Mutability;
use flowlog_planner::planner::StratumPlanner;
use flowlog_profiler::PlanGraph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;

impl Codegen {
    /// Returns a non-recursive `stratum`: its prelude, then the binding
    /// of every relation it derives. The plan selects its earlier seed
    /// and binding operation; each part keeps its recorded weight until
    /// the union lifts it.
    pub(super) fn gen_non_recursive(
        &mut self,
        stratum: &StratumPlanner,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let prelude = self.gen_prelude(stratum, plan_graph)?;
        let mut bindings = Vec::new();
        for (idb_fp, binding) in stratum.relation_bindings() {
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
                /*recursive*/ false,
                plan_graph,
            )?;
            let export = self.gen_binding_export(binding, plan_graph)?;
            self.global_fp_to_mutability
                .insert(*idb_fp, binding.output().mutability());
            bindings.push(quote! {
                let #collapse_ident = #rows;
                #export
            });
        }
        Ok(quote! { #(#prelude)* #(#bindings)* })
    }
}
