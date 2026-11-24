//! A non-recursive stratum: its prelude, then each relation's binding,
//! the union of its rule results with its earlier binding, merged by its
//! lattice column or its choice domain when it has one, outside any loop,
//! and mapped into the relation's own layout right where it collapses.

use std::sync::Arc;

use flowlog_parser::Mutability;
use flowlog_planner::planner::StratumPlanner;
use flowlog_profiler::PlanGraph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;
use crate::skeleton::Read;
use crate::ty::time::Clock;

impl Codegen {
    /// Returns a non-recursive `stratum` at `clock`: its prelude, then the
    /// binding of every relation it derives at `clock`, each after the
    /// static parts it enters. The plan selects its earlier seed and
    /// binding operation; each part keeps its recorded weight until the
    /// union lifts it.
    pub(super) fn gen_non_recursive(
        &mut self,
        stratum: &StratumPlanner,
        clock: Clock,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let prelude = self.gen_prelude(stratum, clock, plan_graph)?;
        let mut bindings = Vec::new();
        for (idb_fp, binding) in stratum.relation_bindings() {
            // A static relation binds once, in the `Once` pass, before any
            // collection that can change is generated: its parts are all
            // static (Lemma 4 of `docs/design/mutability.md`).
            let mutability = binding.output().mutability();
            if Clock::from_mutability(mutability) != clock {
                continue;
            }
            // Rebinding must retain earlier inputs and results, at their
            // original weight until the union lifts them.
            let sources = binding
                .input_seed()
                .into_iter()
                .chain(binding.input_contributors().iter().map(Arc::as_ref));
            let mut parts: Vec<(Ident, Mutability)> = Vec::new();
            for input in sources {
                let fp = input.fingerprint();
                bindings.extend(self.gen_epoch_read(fp, Read::Rows, clock, plan_graph)?);
                parts.push((self.find_global_ident(fp), input.mutability()));
            }
            let collapse_ident = self.find_global_ident(binding.collapse().fingerprint());
            let rows = self.gen_relation_binding(
                &parts,
                binding,
                &collapse_ident,
                /*recursive*/ false,
                plan_graph,
            )?;
            let export = self.gen_binding_export(binding, plan_graph)?;
            self.global_fp_to_mutability.insert(*idb_fp, mutability);
            bindings.push(quote! {
                let #collapse_ident = #rows;
                #export
            });
        }
        Ok(quote! { #(#prelude)* #(#bindings)* })
    }
}
