//! A non-recursive stratum: its prelude, then each relation's binding,
//! the union of its rule results with its earlier binding, merged by its
//! lattice column when it has one, outside any loop.

use flowlog_parser::Mutability;
use flowlog_planner::planner::StratumPlanner;
use flowlog_profiler::PlanGraph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;
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
        for (idb_fp, binding_plan) in stratum.relation_bindings() {
            let name = self.display_name(*idb_fp);
            // A static relation binds once, in the `Once` pass, before any
            // collection that can change is generated: its parts are all
            // static (Lemma 4 of `docs/design/mutability.md`).
            let planned = stratum.mutability(*idb_fp).ok_or_else(|| {
                CodegenError::internal(format!("`{name}` has no planned mutability"))
            })?;
            if Clock::of(planned) != clock {
                continue;
            }
            let binding = self.find_global_ident(*idb_fp);
            // Rebinding must retain earlier inputs and results, at their
            // original weight until the union lifts them.
            let earlier = binding_plan.seed();
            let sources = earlier.iter().chain(binding_plan.inputs());
            let mut parts: Vec<(Ident, Mutability)> = Vec::new();
            for fp in sources {
                let part = self.find_global_ident(*fp);
                bindings.extend(self.gen_epoch_enter(&part, *fp, clock, plan_graph)?);
                parts.push((part, self.mutability(*fp)?));
            }
            let (rows, mutability) = self.gen_relation_binding(
                *idb_fp,
                &parts,
                binding_plan.operation(),
                &binding,
                /*recursive*/ false,
                plan_graph,
            )?;
            // The stratifier assigns the relation the same value on its own;
            // the two derivations agree by Lemma 4 of
            // `docs/design/mutability.md`.
            debug_assert_eq!(
                mutability, planned,
                "`{name}` binds at a weight the stratifier does not give it",
            );
            self.global_fp_to_mutability.insert(*idb_fp, mutability);
            bindings.push(quote! { let #binding = #rows; });
        }
        Ok(quote! { #(#prelude)* #(#bindings)* })
    }
}
