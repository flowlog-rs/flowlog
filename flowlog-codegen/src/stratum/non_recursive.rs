//! A non-recursive stratum: its prelude, then each relation's binding,
//! the union of its rule results with its earlier binding, merged by its
//! lattice column when it has one, outside any loop.

use flowlog_planner::planner::StratumPlanner;
use flowlog_profiler::PlanGraph;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;
use crate::operator::BindingPart;

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
        for (idb_fp, binding_plan) in stratum.relation_bindings() {
            let name = self.display_name(*idb_fp);
            let binding = self.find_global_ident(*idb_fp);
            // Rebinding must retain earlier inputs and results, at their
            // original weight until the union lifts them.
            let parts: Vec<BindingPart> = binding_plan
                .seed()
                .into_iter()
                .chain(binding_plan.inputs().iter().map(|input| input.as_ref()))
                .map(|input| BindingPart {
                    collection: self.find_global_ident(input.fingerprint()),
                    mutability: input.mutability(),
                })
                .collect();
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
                stratum.mutability(*idb_fp),
                Some(mutability),
                "`{name}` binds at a weight the stratifier does not give it",
            );
            self.global_fp_to_mutability.insert(*idb_fp, mutability);
            bindings.push(quote! { let #binding = #rows; });
        }
        Ok(quote! { #(#prelude)* #(#bindings)* })
    }
}

#[cfg(test)]
mod tests {
    use flowlog_planner::planner::ProgramPlanner;

    use crate::test_harness::codegen;

    #[test]
    fn logical_binding_inputs_connect_without_runtime_maps() {
        let mut codegen = codegen(
            ".decl R(a:int32,b:symbol)\n.input R\n\
             .decl Out(a:int32,b:symbol)\n.output Out\nOut(a,b) :- R(a,b).\n",
        );
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).unwrap();
        let dataflow = codegen
            .generate(&plan, &mut None)
            .unwrap()
            .dataflow
            .to_string();
        assert!(dataflow.contains("flowlog_dedup"));
        assert!(!dataflow.contains("flowlog_map"));
    }
}
