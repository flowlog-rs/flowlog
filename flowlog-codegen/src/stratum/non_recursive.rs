//! A non-recursive stratum: its prelude, then each relation's binding,
//! the union of its rule results with its earlier binding, merged by its
//! lattice column when it has one, outside any loop.

use std::collections::HashMap;

use flowlog_planner::planner::StratumPlanner;
use flowlog_profiler::PlanGraph;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;
use crate::ident::find_local_ident;
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
            let mut parts = Vec::new();
            let projections = binding_plan.seed().into_iter().chain(binding_plan.inputs());
            for projection in projections {
                let source = projection.unary_input().fingerprint();
                let local = HashMap::from([(source, self.find_global_ident(source))]);
                bindings.push(self.gen_operator(
                    &local,
                    projection,
                    &mut HashMap::new(),
                    stratum,
                    plan_graph,
                )?);
                parts.push(BindingPart {
                    collection: find_local_ident(&local, projection.output().fingerprint()),
                    mutability: projection.output().mutability(),
                });
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
    use flowlog_common::compute_fp;
    use flowlog_parser::DataType;
    use flowlog_planner::planner::ProgramPlanner;
    use flowlog_profiler::PlanGraph;
    use quote::quote;
    use rstest::rstest;

    use crate::test_harness::codegen;

    #[test]
    fn identity_binding_projections_emit_aliases_without_runtime_maps() {
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
        assert!(!dataflow.contains("DedupOrder"));
    }

    // Cases: earlier input declaration, expected seed presence.
    #[rstest]
    #[case::rule_results_only("", false)]
    #[case::with_seed(".input Out", true)]
    fn non_identity_projections_align_every_binding_input_before_union(
        #[case] seed: &str,
        #[case] expected_seed: bool,
    ) {
        let mut codegen = codegen(&format!(
            ".decl A(a:int64,b:symbol,c:int32)\n.input A\n\
             .decl B(a:int64,b:symbol,c:int32)\n.input B\n\
             .decl Out(a:int64,b:symbol,c:int32)\n{seed}\n.output Out\n\
             .decl Other(a:int64,b:symbol,c:int32)\n.output Other\n\
             Out(a,b,c) :- A(a,b,c).\nOut(a,b,c) :- B(a,b,c).\n\
             Other(a,b,c) :- A(a,b,c).\n",
        ));
        let relation = compute_fp("out");
        let mut plan = ProgramPlanner::from_program(&codegen.program, &mut None).unwrap();
        plan.set_binding_order_for_test(relation, &[2, 0, 1]);
        let binding = &plan.strata()[0].relation_bindings()[&relation];
        assert_eq!(binding.inputs().len(), 2);
        assert_eq!(binding.seed().is_some(), expected_seed);
        let other = &plan.strata()[0].relation_bindings()[&compute_fp("other")].inputs()[0];
        assert!(
            binding
                .input_fingerprints()
                .any(|source| source == other.unary_input().fingerprint())
        );
        assert_eq!(other.flow().to_string(), "V:(V0, V1, V2)");

        let dataflow = codegen.generate(&plan, &mut None).unwrap().dataflow;
        syn::parse2::<syn::Block>(quote! {{ #dataflow }}).unwrap();
        let dataflow = dataflow.to_string();
        let projections = binding.seed().into_iter().chain(binding.inputs());
        for projection in projections {
            let source = codegen.find_global_ident(projection.unary_input().fingerprint());
            let output = codegen.find_global_ident(projection.output().fingerprint());
            let declaration = format!("let {output} = ");
            let initializer = dataflow
                .split_once(&declaration)
                .unwrap()
                .1
                .split_once(';')
                .unwrap()
                .0;
            assert!(initializer.starts_with(":: flowlog_runtime :: operators :: flowlog_map ("));
            assert!(initializer.contains(&quote! { #source.clone() }.to_string()));
            assert!(
                initializer
                    .contains(&quote! { std::iter::once(((x2,x0,x1.clone()),t,d)) }.to_string()),
                "{initializer}",
            );
            assert_eq!(
                codegen
                    .find_global_type(projection.output().fingerprint())
                    .unwrap()
                    .1,
                [DataType::Int32, DataType::Int64, DataType::String],
            );
        }
        let left = codegen.find_global_ident(binding.inputs()[0].output().fingerprint());
        let right = codegen.find_global_ident(binding.inputs()[1].output().fingerprint());
        let union = match binding.seed() {
            Some(seed) => {
                let seed = codegen.find_global_ident(seed.output().fingerprint());
                quote! { #seed.clone().concatenate([#left.clone(), #right.clone()]) }
            }
            None => quote! { #left.clone().concatenate([#right.clone()]) },
        };
        let expected = quote! {
            ::flowlog_runtime::operators::flowlog_map(
                ::flowlog_runtime::operators::flowlog_dedup(#union),
                "DedupRestore: Out",
                |(x2,x0,x1), t, d| std::iter::once(((x0,x1,x2),t,d)),
            )
        };
        assert!(dataflow.contains(&expected.to_string()));
    }

    #[test]
    fn profiling_counts_the_logical_seed_projection_separately_from_the_union() {
        let mut codegen = codegen(
            ".decl R(a:int64,b:symbol,c:int32)\n.input R\n\
             .decl S(a:int64,b:symbol,c:int32)\n.input S\n.output S\n\
             S(a,b,c) :- R(a,b,c).\n",
        );
        codegen.config.profile = true;
        let relation = compute_fp("s");
        let mut plan = ProgramPlanner::from_program(&codegen.program, &mut None).unwrap();
        plan.set_binding_order_for_test(relation, &[2, 0, 1]);
        let stratum = &plan.strata()[0];
        let binding = &stratum.relation_bindings()[&relation];
        let seed = binding.seed().unwrap();
        assert_eq!(binding.inputs().len(), 1);
        let rule = &binding.inputs()[0];
        let mut graph = PlanGraph::new(false);
        graph.enter_scope();
        let mut graph = Some(graph);

        let statements = codegen.gen_non_recursive(stratum, &mut graph).unwrap();
        syn::parse2::<syn::Block>(quote! {{ #statements }}).unwrap();
        let graph = graph.as_mut().unwrap();
        let output = codegen.find_global_ident(relation).to_string();
        graph.map_join_operator("following".into(), vec![output], "after".into(), 0x123);
        let nodes = graph.nodes();
        let seed_fp = format!("0x{:016x}", seed.output().fingerprint());
        let rule_fp = format!("0x{:016x}", rule.output().fingerprint());
        let seed_node = nodes
            .iter()
            .find(|node| node.fingerprint.as_deref() == Some(&seed_fp))
            .unwrap();
        let rule_node = nodes
            .iter()
            .find(|node| node.fingerprint.as_deref() == Some(&rule_fp))
            .unwrap();
        assert_eq!(seed_node.operators.len(), 1);
        assert_eq!(seed_node.operators[0].0, [0, 1]);
        assert_eq!(rule_node.operators.len(), 1);
        assert_eq!(rule_node.operators[0].0, [0, 2]);

        let union = nodes
            .iter()
            .find(|node| node.name == "S: concat & dedup")
            .unwrap();
        let range: Vec<Vec<u32>> = union
            .operators
            .iter()
            .map(|address| address.0.clone())
            .collect();
        assert_eq!(
            range,
            [vec![0, 3], vec![0, 4], vec![0, 5], vec![0, 6], vec![0, 7]]
        );
        assert_eq!(union.parents, [seed_node.id, rule_node.id]);
        let following = nodes.last().unwrap();
        assert_eq!(following.operators.len(), 1);
        assert_eq!(following.operators[0].0, [0, 8]);
        assert_eq!(following.parents, [union.id]);
    }
}
