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
    use std::collections::HashMap;

    use flowlog_common::compute_fp;
    use flowlog_parser::DataType;
    use flowlog_planner::planner::ProgramPlanner;
    use flowlog_profiler::PlanGraph;
    use proc_macro2::Ident;
    use quote::quote;
    use rstest::rstest;

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

    // Cases: earlier input declaration, total map count.
    #[rstest]
    #[case::rules_only("", 4)]
    #[case::with_seed(".input Out", 5)]
    fn differently_ordered_rule_inputs_emit_one_binding_layout(
        #[case] declaration: &str,
        #[case] maps: usize,
    ) {
        let mut codegen = codegen(&format!(
            ".decl A(a:int64,b:symbol,c:int32)\n.input A\n\
             .decl B(b:symbol,c:int32,a:int64)\n.input B\n\
             .decl C(c:int32,a:int64,b:symbol)\n.input C\n\
             .decl Out(a:int64,b:symbol,c:int32)\n{declaration}\n.output Out\n\
             Out(a+1,b,c) :- A(a,b,c).\n\
             Out(a+2,b,c) :- B(b,c,a).\n\
             Out(a+3,b,c) :- C(c,a,b).\n",
        ));
        codegen.config.profile = true;
        let relation = compute_fp("out");
        let plan = ProgramPlanner::from_program_with_binding_orders_for_test(
            &codegen.program,
            &HashMap::from([(relation, vec![1, 0, 2])]),
        )
        .unwrap();
        let stratum = &plan.strata()[0];
        let binding = &stratum.relation_bindings()[&relation];
        assert_eq!(binding.inputs().len(), 3);
        let cases = binding.inputs().iter().zip([
            "V:(V1, V0 + 1, V2)",
            "V:(V0, V2 + 2, V1)",
            "V:(V2, V1 + 3, V0)",
        ]);
        for (input, expected) in cases {
            let head = stratum
                .non_recursive_transformations()
                .iter()
                .find(|step| step.output().fingerprint() == input.fingerprint())
                .unwrap();
            assert_eq!(head.flow().to_string(), expected);
            assert!(std::sync::Arc::ptr_eq(input, head.output()));
        }
        let mut graph = PlanGraph::new(false);
        graph.enter_scope();
        let mut graph = Some(graph);
        let generated = codegen.gen_non_recursive(stratum, &mut graph).unwrap();
        syn::parse2::<syn::Block>(quote! {{ #generated }}).unwrap();
        let generated = generated.to_string();
        let parts: Vec<Ident> = binding
            .seed()
            .into_iter()
            .chain(binding.inputs().iter().map(|input| input.as_ref()))
            .map(|input| codegen.find_global_ident(input.fingerprint()))
            .collect();
        let (first, rest) = parts.split_first().unwrap();
        let direct = quote! {
            ::flowlog_runtime::operators::flowlog_dedup(
                #first.clone().concatenate([#(#rest.clone()),*])
            )
        };
        assert!(generated.contains(&direct.to_string()));
        assert!(!generated.contains("DedupOrder"));
        assert_eq!(generated.matches("DedupRestore: Out").count(), 1);
        let map = quote! { ::flowlog_runtime::operators::flowlog_map }.to_string();
        assert_eq!(generated.matches(&map).count(), maps);
        for input in binding.inputs() {
            assert_eq!(
                codegen.find_global_type(input.fingerprint()).unwrap().1,
                [DataType::String, DataType::Int64, DataType::Int32]
            );
        }
        assert_eq!(
            codegen.find_global_type(relation).unwrap().1,
            [DataType::Int64, DataType::String, DataType::Int32]
        );
    }

    // Cases: selected order, head expressions, types, map count.
    #[rstest]
    #[case::direct(vec![1,0,2], "V:(RV0, LV0, RV1)", vec![DataType::String, DataType::Int64, DataType::Int32], 3)]
    #[case::last_first(vec![2,0,1], "V:(RV1, LV0, RV0)", vec![DataType::Int32, DataType::Int64, DataType::String], 3)]
    #[case::logical(vec![0,1,2], "V:(LV0, RV0, RV1)", vec![DataType::Int64, DataType::String, DataType::Int32], 2)]
    fn join_heads_emit_the_binding_format_directly(
        #[case] order: Vec<usize>,
        #[case] expected: &str,
        #[case] types: Vec<DataType>,
        #[case] maps: usize,
    ) {
        let mut codegen = codegen(
            ".decl R(k:int32,a:int64)\n.input R\n\
             .decl S(k:int32,b:symbol,c:int32)\n.input S\n\
             .decl Out(a:int64,b:symbol,c:int32)\n.output Out\n\
             Out(a,b,c) :- R(k,a), S(k,b,c).\n",
        );
        let relation = compute_fp("out");
        let plan = ProgramPlanner::from_program_with_binding_orders_for_test(
            &codegen.program,
            &HashMap::from([(relation, order)]),
        )
        .unwrap();
        let stratum = &plan.strata()[0];
        let input = &stratum.relation_bindings()[&relation].inputs()[0];
        let head = stratum
            .non_recursive_transformations()
            .iter()
            .find(|step| step.output().fingerprint() == input.fingerprint())
            .unwrap();
        assert_eq!(head.flow().to_string(), expected);
        assert!(std::sync::Arc::ptr_eq(input, head.output()));
        let dataflow = codegen.generate(&plan, &mut None).unwrap().dataflow;
        syn::parse2::<syn::Block>(quote! {{ #dataflow }}).unwrap();
        let dataflow = dataflow.to_string();
        let rows = codegen.find_global_ident(input.fingerprint());
        assert!(dataflow.contains(
            &quote! { ::flowlog_runtime::operators::flowlog_dedup(#rows.clone()) }.to_string()
        ));
        assert_eq!(
            codegen.find_global_type(input.fingerprint()).unwrap().1,
            types
        );
        let map = quote! { ::flowlog_runtime::operators::flowlog_map }.to_string();
        assert_eq!(dataflow.matches(&map).count(), maps);
    }

    // Cases: earlier input declaration, seed presence, total map count.
    #[rstest]
    #[case::rules_only("", false, 3)]
    #[case::with_seed(".input Out", true, 4)]
    fn rule_heads_emit_the_selected_order_before_union(
        #[case] declaration: &str,
        #[case] has_seed: bool,
        #[case] maps: usize,
    ) {
        let mut codegen = codegen(&format!(
            ".decl A(a:int64,b:symbol,c:int32)\n.input A\n\
             .decl B(a:int64,b:symbol,c:int32)\n.input B\n\
             .decl Out(a:int64,b:symbol,c:int32)\n{declaration}\n.output Out\n\
             Out(a+1,b,c) :- A(a,b,c).\nOut(a+2,b,c) :- B(a,b,c).\n",
        ));
        let relation = compute_fp("out");
        let plan = ProgramPlanner::from_program_with_binding_orders_for_test(
            &codegen.program,
            &HashMap::from([(relation, vec![2, 0, 1])]),
        )
        .unwrap();
        let stratum = &plan.strata()[0];
        let binding = &stratum.relation_bindings()[&relation];
        assert_eq!(binding.inputs().len(), 2);
        assert_eq!(binding.seed().is_some(), has_seed);
        let cases = binding
            .inputs()
            .iter()
            .zip(["V:(V2, V0 + 1, V1)", "V:(V2, V0 + 2, V1)"]);
        for (input, expected) in cases {
            let head = stratum
                .non_recursive_transformations()
                .iter()
                .find(|step| step.output().fingerprint() == input.fingerprint())
                .unwrap();
            assert_eq!(head.flow().to_string(), expected);
        }
        let dataflow = codegen.generate(&plan, &mut None).unwrap().dataflow;
        syn::parse2::<syn::Block>(quote! {{ #dataflow }}).unwrap();
        let dataflow = dataflow.to_string();
        for input in binding.inputs() {
            assert_eq!(
                codegen.find_global_type(input.fingerprint()).unwrap().1,
                [DataType::Int32, DataType::Int64, DataType::String]
            );
        }
        let left = codegen.find_global_ident(binding.inputs()[0].fingerprint());
        let right = codegen.find_global_ident(binding.inputs()[1].fingerprint());
        let union = match binding.seed() {
            Some(seed) => {
                let conversion = stratum
                    .non_recursive_transformations()
                    .iter()
                    .find(|step| step.output().fingerprint() == seed.fingerprint())
                    .unwrap();
                assert_eq!(conversion.unary_input().fingerprint(), relation);
                assert_eq!(conversion.flow().to_string(), "V:(V2, V0, V1)");
                let seed_rows = codegen.find_global_ident(seed.fingerprint());
                quote! { #seed_rows.clone().concatenate([#left.clone(), #right.clone()]) }
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
        let map = quote! { ::flowlog_runtime::operators::flowlog_map }.to_string();
        assert_eq!(dataflow.matches(&map).count(), maps);
    }

    // Cases: the other consumer selects the same physical order.
    #[rstest]
    #[case::conflicting(false)]
    #[case::matching(true)]
    fn sharing_respects_each_bindings_input_format(#[case] matching: bool) {
        let mut codegen = codegen(
            ".decl A(a:int64,b:symbol,c:int32)\n.input A\n\
             .decl Out(a:int64,b:symbol,c:int32)\n.output Out\n\
             .decl Other(a:int64,b:symbol,c:int32)\n.output Other\n\
             Out(a+1,b,c) :- A(a,b,c).\nOther(a+1,b,c) :- A(a,b,c).\n",
        );
        let mut orders = HashMap::from([(compute_fp("out"), vec![2, 0, 1])]);
        if matching {
            orders.insert(compute_fp("other"), vec![2, 0, 1]);
        }
        let plan =
            ProgramPlanner::from_program_with_binding_orders_for_test(&codegen.program, &orders)
                .unwrap();
        let stratum = &plan.strata()[0];
        let out = &stratum.relation_bindings()[&compute_fp("out")].inputs()[0];
        let other = &stratum.relation_bindings()[&compute_fp("other")].inputs()[0];
        assert_eq!(out.fingerprint() == other.fingerprint(), matching);
        let head = stratum
            .non_recursive_transformations()
            .iter()
            .find(|step| step.output().fingerprint() == out.fingerprint())
            .unwrap();
        assert_eq!(head.flow().to_string(), "V:(V2, V0 + 1, V1)");
        if !matching {
            let head = stratum
                .non_recursive_transformations()
                .iter()
                .find(|step| step.output().fingerprint() == other.fingerprint())
                .unwrap();
            assert_eq!(head.flow().to_string(), "V:(V0 + 1, V1, V2)");
        }
        let dataflow = codegen
            .generate(&plan, &mut None)
            .unwrap()
            .dataflow
            .to_string();
        for input in [out, other] {
            let rows = codegen.find_global_ident(input.fingerprint());
            assert!(dataflow.contains(
                &quote! { ::flowlog_runtime::operators::flowlog_dedup(#rows.clone()) }.to_string()
            ));
        }
        let map = quote! { ::flowlog_runtime::operators::flowlog_map }.to_string();
        assert_eq!(dataflow.matches(&map).count(), 3);
    }

    #[test]
    fn profiling_tracks_head_and_seed_maps_without_a_binding_projection() {
        let mut codegen = codegen(
            ".decl R(a:int64,b:symbol,c:int32)\n.input R\n\
             .decl S(a:int64,b:symbol,c:int32)\n.input S\n.output S\n\
             S(a,b,c) :- R(a,b,c).\n",
        );
        codegen.config.profile = true;
        let relation = compute_fp("s");
        let plan = ProgramPlanner::from_program_with_binding_orders_for_test(
            &codegen.program,
            &HashMap::from([(relation, vec![2, 0, 1])]),
        )
        .unwrap();
        let stratum = &plan.strata()[0];
        let binding = &stratum.relation_bindings()[&relation];
        let seed = binding.seed().unwrap();
        let input = &binding.inputs()[0];
        let mut graph = PlanGraph::new(false);
        graph.enter_scope();
        let mut graph = Some(graph);
        codegen.gen_non_recursive(stratum, &mut graph).unwrap();
        let graph = graph.as_mut().unwrap();
        let output = codegen.find_global_ident(relation).to_string();
        graph.map_join_operator("following".into(), vec![output], "after".into(), 0x123);
        let nodes = graph.nodes();
        let head_fp = format!("0x{:016x}", input.fingerprint());
        let seed_fp = format!("0x{:016x}", seed.fingerprint());
        let head = nodes
            .iter()
            .find(|node| node.fingerprint.as_deref() == Some(&head_fp))
            .unwrap();
        let seed = nodes
            .iter()
            .find(|node| node.fingerprint.as_deref() == Some(&seed_fp))
            .unwrap();
        assert_eq!(head.operators.len(), 1);
        assert_eq!(head.operators[0].0, [0, 1]);
        assert_eq!(seed.operators.len(), 1);
        assert_eq!(seed.operators[0].0, [0, 2]);
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
        assert_eq!(union.parents, [seed.id, head.id]);
        let following = nodes.last().unwrap();
        assert_eq!(following.operators[0].0, [0, 8]);
        assert_eq!(following.parents, [union.id]);
    }
}
