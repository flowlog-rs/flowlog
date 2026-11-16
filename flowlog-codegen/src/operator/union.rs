//! Relation-binding lowering: unions rule results with an earlier binding,
//! applies the planned operation, and records the runtime steps.

use flowlog_parser::Mutability;
use flowlog_planner::planner::RelationBinding;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;
use crate::ty::diff::weight_tokens;

impl Codegen {
    /// Returns the bound relation's rows: `binding`'s operation applied to
    /// the union of `parts`, each a producer's rows with their weight,
    /// already in the relation's row layout. The union lifts every part to
    /// the most mutable part's weight. Records the runtime steps under
    /// `output`, inside a loop when `recursive` is true.
    ///
    /// # Errors
    ///
    /// Returns an internal error for no parts, or a lattice merge index
    /// outside the row.
    pub(crate) fn gen_relation_binding(
        &self,
        parts: &[(Ident, Mutability)],
        binding: &RelationBinding,
        output: &Ident,
        recursive: bool,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let idb_fp = binding.output().fingerprint();
        let name = self.display_name(idb_fp);
        let Some((first, rest)) = parts.split_first() else {
            return Err(CodegenError::internal(format!(
                "relation `{name}` has no collections to union"
            )));
        };
        let dedup = match binding {
            RelationBinding::UnionDedup { .. } => true,
            RelationBinding::UnionLattice { .. } => false,
        };
        let mutability = rest
            .iter()
            .map(|(_, part)| *part)
            .fold(first.1, Mutability::max);
        // The lift names its target weight before concatenation or dedup
        // would otherwise fix it by type inference.
        let lift_name = format!("Lift: {name}");
        let weight = weight_tokens(mutability);
        let input_rows = |(collection, part): &(Ident, Mutability)| {
            let mut rows = quote! { #collection.clone() };
            if *part < mutability {
                rows = quote! {
                    ::flowlog_runtime::operators::flowlog_lift::<#weight, _, _, _>(
                        #rows, #lift_name,
                    )
                };
            }
            rows
        };
        let first_rows = input_rows(first);
        let tail: Vec<TokenStream> = rest.iter().map(input_rows).collect();
        let union = if tail.is_empty() {
            first_rows
        } else {
            quote! { #first_rows.concatenate([ #( #tail ),* ]) }
        };

        with_plan_graph(plan_graph, |plan_graph| {
            let lifts: u32 = parts
                .iter()
                .map(|(_, part)| u32::from(*part < mutability))
                .sum();
            plan_graph.union_operator(
                name.to_string(),
                parts.iter().map(|(id, _)| id.to_string()).collect(),
                output.to_string(),
                lifts,
                u32::from(!tail.is_empty()),
                dedup.then_some((mutability, recursive)),
            );
        });

        match binding {
            RelationBinding::UnionDedup { .. } => {
                Ok(quote! { ::flowlog_runtime::operators::flowlog_dedup(#union) })
            }
            RelationBinding::UnionLattice {
                merge_index,
                lattice,
                ..
            } => {
                // The union feeding the merge is recorded under the output's
                // name too, so the merge reads from it.
                with_plan_graph(plan_graph, |plan_graph| {
                    plan_graph.lattice_operator(
                        name.to_string(),
                        output.to_string(),
                        output.to_string(),
                        idb_fp,
                        mutability,
                    );
                });
                self.gen_lattice_merge(idb_fp, union, *merge_index, *lattice)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use flowlog_common::compute_fp;
    use flowlog_planner::planner::ProgramPlanner;
    use proc_macro2::Span;
    use rstest::rstest;

    use super::*;
    use crate::test_harness::codegen;

    /// A static input `R` and a static output `S` derived from it.
    const PROGRAM: &str =
        ".decl R(a: int32)\n.input R\n.decl S(a: int32)\nS(a) :- R(a).\n.output S\n";

    /// As [`PROGRAM`], with `S` a lattice relation.
    const LATTICE_PROGRAM: &str = ".decl R(a: int32, b: int32)\n.input R\n\
        .decl S(a: int32, b: min<int32>)\nS(a, b) :- R(a, b).\n.output S\n";

    fn ident(name: &str) -> Ident {
        Ident::new(name, Span::call_site())
    }

    fn part(name: &str, mutability: Mutability) -> (Ident, Mutability) {
        (ident(name), mutability)
    }

    /// The binding `plan` gives relation `name`.
    fn binding_of<'p>(plan: &'p ProgramPlanner, name: &str) -> &'p RelationBinding {
        plan.strata()
            .iter()
            .find_map(|stratum| stratum.relation_bindings().get(&compute_fp(name)))
            .expect("the relation is bound")
    }

    /// `S` unions its parts in order at its most mutable part's weight,
    /// lifting only the parts below it.
    // Cases: parts with their weight, expected.
    #[rstest]
    #[case::one_static_part(
        vec![("t_1", Mutability::Static)],
        quote! { ::flowlog_runtime::operators::flowlog_dedup(t_1.clone()) },
    )]
    #[case::static_parts(
        vec![("t_1", Mutability::Static), ("t_2", Mutability::Static)],
        quote! { ::flowlog_runtime::operators::flowlog_dedup(t_1.clone().concatenate([t_2.clone()])) },
    )]
    #[case::mixed_parts(
        vec![("t_1", Mutability::Static), ("t_2", Mutability::Mutable)],
        quote! {
            ::flowlog_runtime::operators::flowlog_dedup(
                ::flowlog_runtime::operators::flowlog_lift::<::flowlog_runtime::diff::Mutable, _, _, _>(
                    t_1.clone(), "Lift: S",
                )
                .concatenate([t_2.clone()])
            )
        },
    )]
    #[case::earlier_binding_first(
        vec![("earlier", Mutability::Static), ("t_1", Mutability::Static)],
        quote! { ::flowlog_runtime::operators::flowlog_dedup(earlier.clone().concatenate([t_1.clone()])) },
    )]
    fn a_relation_lifts_its_parts_to_the_most_mutable_weight(
        #[case] parts: Vec<(&str, Mutability)>,
        #[case] expected: TokenStream,
    ) {
        let codegen = codegen(PROGRAM);
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let binding = binding_of(&plan, "s");
        let parts: Vec<(Ident, Mutability)> = parts.into_iter().map(|(p, m)| part(p, m)).collect();
        let deduped = codegen
            .gen_relation_binding(
                &parts,
                binding,
                &ident("r"),
                /*recursive*/ false,
                &mut None,
            )
            .expect("parts to union");
        assert_eq!(deduped.to_string(), expected.to_string());
    }

    const THREE_COLUMNS: &str = ".decl R(a: int32, b: symbol, c: int64)\n.input R\n\
         .decl S(a: int32, b: symbol, c: int64)\nS(a,b,c) :- R(a,b,c).\n.output S\n";

    // Cases: second input weight, recursive, union range, next address.
    #[rstest]
    #[case::same_weight(
        Mutability::Static, false,
        vec![vec![0, 1], vec![0, 2], vec![0, 3], vec![0, 4]],
        vec![0, 5],
    )]
    #[case::lift(
        Mutability::Mutable, false,
        vec![vec![0, 1], vec![0, 2], vec![0, 3], vec![0, 4], vec![0, 5]],
        vec![0, 6],
    )]
    #[case::recursive_lift(
        Mutability::Mutable, true,
        vec![vec![0, 1, 1], vec![0, 1, 2], vec![0, 1, 3], vec![0, 1, 4],
             vec![0, 1, 5], vec![0, 1, 6]],
        vec![0, 1, 7],
    )]
    fn profiled_binding_ranges_include_only_the_emitted_operators(
        #[case] right_weight: Mutability,
        #[case] recursive: bool,
        #[case] expected_range: Vec<Vec<u32>>,
        #[case] expected_next: Vec<u32>,
    ) {
        let mut codegen = codegen(THREE_COLUMNS);
        codegen.config.profile = true;
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let binding = binding_of(&plan, "s");
        let parts = [
            part("left", Mutability::Static),
            part("right", right_weight),
        ];
        let mut graph = PlanGraph::new(false);
        graph.enter_scope();
        if recursive {
            graph.enter_scope();
        }
        let mut graph = Some(graph);
        let rows = codegen
            .gen_relation_binding(&parts, binding, &ident("out"), recursive, &mut graph)
            .unwrap();
        syn::parse2::<syn::Expr>(rows).unwrap();
        let graph = graph.as_mut().unwrap();
        graph.map_join_operator(
            "following".into(),
            vec!["out".into()],
            "after".into(),
            0x123,
        );
        let nodes = graph.nodes();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].name, "S: concat & dedup");
        let range: Vec<Vec<u32>> = nodes[0]
            .operators
            .iter()
            .map(|address| address.0.clone())
            .collect();
        assert_eq!(range, expected_range);
        assert_eq!(nodes[1].operators.len(), 1);
        assert_eq!(nodes[1].operators[0].0, expected_next);
        assert_eq!(nodes[1].parents, [nodes[0].id]);
    }

    #[test]
    fn a_lattice_binding_merges_without_a_whole_row_dedup() {
        let codegen = codegen(LATTICE_PROGRAM);
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let binding = binding_of(&plan, "s");
        let parts = [
            part("t_1", Mutability::Static),
            part("t_2", Mutability::Static),
        ];
        let rows = codegen
            .gen_relation_binding(
                &parts,
                binding,
                &ident("r"),
                /*recursive*/ false,
                &mut None,
            )
            .expect("parts to union");
        let expected = quote! {
            ::flowlog_runtime::operators::flowlog_lattice(
                t_1.clone().concatenate([t_2.clone()]),
                "Merge: S",
                ::flowlog_runtime::operators::Min,
                |(x0, x1)| ((x0,), x1),
                |(k0,), v: i32| (k0, v),
            )
        };
        assert_eq!(rows.to_string(), expected.to_string());
    }

    #[test]
    fn a_lattice_binding_merges_its_merge_index_in_place() {
        let codegen = codegen(
            r#"
            .decl E(a: int32, b: int32, c: int32)
            .input E
            .decl R(a: int32, b: min<int32>, c: int32)
            .output R
            R(a,b,c) :- E(a,b,c).
        "#,
        );
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let binding = binding_of(&plan, "r");
        let rows = codegen
            .gen_relation_binding(
                &[part("rows", Mutability::Static)],
                binding,
                &ident("r"),
                /*recursive*/ false,
                &mut None,
            )
            .unwrap();
        let expected = quote! {
            ::flowlog_runtime::operators::flowlog_lattice(
                rows.clone(), "Merge: R", ::flowlog_runtime::operators::Min,
                |(x0, x1, x2)| ((x0, x2), x1),
                |(k0, k1), v: i32| (k0, v, k1),
            )
        };
        assert_eq!(rows.to_string(), expected.to_string());
    }

    /// A relation with no part has nothing to union: a planner bug, not an
    /// empty collection.
    #[test]
    fn a_relation_with_no_part_is_an_internal_error() {
        let codegen = codegen(PROGRAM);
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let binding = binding_of(&plan, "s");
        let error = codegen
            .gen_relation_binding(
                &[],
                binding,
                &ident("r"),
                /*recursive*/ false,
                &mut None,
            )
            .expect_err("nothing to union");
        assert!(matches!(error, CodegenError::Internal(_)));
    }
}
