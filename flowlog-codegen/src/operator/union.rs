//! Relation-binding lowering: unions rule results with an earlier binding,
//! applies the planned operation, and records the runtime steps.

use flowlog_parser::Mutability;
use flowlog_planner::planner::BindingOperation;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;
use crate::ty::diff::weight_tokens;

impl Codegen {
    /// Returns the relation's rows and output mutability after applying
    /// `operation` to the union of `parts`, each a producer's rows with
    /// their weight. Every producer must already emit the operation's
    /// input format. Dedup preserves the union's most mutable weight.
    /// Aggregation returns `Static` for static rows and `Mutable`
    /// otherwise.
    /// Records the runtime steps under `output`, inside a loop when
    /// `recursive` is true.
    ///
    /// # Errors
    ///
    /// Returns an internal error for no inputs, an invalid dedup order,
    /// or an aggregation without a declaration or declared min/max column.
    pub(crate) fn gen_relation_binding(
        &mut self,
        idb_fp: u64,
        parts: &[(Ident, Mutability)],
        operation: &BindingOperation,
        output: &Ident,
        recursive: bool,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<(TokenStream, Mutability), CodegenError> {
        let name = self.display_name(idb_fp);
        let Some((first, rest)) = parts.split_first() else {
            return Err(CodegenError::internal(format!(
                "relation `{name}` has no collections to union"
            )));
        };
        self.record_binding_layout(idb_fp, operation)?;
        let dedup = matches!(operation, BindingOperation::Dedup { .. });
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
        let rows = if dedup {
            quote! { ::flowlog_runtime::operators::flowlog_dedup(#union) }
        } else {
            union
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

        match operation {
            BindingOperation::Dedup { .. } => Ok((rows, mutability)),
            BindingOperation::Aggregation { order } => {
                self.gen_lattice_merge(idb_fp, rows, mutability, order, output, plan_graph)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use flowlog_common::compute_fp;
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

    /// `S` unions its parts in order at its most mutable part's weight,
    /// lifting only the parts below it.
    // Cases: parts with their weight, expected, weight.
    #[rstest]
    #[case::one_static_part(
        vec![("t_1", Mutability::Static)],
        quote! { ::flowlog_runtime::operators::flowlog_dedup(t_1.clone()) },
        Mutability::Static
    )]
    #[case::static_parts(
        vec![("t_1", Mutability::Static), ("t_2", Mutability::Static)],
        quote! { ::flowlog_runtime::operators::flowlog_dedup(t_1.clone().concatenate([t_2.clone()])) },
        Mutability::Static
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
        Mutability::Mutable
    )]
    #[case::earlier_binding_first(
        vec![("earlier", Mutability::Static), ("t_1", Mutability::Static)],
        quote! { ::flowlog_runtime::operators::flowlog_dedup(earlier.clone().concatenate([t_1.clone()])) },
        Mutability::Static
    )]
    fn a_relation_unions_its_parts_at_the_most_mutable_weight(
        #[case] parts: Vec<(&str, Mutability)>,
        #[case] expected: TokenStream,
        #[case] mutability: Mutability,
    ) {
        let mut codegen = codegen(PROGRAM);
        let s = codegen.program.idbs()[0].fingerprint();
        let parts: Vec<(Ident, Mutability)> = parts.into_iter().map(|(p, m)| part(p, m)).collect();
        let (deduped, found) = codegen
            .gen_relation_binding(
                s,
                &parts,
                &BindingOperation::Dedup { order: vec![0] },
                &ident("r"),
                /*recursive*/ false,
                &mut None,
            )
            .expect("parts to union");
        assert_eq!(
            (deduped.to_string(), found),
            (expected.to_string(), mutability)
        );
    }

    const THREE_COLUMNS: &str = ".decl R(a: int32, b: symbol, c: int64)\n.input R\n\
         .decl S(a: int32, b: symbol, c: int64)\nS(a,b,c) :- R(a,b,c).\n.output S\n";

    #[test]
    fn aligned_heads_feed_the_union_without_a_reorder_map() {
        let mut codegen = codegen(THREE_COLUMNS);
        let relation = codegen.program.idbs()[0].fingerprint();
        let parts = [
            part("left", Mutability::Static),
            part("right", Mutability::Static),
        ];
        let (rows, _) = codegen
            .gen_relation_binding(
                relation,
                &parts,
                &BindingOperation::Dedup {
                    order: vec![2, 0, 1],
                },
                &ident("out"),
                /*recursive*/ false,
                &mut None,
            )
            .unwrap();
        let expected = quote! {
            ::flowlog_runtime::operators::flowlog_dedup(
                left.clone().concatenate([right.clone()])
            )
        };
        syn::parse2::<syn::Expr>(rows.clone()).expect("a valid Rust expression");
        assert_eq!(rows.to_string(), expected.to_string());
    }

    // Cases: order, second input weight, recursive, union range, next address.
    #[rstest]
    #[case::identity_order(
        vec![0, 1, 2], Mutability::Static, false,
        vec![vec![0, 1], vec![0, 2], vec![0, 3], vec![0, 4]],
        vec![0, 5],
    )]
    #[case::aligned_heads(
        vec![2, 0, 1], Mutability::Static, false,
        vec![vec![0, 1], vec![0, 2], vec![0, 3], vec![0, 4]],
        vec![0, 5],
    )]
    #[case::permutation_and_lift(
        vec![2, 0, 1], Mutability::Mutable, false,
        vec![vec![0, 1], vec![0, 2], vec![0, 3], vec![0, 4], vec![0, 5]],
        vec![0, 6],
    )]
    #[case::recursive_permutation_and_lift(
        vec![2, 0, 1], Mutability::Mutable, true,
        vec![vec![0, 1, 1], vec![0, 1, 2], vec![0, 1, 3], vec![0, 1, 4],
             vec![0, 1, 5], vec![0, 1, 6]],
        vec![0, 1, 7],
    )]
    fn profiled_binding_ranges_include_only_the_emitted_maps(
        #[case] order: Vec<usize>,
        #[case] right_weight: Mutability,
        #[case] recursive: bool,
        #[case] expected_range: Vec<Vec<u32>>,
        #[case] expected_next: Vec<u32>,
    ) {
        let mut codegen = codegen(THREE_COLUMNS);
        codegen.config.profile = true;
        let relation = codegen.program.idbs()[0].fingerprint();
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
        let (rows, _) = codegen
            .gen_relation_binding(
                relation,
                &parts,
                &BindingOperation::Dedup { order },
                &ident("out"),
                recursive,
                &mut graph,
            )
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

    // Cases: input weight, output weight.
    #[rstest]
    #[case::static_rows(Mutability::Static, Mutability::Static)]
    #[case::append_rows(Mutability::Append, Mutability::Mutable)]
    #[case::mutable_rows(Mutability::Mutable, Mutability::Mutable)]
    fn an_aggregation_binding_merges_without_a_whole_row_dedup(
        #[case] input: Mutability,
        #[case] expected: Mutability,
    ) {
        let mut codegen = codegen(LATTICE_PROGRAM);
        let s = codegen.program.idbs()[0].fingerprint();
        let parts = [part("t_1", input), part("t_2", input)];
        let (rows, found) = codegen
            .gen_relation_binding(
                s,
                &parts,
                &BindingOperation::Aggregation { order: vec![0] },
                &ident("r"),
                /*recursive*/ false,
                &mut None,
            )
            .expect("parts to union");
        assert_eq!(
            (rows.to_string(), found),
            (
                quote! {
                    ::flowlog_runtime::operators::flowlog_lattice(
                        t_1.clone().concatenate([t_2.clone()]),
                        "Merge: S",
                        ::flowlog_runtime::operators::Min,
                        |(x0, x1)| ((x0,), x1),
                        |(k0,), v: i32| (k0, v),
                    )
                }
                .to_string(),
                expected
            )
        );
    }

    #[test]
    fn an_aggregation_binding_without_a_min_max_column_is_an_internal_error() {
        let mut codegen = codegen(PROGRAM);
        let relation = codegen.program.idbs()[0].fingerprint();
        let error = codegen
            .gen_relation_binding(
                relation,
                &[part("rows", Mutability::Static)],
                &BindingOperation::Aggregation { order: vec![0] },
                &ident("out"),
                /*recursive*/ false,
                &mut None,
            )
            .expect_err("S has no min/max column");
        assert!(matches!(&error, CodegenError::Internal(_)));
        assert!(
            error
                .to_string()
                .contains("relation `S` has no declared min/max column"),
            "{error}"
        );
    }

    #[test]
    fn an_aggregation_binding_uses_the_planned_group_order_and_restores_the_row() {
        let mut codegen = codegen(
            r#"
            .decl E(a: int32, b: int32, c: int32)
            .input E
            .decl R(a: int32, b: min<int32>, c: int32)
            .output R
            R(a,b,c) :- E(a,b,c).
        "#,
        );
        let (rows, _) = codegen
            .gen_relation_binding(
                compute_fp("r"),
                &[part("rows", Mutability::Static)],
                &BindingOperation::Aggregation { order: vec![2, 0] },
                &ident("r"),
                /*recursive*/ false,
                &mut None,
            )
            .unwrap();
        assert_eq!(
            rows.to_string(),
            quote! {
                ::flowlog_runtime::operators::flowlog_lattice(
                    rows.clone(), "Merge: R", ::flowlog_runtime::operators::Min,
                    |(x0, x1, x2)| ((x2, x0), x1),
                    |(k0, k1), v: i32| (k1, v, k0),
                )
            }
            .to_string()
        );
    }

    /// A relation with no part has nothing to union: a planner bug, not an
    /// empty collection.
    #[test]
    fn a_relation_with_no_part_is_an_internal_error() {
        let mut codegen = codegen(PROGRAM);
        let s = codegen.program.idbs()[0].fingerprint();
        let error = codegen
            .gen_relation_binding(
                s,
                &[],
                &BindingOperation::Dedup { order: vec![0] },
                &ident("r"),
                /*recursive*/ false,
                &mut None,
            )
            .expect_err("nothing to union");
        assert!(matches!(error, CodegenError::Internal(_)));
    }
}
