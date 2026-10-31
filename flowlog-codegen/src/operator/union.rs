//! Relation-binding lowering: unions rule results with an earlier binding,
//! applies the planned operation, and records the runtime steps.

use flowlog_parser::Mutability;
use flowlog_planner::planner::BindingOperation;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;
use crate::tuple_tokens;
use crate::ty::diff::weight_tokens;

/// One emitted projection result, ready for concatenation.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct BindingPart {
    pub(crate) collection: Ident,
    pub(crate) mutability: Mutability,
}

/// Row patterns for a non-identity dedup order and its inverse.
#[derive(Debug)]
struct DedupLayout {
    logical: TokenStream,
    physical: TokenStream,
}

impl Codegen {
    /// Returns the relation's rows and output mutability after applying
    /// `operation` to the union of `parts`. Every input projection must
    /// already emit the operation's input format. Dedup restores logical
    /// column positions after sorting the projected rows.
    /// Dedup preserves the union's most mutable weight. Aggregation returns
    /// `Static` for static rows and `Mutable` otherwise.
    /// Records the runtime steps under `output`, inside a loop when
    /// `recursive` is true.
    ///
    /// # Errors
    ///
    /// Returns an internal error for no inputs, an invalid dedup order,
    /// or an aggregation without a declaration or declared min/max column.
    pub(crate) fn gen_relation_binding(
        &self,
        idb_fp: u64,
        parts: &[BindingPart],
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
        let (layout, dedup) = match operation {
            BindingOperation::Dedup { order } => (self.dedup_layout(idb_fp, order)?, true),
            BindingOperation::Aggregation { .. } => (None, false),
        };
        let mutability = rest
            .iter()
            .map(|part| part.mutability)
            .fold(first.mutability, Mutability::max);
        // The lift names its target weight before concatenation or dedup
        // would otherwise fix it by type inference.
        let lift_name = format!("Lift: {name}");
        let weight = weight_tokens(mutability);
        let input_rows = |part: &BindingPart| {
            let collection = &part.collection;
            let mut rows = quote! { #collection.clone() };
            if part.mutability < mutability {
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
        let mut rows = if dedup {
            quote! { ::flowlog_runtime::operators::flowlog_dedup(#union) }
        } else {
            union
        };
        if let Some(DedupLayout { logical, physical }) = &layout {
            let restore_name = format!("DedupRestore: {name}");
            rows = quote! {
                ::flowlog_runtime::operators::flowlog_map(
                    #rows, #restore_name,
                    |#physical, t, d| std::iter::once((#logical, t, d)),
                )
            };
        }

        with_plan_graph(plan_graph, |plan_graph| {
            let lifts: u32 = parts
                .iter()
                .map(|part| u32::from(part.mutability < mutability))
                .sum();
            let layout_maps = u32::from(layout.is_some());
            plan_graph.union_operator(
                name.to_string(),
                parts
                    .iter()
                    .map(|part| part.collection.to_string())
                    .collect(),
                output.to_string(),
                lifts + layout_maps,
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

    /// Returns patterns for a whole-row permutation, or `None` for logical
    /// order. Invalid column memberships are internal plan errors.
    fn dedup_layout(
        &self,
        relation: u64,
        order: &[usize],
    ) -> Result<Option<DedupLayout>, CodegenError> {
        let columns = self.find_global_type(relation)?.1.len();
        let mut sorted = order.to_vec();
        sorted.sort_unstable();
        if !sorted.iter().copied().eq(0..columns) {
            return Err(CodegenError::internal(format!(
                "relation dedup of `{}` has order {order:?}, expected a permutation of {columns} columns",
                self.display_name(relation),
            )));
        }
        if order.iter().copied().eq(0..columns) {
            return Ok(None);
        }
        let fields: Vec<Ident> = (0..columns)
            .map(|column| format_ident!("x{column}"))
            .collect();
        Ok(Some(DedupLayout {
            logical: tuple_tokens(fields.iter().map(|field| quote! { #field })),
            physical: tuple_tokens(order.iter().map(|&column| {
                let field = &fields[column];
                quote! { #field }
            })),
        }))
    }
}

#[cfg(test)]
mod tests {
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

    fn part(name: &str, mutability: Mutability) -> BindingPart {
        BindingPart {
            collection: ident(name),
            mutability,
        }
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
        let codegen = codegen(PROGRAM);
        let s = codegen.program.idbs()[0].fingerprint();
        let parts: Vec<BindingPart> = parts.into_iter().map(|(p, m)| part(p, m)).collect();
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
        let codegen = codegen(THREE_COLUMNS);
        let relation = codegen.program.idbs()[0].fingerprint();
        let parts = [
            BindingPart {
                collection: ident("left"),
                mutability: Mutability::Static,
            },
            BindingPart {
                collection: ident("right"),
                mutability: Mutability::Static,
            },
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
            ::flowlog_runtime::operators::flowlog_map(
                ::flowlog_runtime::operators::flowlog_dedup(
                    left.clone().concatenate([right.clone()])
                ),
                "DedupRestore: S",
                |(x2,x0,x1), t, d| std::iter::once(((x0,x1,x2), t, d)),
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
        vec![vec![0, 1], vec![0, 2], vec![0, 3], vec![0, 4], vec![0, 5]],
        vec![0, 6],
    )]
    #[case::permutation_and_lift(
        vec![2, 0, 1], Mutability::Mutable, false,
        vec![vec![0, 1], vec![0, 2], vec![0, 3], vec![0, 4], vec![0, 5], vec![0, 6]],
        vec![0, 7],
    )]
    #[case::recursive_permutation_and_lift(
        vec![2, 0, 1], Mutability::Mutable, true,
        vec![vec![0, 1, 1], vec![0, 1, 2], vec![0, 1, 3], vec![0, 1, 4],
             vec![0, 1, 5], vec![0, 1, 6], vec![0, 1, 7]],
        vec![0, 1, 8],
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

    // Cases: invalid whole-row permutations.
    #[rstest]
    #[case(vec![])]
    #[case(vec![0, 0])]
    #[case(vec![1])]
    fn an_invalid_dedup_permutation_is_an_internal_error(#[case] order: Vec<usize>) {
        let codegen = codegen(PROGRAM);
        let relation = codegen.program.idbs()[0].fingerprint();
        let error = codegen
            .gen_relation_binding(
                relation,
                &[part("rows", Mutability::Static)],
                &BindingOperation::Dedup { order },
                &ident("out"),
                /*recursive*/ false,
                &mut None,
            )
            .expect_err("the one column must appear exactly once");
        assert!(matches!(error, CodegenError::Internal(_)));
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
        let codegen = codegen(LATTICE_PROGRAM);
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
        let codegen = codegen(PROGRAM);
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
        let codegen = codegen(
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
                flowlog_common::compute_fp("r"),
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
        let codegen = codegen(PROGRAM);
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
