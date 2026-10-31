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

impl Codegen {
    /// Returns the relation's rows and output mutability after applying
    /// `operation` to the union of `parts`. Dedup preserves the union's
    /// most mutable weight. Aggregation returns `Static` for static rows
    /// and `Mutable` otherwise, including for `Append` inputs.
    /// Records the runtime steps under `output`, inside a loop when
    /// `recursive` is true.
    ///
    /// # Errors
    ///
    /// Returns an internal error when the binding has no inputs, a dedup
    /// order is invalid, or a selected aggregation lacks a relation declaration
    /// or a min/max column.
    pub(crate) fn gen_relation_binding(
        &self,
        idb_fp: u64,
        parts: &[(Ident, Mutability)],
        operation: &BindingOperation,
        output: &Ident,
        recursive: bool,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<(TokenStream, Mutability), CodegenError> {
        let name = self.display_name(idb_fp);
        let Some(((first, first_mutability), rest)) = parts.split_first() else {
            return Err(CodegenError::internal(format!(
                "relation `{name}` has no collections to union"
            )));
        };
        let (rows_order, dedup) = match operation {
            BindingOperation::Dedup { order } => (Some(order.as_slice()), true),
            BindingOperation::Aggregation { .. } => (None, false),
        };
        let mutability = rest
            .iter()
            .map(|(_, part)| *part)
            .fold(*first_mutability, Mutability::max);
        // The lift names its target weight: an arrangement of the union
        // dispatches on the weight before the concatenation would fix it.
        let lift_name = format!("Lift: {name}");
        let weight = weight_tokens(mutability);
        let lift = |collection: &Ident, part: Mutability| {
            if part < mutability {
                quote! {
                    ::flowlog_runtime::operators::flowlog_lift::<#weight, _, _, _>(
                        #collection.clone(), #lift_name,
                    )
                }
            } else {
                quote! { #collection.clone() }
            }
        };
        let first_rows = lift(first, *first_mutability);
        let tail: Vec<TokenStream> = rest.iter().map(|(c, part)| lift(c, *part)).collect();
        let union = if tail.is_empty() {
            first_rows
        } else {
            quote! { #first_rows.concatenate([ #( #tail ),* ]) }
        };

        let (rows, layout_maps) = match rows_order {
            Some(order) => self.gen_relation_dedup(idb_fp, union, order)?,
            None => (union, 0),
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

    /// Applies the planned whole-row comparator while keeping the logical
    /// output schema. Returns the generated maps' count for profiling.
    fn gen_relation_dedup(
        &self,
        relation: u64,
        rows: TokenStream,
        order: &[usize],
    ) -> Result<(TokenStream, u32), CodegenError> {
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
            return Ok((
                quote! { ::flowlog_runtime::operators::flowlog_dedup(#rows) },
                0,
            ));
        }
        let fields: Vec<Ident> = (0..columns)
            .map(|column| format_ident!("x{column}"))
            .collect();
        let logical = tuple_tokens(fields.iter().map(|field| quote! { #field }));
        let physical = tuple_tokens(order.iter().map(|&column| {
            let field = &fields[column];
            quote! { #field }
        }));
        let name = self.display_name(relation);
        let reorder_name = format!("DedupOrder: {name}");
        let restore_name = format!("DedupRestore: {name}");
        Ok((
            quote! {
                ::flowlog_runtime::operators::flowlog_map(
                    ::flowlog_runtime::operators::flowlog_dedup(
                        ::flowlog_runtime::operators::flowlog_map(
                            #rows, #reorder_name,
                            |#logical, t, d| std::iter::once((#physical, t, d)),
                        )
                    ),
                    #restore_name,
                    |#physical, t, d| std::iter::once((#logical, t, d)),
                )
            },
            2,
        ))
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
        let parts: Vec<(Ident, Mutability)> =
            parts.into_iter().map(|(p, m)| (ident(p), m)).collect();
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

    #[test]
    fn a_planned_dedup_order_is_restored_after_deduplication() {
        let codegen = codegen(
            ".decl R(a: int32, b: symbol, c: int64)\n.input R\n\
             .decl S(a: int32, b: symbol, c: int64)\nS(a,b,c) :- R(a,b,c).\n.output S\n",
        );
        let relation = codegen.program.idbs()[0].fingerprint();
        let (rows, maps) = codegen
            .gen_relation_dedup(relation, quote! { rows }, &[2, 0, 1])
            .expect("a permutation of all three columns");
        let expected = quote! {
            ::flowlog_runtime::operators::flowlog_map(
                ::flowlog_runtime::operators::flowlog_dedup(
                    ::flowlog_runtime::operators::flowlog_map(
                        rows, "DedupOrder: S",
                        |(x0,x1,x2), t, d| std::iter::once(((x2,x0,x1), t, d)),
                    )
                ),
                "DedupRestore: S",
                |(x2,x0,x1), t, d| std::iter::once(((x0,x1,x2), t, d)),
            )
        };
        syn::parse2::<syn::Expr>(rows.clone()).expect("a valid Rust expression");
        assert_eq!((rows.to_string(), maps), (expected.to_string(), 2));
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
            .gen_relation_dedup(relation, quote! { rows }, &order)
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
        let parts = [(ident("t_1"), input), (ident("t_2"), input)];
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
                &[(ident("rows"), Mutability::Static)],
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
                &[(ident("rows"), Mutability::Static)],
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
                false,
                &mut None,
            )
            .expect_err("nothing to union");
        assert!(matches!(error, CodegenError::Internal(_)));
    }
}
