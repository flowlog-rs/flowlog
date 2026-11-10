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
    /// `operation` to the union of `parts`. Dedup preserves the union's
    /// most mutable weight. A lattice or choice merge returns `Static` for
    /// static rows and `Mutable` otherwise, including for `Append` inputs.
    /// Records the runtime steps under `output`, inside a loop when
    /// `recursive` is true.
    ///
    /// # Errors
    ///
    /// Returns an internal error when the binding has no inputs, a planned
    /// aggregation has no relation declaration or no min/max column, or a
    /// planned choice has no relation declaration or no choice domain.
    pub(crate) fn gen_relation_binding(
        &self,
        idb_fp: u64,
        parts: &[(Ident, Mutability)],
        operation: BindingOperation,
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
        let dedup = match operation {
            BindingOperation::Dedup => true,
            BindingOperation::Aggregation | BindingOperation::Choice => false,
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
            BindingOperation::Dedup => Ok((
                quote! { ::flowlog_runtime::operators::flowlog_dedup(#union) },
                mutability,
            )),
            BindingOperation::Aggregation => {
                self.gen_lattice_merge(idb_fp, union, mutability, output, plan_graph)
            }
            BindingOperation::Choice => {
                self.gen_choice_merge(idb_fp, union, mutability, output, plan_graph)
            }
        }
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
                BindingOperation::Dedup,
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
                BindingOperation::Aggregation,
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

    /// A choice binding keeps one row per domain value of the whole union,
    /// at an aggregate's weight over it, without a whole-row dedup.
    #[test]
    fn a_choice_binding_chooses_from_the_union_without_a_whole_row_dedup() {
        let codegen = codegen(
            ".decl R(a: int32, b: int32)\n.input R\n\
             .decl S(a: int32, b: int32) choice-domain a\nS(a, b) :- R(a, b).\n.output S\n",
        );
        let s = codegen.program.idbs()[0].fingerprint();
        let parts = [
            (ident("t_1"), Mutability::Static),
            (ident("t_2"), Mutability::Append),
        ];
        let (rows, found) = codegen
            .gen_relation_binding(
                s,
                &parts,
                BindingOperation::Choice,
                &ident("r"),
                /*recursive*/ false,
                &mut None,
            )
            .expect("parts to union");
        assert_eq!(
            (rows.to_string(), found),
            (
                quote! {
                    ::flowlog_runtime::operators::flowlog_choice(
                        ::flowlog_runtime::operators::flowlog_lift::<::flowlog_runtime::diff::Append, _, _, _>(
                            t_1.clone(), "Lift: S",
                        )
                        .concatenate([t_2.clone()]),
                        "Choice: S",
                        |(x0, x1)| ((x0,), (x1,)),
                        |(k0,), (v0,)| (k0, v0)
                    )
                }
                .to_string(),
                Mutability::Mutable
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
                BindingOperation::Aggregation,
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
                BindingOperation::Dedup,
                &ident("r"),
                /*recursive*/ false,
                &mut None,
            )
            .expect_err("nothing to union");
        assert!(matches!(error, CodegenError::Internal(_)));
    }
}
