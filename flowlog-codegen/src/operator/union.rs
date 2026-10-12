//! Unions: the step a stratum chains to bind a relation from its parts,
//! the collections its rules produce for it and its earlier binding, each
//! at the weight the stratum gives it. [`Codegen::gen_union_dedup`] unions
//! and dedups them. The step binds the ident its caller names and records
//! itself in the plan graph; the strata own the parts, the names and the
//! order.
//!
//! Differential Dataflow's `concatenate` needs every part at one weight,
//! so a relation unions at its most mutable part's weight, and each part
//! below that weight is lifted to it first.

use flowlog_parser::Mutability;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;
use crate::ty::diff::weight_tokens;

impl Codegen {
    /// Returns `let output = flowlog_dedup(<union>);`, the dedup of the
    /// union of `parts`, each a binding at its weight, as relation
    /// `idb_fp`'s, and the relation's weight: the most mutable part's.
    /// Records the step in the plan graph, where `recursive` says it sits
    /// inside a loop. A relation with no part is a planner bug, reported
    /// as an internal error.
    ///
    /// A presence part may announce a row more than once, each announcement
    /// lifting to its own count, but the dedup clamps every count to one.
    pub(crate) fn gen_union_dedup(
        &self,
        idb_fp: u64,
        parts: &[(Ident, Mutability)],
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
        let head = lift(first, *first_mutability);
        let tail: Vec<TokenStream> = rest.iter().map(|(c, part)| lift(c, *part)).collect();
        let union = if tail.is_empty() {
            head
        } else {
            quote! { #head.concatenate([ #( #tail ),* ]) }
        };

        with_plan_graph(plan_graph, |plan_graph| {
            // A part less mutable than the relation is lifted.
            let lifts: u32 = parts
                .iter()
                .map(|(_, part)| u32::from(*part < mutability))
                .sum();
            plan_graph.concat_dedup_operator(
                name.to_string(),
                parts.iter().map(|(id, _)| id.to_string()).collect(),
                output.to_string(),
                lifts,
                u32::from(!tail.is_empty()),
                mutability,
                recursive,
            );
        });

        Ok((
            quote! { let #output = ::flowlog_runtime::operators::flowlog_dedup(#union); },
            mutability,
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

    fn ident(name: &str) -> Ident {
        Ident::new(name, Span::call_site())
    }

    /// `S` unions its parts in order at its most mutable part's weight,
    /// lifting only the parts below it.
    // Cases: parts with their weight, expected, weight.
    #[rstest]
    #[case::one_static_part(
        vec![("t_1", Mutability::Static)],
        quote! { let r = ::flowlog_runtime::operators::flowlog_dedup(t_1.clone()); },
        Mutability::Static
    )]
    #[case::static_parts(
        vec![("t_1", Mutability::Static), ("t_2", Mutability::Static)],
        quote! { let r = ::flowlog_runtime::operators::flowlog_dedup(t_1.clone().concatenate([t_2.clone()])); },
        Mutability::Static
    )]
    #[case::mixed_parts(
        vec![("t_1", Mutability::Static), ("t_2", Mutability::Mutable)],
        quote! {
            let r = ::flowlog_runtime::operators::flowlog_dedup(
                ::flowlog_runtime::operators::flowlog_lift::<::flowlog_runtime::diff::Mutable, _, _, _>(
                    t_1.clone(), "Lift: S",
                )
                .concatenate([t_2.clone()])
            );
        },
        Mutability::Mutable
    )]
    #[case::earlier_binding_first(
        vec![("earlier", Mutability::Static), ("t_1", Mutability::Static)],
        quote! { let r = ::flowlog_runtime::operators::flowlog_dedup(earlier.clone().concatenate([t_1.clone()])); },
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
            .gen_union_dedup(s, &parts, &ident("r"), false, &mut None)
            .expect("parts to union");
        assert_eq!(
            (deduped.to_string(), found),
            (expected.to_string(), mutability)
        );
    }

    /// A relation with no part has nothing to union: a planner bug, not an
    /// empty collection.
    #[test]
    fn a_relation_with_no_part_is_an_internal_error() {
        let codegen = codegen(PROGRAM);
        let s = codegen.program.idbs()[0].fingerprint();
        let error = codegen
            .gen_union_dedup(s, &[], &ident("r"), false, &mut None)
            .expect_err("nothing to union");
        assert!(matches!(error, CodegenError::Internal(_)));
    }
}
