//! Choice merges: a relation with a choice domain keeps one row per value
//! of the domain's columns. [`Codegen::gen_choice_merge`] wraps the union
//! of the rows its rules derive where a stratum binds that union, outside
//! or inside a loop.

use flowlog_parser::Mutability;
use flowlog_parser::Relation;
use flowlog_planner::planner::aggregate_mutability;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;
use crate::expr::aggregation::choice_merge;
use crate::expr::aggregation::choice_split;

impl Codegen {
    /// Returns rows merged to one per value of relation `idb_fp`'s choice
    /// domain and the output mutability, as a lattice merge's: static for
    /// static rows, mutable otherwise, since a lesser candidate arriving
    /// later replaces a choice. Records the runtime steps under `output`.
    ///
    /// Duplicate rows do not change a choice, so the input need not be
    /// deduped first.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `idb_fp` has no declaration or no
    /// choice domain.
    pub(crate) fn gen_choice_merge(
        &self,
        idb_fp: u64,
        rows: TokenStream,
        rows_mutability: Mutability,
        output: &Ident,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<(TokenStream, Mutability), CodegenError> {
        let Some(relation) = self.program.relation_by_fingerprint(idb_fp) else {
            return Err(CodegenError::internal(format!(
                "cannot generate a choice for undeclared relation `{}`",
                self.display_name(idb_fp)
            )));
        };
        let Some(domain) = relation.choice_domain() else {
            return Err(CodegenError::internal(format!(
                "relation `{}` has no choice domain",
                relation.raw_name()
            )));
        };
        let chosen = self.gen_choice(relation, domain, rows, rows_mutability, output, plan_graph);
        Ok((chosen, aggregate_mutability(rows_mutability)))
    }

    /// Returns `flowlog_choice(<rows>, ..)`, `relation`'s rows of weight
    /// `rows_mutability` merged to one per value of its choice `domain`.
    /// Records the step under `output`.
    pub(crate) fn gen_choice(
        &self,
        relation: &Relation,
        domain: &[usize],
        rows: TokenStream,
        rows_mutability: Mutability,
        output: &Ident,
        plan_graph: &mut Option<PlanGraph>,
    ) -> TokenStream {
        let name = self.display_name(relation.fingerprint());
        // The rows feeding the choice are recorded under the output's name
        // too, so the choice reads from them.
        with_plan_graph(plan_graph, |plan_graph| {
            plan_graph.choice_operator(
                name.clone(),
                output.to_string(),
                output.to_string(),
                relation.fingerprint(),
                rows_mutability,
            );
        });
        let op_name = format!("Choice: {name}");
        let split = choice_split(relation.arity(), domain);
        let merge = choice_merge(relation.arity(), domain);
        quote! {
            ::flowlog_runtime::operators::flowlog_choice(#rows, #op_name, #split, #merge)
        }
    }
}

#[cfg(test)]
mod tests {
    use proc_macro2::Span;
    use rstest::rstest;

    use super::*;
    use crate::test_harness::codegen;

    fn ident(name: &str) -> Ident {
        Ident::new(name, Span::call_site())
    }

    /// A code generator over `Tree`, a choice over `Cand` declared
    /// `declaration`, with `Tree`'s fingerprint and `Cand`'s mutability.
    fn choice_program(declaration: &str) -> (Codegen, u64, Mutability) {
        let codegen = codegen(&format!(
            ".decl Cand(u: int32, v: int32){declaration}\n.input Cand\n\
             .decl Tree(u: int32, v: int32) choice-domain v\nTree(u, v) :- Cand(u, v).\n\
             .output Tree\n"
        ));
        let tree = codegen.program.idbs()[0].fingerprint();
        let rows_mutability = codegen.program.edbs()[0].input_mutability();
        (codegen, tree, rows_mutability)
    }

    /// Rows merge to one per domain value and answer at an aggregate's
    /// weight over them.
    // Cases: rows declaration, output weight.
    #[rstest]
    #[case::static_rows("", Mutability::Static)]
    #[case::append_rows(" append", Mutability::Mutable)]
    #[case::mutable_rows(" mutable", Mutability::Mutable)]
    fn a_choice_answers_at_an_aggregates_weight(
        #[case] declaration: &str,
        #[case] expected: Mutability,
    ) {
        let (codegen, tree, rows_mutability) = choice_program(declaration);
        let (tokens, mutability) = codegen
            .gen_choice_merge(
                tree,
                quote! { rows },
                rows_mutability,
                &ident("out"),
                &mut None,
            )
            .expect("Tree has a choice domain");
        let expected_call = quote! {
            ::flowlog_runtime::operators::flowlog_choice(
                rows,
                "Choice: Tree",
                |(x0, x1)| ((x1,), (x0,)),
                |(k0,), (v0,)| (v0, k0)
            )
        };
        assert_eq!(
            (tokens.to_string(), mutability),
            (expected_call.to_string(), expected)
        );
    }

    #[test]
    fn choosing_in_a_relation_without_a_domain_is_an_internal_error() {
        let codegen =
            codegen(".decl R(x: int32)\n.input R\n.decl S(x: int32)\nS(x) :- R(x).\n.output S\n");
        let s = codegen.program.idbs()[0].fingerprint();
        let error = codegen
            .gen_choice_merge(
                s,
                quote! { rows },
                Mutability::Static,
                &ident("out"),
                &mut None,
            )
            .expect_err("S has no choice domain");
        assert!(matches!(&error, CodegenError::Internal(_)));
        assert!(
            error
                .to_string()
                .contains("relation `S` has no choice domain"),
            "{error}"
        );
    }
}
