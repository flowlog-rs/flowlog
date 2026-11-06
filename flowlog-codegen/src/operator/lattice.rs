//! Lattice merges: a lattice relation keeps, per value of its other
//! columns, the least or greatest value of its lattice column, as its
//! declaration says. [`Codegen::gen_lattice_merge`] wraps the union of the
//! rows its rules derive where a stratum binds that union, outside or
//! inside a loop; [`Codegen::gen_lattice_fold`] merges a static loop's
//! answers once more at its exit, since a static merge keeps every
//! iteration's answer.

use flowlog_parser::Mutability;
use flowlog_planner::planner::aggregate_mutability;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;
use crate::expr::aggregation::aggregation_merge;
use crate::expr::aggregation::aggregation_split;
use crate::expr::aggregation::lattice_kind;

// =============================================================================
// MergeArgs
// =============================================================================

/// Runtime aggregation kind and closures for a declared min/max column.
#[derive(Debug)]
struct MergeArgs {
    kind: TokenStream,
    split: TokenStream,
    merge: TokenStream,
}

// =============================================================================
// Codegen
// =============================================================================

impl Codegen {
    /// Returns rows merged by the declared min/max column and the output
    /// mutability: static for static rows, mutable otherwise. Groups are
    /// identified by every other column, arranged in `order`, which must
    /// list each grouping column exactly once. Output rows retain logical
    /// column positions. Records the runtime steps under `output`.
    ///
    /// Duplicate rows do not change the result, so the input need not be
    /// deduped first.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `idb_fp` has no declaration or no
    /// declared min/max column, or `order` is not a permutation of its
    /// grouping columns.
    pub(crate) fn gen_lattice_merge(
        &self,
        idb_fp: u64,
        rows: TokenStream,
        rows_mutability: Mutability,
        order: &[usize],
        output: &Ident,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<(TokenStream, Mutability), CodegenError> {
        let MergeArgs { kind, split, merge } = self.merge_args(idb_fp, Some(order))?;
        let name = self.display_name(idb_fp);
        // The append merge mirrors the mutable one operator for operator.
        // The union feeding the merge is recorded under the output's name
        // too, so the merge reads from it.
        with_plan_graph(plan_graph, |plan_graph| match rows_mutability {
            Mutability::Static => plan_graph.static_lattice_operator(
                name.clone(),
                output.to_string(),
                output.to_string(),
                idb_fp,
            ),
            Mutability::Append | Mutability::Mutable => plan_graph.mutable_lattice_operator(
                name.clone(),
                output.to_string(),
                output.to_string(),
                idb_fp,
            ),
        });
        let op_name = format!("Merge: {name}");
        Ok((
            quote! {
                ::flowlog_runtime::operators::flowlog_lattice(
                    #rows, #op_name, #kind, #split, #merge,
                )
            },
            aggregate_mutability(rows_mutability),
        ))
    }

    /// Returns `let <output> = flowlog_lattice_leave(<answers>, scope, ..);`,
    /// the merge of relation `idb_fp`'s `answers`, a static loop's, bound
    /// outside the loop as `output`, static. Records the step's inner side
    /// in the plan graph; the stratum records the outer side once the
    /// scope is left.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `idb_fp` is no lattice relation.
    pub(crate) fn gen_lattice_fold(
        &self,
        idb_fp: u64,
        answers: &Ident,
        output: &Ident,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let name = self.display_name(idb_fp);
        let MergeArgs { kind, split, merge } = self.merge_args(idb_fp, None)?;
        with_plan_graph(plan_graph, |plan_graph| {
            plan_graph.recursive_pre_leave_lattice_operator(
                name.clone(),
                answers.to_string(),
                output.to_string(),
            );
        });
        let op_name = format!("MergeLeave: {name}");
        Ok(quote! {
            let #output = ::flowlog_runtime::operators::flowlog_lattice_leave(
                #answers, scope, #op_name, #kind, #split, #merge,
            );
        })
    }

    /// Returns the aggregation kind and row split/merge closures for
    /// relation `idb_fp`'s declared min/max column. `Some(order)` selects the
    /// grouping columns' physical order; `None` follows declaration order.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `idb_fp` has no declaration or no
    /// declared min/max column, or the grouping order omits, repeats or
    /// includes a column outside the group key.
    fn merge_args(&self, idb_fp: u64, order: Option<&[usize]>) -> Result<MergeArgs, CodegenError> {
        let Some(relation) = self.program.relation_by_fingerprint(idb_fp) else {
            return Err(CodegenError::internal(format!(
                "cannot generate a min/max merge for undeclared relation `{}`",
                self.display_name(idb_fp)
            )));
        };
        let Some((position, lattice)) = relation.lattice() else {
            return Err(CodegenError::internal(format!(
                "relation `{}` has no declared min/max column",
                relation.raw_name()
            )));
        };
        let arity = relation.arity();
        let default_order: Vec<usize> = (0..arity).filter(|&column| column != position).collect();
        let order = order.unwrap_or(&default_order);
        let mut sorted = order.to_vec();
        sorted.sort_unstable();
        if sorted != default_order {
            return Err(CodegenError::internal(format!(
                "aggregation of `{}` has grouping order {order:?}, expected columns {default_order:?}",
                relation.raw_name(),
            )));
        }
        let kind = lattice_kind(lattice);
        let split = aggregation_split(arity, position, order);
        let merge = aggregation_merge(
            arity,
            position,
            order,
            &relation.data_type()[position],
            self.config.str_intern_enabled(),
        );
        Ok(MergeArgs { kind, split, merge })
    }
}

#[cfg(test)]
mod tests {
    use proc_macro2::Span;
    use rstest::rstest;

    use super::*;
    use crate::test_harness::codegen;
    use crate::test_harness::rendered;

    /// A plain relation `S` copied from a static input.
    const PLAIN_PROGRAM: &str =
        ".decl R(x: int32)\n.input R\n.decl S(x: int32)\nS(x) :- R(x).\n.output S\n";

    fn ident(name: &str) -> Ident {
        Ident::new(name, Span::call_site())
    }

    /// A code generator over `Dist`, a lattice over `Cand` declared
    /// `declaration`, with `Dist`'s fingerprint and `Cand`'s mutability.
    fn lattice_program(declaration: &str) -> (Codegen, u64, Mutability) {
        let codegen = codegen(&format!(
            ".decl Cand(n: int32, d: int32){declaration}\n.input Cand\n\
             .decl Dist(n: int32, d: min<int32>)\nDist(n, d) :- Cand(n, d).\n.output Dist\n"
        ));
        let dist = codegen.program.idbs()[0].fingerprint();
        let rows_mutability = codegen.program.edbs()[0].input_mutability();
        (codegen, dist, rows_mutability)
    }

    /// Rows merge by the lattice column and answer at an aggregate's
    /// weight over them.
    // Cases: rows declaration, output weight.
    #[rstest]
    #[case::static_rows("", Mutability::Static)]
    #[case::append_rows(" append", Mutability::Mutable)]
    #[case::mutable_rows(" mutable", Mutability::Mutable)]
    fn a_merge_answers_at_an_aggregates_weight(
        #[case] declaration: &str,
        #[case] expected: Mutability,
    ) {
        let (codegen, dist, rows_mutability) = lattice_program(declaration);
        let (tokens, mutability) = codegen
            .gen_lattice_merge(
                dist,
                quote! { rows },
                rows_mutability,
                &[0],
                &ident("out"),
                &mut None,
            )
            .expect("Dist is a relation");
        let expected_call = ":: flowlog_runtime :: operators :: flowlog_lattice (rows , \"Merge: Dist\" , \
                             :: flowlog_runtime :: operators :: Min , | (x0 , x1) | ((x0 ,) , x1) , \
                             | (k0 ,) , v : i32 | (k0 , v) ,)";
        assert_eq!(
            (tokens.to_string(), mutability),
            (expected_call.to_string(), expected)
        );
    }

    #[test]
    fn merging_a_plain_relation_is_an_internal_error() {
        let codegen = codegen(PLAIN_PROGRAM);
        let s = codegen.program.idbs()[0].fingerprint();
        let error = codegen
            .gen_lattice_merge(
                s,
                quote! { rows },
                Mutability::Static,
                &[0],
                &ident("out"),
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

    // Cases: invalid grouping order, diagnostic detail.
    #[rstest]
    #[case(&[], "aggregation of `Dist` has grouping order [], expected columns [0]")]
    #[case(&[0, 0], "aggregation of `Dist` has grouping order [0, 0], expected columns [0]")]
    #[case(&[1], "aggregation of `Dist` has grouping order [1], expected columns [0]")]
    #[case(&[2], "aggregation of `Dist` has grouping order [2], expected columns [0]")]
    fn invalid_grouping_orders_are_internal_errors(
        #[case] order: &[usize],
        #[case] expected: &str,
    ) {
        let (codegen, dist, rows_mutability) = lattice_program("");
        let error = codegen
            .gen_lattice_merge(
                dist,
                quote! { rows },
                rows_mutability,
                order,
                &ident("out"),
                &mut None,
            )
            .expect_err("only column 0 belongs to Dist's group key");

        assert!(matches!(&error, CodegenError::Internal(_)));
        assert!(error.to_string().contains(expected), "{error}");
    }

    /// At a static loop's exit the fold merges the loop's answers outside
    /// the loop.
    #[test]
    fn a_fold_merges_the_answers_outside_the_loop() {
        let (codegen, dist, _) = lattice_program("");
        let tokens = codegen
            .gen_lattice_fold(dist, &ident("next"), &ident("out"), &mut None)
            .expect("Dist is a lattice relation");
        let exit = rendered(quote! { fn fold() { #tokens } });
        assert!(
            exit.contains(
                "let out = :: flowlog_runtime :: operators :: flowlog_lattice_leave (next , scope , \
                 \"MergeLeave: Dist\" , :: flowlog_runtime :: operators :: Min , \
                 | (x0 , x1) | ((x0 ,) , x1) ,"
            ),
            "{exit}"
        );
    }

    /// A plain relation has no answers to fold at a loop's exit.
    #[test]
    fn folding_a_plain_relation_is_an_internal_error() {
        let codegen = codegen(PLAIN_PROGRAM);
        let s = codegen.program.idbs()[0].fingerprint();
        let error = codegen
            .gen_lattice_fold(s, &ident("next"), &ident("out"), &mut None)
            .expect_err("S has no lattice column");
        assert!(matches!(&error, CodegenError::Internal(_)));
        assert!(
            error
                .to_string()
                .contains("relation `S` has no declared min/max column"),
            "{error}"
        );
    }
}
