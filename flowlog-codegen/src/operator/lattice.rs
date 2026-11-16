//! Lattice merges: a lattice relation keeps, per value of its other
//! columns, the least or greatest value of its lattice column, as its
//! binding says. [`Codegen::gen_lattice_merge`] wraps the union of the
//! rows its rules derive where a stratum binds that union, outside or
//! inside a loop; [`Codegen::gen_lattice_fold`] merges a static loop's
//! answers once more at its exit, since a static merge keeps every
//! iteration's answer.

use flowlog_parser::Lattice;
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

/// Runtime aggregation kind and closures for a lattice merge.
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
    /// Returns `rows` of relation `idb_fp` merged by `lattice` on column
    /// `merge_index`, grouped by every other column.
    ///
    /// Duplicate rows do not change the result, so the input need not be
    /// deduped first.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `merge_index` is outside the row.
    pub(crate) fn gen_lattice_merge(
        &self,
        idb_fp: u64,
        rows: TokenStream,
        merge_index: usize,
        lattice: Lattice,
    ) -> Result<TokenStream, CodegenError> {
        let MergeArgs { kind, split, merge } = self.merge_args(idb_fp, merge_index, lattice)?;
        let op_name = format!("Merge: {}", self.display_name(idb_fp));
        Ok(quote! {
            ::flowlog_runtime::operators::flowlog_lattice(
                #rows, #op_name, #kind, #split, #merge,
            )
        })
    }

    /// Returns `let <output> = flowlog_lattice_leave(<answers>, scope, ..);`,
    /// the merge of relation `idb_fp`'s `answers`, a static loop's, by
    /// `lattice` on column `merge_index`, bound outside the loop as
    /// `output`, static. Records the step's inner side in the plan graph;
    /// the stratum records the outer side once the scope is left.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `merge_index` is outside the row.
    pub(crate) fn gen_lattice_fold(
        &self,
        idb_fp: u64,
        merge_index: usize,
        lattice: Lattice,
        answers: &Ident,
        output: &Ident,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let name = self.display_name(idb_fp);
        let MergeArgs { kind, split, merge } = self.merge_args(idb_fp, merge_index, lattice)?;
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

    /// Returns the aggregation kind and row split/merge closures that merge
    /// relation `idb_fp`'s rows by `lattice` on column `merge_index`.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `merge_index` is outside the row.
    fn merge_args(
        &self,
        idb_fp: u64,
        merge_index: usize,
        lattice: Lattice,
    ) -> Result<MergeArgs, CodegenError> {
        let (_, row) = self.find_global_type(idb_fp)?;
        let Some(merged) = row.get(merge_index) else {
            return Err(CodegenError::internal(format!(
                "lattice merge of `{}` reads column {merge_index} of a {}-column row",
                self.display_name(idb_fp),
                row.len(),
            )));
        };
        Ok(MergeArgs {
            kind: lattice_kind(lattice),
            split: aggregation_split(row.len(), merge_index),
            merge: aggregation_merge(
                row.len(),
                merge_index,
                merged,
                self.config.str_intern_enabled(),
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use proc_macro2::Span;

    use super::*;
    use crate::test_harness::codegen;
    use crate::test_harness::rendered;

    fn ident(name: &str) -> Ident {
        Ident::new(name, Span::call_site())
    }

    /// A code generator over `Dist`, a lattice over the static `Cand`,
    /// with `Dist`'s fingerprint.
    fn lattice_program() -> (Codegen, u64) {
        let codegen = codegen(
            ".decl Cand(n: int32, d: int32)\n.input Cand\n\
             .decl Dist(n: int32, d: min<int32>)\nDist(n, d) :- Cand(n, d).\n.output Dist\n",
        );
        let dist = codegen.program.idbs()[0].fingerprint();
        (codegen, dist)
    }

    /// The merge splits the lattice column out of each row and puts the
    /// group's answer back at the same position.
    #[test]
    fn a_merge_groups_by_every_column_but_the_merge_index() {
        let (codegen, dist) = lattice_program();
        let tokens = codegen
            .gen_lattice_merge(dist, quote! { rows }, 1, Lattice::Min)
            .expect("column 1 is in Dist's row");
        let expected = quote! {
            ::flowlog_runtime::operators::flowlog_lattice(
                rows, "Merge: Dist", ::flowlog_runtime::operators::Min,
                |(x0, x1)| ((x0,), x1),
                |(k0,), v: i32| (k0, v),
            )
        };
        assert_eq!(tokens.to_string(), expected.to_string());
    }

    #[test]
    fn a_merge_index_outside_the_row_is_an_internal_error() {
        let (codegen, dist) = lattice_program();
        let error = codegen
            .gen_lattice_merge(dist, quote! { rows }, 2, Lattice::Min)
            .expect_err("Dist has two columns");
        assert!(matches!(&error, CodegenError::Internal(_)));
        assert!(
            error
                .to_string()
                .contains("lattice merge of `Dist` reads column 2 of a 2-column row"),
            "{error}"
        );
    }

    /// At a static loop's exit the fold merges the loop's answers outside
    /// the loop.
    #[test]
    fn a_fold_merges_the_answers_outside_the_loop() {
        let (codegen, dist) = lattice_program();
        let tokens = codegen
            .gen_lattice_fold(
                dist,
                1,
                Lattice::Min,
                &ident("next"),
                &ident("out"),
                &mut None,
            )
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
}
