//! Lattice merges: a lattice relation keeps, per value of its other
//! columns, the least or greatest value of its lattice column, as its
//! binding says. [`Codegen::gen_lattice_merge`] wraps the union of the
//! rows its rules derive where a stratum binds that union, outside or
//! inside a loop; [`Codegen::gen_lattice_fold`] merges a static loop's
//! answers once more at its exit, since a static merge keeps every
//! iteration's answer. Both merge rows as the binding's collapse holds
//! them, the layout its merge index indexes.

use flowlog_planner::planner::RelationBinding;
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
    /// Returns `rows`, `binding`'s union in its collapse layout, merged by
    /// its lattice on its merge column, grouped by every other column.
    ///
    /// Duplicate rows do not change the result, so the input need not be
    /// deduped first.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `binding` is not a lattice binding, its
    /// collapse has no recorded type, or its merge index is outside the
    /// row.
    pub(crate) fn gen_lattice_merge(
        &self,
        binding: &RelationBinding,
        rows: TokenStream,
    ) -> Result<TokenStream, CodegenError> {
        let MergeArgs { kind, split, merge } = self.merge_args(binding)?;
        let op_name = format!(
            "Merge: {}",
            self.display_name(binding.output().fingerprint())
        );
        Ok(quote! {
            ::flowlog_runtime::operators::lattice(
                #rows, #op_name, #kind, #split, #merge,
            )
        })
    }

    /// Returns `let <output> = lattice_leave(<answers>, scope, ..);`,
    /// the merge of `answers`, a static loop's collapsed rows of `binding`,
    /// as [`Self::gen_lattice_merge`] merges them, bound outside the loop
    /// as `output`, static. Records the step's inner side in the plan
    /// graph; the stratum records the outer side once the scope is left.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `binding` is not a lattice binding, its
    /// collapse has no recorded type, or its merge index is outside the
    /// row.
    pub(crate) fn gen_lattice_fold(
        &self,
        binding: &RelationBinding,
        answers: &Ident,
        output: &Ident,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let name = self.display_name(binding.output().fingerprint());
        let MergeArgs { kind, split, merge } = self.merge_args(binding)?;
        with_plan_graph(plan_graph, |plan_graph| {
            plan_graph.recursive_pre_leave_lattice_operator(
                name.clone(),
                answers.to_string(),
                output.to_string(),
            );
        });
        let op_name = format!("MergeLeave: {name}");
        Ok(quote! {
            let #output = ::flowlog_runtime::operators::lattice_leave(
                #answers, scope, #op_name, #kind, #split, #merge,
            );
        })
    }

    /// Returns the aggregation kind and row split/merge closures that merge
    /// `binding`'s collapsed rows by its lattice on its merge column.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `binding` is not a lattice binding, its
    /// collapse has no recorded type, or its merge index is outside the
    /// row.
    fn merge_args(&self, binding: &RelationBinding) -> Result<MergeArgs, CodegenError> {
        let name = self.display_name(binding.output().fingerprint());
        let RelationBinding::Lattice {
            merge_index,
            lattice,
            ..
        } = binding
        else {
            return Err(CodegenError::internal(format!(
                "relation `{name}` dedups, so it has no lattice merge"
            )));
        };
        let (_, row) = self.find_global_type(binding.collapse().fingerprint())?;
        let Some(merged) = row.get(*merge_index) else {
            return Err(CodegenError::internal(format!(
                "lattice merge of `{name}` reads column {merge_index} of a {}-column row",
                row.len(),
            )));
        };
        Ok(MergeArgs {
            kind: lattice_kind(*lattice),
            split: aggregation_split(row.len(), *merge_index),
            merge: aggregation_merge(
                row.len(),
                *merge_index,
                merged,
                self.config.str_intern_enabled(),
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use flowlog_common::compute_fp;
    use flowlog_parser::DataType;
    use flowlog_planner::planner::ProgramPlanner;
    use proc_macro2::Span;

    use super::*;
    use crate::test_harness::codegen;
    use crate::test_harness::rendered;

    fn ident(name: &str) -> Ident {
        Ident::new(name, Span::call_site())
    }

    /// A code generator over `Dist`, a lattice over the static `Cand`, and
    /// `Dist`'s binding rebuilt to collapse into its rule result, typed
    /// `row`, and merge its column `merge_index`.
    fn dist(merge_index: usize, row: Vec<DataType>) -> (Codegen, RelationBinding) {
        let mut codegen = codegen(
            ".decl Cand(n: int32, d: int64)\n.input Cand\n\
             .decl Dist(n: int32, d: min<int64>)\nDist(n, d) :- Cand(n, d).\n.output Dist\n",
        );
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let Some(RelationBinding::Lattice {
            input_seed,
            input_contributors,
            lattice,
            flow,
            output,
            ..
        }) = plan
            .strata()
            .iter()
            .find_map(|stratum| stratum.relation_bindings().get(&compute_fp("dist")))
        else {
            panic!("Dist merges its lattice column");
        };
        let collapse = Arc::clone(&input_contributors[0]);
        codegen
            .global_fp_to_type
            .insert(collapse.fingerprint(), (Vec::new(), row));
        let binding = RelationBinding::Lattice {
            input_seed: input_seed.clone(),
            input_contributors: input_contributors.clone(),
            merge_index,
            lattice: *lattice,
            collapse,
            flow: flow.clone(),
            output: Arc::clone(output),
        };
        (codegen, binding)
    }

    /// The merge splits the lattice column out of each row and puts the
    /// group's answer back at the same position.
    #[test]
    fn a_merge_groups_by_every_column_but_the_merge_index() {
        let (codegen, binding) = dist(1, vec![DataType::Int32, DataType::Int64]);
        let tokens = codegen
            .gen_lattice_merge(&binding, quote! { rows })
            .expect("column 1 is in Dist's row");
        let expected = quote! {
            ::flowlog_runtime::operators::lattice(
                rows, "Merge: Dist", ::flowlog_runtime::operators::Min,
                |(x0, x1)| ((x0,), x1),
                |(k0,), v: i64| (k0, v),
            )
        };
        assert_eq!(tokens.to_string(), expected.to_string());
    }

    #[test]
    fn a_dedup_binding_has_no_lattice_merge() {
        let (codegen, binding) = dist(1, vec![DataType::Int32, DataType::Int64]);
        let RelationBinding::Lattice {
            input_seed,
            input_contributors,
            collapse,
            flow,
            output,
            ..
        } = binding
        else {
            panic!("dist builds a lattice binding");
        };
        let dedup = RelationBinding::Dedup {
            input_seed,
            input_contributors,
            collapse,
            flow,
            output,
        };
        let error = codegen
            .gen_lattice_merge(&dedup, quote! { rows })
            .expect_err("a dedup binding merges no column");
        assert!(matches!(&error, CodegenError::Internal(_)));
        assert!(
            error
                .to_string()
                .contains("relation `Dist` dedups, so it has no lattice merge"),
            "{error}"
        );
    }

    /// The merge reads rows as the collapse holds them, so the collapse's
    /// type, not the relation's, types the merged column: here it holds
    /// `Dist`'s columns swapped.
    #[test]
    fn a_merge_types_its_column_by_the_collapsed_rows() {
        let (codegen, binding) = dist(0, vec![DataType::Int64, DataType::Int32]);
        let tokens = codegen
            .gen_lattice_merge(&binding, quote! { rows })
            .expect("column 0 is in the collapsed row");
        let expected = quote! {
            ::flowlog_runtime::operators::lattice(
                rows, "Merge: Dist", ::flowlog_runtime::operators::Min,
                |(x0, x1)| ((x1,), x0),
                |(k0,), v: i64| (v, k0),
            )
        };
        assert_eq!(tokens.to_string(), expected.to_string());
    }

    #[test]
    fn a_merge_index_outside_the_row_is_an_internal_error() {
        let (codegen, binding) = dist(2, vec![DataType::Int32, DataType::Int64]);
        let error = codegen
            .gen_lattice_merge(&binding, quote! { rows })
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
        let (codegen, binding) = dist(1, vec![DataType::Int32, DataType::Int64]);
        let tokens = codegen
            .gen_lattice_fold(&binding, &ident("next"), &ident("out"), &mut None)
            .expect("Dist is a lattice relation");
        let exit = rendered(quote! { fn fold() { #tokens } });
        assert!(
            exit.contains(
                "let out = :: flowlog_runtime :: operators :: lattice_leave (next , scope , \
                 \"MergeLeave: Dist\" , :: flowlog_runtime :: operators :: Min , \
                 | (x0 , x1) | ((x0 ,) , x1) ,"
            ),
            "{exit}"
        );
    }
}
