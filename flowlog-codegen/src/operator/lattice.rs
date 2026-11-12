//! Lattice merges: a lattice relation keeps, per value of its other
//! columns, the least or greatest value of its lattice column, as its
//! declaration says. [`Codegen::gen_lattice_merge`] merges the rows its
//! rules derive where a stratum binds them, outside or inside a loop, and
//! again as a presence loop's answers leave it. The planner's weights pick
//! the merge, through [`Codegen::merge_path`]: it climbs where its answers
//! keep its rows' presence weight, a beaten answer staying, and pays where
//! they are mutable, taking each beaten answer back.

use flowlog_parser::Mutability;
use flowlog_profiler::Answers;
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
    /// Returns how relation `idb_fp`'s merge turns rows of weight `rows`
    /// into answers of weight `answers`, the planner's: it climbs where
    /// the answers keep the rows' presence weight, and pays where they are
    /// mutable, replacing an appended key's beaten answer or reducing a
    /// mutable key's rows again. Static rows arrive at once, where a climb
    /// pays already.
    ///
    /// # Errors
    ///
    /// Returns an internal error when no merge answers the rows at
    /// `answers`: the planner gave the relation a weight its rows cannot
    /// reach.
    pub(crate) fn merge_path(
        &self,
        idb_fp: u64,
        rows: Mutability,
        answers: Mutability,
    ) -> Result<Answers, CodegenError> {
        match (rows, answers) {
            (Mutability::Static, Mutability::Static) | (Mutability::Append, Mutability::Append) => {
                Ok(Answers::Settled)
            }
            (Mutability::Append, Mutability::Mutable) => Ok(Answers::Replaced),
            (Mutability::Mutable, Mutability::Mutable) => Ok(Answers::Reduced),
            (Mutability::Static, Mutability::Append | Mutability::Mutable)
            | (Mutability::Append, Mutability::Static)
            | (Mutability::Mutable, Mutability::Static | Mutability::Append) => {
                Err(CodegenError::internal(format!(
                    "no merge answers `{}`'s {rows} rows at {answers}",
                    self.display_name(idb_fp)
                )))
            }
        }
    }

    /// Returns `rows` merged by relation `idb_fp`'s declared min/max column
    /// along `path`, from [`Self::merge_path`]: a climb answers at the
    /// rows' weight, a pay at `diff::Mutable`. Groups are identified by
    /// every other column, arranged in `order`, which must list each
    /// grouping column exactly once, or in declaration order for `None`.
    /// Output rows retain logical column positions. Records the runtime
    /// steps under `output`.
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
        path: Answers,
        order: Option<&[usize]>,
        output: &Ident,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let MergeArgs { kind, split, merge } = self.merge_args(idb_fp, order)?;
        let name = self.display_name(idb_fp);
        // The collection feeding the merge is recorded under the output's
        // name too, so the merge reads from it.
        with_plan_graph(plan_graph, |plan_graph| {
            plan_graph.lattice_operator(
                name.clone(),
                output.to_string(),
                output.to_string(),
                idb_fp,
                path,
            );
        });
        let merged = match path {
            Answers::Settled => quote! { ::flowlog_runtime::operators::flowlog_lattice_climb },
            Answers::Replaced | Answers::Reduced => {
                quote! { ::flowlog_runtime::operators::flowlog_lattice }
            }
        };
        let op_name = format!("Merge: {name}");
        Ok(quote! { #merged(#rows, #op_name, #kind, #split, #merge,) })
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

    /// A plain relation `S` copied from a static input.
    const PLAIN_PROGRAM: &str =
        ".decl R(x: int32)\n.input R\n.decl S(x: int32)\nS(x) :- R(x).\n.output S\n";

    fn ident(name: &str) -> Ident {
        Ident::new(name, Span::call_site())
    }

    /// A code generator over `Dist`, a lattice over the static input
    /// `Cand`, with `Dist`'s fingerprint.
    fn lattice_program() -> (Codegen, u64) {
        let codegen = codegen(
            ".decl Cand(n: int32, d: int32)\n.input Cand\n\
             .decl Dist(n: int32, d: min<int32>)\nDist(n, d) :- Cand(n, d).\n.output Dist\n",
        );
        let dist = codegen.program.idbs()[0].fingerprint();
        (codegen, dist)
    }

    /// A merge climbs where its answers keep its rows' presence weight,
    /// and pays where they are mutable.
    // Cases: rows weight, answers weight, path.
    #[rstest]
    #[case::static_climbs(Mutability::Static, Mutability::Static, Answers::Settled)]
    #[case::append_climbs(Mutability::Append, Mutability::Append, Answers::Settled)]
    #[case::append_replaces(Mutability::Append, Mutability::Mutable, Answers::Replaced)]
    #[case::mutable_reduces(Mutability::Mutable, Mutability::Mutable, Answers::Reduced)]
    fn a_merge_climbs_or_pays_to_its_answers_weight(
        #[case] rows: Mutability,
        #[case] answers: Mutability,
        #[case] expected: Answers,
    ) {
        let (codegen, dist) = lattice_program();
        assert_eq!(codegen.merge_path(dist, rows, answers).unwrap(), expected);
    }

    /// No merge answers below its rows' weight, nor answers static rows at
    /// a weight that changes.
    // Cases: rows weight, answers weight.
    #[rstest]
    #[case(Mutability::Static, Mutability::Append)]
    #[case(Mutability::Static, Mutability::Mutable)]
    #[case(Mutability::Append, Mutability::Static)]
    #[case(Mutability::Mutable, Mutability::Static)]
    #[case(Mutability::Mutable, Mutability::Append)]
    fn a_weight_no_merge_reaches_is_an_internal_error(
        #[case] rows: Mutability,
        #[case] answers: Mutability,
    ) {
        let (codegen, dist) = lattice_program();
        let error = codegen
            .merge_path(dist, rows, answers)
            .expect_err("no merge reaches the weight");
        assert!(matches!(&error, CodegenError::Internal(_)));
        assert!(
            error.to_string().contains(&format!(
                "no merge answers `Dist`'s {rows} rows at {answers}"
            )),
            "{error}"
        );
    }

    /// A climb calls the climbing merge, a pay the paying one, which picks
    /// its strategy by the rows' weight. With no order the group key
    /// follows the declaration.
    // Cases: path, grouping order, runtime merge.
    #[rstest]
    #[case::climb(Answers::Settled, None, "flowlog_lattice_climb")]
    #[case::replace(Answers::Replaced, Some(&[0][..]), "flowlog_lattice")]
    #[case::reduce(Answers::Reduced, Some(&[0][..]), "flowlog_lattice")]
    fn a_merge_calls_the_runtime_merge_of_its_path(
        #[case] path: Answers,
        #[case] order: Option<&[usize]>,
        #[case] function: &str,
    ) {
        let (codegen, dist) = lattice_program();
        let tokens = codegen
            .gen_lattice_merge(dist, quote! { rows }, path, order, &ident("out"), &mut None)
            .expect("Dist is a lattice relation");
        let expected_call = format!(
            ":: flowlog_runtime :: operators :: {function} (rows , \"Merge: Dist\" , \
             :: flowlog_runtime :: operators :: Min , | (x0 , x1) | ((x0 ,) , x1) , \
             | (k0 ,) , v : i32 | (k0 , v) ,)"
        );
        assert_eq!(tokens.to_string(), expected_call);
    }

    #[test]
    fn merging_a_plain_relation_is_an_internal_error() {
        let codegen = codegen(PLAIN_PROGRAM);
        let s = codegen.program.idbs()[0].fingerprint();
        let error = codegen
            .gen_lattice_merge(
                s,
                quote! { rows },
                Answers::Settled,
                Some(&[0]),
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
        let (codegen, dist) = lattice_program();
        let error = codegen
            .gen_lattice_merge(
                dist,
                quote! { rows },
                Answers::Settled,
                Some(order),
                &ident("out"),
                &mut None,
            )
            .expect_err("only column 0 belongs to Dist's group key");

        assert!(matches!(&error, CodegenError::Internal(_)));
        assert!(error.to_string().contains(expected), "{error}");
    }
}
