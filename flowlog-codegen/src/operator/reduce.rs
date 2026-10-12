//! Reduces: the step ending an aggregating rule, the one step a stratum
//! emits twice. [`Codegen::gen_reduce`] builds it at the rule's step, and
//! again at a static loop's exit, where the step runs once more at the
//! outer time (see [`ReduceSite`]).

use std::collections::HashMap;

use flowlog_parser::AggregationOperator;
use flowlog_parser::Mutability;
use flowlog_planner::planner::Collection;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;
use crate::expr::aggregation::aggregation_empty_key;
use crate::expr::aggregation::aggregation_kind;
use crate::expr::aggregation::aggregation_merge;
use crate::expr::aggregation::aggregation_split;
use crate::ident::find_local_ident;

/// Where a reduce runs. A static loop cannot retract an answer its reduce
/// gave in an earlier iteration, so the loop's step keeps the best answer
/// so far for its feedback, and the step runs once more at the exit,
/// folding every iteration at the outer time into the final answers.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ReduceSite {
    /// The rule's step, inside a loop when `recursive`.
    Step { recursive: bool },
    /// A static loop's exit, binding [`folded_ident`] of the reduce's
    /// output outside the loop.
    Leave,
}

/// Returns the binding of the reduce producing `fp`, run once more at its
/// loop's exit.
pub(crate) fn folded_ident(fp: u64) -> Ident {
    format_ident!("folded_{fp}")
}

impl Codegen {
    /// Returns the statements reducing the rows of `input` into `output`
    /// by `aggregate`: an operator over a column, answering for the
    /// relation whose declaration types the answer. At a step, the rows
    /// are deduped first when the reduce needs a set of its own, then
    /// reduced through the operator the input's weight selects. At a
    /// loop's exit, the reduce folds what it read inside the loop, or its
    /// own answers when the operator is idempotent, through the loop's
    /// `scope` binding. Records each operator in the plan graph, except an
    /// exit's outer-scope side, which the stratum records after the
    /// scope exit.
    pub(crate) fn gen_reduce(
        &self,
        local_fp_to_ident: &HashMap<u64, Ident>,
        input: &Collection,
        output: &Collection,
        (operator, position, relation): (AggregationOperator, usize, u64),
        site: ReduceSite,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let name = self.display_name(relation);
        let arity = output.arity().1;
        let answer_type = self.agg_column_type(relation, position)?;
        let kind = aggregation_kind(operator);
        let empty_key = aggregation_empty_key(arity);
        let split = aggregation_split(arity, position);
        let merge = aggregation_merge(arity, position, &answer_type);
        let args = quote! { #kind, #empty_key, #split, #merge };

        let inp = find_local_ident(local_fp_to_ident, input.fingerprint());
        let out = find_local_ident(local_fp_to_ident, output.fingerprint());
        let mutability = input.mutability();

        // The rows hold a row per derivation, which `count`, `sum` and
        // `avg` would each add up. An idempotent operator gives the same
        // answer over a repeated row, and over static rows its reduce folds
        // each derivation into one weight per group with no work per row,
        // so a dedup would only hold a second copy of the rows. Over signed
        // rows a repeated derivation changes a group's count and has the
        // reduce rescan the group, which a dedup absorbs first, so there
        // it stays.
        let dedups = !(operator.is_idempotent() && mutability == Mutability::Static);
        let rows = if dedups {
            format_ident!("contributions_{}", output.fingerprint())
        } else {
            inp.clone()
        };

        match site {
            ReduceSite::Step { recursive } => {
                let dedup = if dedups {
                    with_plan_graph(plan_graph, |plan_graph| {
                        plan_graph.concat_dedup_operator(
                            format!("Contributions: {name}"),
                            vec![inp.to_string()],
                            rows.to_string(),
                            0,
                            0,
                            mutability,
                            recursive,
                        );
                    });
                    quote! { let #rows = ::flowlog_runtime::operators::flowlog_dedup(#inp.clone()); }
                } else {
                    quote! {}
                };

                // The runtime builds a different operator for each weight,
                // and seeds the empty group of a `count` or `sum` over an
                // empty key with its zero; the plan graph predicts both so
                // it counts the same operators.
                let seeded = arity == 1
                    && match operator {
                        AggregationOperator::Count | AggregationOperator::Sum => true,
                        AggregationOperator::Min
                        | AggregationOperator::Max
                        | AggregationOperator::Avg => false,
                    };
                // The append reduce mirrors the mutable one operator for
                // operator.
                with_plan_graph(plan_graph, |plan_graph| match mutability {
                    Mutability::Static => plan_graph.static_aggregate_operator(
                        name.clone(),
                        rows.to_string(),
                        out.to_string(),
                        output.fingerprint(),
                    ),
                    Mutability::Append | Mutability::Mutable => plan_graph
                        .mutable_aggregate_operator(
                            name.clone(),
                            rows.to_string(),
                            out.to_string(),
                            output.fingerprint(),
                            seeded,
                        ),
                });
                let reduce = match mutability {
                    Mutability::Static | Mutability::Mutable => quote! { flowlog_reduce },
                    Mutability::Append => quote! { flowlog_reduce_append },
                };
                let op_name = format!("Reduce: {name}");
                Ok(quote! {
                    #dedup
                    let #out = ::flowlog_runtime::operators::#reduce(#rows.clone(), #op_name, #args);
                })
            }
            ReduceSite::Leave => {
                // An idempotent operator folds its own improving answers,
                // which leaves the final extreme unchanged and sends fewer
                // rows across the boundary. The others need the rows:
                // summing the running answers 2 and 5 would give 7, although
                // the inputs 2 and 3 sum to 5.
                let read = if operator.is_idempotent() {
                    &out
                } else {
                    &rows
                };
                let folded = folded_ident(output.fingerprint());
                with_plan_graph(plan_graph, |plan_graph| {
                    plan_graph.recursive_pre_leave_static_aggregate_operator(
                        name.clone(),
                        read.to_string(),
                        folded.to_string(),
                    );
                });
                let op_name = format!("ReduceLeave: {name}");
                Ok(quote! {
                    let #folded = ::flowlog_runtime::operators::flowlog_reduce_leave(
                        #read, scope, #op_name, #args,
                    );
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use flowlog_planner::planner::ProgramPlanner;
    use quote::quote;
    use rstest::rstest;

    use super::*;
    use crate::test_harness::codegen;
    use crate::test_harness::rendered;

    /// The reduce step of `source`, a program with one aggregating rule,
    /// emitted at `site`: the statements rendered, the binding of the rows
    /// the step reads, and the fingerprint of its answers. The planner is
    /// the smallest entry that produces the collections a reduce reads and
    /// writes.
    fn reduce(source: &str, site: ReduceSite) -> (String, Ident, u64) {
        let codegen = codegen(source);
        let planner = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let reduce = planner.strata()[0]
            .non_recursive_transformations()
            .iter()
            .find(|tx| tx.flow().aggregate().is_some())
            .expect("the program has one aggregating rule");
        let aggregate = reduce.flow().aggregate().expect("a reduce");
        let local_fp_to_ident = &codegen.global_fp_to_ident;
        let tokens = codegen
            .gen_reduce(
                local_fp_to_ident,
                reduce.unary_input(),
                reduce.output(),
                aggregate,
                site,
                &mut None,
            )
            .expect("the declared column types the answer");
        (
            rendered(quote! { fn reduce() { #tokens } }),
            find_local_ident(local_fp_to_ident, reduce.unary_input().fingerprint()),
            reduce.output().fingerprint(),
        )
    }

    const STEP: ReduceSite = ReduceSite::Step { recursive: false };

    /// A reduce dedups the rows it reads under a binding of its own unless
    /// a static extreme reads them, which a repeated row cannot change.
    // Cases: program, dedups.
    #[rstest]
    #[case::count_over_derived_rows(
        ".decl Edge(x: int32, y: int32)\n.input Edge\n\
         .decl Deg(x: int32, n: int32)\nDeg(x, count(y)) :- Edge(x, y), x > 0.\n.output Deg\n",
        true
    )]
    #[case::extreme_over_a_relation(
        ".decl Cand(n: int32, d: int32)\n.input Cand\n\
         .decl Dist(n: int32, d: int32)\nDist(n, min(d)) :- Cand(n, d).\n.output Dist\n",
        false
    )]
    #[case::count_over_a_relation(
        ".decl Cand(n: int32, d: int32)\n.input Cand\n\
         .decl Deg(n: int32, c: int32)\nDeg(n, count(d)) :- Cand(n, d).\n.output Deg\n",
        true
    )]
    #[case::static_extreme_over_derived_rows(
        ".decl Edge(x: int32, y: int32)\n.input Edge\n\
         .decl Near(x: int32, y: int32)\nNear(x, min(y)) :- Edge(x, y), y > 0.\n.output Near\n",
        false
    )]
    #[case::mutable_extreme_over_derived_rows(
        ".decl Edge(x: int32, y: int32) mutable\n.input Edge\n\
         .decl Near(x: int32, y: int32)\nNear(x, max(y)) :- Edge(x, y), y > 0.\n.output Near\n",
        true
    )]
    #[case::append_extreme_over_derived_rows(
        ".decl Edge(x: int32, y: int32) append\n.input Edge\n\
         .decl Near(x: int32, y: int32)\nNear(x, min(y)) :- Edge(x, y), y > 0.\n.output Near\n",
        true
    )]
    fn a_reduce_dedups_unless_its_rows_are_a_set_or_a_static_extreme_reads_them(
        #[case] source: &str,
        #[case] dedups: bool,
    ) {
        let (step, input, answers) = reduce(source, STEP);
        let contributions = format!("contributions_{answers}");
        let reads = |rows: &str| step.contains(&format!("({rows} . clone () , \"Reduce: "));
        assert_eq!(
            step.contains(&format!("let {contributions} =")),
            dedups,
            "{step}"
        );
        assert_eq!(reads(&contributions), dedups, "{step}");
        assert_eq!(reads(&input.to_string()), !dedups, "{step}");
    }

    /// Append rows reduce through the signed append reduce; static and
    /// mutable rows through the reduce their own weight selects.
    // Cases: input declaration, reduce.
    #[rstest]
    #[case::static_rows("", "flowlog_reduce (")]
    #[case::append_rows(" append", "flowlog_reduce_append (")]
    #[case::mutable_rows(" mutable", "flowlog_reduce (")]
    fn a_reduce_runs_through_its_weights_operator(
        #[case] declaration: &str,
        #[case] operator: &str,
    ) {
        let (step, ..) = reduce(
            &format!(
                ".decl Edge(x: int32, y: int32){declaration}\n.input Edge\n\
                 .decl Size(n: int32)\nSize(count(y)) :- Edge(_, y).\n.output Size\n"
            ),
            STEP,
        );
        let expected = format!(":: flowlog_runtime :: operators :: {operator}");
        assert!(step.contains(&expected), "{step}");
        assert!(!step.contains("flowlog_reduce_leave"), "{step}");
    }

    /// At a loop's exit the reduce binds its fold outside the loop, over
    /// its own answers when the operator is idempotent and over the rows
    /// it read otherwise.
    // Cases: rule, the fold reads the answers.
    #[rstest]
    #[case::extreme("Near(x, min(y)) :- Edge(x, y), y > 0.", true)]
    #[case::count("Near(x, count(y)) :- Edge(x, y), y > 0.", false)]
    fn a_loop_exit_folds_answers_or_rows(#[case] rule: &str, #[case] answers: bool) {
        let (exit, _, fp) = reduce(
            &format!(
                ".decl Edge(x: int32, y: int32)\n.input Edge\n\
                 .decl Near(x: int32, y: int32)\n{rule}\n.output Near\n"
            ),
            ReduceSite::Leave,
        );
        let read = if answers {
            format!("t_{fp}")
        } else {
            format!("contributions_{fp}")
        };
        let expected = format!(
            "let folded_{fp} = :: flowlog_runtime :: operators :: flowlog_reduce_leave ({read} , scope ,"
        );
        assert!(exit.contains(&expected), "{exit}");
        assert!(!exit.contains("flowlog_dedup"), "{exit}");
    }
}
