//! Aggregates: the operator reducing the matches of an aggregate's braces
//! to one answer per group, then mapping each answer row to the step's
//! output. The matches come arranged by the group key, one row per match;
//! the groups, when the step has them, come arranged by themselves. The
//! runtime reduces the matches' arrangement and answers the groups it is
//! given as the aggregation allows, so the code here only picks the
//! runtime's entry point for the path the aggregate answers by, names the
//! operator and records its plan graph entry.

use std::collections::HashMap;

use flowlog_parser::AggregationOperator;
use flowlog_parser::DataType;
use flowlog_parser::Mutability;
use flowlog_planner::planner::Collection;
use flowlog_planner::planner::TransformationArgument;
use flowlog_planner::planner::TransformationFlow;
use flowlog_planner::planner::aggregate_mutability;
use flowlog_profiler::Answers;
use flowlog_profiler::Groups;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;
use syn::Index;

use super::Operator;
use super::arranged_input;
use super::keyed_output;
use super::map::flat_map_body_tokens;
use super::register_arrangement;
use crate::Codegen;
use crate::CodegenError;
use crate::expr::aggregation::aggregation_kind;
use crate::expr::param::kv_params;
use crate::ident::arrangement_ident;
use crate::ident::find_local_ident;
use crate::ty::data::KvTypes;
use crate::ty::data::internal_column_tokens;
use crate::ty::diff::weight_tokens;

impl Codegen {
    /// Returns the reduce of the arrangement of `matches` under `flow`'s
    /// aggregation, answering the arrangement of `groups` or, without
    /// groups, the empty key when the matches have no key, its answer
    /// rows mapped into `output` by `flow`, as rows or keyed as
    /// `operator` says, and the arrangement of a keyed output.
    ///
    /// A filter on the answer row needs its own map after the reduce;
    /// without one the projection runs inside the reduce's merge.
    pub(super) fn gen_aggregate(
        &mut self,
        operator: Operator<'_>,
        (matches, groups): (&Collection, Option<&Collection>),
        output: &Collection,
        flow: &TransformationFlow,
        arranged_map: &mut HashMap<u64, Ident>,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let TransformationFlow::Aggregate {
            operator: op,
            target,
            ty,
            key,
            value,
            compares,
        } = flow
        else {
            return Err(CodegenError::internal(format!(
                "aggregate {output} has the flow of a map or a join"
            )));
        };
        let rows = arranged_input(operator.local_fp_to_ident, arranged_map, matches)?;
        let groups = groups
            .map(|groups| arranged_input(operator.local_fp_to_ident, arranged_map, groups))
            .transpose()?;
        let out = find_local_ident(operator.local_fp_to_ident, output.fingerprint());
        let Operator {
            name,
            literal,
            arrange,
            recursive,
            si,
            ..
        } = operator;

        let matches_mutability = matches.mutability();
        debug_assert_eq!(
            output.mutability(),
            aggregate_mutability(matches_mutability),
            "aggregate {output} answers at a weight the planner does not give it",
        );
        // The groups the runtime answers as the aggregation allows: the
        // arrangement the step has, or the empty key when the matches
        // have no key, so the one group is answered whether or not any
        // match exists; a keyed aggregate without groups answers only the
        // keys its matches hold. Groups without an arrangement take the
        // matches' weight, which nothing else fixes.
        let weight = weight_tokens(matches_mutability);
        let (groups_kind, seeds) = match &groups {
            Some(groups) => (
                Groups::Arranged,
                quote! { ::flowlog_runtime::operators::Groups::Arranged(#groups.clone()) },
            ),
            None if matches.arity().0 == 0 => (
                Groups::Key,
                quote! { ::flowlog_runtime::operators::Groups::<_, _, #weight>::Key(()) },
            ),
            None => (
                Groups::None,
                quote! { ::flowlog_runtime::operators::Groups::<_, _, #weight>::None },
            ),
        };

        let matches_type = self.find_global_type(matches.fingerprint())?;
        let column = target_column(*target, &matches_type.1)?;
        // `count` adds up values of its answer type; the other operators
        // add up their target column.
        let accumulated = column.map_or(ty, |idx| &matches_type.1[idx]);
        let total_clock = self.program.is_incremental() && !recursive;
        let answers = answers(*op, matches_mutability, accumulated, total_clock);
        let aggregate = entry_point(matches_mutability, answers);
        let target = target_tokens(column);
        let answer_row: KvTypes = (matches_type.0.clone(), vec![ty.clone()]);
        let answer_ty = internal_column_tokens(ty, si);
        let (k, v) = kv_params(key, value, compares, None);
        let value_expr = self.kv_projection(value, si, &answer_row)?;
        let out_expr = if arrange.is_some() {
            keyed_output(
                output,
                self.kv_projection(key, si, &answer_row)?,
                value_expr,
            )
        } else {
            value_expr
        };
        let pred = self.kv_compare_predicate(compares, si, &answer_row)?;

        with_plan_graph(plan_graph, |plan_graph| {
            let inputs = std::iter::once(&rows)
                .chain(groups.as_ref())
                .map(Ident::to_string)
                .collect();
            let (output_name, arrange) = match arrange.as_deref() {
                Some(_) => (
                    arrangement_ident(&out).to_string(),
                    Some(output.is_k_only()),
                ),
                None => (out.to_string(), None),
            };
            plan_graph.aggregate_operator(
                name,
                inputs,
                output_name,
                output.fingerprint(),
                *op,
                answers,
                groups_kind,
                pred.is_some(),
                arrange,
            );
        });

        let kind = aggregation_kind(*op);
        // The merge sees the answer as a scalar; the projection reads it
        // as the answer row's one value column.
        let rebind = (v.to_string() == "v").then(|| quote! { let v = (v,); });
        let arrange_stmt = register_arrangement(arranged_map, output, &out, arrange.as_deref());
        let step = match pred {
            None => quote! {
                let #out = #aggregate(
                    #rows.clone(), #seeds, #literal, #kind, #target,
                    |#k, #v: #answer_ty| { #rebind #out_expr },
                );
            },
            Some(pred) => {
                let answers = format_ident!("{out}_answers");
                let body = flat_map_body_tokens(Some(pred), out_expr);
                quote! {
                    let #answers = #aggregate(
                        #rows.clone(), #seeds, #literal, #kind, #target,
                        |k, v: #answer_ty| (k, (v,)),
                    );
                    let #out = ::flowlog_runtime::operators::flowlog_map(
                        #answers,
                        #literal,
                        |(#k, #v), t, d| { #body },
                    );
                }
            }
        };
        Ok(quote! {
            #step
            #arrange_stmt
        })
    }
}

/// Returns how `op` answers over matches of weight `matches` when it adds
/// up values of type `accumulated`. `total_clock` is `true` when the
/// aggregate's times are totally ordered: outside a loop, in an engine
/// whose epochs advance.
///
/// A mutable aggregate replaces its answers only when its reduction can
/// subtract a deleted match exactly, an integer total, and its times come
/// in order; otherwise it reads a changed group again. The answer picks
/// the runtime's entry point ([`entry_point`]), so the plan graph records
/// the path that runs.
#[must_use]
fn answers(
    op: AggregationOperator,
    matches: Mutability,
    accumulated: &DataType,
    total_clock: bool,
) -> Answers {
    let adds_up = match op {
        AggregationOperator::Count | AggregationOperator::Sum | AggregationOperator::Mean => true,
        AggregationOperator::Min | AggregationOperator::Max => false,
    };
    let abelian = adds_up && accumulated.is_integer();
    match matches {
        Mutability::Static => Answers::Settled,
        Mutability::Append => Answers::Replaced,
        Mutability::Mutable if total_clock && abelian => Answers::Replaced,
        Mutability::Mutable => Answers::Reduced,
    }
}

/// Returns the runtime function that answers as `answers` says over
/// matches of weight `matches`. A mutable aggregate that replaces its
/// answers subtracts deleted matches, which only
/// `flowlog_aggregate_abelian` does; `flowlog_aggregate` picks the
/// other paths by the matches' weight.
#[must_use]
fn entry_point(matches: Mutability, answers: Answers) -> TokenStream {
    match (matches, answers) {
        (Mutability::Mutable, Answers::Replaced) => {
            quote! { ::flowlog_runtime::operators::flowlog_aggregate_abelian }
        }
        (Mutability::Mutable, Answers::Settled | Answers::Reduced)
        | (
            Mutability::Static | Mutability::Append,
            Answers::Settled | Answers::Replaced | Answers::Reduced,
        ) => quote! { ::flowlog_runtime::operators::flowlog_aggregate },
    }
}

/// Returns the index of the value column `target` reads out of a match;
/// `None` for `count`, which has no target.
///
/// # Errors
///
/// Returns an internal error if the target is not a value column of the
/// matches, which the planner's target column rules out.
fn target_column(
    target: Option<TransformationArgument>,
    values: &[DataType],
) -> Result<Option<usize>, CodegenError> {
    let Some(target) = target else {
        return Ok(None);
    };
    let column = match target {
        TransformationArgument::KV((false, idx)) => (idx < values.len()).then_some(idx),
        TransformationArgument::KV((true, _)) | TransformationArgument::Jn(_) => None,
    };
    column.map(Some).ok_or_else(|| {
        CodegenError::internal(format!(
            "aggregate target {target} is not a value column of the matches"
        ))
    })
}

/// Returns the closure reading the value column `column` out of a match's
/// value: nothing for `count`, which has no target.
#[must_use]
fn target_tokens(column: Option<usize>) -> TokenStream {
    match column {
        None => quote! { |_| () },
        Some(idx) => {
            let i = Index::from(idx);
            quote! { |v| v.#i }
        }
    }
}

#[cfg(test)]
mod tests {
    use flowlog_planner::planner::ProgramPlanner;
    use rstest::rstest;

    use super::*;
    use crate::test_harness::codegen;

    fn normalized(tokens: TokenStream) -> String {
        tokens.to_string().split_whitespace().collect()
    }

    // Cases: column, closure.
    #[rstest]
    #[case(None, "|_|()")]
    #[case(Some(2), "|v|v.2")]
    fn the_target_reads_its_value_column(#[case] column: Option<usize>, #[case] expected: &str) {
        assert_eq!(normalized(target_tokens(column)), expected);
    }

    // Cases: operator, accumulated type, total clock, answers.
    #[rstest]
    #[case(AggregationOperator::Count, DataType::Int64, true, Answers::Replaced)]
    #[case(AggregationOperator::Mean, DataType::UInt8, true, Answers::Replaced)]
    #[case(AggregationOperator::Sum, DataType::Float64, true, Answers::Reduced)]
    #[case(AggregationOperator::Min, DataType::Int32, true, Answers::Reduced)]
    #[case(AggregationOperator::Sum, DataType::Int32, false, Answers::Reduced)]
    fn a_mutable_aggregate_folds_only_integer_totals_at_a_total_clock(
        #[case] op: AggregationOperator,
        #[case] accumulated: DataType,
        #[case] total_clock: bool,
        #[case] expected: Answers,
    ) {
        assert_eq!(
            answers(op, Mutability::Mutable, &accumulated, total_clock),
            expected
        );
    }

    /// A mutable total folds deleted matches out only through the abelian
    /// entry point, as does one filtered after its reduce; a float total
    /// reads its group again through the other.
    // Cases: total type, filter on the answer, abelian.
    #[rstest]
    #[case::integer("int32", "", true)]
    #[case::integer_filtered("int32", ", t > 3", true)]
    #[case::float("f64", "", false)]
    fn a_mutable_total_calls_the_entry_point_of_its_path(
        #[case] ty: &str,
        #[case] filter: &str,
        #[case] abelian: bool,
    ) {
        let mut codegen = codegen(&format!(
            ".decl Sale(n: int32, a: {ty}) mutable\n.input Sale\n\
             .decl Total(t: {ty})\nTotal(t) :- t = sum a : {{ Sale(_, a) }}{filter}.\n\
             .output Total\n"
        ));
        let planner = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let dataflow = codegen
            .generate(&planner, &mut None)
            .expect("generates")
            .dataflow
            .to_string();
        let calls = |entry: &str| {
            dataflow
                .matches(&format!(":: flowlog_runtime :: operators :: {entry} ("))
                .count()
        };
        assert_eq!(
            (calls("flowlog_aggregate"), calls("flowlog_aggregate_abelian")),
            if abelian { (0, 1) } else { (1, 0) },
            "{dataflow}"
        );
    }

    // Cases: target, column index.
    #[rstest]
    #[case(None, None)]
    #[case(Some(TransformationArgument::KV((false, 1))), Some(1))]
    fn target_column_selects_a_match_value(
        #[case] target: Option<TransformationArgument>,
        #[case] expected: Option<usize>,
    ) {
        let values = [DataType::Int32, DataType::Float64];
        assert_eq!(target_column(target, &values).unwrap(), expected);
    }

    /// Parsed programs cannot produce these malformed planner targets,
    /// so exercise the validation boundary directly.
    // Cases: target.
    #[rstest]
    #[case::in_the_key(TransformationArgument::KV((true, 0)))]
    #[case::past_the_values(TransformationArgument::KV((false, 1)))]
    #[case::join_target(TransformationArgument::Jn((false, false, 0)))]
    fn a_target_off_the_values_is_an_internal_error(#[case] target: TransformationArgument) {
        let error = target_column(Some(target), &[DataType::Int64]).expect_err("no value column");
        assert!(matches!(error, CodegenError::Internal(_)));
    }
}
