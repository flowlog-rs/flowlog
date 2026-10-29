//! Maps: the operators over one collection, a filter, a projection, an
//! arrangement of rows by key, or a key-value map, and the pieces only
//! they assemble. A map's output is rows or keyed as its [`Operator`]
//! says; a keyed output is arranged for the joins that read it.

use std::collections::HashMap;

use flowlog_planner::planner::ArithmeticArgument;
use flowlog_planner::planner::Collection;
use flowlog_planner::planner::FactorArgument;
use flowlog_planner::planner::TransformationArgument;
use flowlog_planner::planner::TransformationFlow;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;

use super::Operator;
use super::keyed_output;
use super::register_arrangement;
use crate::Codegen;
use crate::CodegenError;
use crate::expr::constraint::kv_constraint_predicate;
use crate::expr::constraint::row_constraint_predicate;
use crate::expr::param::kv_params;
use crate::expr::param::row_params;
use crate::ident::find_local_ident;
use crate::internal_tuple_tokens;
use crate::row_is_copy;
use crate::ty::data::KvTypes;

impl Codegen {
    /// Returns the operator filtering and projecting the rows of `input` into
    /// `output` by `flow`: an alias, a filter or a rewrite in place when the
    /// flow allows, else a map.
    pub(super) fn gen_row_to_row(
        &mut self,
        operator: Operator<'_>,
        input: &Collection,
        output: &Collection,
        flow: &TransformationFlow,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let inp = find_local_ident(operator.local_fp_to_ident, input.fingerprint());
        let out = find_local_ident(operator.local_fp_to_ident, output.fingerprint());
        let Operator {
            name, literal, si, ..
        } = operator;

        let input_arity = input.arity().1;
        let input_type = self.find_global_type(input.fingerprint())?.clone();
        let row_types = input_type.1.clone();
        let row_ty = internal_tuple_tokens(&row_types, si);

        // The cheapest operator that fits, in-place forms first: an
        // identity projection with no predicate aliases the input
        // (no operator); an identity with a predicate filters the
        // input's rows in place; a rewrite that keeps every column's
        // type overwrites the rows in place; anything else rebuilds
        // each row with a map. In-place forms need a `Copy` row, as
        // fields are copied out through `*row`; the in-place rewrite
        // also needs the projection to keep every column's type, so
        // `*row = <projection>` typechecks.
        let identity_projection = is_identity_row_projection(flow.value(), input_arity);
        let has_predicate = !flow.compares().is_empty() || !flow.constraints().is_empty();
        let row_copy = row_is_copy(&row_types, si);
        let is_alias = identity_projection && !has_predicate;
        let is_filter = identity_projection && has_predicate && row_copy;
        let type_preserving = !identity_projection
            && row_copy
            && self.row_projection_preserves_type(flow.value(), &input_type)?;

        // The row closure names only the columns the chosen form
        // reads; an alias or a filter emits no projection.
        let projected: &[ArithmeticArgument] = if is_alias || is_filter {
            &[]
        } else {
            flow.value()
        };
        let (row_pat, row_fields) = row_params(
            input_arity,
            flow.key(),
            projected,
            flow.compares(),
            flow.constraints(),
        );
        let out_val = self.row_projection(flow.value(), &row_fields, si, &input_type)?;
        let pred = self.row_predicate(flow, &row_fields, si, &input_type)?;

        with_plan_graph(plan_graph, |plan_graph| {
            if is_alias {
                // An arrangement sharing held as rows, with every
                // column kept: no operator is emitted, but a 0-op
                // alias node still lets downstream references to
                // this fingerprint resolve in the profiler model.
                plan_graph.identity_alias_operator(
                    name,
                    vec![inp.to_string()],
                    out.to_string(),
                    output.fingerprint(),
                );
            } else {
                plan_graph.map_join_operator(
                    name,
                    vec![inp.to_string()],
                    out.to_string(),
                    output.fingerprint(),
                );
            }
        });

        match pred {
            None if is_alias => Ok(quote! { let #out = #inp.clone(); }),
            Some(p) if is_filter => Ok(quote! {
                let #out = ::flowlog_runtime::operators::flowlog_filter(
                    #inp.clone(),
                    #literal,
                    |&#row_pat: &#row_ty, _, _| #p,
                );
            }),
            None if type_preserving => Ok(quote! {
                let #out = ::flowlog_runtime::operators::flowlog_map_in_place(
                    #inp.clone(),
                    #literal,
                    |row: &mut #row_ty, _, _| {
                        let #row_pat = *row;
                        *row = #out_val;
                    },
                );
            }),
            pred => {
                let body = flat_map_body_tokens(pred, out_val);
                Ok(quote! {
                    let #out = ::flowlog_runtime::operators::flowlog_map(
                        #inp.clone(),
                        #literal,
                        |#row_pat: #row_ty, t, d| { #body },
                    );
                })
            }
        }
    }

    /// Returns the operator arranging the rows of `input` into the keyed
    /// `output` by `flow`, and the arrangement.
    pub(super) fn gen_row_to_kv(
        &mut self,
        operator: Operator<'_>,
        input: &Collection,
        output: &Collection,
        flow: &TransformationFlow,
        arranged_map: &mut HashMap<u64, Ident>,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let inp = find_local_ident(operator.local_fp_to_ident, input.fingerprint());
        let out = find_local_ident(operator.local_fp_to_ident, output.fingerprint());
        let Operator {
            name,
            literal,
            arrange,
            si,
            ..
        } = operator;

        let input_arity = input.arity().1;
        let (row_pat, row_fields) = row_params(
            input_arity,
            flow.key(),
            flow.value(),
            flow.compares(),
            flow.constraints(),
        );
        let input_type = self.find_global_type(input.fingerprint())?.clone();
        let row_ty = internal_tuple_tokens(&input_type.1, si);
        let out_expr = keyed_output(
            output,
            self.row_projection(flow.key(), &row_fields, si, &input_type)?,
            self.row_projection(flow.value(), &row_fields, si, &input_type)?,
        );
        let pred = self.row_predicate(flow, &row_fields, si, &input_type)?;

        // An identity projection into a key-only arrangement aliases
        // the input, which the arrangement then reads directly.
        let is_identity = pred.is_none()
            && output.is_k_only()
            && flow.value().is_empty()
            && is_identity_row_projection(flow.key(), input_arity);

        with_plan_graph(plan_graph, |plan_graph| {
            let inputs = vec![inp.to_string()];
            let arr = format!("{}_arr", out);
            let fp = output.fingerprint();
            if is_identity {
                plan_graph.arrange_operator(name, inputs, arr, fp, output.is_k_only());
            } else {
                plan_graph.map_join_arrange_operator(name, inputs, arr, fp, output.is_k_only());
            }
        });

        let map_stmt = if is_identity {
            quote! { let #out = #inp.clone(); }
        } else {
            let body = flat_map_body_tokens(pred, out_expr);
            quote! {
                let #out = ::flowlog_runtime::operators::flowlog_map(
                    #inp.clone(),
                    #literal,
                    |#row_pat: #row_ty, t, d| { #body },
                );
            }
        };
        let arrange_stmt = register_arrangement(arranged_map, output, &out, arrange.as_deref());
        Ok(quote! {
            #map_stmt
            #arrange_stmt
        })
    }

    /// Returns the operator mapping the keyed `input` into `output` by
    /// `flow`, as rows or keyed as `operator` says, and the arrangement of
    /// a keyed output.
    pub(super) fn gen_kv_map(
        &mut self,
        operator: Operator<'_>,
        input: &Collection,
        output: &Collection,
        flow: &TransformationFlow,
        arranged_map: &mut HashMap<u64, Ident>,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let inp = find_local_ident(operator.local_fp_to_ident, input.fingerprint());
        let out = find_local_ident(operator.local_fp_to_ident, output.fingerprint());
        let Operator {
            name,
            literal,
            arrange,
            si,
            ..
        } = operator;

        let input_type = self.find_global_type(input.fingerprint())?.clone();
        let value = self.kv_projection(flow.value(), si, &input_type)?;
        let out_expr = if arrange.is_some() {
            keyed_output(
                output,
                self.kv_projection(flow.key(), si, &input_type)?,
                value,
            )
        } else {
            value
        };
        let pred = self.kv_predicate(flow, si, &input_type)?;

        with_plan_graph(plan_graph, |plan_graph| {
            let inputs = vec![inp.to_string()];
            let fp = output.fingerprint();
            if arrange.is_some() {
                let arr = format!("{}_arr", out);
                plan_graph.map_join_arrange_operator(name, inputs, arr, fp, output.is_k_only());
            } else {
                plan_graph.map_join_operator(name, inputs, out.to_string(), fp);
            }
        });

        let closure_param = kv_closure_param(input, flow);
        let body = flat_map_body_tokens(pred, out_expr);
        let arrange_stmt = register_arrangement(arranged_map, output, &out, arrange.as_deref());
        Ok(quote! {
            let #out = ::flowlog_runtime::operators::flowlog_map(
                #inp.clone(),
                #literal,
                #closure_param { #body },
            );
            #arrange_stmt
        })
    }

    /// Returns a step's filter over rows, `fields` naming the columns
    /// `flow` reads: its constraints and comparisons joined with `&&`, or
    /// `None` when it has neither.
    fn row_predicate(
        &mut self,
        flow: &TransformationFlow,
        fields: &[Ident],
        si: bool,
        input_type: &KvTypes,
    ) -> Result<Option<TokenStream>, CodegenError> {
        let constraint = row_constraint_predicate(flow.constraints(), fields, si)?;
        let compare = self.row_compare_predicate(flow.compares(), fields, si, input_type)?;
        Ok(combine_predicates(constraint, compare))
    }

    /// Returns a step's filter over key-value pairs: its constraints and
    /// comparisons joined with `&&`, or `None` when it has neither.
    fn kv_predicate(
        &mut self,
        flow: &TransformationFlow,
        si: bool,
        input_type: &KvTypes,
    ) -> Result<Option<TokenStream>, CodegenError> {
        let constraint = kv_constraint_predicate(flow.constraints(), si)?;
        let compare = self.kv_compare_predicate(flow.compares(), si, input_type)?;
        Ok(combine_predicates(constraint, compare))
    }
}

/// Returns the parameter list of a map over the key-value `input`: the key
/// alone when the input is key-only, else `(key, value)`, then the time and
/// weight.
fn kv_closure_param(input: &Collection, flow: &TransformationFlow) -> TokenStream {
    let (k, v) = kv_params(
        flow.key(),
        flow.value(),
        flow.compares(),
        Some(flow.constraints()),
    );
    if input.is_k_only() {
        quote! { |#k, t, d| }
    } else {
        quote! { |( #k, #v ), t, d| }
    }
}

/// Returns the body of a `flowlog_map` closure that yields `out`, only when
/// `pred` holds if there is one.
///
/// A projection rewrites the row only, so it hands back the `t` and `d` its
/// closure was given: the row keeps the time and weight it arrived with.
/// With no predicate the body is `std::iter::once`, as the operator expects
/// an iterator.
pub(super) fn flat_map_body_tokens(pred: Option<TokenStream>, out: TokenStream) -> TokenStream {
    match pred {
        Some(pred) => quote! { if #pred { Some(( #out, t, d )) } else { None } },
        None => quote! { std::iter::once(( #out, t, d )) },
    }
}

/// Returns the predicates joined as `constraint && compare`, or the one
/// there is, or `None` when there is neither. The constraint goes first so
/// that `&&` skips a comparison such as `a / b > 1` on a row the atom's
/// constants rule out, where `b` may be zero.
fn combine_predicates(
    constraint: Option<TokenStream>,
    compare: Option<TokenStream>,
) -> Option<TokenStream> {
    match (constraint, compare) {
        (Some(constraint), Some(compare)) => Some(quote! { (#constraint) && (#compare) }),
        (Some(one), None) | (None, Some(one)) => Some(one),
        (None, None) => None,
    }
}

/// Returns `true` if `args` reproduce each of the input row's `row_arity`
/// columns once, in order, as a bare variable: no arithmetic, cast,
/// constant, reordering, or dropped or added column.
fn is_identity_row_projection(args: &[ArithmeticArgument], row_arity: usize) -> bool {
    args.len() == row_arity
        && args.iter().enumerate().all(|(idx, arg)| {
            arg.rest().is_empty()
                && matches!(
                    arg.init(),
                    FactorArgument::Var(TransformationArgument::KV((_, i))) if *i == idx,
                )
        })
}

#[cfg(test)]
mod tests {
    use flowlog_parser::ArithmeticOperator;
    use rstest::rstest;

    use super::*;

    /// A bare row column `KV((is_key, idx))`.
    fn col(is_key: bool, idx: usize) -> ArithmeticArgument {
        ArithmeticArgument {
            init: FactorArgument::Var(TransformationArgument::KV((is_key, idx))),
            rest: Vec::new(),
        }
    }

    /// A row column is addressed by position, so its key flag never matters.
    // Cases: projected columns, row arity, identity.
    #[rstest]
    #[case::every_column_in_order(vec![col(false, 0), col(false, 1), col(false, 2)], 3, true)]
    #[case::one_column(vec![col(false, 0)], 1, true)]
    #[case::no_column(vec![], 0, true)]
    #[case::key_flag_ignored(vec![col(true, 0), col(false, 1)], 2, true)]
    #[case::reordered(vec![col(false, 1), col(false, 0)], 2, false)]
    #[case::duplicated(vec![col(false, 0), col(false, 0)], 2, false)]
    #[case::fewer_columns(vec![col(false, 0)], 2, false)]
    #[case::more_columns(vec![col(false, 0), col(false, 1)], 1, false)]
    fn an_identity_projection_reproduces_every_column_in_order(
        #[case] args: Vec<ArithmeticArgument>,
        #[case] row_arity: usize,
        #[case] expected: bool,
    ) {
        assert_eq!(is_identity_row_projection(&args, row_arity), expected);
    }

    #[test]
    fn an_arithmetic_column_is_not_identity() {
        let sum = ArithmeticArgument {
            init: FactorArgument::Var(TransformationArgument::KV((false, 0))),
            rest: vec![(
                ArithmeticOperator::Plus,
                FactorArgument::Var(TransformationArgument::KV((false, 1))),
            )],
        };
        assert!(!is_identity_row_projection(&[sum], 1));
    }

    /// Two predicates join with `&&`, the constraint first; one stands
    /// alone, and none is none.
    // Cases: constraint, compare, joined.
    #[rstest]
    #[case::both(Some(quote! { b }), Some(quote! { a }), Some("(b) && (a)"))]
    #[case::constraint_only(Some(quote! { b }), None, Some("b"))]
    #[case::compare_only(None, Some(quote! { a }), Some("a"))]
    #[case::neither(None, None, None)]
    fn predicates_join_with_and_skipping_absent_ones(
        #[case] constraint: Option<TokenStream>,
        #[case] compare: Option<TokenStream>,
        #[case] joined: Option<&str>,
    ) {
        assert_eq!(
            combine_predicates(constraint, compare).map(|p| p.to_string()),
            joined.map(str::to_string)
        );
    }
}
