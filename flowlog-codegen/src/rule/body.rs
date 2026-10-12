//! Rule bodies: [`Codegen::gen_transformation`] lowers one planner
//! transformation to the operator that computes it (a row map or filter, a
//! key-value map, a join, or an antijoin) and, when its output is keyed, the
//! arrangement a later join reads. The closures' pieces come from
//! [`expr`](crate::expr).

use std::collections::HashMap;

use flowlog_planner::planner::ArithmeticArgument;
use flowlog_planner::planner::Collection;
use flowlog_planner::planner::FactorArgument;
use flowlog_planner::planner::StratumPlanner;
use flowlog_planner::planner::Transformation;
use flowlog_planner::planner::TransformationArgument;
use flowlog_planner::planner::TransformationFlow;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::Span;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;
use syn::LitStr;

use crate::Codegen;
use crate::CodegenError;
use crate::expr::constraint::kv_constraint_predicate;
use crate::expr::constraint::row_constraint_predicate;
use crate::expr::param::join_params;
use crate::expr::param::kv_params;
use crate::expr::param::row_params;
use crate::ident::find_local_ident;
use crate::internal_tuple_tokens;
use crate::row_is_copy;

impl Codegen {
    /// Returns the operator that computes `transformation`, followed by its
    /// output's arrangement when the output is keyed.
    ///
    /// `local_fp_to_ident` names every collection in scope: the global
    /// bindings outside a loop, the loop's own inside one. `arranged_map`
    /// holds the arrangement of every keyed collection built so far; a join
    /// reads its inputs from it and a keyed output is added to it. Also
    /// records the output's types and mutability by fingerprint, and its
    /// profiler node.
    pub(crate) fn gen_transformation(
        &mut self,
        local_fp_to_ident: &HashMap<u64, Ident>,
        transformation: &Transformation,
        arranged_map: &mut HashMap<u64, Ident>,
        stratum: &StratumPlanner,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let recursive = stratum.is_recursive_transformation(transformation);

        // `atom_fps` decides *which* inputs are named atoms; the label
        // text comes from `display_name` (the user's spelling).
        let atom_fps = stratum.atom_fps();
        let edb_names = transformation
            .input_fingerprints()
            .into_iter()
            .filter(|fp| atom_fps.contains(fp))
            .map(|fp| self.display_name(fp))
            .collect::<Vec<_>>();
        let edb_suffix = if edb_names.is_empty() {
            String::new()
        } else {
            format!(" <- {}", edb_names.join(", "))
        };
        let transformation_name = format!(
            "{}: {}{}",
            transformation.profile_operation_name(),
            transformation.flow(),
            edb_suffix,
        );
        let operator_name = LitStr::new(&transformation_name, Span::call_site());
        // The arrangement is named after the collection it holds, which
        // other steps share by fingerprint, not after the step producing it.
        let arrange_name = format!("Arrange: {}", transformation.output());
        let si = self.config.str_intern_enabled();

        // Cache the planner's value for this collection by fingerprint:
        // unions and outputs downstream know their parts only by fingerprint.
        let output_fp = transformation.output().fingerprint();
        self.global_fp_to_mutability
            .insert(output_fp, transformation.output().mutability());
        let inputs = transformation.input_fingerprints();
        self.record_output_type(
            inputs[0],
            inputs.get(1).copied(),
            output_fp,
            transformation.flow(),
        )?;

        match transformation {
            Transformation::RowToRow {
                input,
                output,
                flow,
            } => {
                let inp = find_local_ident(local_fp_to_ident, input.fingerprint());
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                let input_arity = input.arity().1;
                let input_type = self.find_global_type(input.fingerprint())?.clone();
                let itype = input_type.1.clone();
                let row_ty = internal_tuple_tokens(&itype, si);

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
                let row_copy = row_is_copy(&itype, si);
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
                let cmp_pred =
                    self.row_compare_predicate(flow.compares(), &row_fields, si, &input_type)?;
                let cst_pred = row_constraint_predicate(flow.constraints(), &row_fields, si)?;
                let pred = combine_predicates(vec![cmp_pred, cst_pred]);

                with_plan_graph(plan_graph, |plan_graph| {
                    if is_alias {
                        // Copy rule `B :- A`: no operator is emitted, but still
                        // register a 0-op alias node so downstream references to
                        // this relation's fingerprint resolve in the profiler model.
                        plan_graph.identity_alias_operator(
                            transformation_name,
                            vec![inp.to_string()],
                            out.to_string(),
                            output.fingerprint(),
                        );
                    } else {
                        plan_graph.map_join_operator(
                            transformation_name,
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
                            #operator_name,
                            |&#row_pat: &#row_ty, _, _| #p,
                        );
                    }),
                    None if type_preserving => Ok(quote! {
                        let #out = ::flowlog_runtime::operators::flowlog_map_in_place(
                            #inp.clone(),
                            #operator_name,
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
                                #operator_name,
                                |#row_pat: #row_ty, t, d| { #body },
                            );
                        })
                    }
                }
            }

            Transformation::RowToKv {
                input,
                output,
                flow,
            } => {
                let inp = find_local_ident(local_fp_to_ident, input.fingerprint());
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                let input_arity = input.arity().1;
                let (row_pat, row_fields) = row_params(
                    input_arity,
                    flow.key(),
                    flow.value(),
                    flow.compares(),
                    flow.constraints(),
                );
                let input_type = self.find_global_type(input.fingerprint())?.clone();
                let itype = input_type.1.clone();

                let row_ty = internal_tuple_tokens(&itype, si);
                let out_expr = keyed_output(
                    output,
                    self.row_projection(flow.key(), &row_fields, si, &input_type)?,
                    self.row_projection(flow.value(), &row_fields, si, &input_type)?,
                );
                let cmp_pred =
                    self.row_compare_predicate(flow.compares(), &row_fields, si, &input_type)?;
                let cst_pred = row_constraint_predicate(flow.constraints(), &row_fields, si)?;
                let pred = combine_predicates(vec![cmp_pred, cst_pred]);

                // An identity projection into a key-only arrangement aliases
                // the input, which the arrangement then reads directly.
                let is_identity = pred.is_none()
                    && output.is_k_only()
                    && flow.value().is_empty()
                    && is_identity_row_projection(flow.key(), input_arity);

                let needs_dedup =
                    projection_loses_columns(flow.key(), flow.value(), input.arity(), true);

                with_plan_graph(plan_graph, |plan_graph| {
                    let name = transformation_name;
                    let inputs = vec![inp.to_string()];
                    let arr = format!("{}_arr", out);
                    let fp = output.fingerprint();
                    if is_identity {
                        plan_graph.arrange_operator(name, inputs, arr, fp, output.is_k_only());
                    } else {
                        plan_graph.map_join_arrange_operator(
                            name,
                            inputs,
                            arr,
                            fp,
                            output.is_k_only(),
                            needs_dedup.then_some((output.mutability(), recursive)),
                        );
                    }
                });

                let transformation = if is_identity {
                    quote! { let #out = #inp.clone(); }
                } else {
                    let body = flat_map_body_tokens(pred, out_expr);
                    quote! {
                        let #out = ::flowlog_runtime::operators::flowlog_map(
                            #inp.clone(),
                            #operator_name,
                            |#row_pat: #row_ty, t, d| { #body },
                        );
                    }
                };
                let projection_dedup = needs_dedup.then(|| {
                    quote! {
                        let #out = ::flowlog_runtime::operators::flowlog_dedup(#out);
                    }
                });
                let arrange_stmt = register_arrangement(arranged_map, output, &out, &arrange_name);
                Ok(quote! {
                    #transformation
                    #projection_dedup
                    #arrange_stmt
                })
            }

            Transformation::KvToRow {
                input,
                output,
                flow,
            } => {
                let inp = find_local_ident(local_fp_to_ident, input.fingerprint());
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                with_plan_graph(plan_graph, |plan_graph| {
                    plan_graph.map_join_operator(
                        transformation_name,
                        vec![inp.to_string()],
                        out.to_string(),
                        output.fingerprint(),
                    );
                });

                let input_type = self.find_global_type(input.fingerprint())?.clone();
                let out_val = self.kv_projection(flow.value(), si, &input_type)?;
                let cmp_pred = self.kv_compare_predicate(flow.compares(), si, &input_type)?;
                let cst_pred = kv_constraint_predicate(flow.constraints(), si)?;
                let pred = combine_predicates(vec![cmp_pred, cst_pred]);
                let closure_param = kv_closure_param(input, flow);
                let body = flat_map_body_tokens(pred, out_val);

                Ok(quote! {
                    let #out = ::flowlog_runtime::operators::flowlog_map(
                        #inp.clone(),
                        #operator_name,
                        #closure_param { #body },
                    );
                })
            }

            Transformation::KvToKv {
                input,
                output,
                flow,
            } => {
                let inp = find_local_ident(local_fp_to_ident, input.fingerprint());
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                let input_type = self.find_global_type(input.fingerprint())?.clone();
                let out_expr = keyed_output(
                    output,
                    self.kv_projection(flow.key(), si, &input_type)?,
                    self.kv_projection(flow.value(), si, &input_type)?,
                );
                let cmp_pred = self.kv_compare_predicate(flow.compares(), si, &input_type)?;
                let cst_pred = kv_constraint_predicate(flow.constraints(), si)?;
                let pred = combine_predicates(vec![cmp_pred, cst_pred]);

                let needs_dedup =
                    projection_loses_columns(flow.key(), flow.value(), input.arity(), false);

                with_plan_graph(plan_graph, |plan_graph| {
                    plan_graph.map_join_arrange_operator(
                        transformation_name,
                        vec![inp.to_string()],
                        format!("{}_arr", out),
                        output.fingerprint(),
                        output.is_k_only(),
                        needs_dedup.then_some((output.mutability(), recursive)),
                    );
                });

                let closure_param = kv_closure_param(input, flow);
                let body = flat_map_body_tokens(pred, out_expr);
                let projection_dedup = needs_dedup.then(|| {
                    quote! {
                        let #out = ::flowlog_runtime::operators::flowlog_dedup(#out);
                    }
                });
                let arrange_stmt = register_arrangement(arranged_map, output, &out, &arrange_name);
                Ok(quote! {
                    let #out = ::flowlog_runtime::operators::flowlog_map(
                        #inp.clone(),
                        #operator_name,
                        #closure_param { #body },
                    );
                    #projection_dedup
                    #arrange_stmt
                })
            }

            Transformation::JnToRow {
                input: (left, right),
                output,
                flow,
            } => {
                let (l, r) = arranged_inputs(local_fp_to_ident, arranged_map, left, right)?;
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                with_plan_graph(plan_graph, |plan_graph| {
                    plan_graph.map_join_operator(
                        transformation_name,
                        vec![l.to_string(), r.to_string()],
                        out.to_string(),
                        output.fingerprint(),
                    );
                });

                let (jn_k, jn_lv, jn_rv) = join_params(flow.key(), flow.value(), flow.compares());
                let left_type = self.find_global_type(left.fingerprint())?.clone();
                let right_type = self.find_global_type(right.fingerprint())?.clone();
                let out_val = self.join_projection(flow.value(), si, &left_type, &right_type)?;
                let cmp_pred =
                    self.join_compare_predicate(flow.compares(), si, &left_type, &right_type)?;
                let join_body = join_body_tokens(cmp_pred, out_val);

                Ok(quote! {
                    let #out = ::flowlog_runtime::operators::flowlog_join(
                        #l.clone(),
                        #r.clone(),
                        #operator_name,
                        |#jn_k, #jn_lv, #jn_rv| { #join_body },
                    );
                })
            }

            Transformation::JnToKv {
                input: (left, right),
                output,
                flow,
            } => {
                let (l, r) = arranged_inputs(local_fp_to_ident, arranged_map, left, right)?;
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                with_plan_graph(plan_graph, |plan_graph| {
                    plan_graph.map_join_arrange_operator(
                        transformation_name,
                        vec![l.to_string(), r.to_string()],
                        format!("{}_arr", out),
                        output.fingerprint(),
                        output.is_k_only(),
                        None,
                    );
                });

                let (jn_k, jn_lv, jn_rv) = join_params(flow.key(), flow.value(), flow.compares());
                let left_type = self.find_global_type(left.fingerprint())?.clone();
                let right_type = self.find_global_type(right.fingerprint())?.clone();
                let out_expr = keyed_output(
                    output,
                    self.join_projection(flow.key(), si, &left_type, &right_type)?,
                    self.join_projection(flow.value(), si, &left_type, &right_type)?,
                );
                let cmp_pred =
                    self.join_compare_predicate(flow.compares(), si, &left_type, &right_type)?;
                let join_body = join_body_tokens(cmp_pred, out_expr);

                let arrange_stmt = register_arrangement(arranged_map, output, &out, &arrange_name);
                Ok(quote! {
                    let #out = ::flowlog_runtime::operators::flowlog_join(
                        #l.clone(),
                        #r.clone(),
                        #operator_name,
                        |#jn_k, #jn_lv, #jn_rv| { #join_body },
                    );
                    #arrange_stmt
                })
            }

            // A key-value source minus the keys of a key-only filter, to a row.
            Transformation::NJnToRow {
                input: (left, right),
                output,
                flow,
            } => {
                let (l, r) = arranged_inputs(local_fp_to_ident, arranged_map, left, right)?;
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                with_plan_graph(plan_graph, |plan_graph| {
                    plan_graph.anti_join_operator(
                        transformation_name,
                        vec![l.to_string(), r.to_string()],
                        out.to_string(),
                        output.fingerprint(),
                        left.mutability(),
                        right.mutability(),
                        recursive,
                    );
                });

                let (anti_param_k, anti_param_v) =
                    kv_params(flow.key(), flow.value(), flow.compares(), None);
                // The closure sees the surviving right side's `(k, v)`; the
                // left side only filters by key.
                let input_type = self.find_global_type(right.fingerprint())?.clone();
                let out_map_value = self.kv_projection(flow.value(), si, &input_type)?;
                Ok(quote! {
                    let #out = ::flowlog_runtime::operators::flowlog_antijoin(
                        #l.clone(),
                        #r.clone(),
                        #operator_name,
                        |( #anti_param_k, #anti_param_v )| #out_map_value,
                    );
                })
            }

            // A key-only source minus the keys of a key-only filter, to a
            // keyed output.
            Transformation::NJnToKv {
                input: (left, right),
                output,
                flow,
            } => {
                let (l, r) = arranged_inputs(local_fp_to_ident, arranged_map, left, right)?;
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                with_plan_graph(plan_graph, |plan_graph| {
                    plan_graph.anti_join_arrange_operator(
                        transformation_name,
                        vec![l.to_string(), r.to_string()],
                        format!("{}_arr", out),
                        output.fingerprint(),
                        output.is_k_only(),
                        left.mutability(),
                        right.mutability(),
                        recursive,
                    );
                });

                let (anti_param_k, anti_param_v) =
                    kv_params(flow.key(), flow.value(), flow.compares(), None);
                // The closure sees the surviving right side's `(k, v)`; the
                // left side only filters by key.
                let input_type = self.find_global_type(right.fingerprint())?.clone();
                let out_map_expr = keyed_output(
                    output,
                    self.kv_projection(flow.key(), si, &input_type)?,
                    self.kv_projection(flow.value(), si, &input_type)?,
                );
                let arrange_stmt = register_arrangement(arranged_map, output, &out, &arrange_name);
                Ok(quote! {
                    let #out = ::flowlog_runtime::operators::flowlog_antijoin(
                        #l.clone(),
                        #r.clone(),
                        #operator_name,
                        |( #anti_param_k, #anti_param_v )| #out_map_expr,
                    );
                    #arrange_stmt
                })
            }
        }
    }
}

// =============================================================================
// Arrangements
// =============================================================================

/// Returns the statement arranging `output`, bound as `<collection>_arr`
/// under `name`: by itself when it is key-only, else by key. Records the
/// arrangement in `arranged_map` for the joins that read it.
fn register_arrangement(
    arranged_map: &mut HashMap<u64, Ident>,
    output: &Collection,
    collection: &Ident,
    name: &str,
) -> TokenStream {
    let arrangement = format_ident!("{}_arr", collection);
    arranged_map.insert(output.fingerprint(), arrangement.clone());
    let arrange = if output.is_k_only() {
        quote! { flowlog_arrange_self }
    } else {
        quote! { flowlog_arrange }
    };
    quote! {
        let #arrangement = ::flowlog_runtime::operators::#arrange(#collection.clone(), #name);
    }
}

/// Returns the arrangements of a join's `left` and `right` inputs, or an
/// internal error when either is not arranged yet: the planner orders every
/// arrangement before the joins that read it.
fn arranged_inputs(
    local_fp_to_ident: &HashMap<u64, Ident>,
    arranged_map: &HashMap<u64, Ident>,
    left: &Collection,
    right: &Collection,
) -> Result<(Ident, Ident), CodegenError> {
    let arranged = |input: &Collection| {
        let fingerprint = input.fingerprint();
        arranged_map.get(&fingerprint).cloned().ok_or_else(|| {
            let base = find_local_ident(local_fp_to_ident, fingerprint);
            CodegenError::internal(format!(
                "collection `{base}` (fingerprint 0x{fingerprint:016x}) \
                 must be arranged before use"
            ))
        })
    };
    Ok((arranged(left)?, arranged(right)?))
}

// =============================================================================
// Closure pieces
// =============================================================================

/// Returns a keyed output's tuple: the key alone when `output` is key-only,
/// else `(key, value)`.
fn keyed_output(output: &Collection, key: TokenStream, value: TokenStream) -> TokenStream {
    if output.is_k_only() {
        quote! { #key }
    } else {
        quote! { ( #key, #value ) }
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
fn flat_map_body_tokens(pred: Option<TokenStream>, out: TokenStream) -> TokenStream {
    match pred {
        Some(pred) => quote! { if #pred { Some(( #out, t, d )) } else { None } },
        None => quote! { std::iter::once(( #out, t, d )) },
    }
}

/// Returns the body of a join closure that yields `out`, only when `pred`
/// holds if there is one. With no predicate the body is a bare `Some`, as
/// the join operator expects an `Option`, not an iterator.
fn join_body_tokens(pred: Option<TokenStream>, out: TokenStream) -> TokenStream {
    match pred {
        Some(pred) => quote! { if #pred { Some( #out ) } else { None } },
        None => quote! { Some( #out ) },
    }
}

/// Returns a step's filter: its comparison and constraint predicates joined
/// with `&&`, or `None` when it has neither.
fn combine_predicates(preds: Vec<Option<TokenStream>>) -> Option<TokenStream> {
    preds
        .into_iter()
        .flatten()
        .reduce(|a, b| quote! { (#a) && (#b) })
}

/// Returns `true` if an input column is not preserved as a bare output column.
/// Reordering or duplicating retained columns does not require normalization.
fn projection_loses_columns(
    key: &[ArithmeticArgument],
    value: &[ArithmeticArgument],
    input_arity: (usize, usize),
    row_input: bool,
) -> bool {
    let preserves = |is_key, index| {
        key.iter().chain(value).any(|arg| {
            arg.rest().is_empty()
                && matches!(
                    arg.init(),
                    FactorArgument::Var(TransformationArgument::KV((k, i)))
                        if *i == index && (row_input || *k == is_key)
                )
        })
    };
    (0..input_arity.0).any(|i| !preserves(true, i))
        || (0..input_arity.1).any(|i| !preserves(false, i))
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

    /// The codegen-only predicate is private; these cases pin its column contract.
    #[rstest]
    #[case(vec![col(false, 0), col(false, 1)], vec![], (0, 3), true, true)]
    #[case(vec![col(false, 0), col(false, 1)], vec![], (0, 2), true, false)]
    #[case(vec![col(false, 1), col(false, 0)], vec![], (0, 2), true, false)]
    #[case(vec![col(false, 0), col(false, 0)], vec![], (0, 2), true, true)]
    #[case(vec![col(true, 0)], vec![], (1, 1), false, true)]
    #[case(vec![col(false, 0)], vec![col(true, 0)], (1, 1), false, false)]
    #[case(vec![col(true, 0)], vec![col(false, 0)], (1, 2), false, true)]
    #[case(vec![], vec![], (0, 0), true, false)]
    fn projection_normalization_tracks_lost_columns(
        #[case] key: Vec<ArithmeticArgument>,
        #[case] value: Vec<ArithmeticArgument>,
        #[case] arity: (usize, usize),
        #[case] row: bool,
        #[case] expected: bool,
    ) {
        assert_eq!(projection_loses_columns(&key, &value, arity, row), expected);
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

    #[test]
    fn predicates_join_with_and_skipping_absent_ones() {
        let joined = combine_predicates(vec![Some(quote! { a }), None, Some(quote! { b })]);
        assert_eq!(
            joined.map(|p| p.to_string()),
            Some(quote! { (a) && (b) }.to_string())
        );
        assert!(combine_predicates(vec![None, None]).is_none());
    }
}
