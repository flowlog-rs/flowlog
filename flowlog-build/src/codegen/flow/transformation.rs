//! Transformation code generation for FlowLog compiler.
//!
//! This module generates Rust code for various data transformations
//! such as Row-to-Row, Row-to-KV, KV-to-KV, KV-to-Row, Joins, and Antijoins
//! in the differential dataflow pipelines.

use std::collections::HashMap;

use flowlog_planner::planner::ArithmeticArgument;
use flowlog_planner::planner::FactorArgument;
use flowlog_planner::planner::StratumPlanner;
use flowlog_planner::planner::Transformation;
use flowlog_planner::planner::TransformationArgument;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::Span;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;
use syn::LitStr;

use crate::codegen::CodeGen;
use crate::codegen::CodegenError;
use crate::codegen::arg::build_kv_constraints_predicate;
use crate::codegen::arg::build_row_constraints_predicate;
use crate::codegen::arg::combine_predicates;
use crate::codegen::arg::compute_join_param_tokens;
use crate::codegen::arg::compute_kv_param_tokens;
use crate::codegen::arg::row_pattern_and_fields;
use crate::codegen::data_type_tokens;
use crate::codegen::ident::find_local_ident;
use crate::codegen::row_is_copy;

impl CodeGen {
    /// Generate differential dataflow pipelines for a single transformation.
    ///
    /// For non-recursive transformations, we use the global fingerprint-to-ident map.
    /// For recursive transformations, we use the local fingerprint-to-ident map.
    /// This function accepts the map as `local_fp_to_ident` to keep the interface unified.
    pub(super) fn gen_transformation(
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
            format!(" ← {}", edb_names.join(", "))
        };
        let transformation_name = format!(
            "{}: {}{}",
            transformation.profile_operation_name(),
            transformation.flow(),
            edb_suffix,
        );
        let operator_name = LitStr::new(&transformation_name, Span::call_site());
        let si = self.features.string_intern();

        match transformation {
            // Row -> Row
            Transformation::RowToRow {
                input,
                output,
                flow,
            } => {
                // Inputs / outputs
                let inp = find_local_ident(local_fp_to_ident, input.fingerprint());
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                // Type inference + row pattern
                let input_arity = input.arity().1;
                let (row_pat, row_fields) = row_pattern_and_fields(
                    input_arity,
                    flow.key(),
                    flow.value(),
                    flow.compares(),
                    flow.constraints(),
                );
                self.record_transformation_output_type(
                    input.fingerprint(),
                    None,
                    output.fingerprint(),
                    flow,
                    stratum,
                )?;
                let input_type = self.find_global_data_type(input.fingerprint())?.clone();
                let itype = input_type.1.clone();

                // Output expression + predicates
                let row_ty = data_type_tokens(&itype, si);
                let out_val = self.build_key_val_from_row_args(flow.value(), &row_fields, si)?;
                let cmp_pred = self.build_row_compare_predicate(
                    flow.compares(),
                    &row_fields,
                    si,
                    &input_type,
                )?;
                let cst_pred =
                    build_row_constraints_predicate(flow.constraints(), &row_fields, si)?;
                let pred = combine_predicates(vec![cmp_pred, cst_pred]);

                // Cheapest operator that fits, in-place forms first:
                //   identity, no predicate → alias        (no operator)
                //   identity + predicate   → filter       (drop rows in the input buffer)
                //   same-typed rewrite     → map_in_place (overwrite them there)
                //   anything else          → flat_map     (rebuild each row)
                // In-place forms need a `Copy` row (fields are copied out via
                // `*row`); `map_in_place` also needs the projection to keep
                // every column's type, so `*row = <projection>` typechecks.
                let key_free = flow.key().is_empty();
                let identity_projection =
                    key_free && is_identity_row_projection(flow.value(), input.arity().1);
                let row_copy = row_is_copy(&itype, si);
                let type_preserving = !identity_projection
                    && key_free
                    && row_copy
                    && self.row_projection_preserves_type(flow.value(), &input_type)?;
                let is_identity = pred.is_none() && identity_projection;

                with_plan_graph(plan_graph, |plan_graph| {
                    if is_identity {
                        // Copy rule `B :- A`: no operator is emitted — but still
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
                    // Identity, no predicate: alias the input.
                    None if identity_projection => Ok(quote! { let #out = #inp.clone(); }),
                    // Identity + predicate: retain surviving rows in place.
                    Some(p) if identity_projection && row_copy => Ok(quote! {
                        let #out = #inp.clone()
                            .filter(|&#row_pat: &#row_ty| #p);
                    }),
                    // Same-typed rewrite, no predicate: overwrite rows in place.
                    None if type_preserving => Ok(quote! {
                        let #out = #inp.clone()
                            .map_in_place(|row: &mut #row_ty| {
                                let #row_pat = *row;
                                *row = #out_val;
                            });
                    }),
                    // General projection: rebuild each row.
                    pred => {
                        let body = flat_map_body_tokens(pred, out_val);
                        Ok(quote! {
                            let #out = ::flowlog_runtime::operators::flowlog_flat_map(
                                #inp.clone(),
                                #operator_name,
                                |#row_pat: #row_ty| { #body },
                            );
                        })
                    }
                }
            }

            // Row -> KV
            Transformation::RowToKv {
                input,
                output,
                flow,
            } => {
                // Inputs / outputs
                let inp = find_local_ident(local_fp_to_ident, input.fingerprint());
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                // Type inference + row pattern
                let input_arity = input.arity().1;
                let (row_pat, row_fields) = row_pattern_and_fields(
                    input_arity,
                    flow.key(),
                    flow.value(),
                    flow.compares(),
                    flow.constraints(),
                );
                self.record_transformation_output_type(
                    input.fingerprint(),
                    None,
                    output.fingerprint(),
                    flow,
                    stratum,
                )?;
                let input_type = self.find_global_data_type(input.fingerprint())?.clone();
                let itype = input_type.1.clone();

                // Output expression + predicates
                let row_ty = data_type_tokens(&itype, si);
                let out_key = self.build_key_val_from_row_args(flow.key(), &row_fields, si)?;
                let out_val = self.build_key_val_from_row_args(flow.value(), &row_fields, si)?;
                let out_expr = if output.is_k_only() {
                    quote! { #out_key }
                } else {
                    quote! { ( #out_key, #out_val ) }
                };

                let cmp_pred = self.build_row_compare_predicate(
                    flow.compares(),
                    &row_fields,
                    si,
                    &input_type,
                )?;
                let cst_pred =
                    build_row_constraints_predicate(flow.constraints(), &row_fields, si)?;
                let pred = combine_predicates(vec![cmp_pred, cst_pred]);

                // Identity projection into a key-only arrangement: alias the input;
                // the `arrange_by_self` below reads it directly, dropping the `flat_map`.
                let is_identity = pred.is_none()
                    && output.is_k_only()
                    && flow.value().is_empty()
                    && is_identity_row_projection(flow.key(), input.arity().1);

                with_plan_graph(plan_graph, |plan_graph| {
                    let name = transformation_name;
                    let inputs = vec![inp.to_string()];
                    let arr = format!("{}_arr", out);
                    let fp = output.fingerprint();
                    // Identity aliases away the `flat_map`; only the arrangement remains.
                    if is_identity {
                        plan_graph.arrange_operator(name, inputs, arr, fp, output.is_k_only());
                    } else {
                        plan_graph.map_join_arrange_operator(
                            name,
                            inputs,
                            arr,
                            fp,
                            output.is_k_only(),
                        );
                    }
                });

                let flat_map_body = flat_map_body_tokens(pred, out_expr);

                let transformation = if is_identity {
                    quote! { let #out = #inp.clone(); }
                } else {
                    quote! {
                        let #out = ::flowlog_runtime::operators::flowlog_flat_map(
                            #inp.clone(),
                            #operator_name,
                            |#row_pat: #row_ty| { #flat_map_body },
                        );
                    }
                };

                // Arrangement registration
                let arrange_stmt = self.register_arrangement(
                    arranged_map,
                    output.fingerprint(),
                    &out,
                    output.is_k_only(),
                );

                Ok(quote! {
                    #transformation
                    #arrange_stmt
                })
            }

            // KV -> Row
            Transformation::KvToRow {
                input,
                output,
                flow,
            } => {
                // Inputs / outputs
                let inp = find_local_ident(local_fp_to_ident, input.fingerprint());
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                // Profiling hook (optional)
                with_plan_graph(plan_graph, |plan_graph| {
                    plan_graph.map_join_operator(
                        transformation_name,
                        vec![inp.to_string()],
                        out.to_string(),
                        output.fingerprint(),
                    );
                });

                // Type inference
                self.record_transformation_output_type(
                    input.fingerprint(),
                    None,
                    output.fingerprint(),
                    flow,
                    stratum,
                )?;

                // Output value + predicates
                let input_type = self.find_global_data_type(input.fingerprint())?.clone();
                let out_val = self.build_key_val_from_kv_args(flow.value(), si)?;
                let cmp_pred = self.build_kv_compare_predicate(flow.compares(), si, &input_type)?;
                let cst_pred = build_kv_constraints_predicate(flow.constraints(), si)?;
                let pred = combine_predicates(vec![cmp_pred, cst_pred]);
                let (kv_param_k, kv_param_v) = compute_kv_param_tokens(
                    flow.key(),
                    flow.value(),
                    flow.compares(),
                    Some(flow.constraints()),
                );

                // Transformation logic
                let flat_map_body = flat_map_body_tokens(pred, out_val);

                Ok(quote! {
                    let #out = ::flowlog_runtime::operators::flowlog_flat_map(
                        #inp.clone(),
                        #operator_name,
                        |( #kv_param_k, #kv_param_v )| { #flat_map_body },
                    );
                })
            }

            // KV -> KV
            Transformation::KvToKv {
                input,
                output,
                flow,
            } => {
                // Inputs / outputs
                let inp = find_local_ident(local_fp_to_ident, input.fingerprint());
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                // Type inference
                self.record_transformation_output_type(
                    input.fingerprint(),
                    None,
                    output.fingerprint(),
                    flow,
                    stratum,
                )?;

                // Output expression + predicates
                let input_type = self.find_global_data_type(input.fingerprint())?.clone();
                let out_key = self.build_key_val_from_kv_args(flow.key(), si)?;
                let out_val = self.build_key_val_from_kv_args(flow.value(), si)?;
                let out_expr = if output.is_k_only() {
                    quote! { #out_key }
                } else {
                    quote! { ( #out_key, #out_val ) }
                };
                let cmp_pred = self.build_kv_compare_predicate(flow.compares(), si, &input_type)?;
                let cst_pred = build_kv_constraints_predicate(flow.constraints(), si)?;
                let pred = combine_predicates(vec![cmp_pred, cst_pred]);

                // One flag drives the emitted dedup and its recording,
                // so the predicted operator count cannot drift.
                let dedups = pred.is_none() && output.is_k_only();

                // Profiling hook (optional), after the predicates so the
                // dedup is known.
                with_plan_graph(plan_graph, |plan_graph| {
                    if dedups {
                        plan_graph.map_dedup_arrange_operator(
                            transformation_name,
                            vec![inp.to_string()],
                            format!("{}_arr", out),
                            output.fingerprint(),
                            output.is_k_only(),
                            recursive,
                        );
                    } else {
                        plan_graph.map_join_arrange_operator(
                            transformation_name,
                            vec![inp.to_string()],
                            format!("{}_arr", out),
                            output.fingerprint(),
                            output.is_k_only(),
                        );
                    }
                });
                let (kv_param_k, kv_param_v) = compute_kv_param_tokens(
                    flow.key(),
                    flow.value(),
                    flow.compares(),
                    Some(flow.constraints()),
                );

                // Closure parameter depends on whether input is key-only
                let closure_param = if input.is_k_only() {
                    quote! { |#kv_param_k| }
                } else {
                    quote! { |( #kv_param_k, #kv_param_v )| }
                };

                // Ideally, in system design, projection (to key) in SIP optimization may introduce duplicates,
                // we have to apply deduplication to avoid incorrect Yannakakis computation bounds.
                // Dedup only applies when there is no predicate (predicate paths already filter).
                let dedup_call = self.dedup_projection(recursive);
                let out_dedup_expr = if dedups {
                    quote! { let #out = #out #dedup_call; }
                } else {
                    quote! {}
                };

                // Flat_map body depends on whether there is a predicate
                let flat_map_body = flat_map_body_tokens(pred, out_expr);

                let transformation = quote! {
                    let #out = ::flowlog_runtime::operators::flowlog_flat_map(
                        #inp.clone(),
                        #operator_name,
                        #closure_param { #flat_map_body },
                    );
                    #out_dedup_expr
                };

                // Arrangement registration
                let arrange_stmt = self.register_arrangement(
                    arranged_map,
                    output.fingerprint(),
                    &out,
                    output.is_k_only(),
                );

                Ok(quote! {
                    #transformation
                    #arrange_stmt
                })
            }

            // Join: Key-value ⋈ Key-value -> Row
            Transformation::JnToRow {
                input,
                output,
                flow,
            } => {
                // Inputs / outputs
                let (left, right) = input;
                let l_base = find_local_ident(local_fp_to_ident, left.fingerprint());
                let r_base = find_local_ident(local_fp_to_ident, right.fingerprint());
                let l = expect_arranged(arranged_map, left.fingerprint(), &l_base)?;
                let r = expect_arranged(arranged_map, right.fingerprint(), &r_base)?;
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                // Profiling hook (optional)
                with_plan_graph(plan_graph, |plan_graph| {
                    plan_graph.map_join_operator(
                        transformation_name,
                        vec![l.to_string(), r.to_string()],
                        out.to_string(),
                        output.fingerprint(),
                    );
                });

                // Type inference
                self.record_transformation_output_type(
                    left.fingerprint(),
                    Some(right.fingerprint()),
                    output.fingerprint(),
                    flow,
                    stratum,
                )?;

                // Output expression + predicates
                let (jn_k, jn_lv, jn_rv) =
                    compute_join_param_tokens(flow.key(), flow.value(), flow.compares());
                let out_val = self.build_key_val_from_join_args(flow.value(), si)?;

                let left_type = self.find_global_data_type(left.fingerprint())?.clone();
                let right_type = self.find_global_data_type(right.fingerprint())?.clone();
                let cmp_pred = self.build_join_compare_predicate(
                    flow.compares(),
                    si,
                    &left_type,
                    &right_type,
                )?;
                let pred = combine_predicates(vec![cmp_pred]);
                let join_body = join_body_tokens(pred, out_val);

                Ok(quote! {
                    let #out = ::flowlog_runtime::operators::flowlog_join_core(
                        #l.clone(),
                        #r.clone(),
                        #operator_name,
                        |#jn_k, #jn_lv, #jn_rv| { #join_body },
                    );
                })
            }

            // Join: Key-value ⋈ Key-value -> key-value
            Transformation::JnToKv {
                input,
                output,
                flow,
            } => {
                // Inputs / outputs
                let (left, right) = input;
                let l_base = find_local_ident(local_fp_to_ident, left.fingerprint());
                let r_base = find_local_ident(local_fp_to_ident, right.fingerprint());
                let l = expect_arranged(arranged_map, left.fingerprint(), &l_base)?;
                let r = expect_arranged(arranged_map, right.fingerprint(), &r_base)?;
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                // Profiling hook (optional)
                with_plan_graph(plan_graph, |plan_graph| {
                    plan_graph.map_join_arrange_operator(
                        transformation_name,
                        vec![l.to_string(), r.to_string()],
                        format!("{}_arr", out),
                        output.fingerprint(),
                        output.is_k_only(),
                    );
                });

                // Type inference
                self.record_transformation_output_type(
                    left.fingerprint(),
                    Some(right.fingerprint()),
                    output.fingerprint(),
                    flow,
                    stratum,
                )?;

                // Output expression + predicates
                let (jn_k, jn_lv, jn_rv) =
                    compute_join_param_tokens(flow.key(), flow.value(), flow.compares());
                let out_key = self.build_key_val_from_join_args(flow.key(), si)?;
                let out_val = self.build_key_val_from_join_args(flow.value(), si)?;
                let out_expr = if output.is_k_only() {
                    quote! { #out_key }
                } else {
                    quote! { ( #out_key, #out_val ) }
                };

                let left_type = self.find_global_data_type(left.fingerprint())?.clone();
                let right_type = self.find_global_data_type(right.fingerprint())?.clone();
                let cmp_pred = self.build_join_compare_predicate(
                    flow.compares(),
                    si,
                    &left_type,
                    &right_type,
                )?;
                let pred = combine_predicates(vec![cmp_pred]);
                let join_body = join_body_tokens(pred, out_expr);

                let transformation = quote! {
                    let #out = ::flowlog_runtime::operators::flowlog_join_core(
                        #l.clone(),
                        #r.clone(),
                        #operator_name,
                        |#jn_k, #jn_lv, #jn_rv| { #join_body },
                    );
                };

                let arrange_stmt = self.register_arrangement(
                    arranged_map,
                    output.fingerprint(),
                    &out,
                    output.is_k_only(),
                );

                Ok(quote! {
                    #transformation
                    #arrange_stmt
                })
            }

            // Antijoin: Key-value ¬ Key-only to Row
            Transformation::NJnToRow {
                input,
                output,
                flow,
            } => {
                self.features.mark_as_collection();
                self.features.mark_timely_map();

                // Inputs / outputs
                let (left, right) = input;
                let l_base = find_local_ident(local_fp_to_ident, left.fingerprint());
                let r_base = find_local_ident(local_fp_to_ident, right.fingerprint());
                let l = expect_arranged(arranged_map, left.fingerprint(), &l_base)?;
                let r = expect_arranged(arranged_map, right.fingerprint(), &r_base)?;
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                // Profiling hook (optional)
                with_plan_graph(plan_graph, |plan_graph| {
                    plan_graph.anti_join_operator(
                        transformation_name,
                        vec![l.to_string(), r.to_string()],
                        out.to_string(),
                        output.fingerprint(),
                        recursive,
                    );
                });

                // Type inference
                self.record_transformation_output_type(
                    left.fingerprint(),
                    Some(right.fingerprint()),
                    output.fingerprint(),
                    flow,
                    stratum,
                )?;

                let (pos_weight_concat, neg_weight_concat) = self.weight_concat_tokens();

                // Output expression
                let (anti_param_k, anti_param_v) =
                    compute_kv_param_tokens(flow.key(), flow.value(), flow.compares(), None);
                let out_map_value = self.build_key_val_from_kv_args(flow.value(), si)?;
                let (inter_dedup, final_normalize) = self.dedup_antijoin(recursive);

                Ok(quote! {
                    let #out =
                        ::flowlog_runtime::operators::flowlog_flat_map(
                            #r.clone()
                                .flat_map_ref(|#anti_param_k, #anti_param_v| std::iter::once(( #anti_param_k.clone(), #anti_param_v.clone() )))
                                #inter_dedup
                                #pos_weight_concat
                                .concat(
                                    {
                                        ::flowlog_runtime::operators::flowlog_join_core(
                                            #l.clone(),
                                            #r.clone(),
                                            #operator_name,
                                            |aj_k, _, aj_rv| {
                                                Some((aj_k.clone(), aj_rv.clone()))
                                            },
                                        )
                                        #inter_dedup
                                        #neg_weight_concat
                                    }
                                ),
                            #operator_name,
                            |( #anti_param_k, #anti_param_v )| std::iter::once( #out_map_value ),
                        )
                            #final_normalize;
                })
            }

            // Antijoin: Key-only ¬ Key-only to key-value
            Transformation::NJnToKv {
                input,
                output,
                flow,
            } => {
                self.features.mark_as_collection();
                self.features.mark_timely_map();

                // Inputs / outputs
                let (left, right) = input;
                let l_base = find_local_ident(local_fp_to_ident, left.fingerprint());
                let r_base = find_local_ident(local_fp_to_ident, right.fingerprint());
                let l = expect_arranged(arranged_map, left.fingerprint(), &l_base)?;
                let r = expect_arranged(arranged_map, right.fingerprint(), &r_base)?;
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                // Profiling hook (optional)
                with_plan_graph(plan_graph, |plan_graph| {
                    plan_graph.anti_join_arrange_operator(
                        transformation_name,
                        vec![l.to_string(), r.to_string()],
                        format!("{}_arr", out),
                        output.fingerprint(),
                        output.is_k_only(),
                        recursive,
                    );
                });

                // Type inference
                self.record_transformation_output_type(
                    left.fingerprint(),
                    Some(right.fingerprint()),
                    output.fingerprint(),
                    flow,
                    stratum,
                )?;

                let (pos_weight_concat, neg_weight_concat) = self.weight_concat_tokens();

                // Output expression
                let (anti_param_k, anti_param_v) =
                    compute_kv_param_tokens(flow.key(), flow.value(), flow.compares(), None);
                let out_map_key = self.build_key_val_from_kv_args(flow.key(), si)?;
                let out_map_value = self.build_key_val_from_kv_args(flow.value(), si)?;
                let out_map_expr = if output.is_k_only() {
                    quote! { #out_map_key }
                } else {
                    quote! { ( #out_map_key, #out_map_value ) }
                };
                let (inter_dedup, final_normalize) = self.dedup_antijoin(recursive);

                let transformation = quote! {
                    let #out =
                        ::flowlog_runtime::operators::flowlog_flat_map(
                            #r.clone()
                                .flat_map_ref(|#anti_param_k, #anti_param_v | std::iter::once( ( #anti_param_k.clone(), #anti_param_v.clone() ) ))
                                #inter_dedup
                                #pos_weight_concat
                                .concat(
                                    {
                                        ::flowlog_runtime::operators::flowlog_join_core(
                                            #l.clone(),
                                            #r.clone(),
                                            #operator_name,
                                            |aj_k, _, aj_rv| {
                                                Some((aj_k.clone(), aj_rv.clone()))
                                            },
                                        )
                                        #inter_dedup
                                        #neg_weight_concat
                                    }
                                ),
                            #operator_name,
                            |( #anti_param_k, #anti_param_v )| std::iter::once( #out_map_expr ),
                        )
                            #final_normalize;
                };

                let arrange_stmt = self.register_arrangement(
                    arranged_map,
                    output.fingerprint(),
                    &out,
                    output.is_k_only(),
                );

                Ok(quote! {
                    #transformation
                    #arrange_stmt
                })
            }
        }
    }
}

// =========================================================================
// Arrangement Management Utilities
// =========================================================================
impl CodeGen {
    /// Weight-conversion tokens for antijoin arithmetic (`pos` / `neg`).
    ///
    /// - `DatalogBatch` (`Present` diff): convert to `1i32` / `-1i32`.
    /// - `ExtendBatch` (`i32` diff, always `1`): `pos` is no-op, `neg` uses fixed `-1`.
    /// - Incremental (`i32` diff, variable): `pos` is no-op, `neg` negates actual diff (`-d`).
    pub(crate) fn weight_concat_tokens(&self) -> (TokenStream, TokenStream) {
        let pos = if self.config.is_datalog_batch() {
            // Convert Present diff → 1i32
            quote! {
                .inner
                .flat_map(move |(x, t, _)| std::iter::once((x, t.clone(), 1i32)))
                .as_collection()
            }
        } else {
            // i32 diff — no conversion needed
            quote! {}
        };
        let neg = if self.config.is_datalog_batch() {
            // Fixed -1 weight (no retractions possible); the Present → i32
            // diff-type change forces rebuilding the triple.
            quote! {
                .inner
                .flat_map(move |(x, t, _)| std::iter::once((x, t.clone(), -1i32)))
                .as_collection()
            }
        } else if self.config.is_batch() {
            // ExtendBatch: i32 diff, always 1 — overwrite to -1 in the
            // input buffer instead of rebuilding each triple.
            quote! {
                .inner
                .map_in_place(|(_, _, d)| *d = -1i32)
                .as_collection()
            }
        } else {
            // Incremental: negate the actual diff in place.
            quote! {
                .inner
                .map_in_place(|(_, _, d)| *d = -*d)
                .as_collection()
            }
        };
        (pos, neg)
    }

    fn register_arrangement(
        &mut self,
        arranged_map: &mut HashMap<u64, Ident>,
        fingerprint: u64,
        collection_ident: &Ident,
        only_key: bool,
    ) -> TokenStream {
        let arrangement_ident = format_ident!("{}_arr", collection_ident);
        arranged_map.insert(fingerprint, arrangement_ident.clone());

        if only_key {
            quote! { let #arrangement_ident = #collection_ident.clone().arrange_by_self(); }
        } else {
            quote! { let #arrangement_ident = #collection_ident.clone().arrange_by_key(); }
        }
    }
}

fn expect_arranged(
    arranged_map: &HashMap<u64, Ident>,
    fingerprint: u64,
    base_ident: &Ident,
) -> Result<Ident, CodegenError> {
    arranged_map.get(&fingerprint).cloned().ok_or_else(|| {
        CodegenError::internal(format!(
            "collection `{base_ident}` (fingerprint 0x{fingerprint:016x}) \
             must be arranged before use"
        ))
    })
}

/// Build the body of a `flat_map` closure that yields `out` either
/// conditionally (when `pred` is `Some`) or unconditionally.
///
/// The unconditional branch uses `std::iter::once` because `flat_map`
/// expects an iterator.
fn flat_map_body_tokens(pred: Option<TokenStream>, out: TokenStream) -> TokenStream {
    match pred {
        Some(pred) => quote! { if #pred { Some( #out ) } else { None } },
        None => quote! { std::iter::once( #out ) },
    }
}

/// `true` iff `args` reproduce every one of the input row's `row_arity` columns exactly once, in
/// order, as a bare variable -- no arithmetic, cast, constant, reorder, or dropped/added column.
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

/// Builds the body of a join closure that yields `out` either
/// conditionally (when `pred` is `Some`) or unconditionally.
///
/// The unconditional branch returns a bare `Some(...)` because
/// The join operator expects an `Option`, not an iterator.
fn join_body_tokens(pred: Option<TokenStream>, out: TokenStream) -> TokenStream {
    match pred {
        Some(pred) => quote! { if #pred { Some( #out ) } else { None } },
        None => quote! { Some( #out ) },
    }
}

#[cfg(test)]
mod identity_projection_tests {
    use flowlog_parser::ArithmeticOperator;
    use flowlog_planner::planner::ArithmeticArgument;
    use flowlog_planner::planner::FactorArgument;
    use flowlog_planner::planner::TransformationArgument;

    use super::is_identity_row_projection;

    /// A bare column reference `KV((is_key, idx))` with no arithmetic tail.
    fn col(is_key: bool, idx: usize) -> ArithmeticArgument {
        ArithmeticArgument {
            init: FactorArgument::Var(TransformationArgument::KV((is_key, idx))),
            rest: Vec::new(),
        }
    }

    #[test]
    fn full_in_order_projection_is_identity() {
        // Every column reproduced once, in order: identity (incl. single- and zero-column).
        assert!(is_identity_row_projection(
            &[col(false, 0), col(false, 1), col(false, 2)],
            3
        ));
        assert!(is_identity_row_projection(&[col(false, 0)], 1));
        assert!(is_identity_row_projection(&[], 0));
    }

    #[test]
    fn key_value_flag_is_ignored_for_row_inputs() {
        // Row columns are addressed by position; the key/value flag must not matter.
        assert!(is_identity_row_projection(
            &[col(true, 0), col(false, 1)],
            2
        ));
    }

    #[test]
    fn wrong_position_is_not_identity() {
        // Reordered, and duplicated (col 0 where col 1 is expected): both break `i == idx`.
        assert!(!is_identity_row_projection(
            &[col(false, 1), col(false, 0)],
            2
        ));
        assert!(!is_identity_row_projection(
            &[col(false, 0), col(false, 0)],
            2
        ));
    }

    #[test]
    fn arity_mismatch_is_not_identity() {
        // Fewer or more args than the row arity are never a full identity.
        assert!(!is_identity_row_projection(&[col(false, 0)], 2));
        assert!(!is_identity_row_projection(
            &[col(false, 0), col(false, 1)],
            1
        ));
    }

    #[test]
    fn arithmetic_tail_is_not_identity() {
        // `col0 + col1` transforms the value, so it must not be treated as identity.
        let arg = ArithmeticArgument {
            init: FactorArgument::Var(TransformationArgument::KV((false, 0))),
            rest: vec![(
                ArithmeticOperator::Plus,
                FactorArgument::Var(TransformationArgument::KV((false, 1))),
            )],
        };
        assert!(!is_identity_row_projection(std::slice::from_ref(&arg), 1));
    }
}
