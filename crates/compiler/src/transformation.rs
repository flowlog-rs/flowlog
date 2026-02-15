//! Transformation code generation for FlowLog compiler.
//!
//! This module generates Rust code for various data transformations
//! such as Row-to-Row, Row-to-KV, KV-to-KV, KV-to-Row, Joins, and Antijoins
//! in the differential dataflow pipelines.

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use std::collections::HashMap;

use profiler::{with_profiler, Profiler};

use super::arg::{
    build_join_compare_predicate, build_key_val_from_join_args, build_key_val_from_kv_args,
    build_key_val_from_row_args, build_kv_compare_predicate, build_kv_constraints_predicate,
    build_row_compare_predicate, build_row_constraints_predicate, combine_predicates,
    compute_join_param_tokens, compute_kv_param_tokens, row_pattern_and_fields,
};
use super::data_type::type_tokens;
use super::ident::find_local_ident;
use super::Compiler;

use planner::Transformation;

/// Generate differential dataflow pipelines for a single transformation.
///
/// For non-recursive transformations, we use the global fingerprint-to-ident map.
/// For recursive transformations, we use the local fingerprint-to-ident map.
/// This function accepts the map as `local_fp_to_ident` to keep the interface unified.
impl Compiler {
    pub(super) fn gen_transformation(
        &mut self,
        local_fp_to_ident: &HashMap<u64, Ident>,
        transformation: &Transformation,
        arranged_map: &mut HashMap<u64, Ident>,
        profiler: &mut Option<Profiler>,
    ) -> TokenStream {
        let transformation_name = format!("{transformation}");
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

                // Profiler hook (optional)
                with_profiler(profiler, |profiler| {
                    profiler.map_join_operator(
                        transformation_name,
                        vec![inp.to_string()],
                        out.to_string(),
                        output.fingerprint(),
                    );
                });

                // Type inference + row pattern
                let input_arity = input.arity().1;
                let (row_pat, row_fields) = row_pattern_and_fields(
                    input_arity,
                    flow.key(),
                    flow.value(),
                    flow.compares(),
                    flow.constraints(),
                );
                self.verify_and_infer_global_type(
                    input.fingerprint(),
                    None,
                    output.fingerprint(),
                    flow,
                );
                let itype = self.find_global_type(input.fingerprint()).1.clone();

                // Output expression + predicates
                let row_ty = type_tokens(&itype);
                let out_val = build_key_val_from_row_args(flow.value(), &row_fields);
                let cmp_pred = build_row_compare_predicate(flow.compares(), &row_fields);
                let cst_pred = build_row_constraints_predicate(flow.constraints(), &row_fields);
                let pred = combine_predicates(cmp_pred, cst_pred);

                let flat_map_body = if let Some(pred) = pred {
                    quote! { if #pred { Some( #out_val ) } else { None } }
                } else {
                    quote! { std::iter::once( #out_val ) }
                };

                quote! {
                    let #out = #inp
                        .flat_map(|#row_pat: #row_ty| { #flat_map_body });
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

                // Profiler hook (optional)
                with_profiler(profiler, |profiler| {
                    profiler.map_join_arrange_operator(
                        transformation_name,
                        vec![inp.to_string()],
                        format!("{}_arr", out),
                        output.fingerprint(),
                        output.is_k_only(),
                    );
                });

                // Type inference + row pattern
                let input_arity = input.arity().1;
                let (row_pat, row_fields) = row_pattern_and_fields(
                    input_arity,
                    flow.key(),
                    flow.value(),
                    flow.compares(),
                    flow.constraints(),
                );
                self.verify_and_infer_global_type(
                    input.fingerprint(),
                    None,
                    output.fingerprint(),
                    flow,
                );
                let itype = self.find_global_type(input.fingerprint()).1.clone();

                // Output expression + predicates
                let row_ty = type_tokens(&itype);
                let out_key = build_key_val_from_row_args(flow.key(), &row_fields);
                let out_val = build_key_val_from_row_args(flow.value(), &row_fields);
                let out_expr = if output.is_k_only() {
                    quote! { #out_key }
                } else {
                    quote! { ( #out_key, #out_val ) }
                };

                let cmp_pred = build_row_compare_predicate(flow.compares(), &row_fields);
                let cst_pred = build_row_constraints_predicate(flow.constraints(), &row_fields);
                let pred = combine_predicates(cmp_pred, cst_pred);

                // Transformation logic
                let flat_map_body = if let Some(pred) = pred {
                    quote! { if #pred { Some( #out_expr ) } else { None } }
                } else {
                    quote! { std::iter::once( #out_expr ) }
                };

                let transformation = quote! {
                    let #out = #inp
                        .flat_map(|#row_pat: #row_ty| { #flat_map_body });
                };

                // Arrangement registration
                let arrange_stmt = self.register_arrangement(
                    arranged_map,
                    output.fingerprint(),
                    &out,
                    output.is_k_only(),
                );

                quote! {
                    #transformation
                    #arrange_stmt
                }
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

                // Profiler hook (optional)
                with_profiler(profiler, |profiler| {
                    profiler.map_join_operator(
                        transformation_name,
                        vec![inp.to_string()],
                        out.to_string(),
                        output.fingerprint(),
                    );
                });

                // Type inference
                self.verify_and_infer_global_type(
                    input.fingerprint(),
                    None,
                    output.fingerprint(),
                    flow,
                );

                // Output value + predicates
                let out_val = build_key_val_from_kv_args(flow.value());
                let cmp_pred = build_kv_compare_predicate(flow.compares());
                let cst_pred = build_kv_constraints_predicate(flow.constraints());
                let pred = combine_predicates(cmp_pred, cst_pred);
                let (kv_param_k, kv_param_v) = compute_kv_param_tokens(
                    flow.key(),
                    flow.value(),
                    flow.compares(),
                    Some(flow.constraints()),
                );

                // Transformation logic
                let flat_map_body = if let Some(pred) = pred {
                    quote! { if #pred { Some( #out_val ) } else { None } }
                } else {
                    quote! { std::iter::once( #out_val ) }
                };

                quote! {
                    let #out = #inp
                        .flat_map(|( #kv_param_k, #kv_param_v )| { #flat_map_body });
                }
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

                // Profiler hook (optional)
                with_profiler(profiler, |profiler| {
                    profiler.map_join_arrange_operator(
                        transformation_name,
                        vec![inp.to_string()],
                        format!("{}_arr", out),
                        output.fingerprint(),
                        output.is_k_only(),
                    );
                });

                // Type inference
                self.verify_and_infer_global_type(
                    input.fingerprint(),
                    None,
                    output.fingerprint(),
                    flow,
                );

                // Output expression + predicates
                let out_key = build_key_val_from_kv_args(flow.key());
                let out_val = build_key_val_from_kv_args(flow.value());
                let out_expr = if output.is_k_only() {
                    quote! { #out_key }
                } else {
                    quote! { ( #out_key, #out_val ) }
                };
                let cmp_pred = build_kv_compare_predicate(flow.compares());
                let cst_pred = build_kv_constraints_predicate(flow.constraints());
                let pred = combine_predicates(cmp_pred, cst_pred);
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
                let dedup_call = self.dedup_collection();
                let out_dedup_expr = if pred.is_none() && output.is_k_only() {
                    quote! { let #out = #out #dedup_call; }
                } else {
                    quote! {}
                };

                // Flat_map body depends on whether there is a predicate
                let flat_map_body = if let Some(pred) = pred {
                    quote! { if #pred { Some( #out_expr ) } else { None } }
                } else {
                    quote! { std::iter::once( #out_expr ) }
                };

                let transformation = quote! {
                    let #out = #inp
                        .flat_map(#closure_param { #flat_map_body });
                    #out_dedup_expr
                };

                // Arrangement registration
                let arrange_stmt = self.register_arrangement(
                    arranged_map,
                    output.fingerprint(),
                    &out,
                    output.is_k_only(),
                );

                quote! {
                    #transformation
                    #arrange_stmt
                }
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
                let l = expect_arranged(arranged_map, left.fingerprint(), &l_base);
                let r = expect_arranged(arranged_map, right.fingerprint(), &r_base);
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                // Profiler hook (optional)
                with_profiler(profiler, |profiler| {
                    profiler.map_join_operator(
                        transformation_name,
                        vec![l.to_string(), r.to_string()],
                        out.to_string(),
                        output.fingerprint(),
                    );
                });

                // Type inference
                self.verify_and_infer_global_type(
                    left.fingerprint(),
                    Some(right.fingerprint()),
                    output.fingerprint(),
                    flow,
                );

                // Output expression + predicates
                let (jn_k, jn_lv, jn_rv) =
                    compute_join_param_tokens(flow.key(), flow.value(), flow.compares());
                let out_val = build_key_val_from_join_args(flow.value());

                let join_body = if let Some(pred) = build_join_compare_predicate(flow.compares()) {
                    quote! { if #pred { Some( #out_val ) } else { None } }
                } else {
                    quote! { Some( #out_val ) }
                };

                quote! {
                    let #out = #l
                        .join_core(&#r, |#jn_k, #jn_lv, #jn_rv| { #join_body });
                }
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
                let l = expect_arranged(arranged_map, left.fingerprint(), &l_base);
                let r = expect_arranged(arranged_map, right.fingerprint(), &r_base);
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                // Profiler hook (optional)
                with_profiler(profiler, |profiler| {
                    profiler.map_join_arrange_operator(
                        transformation_name,
                        vec![l.to_string(), r.to_string()],
                        format!("{}_arr", out),
                        output.fingerprint(),
                        output.is_k_only(),
                    );
                });

                // Type inference
                self.verify_and_infer_global_type(
                    left.fingerprint(),
                    Some(right.fingerprint()),
                    output.fingerprint(),
                    flow,
                );

                // Output expression + predicates
                let (jn_k, jn_lv, jn_rv) =
                    compute_join_param_tokens(flow.key(), flow.value(), flow.compares());
                let out_key = build_key_val_from_join_args(flow.key());
                let out_val = build_key_val_from_join_args(flow.value());
                let out_expr = if output.is_k_only() {
                    quote! { #out_key }
                } else {
                    quote! { ( #out_key, #out_val ) }
                };

                let join_body = if let Some(pred) = build_join_compare_predicate(flow.compares()) {
                    quote! { if #pred { Some( #out_expr ) } else { None } }
                } else {
                    quote! { Some( #out_expr ) }
                };

                let transformation = quote! {
                    let #out = #l
                        .join_core(&#r, |#jn_k, #jn_lv, #jn_rv| { #join_body });
                };

                let arrange_stmt = self.register_arrangement(
                    arranged_map,
                    output.fingerprint(),
                    &out,
                    output.is_k_only(),
                );

                quote! {
                    #transformation
                    #arrange_stmt
                }
            }

            // Antijoin: Key-value ¬ Key-only to Row
            Transformation::NJnToRow {
                input,
                output,
                flow,
            } => {
                self.imports.mark_as_collection();
                self.imports.mark_timely_map();

                // Inputs / outputs
                let (left, right) = input;
                let l_base = find_local_ident(local_fp_to_ident, left.fingerprint());
                let r_base = find_local_ident(local_fp_to_ident, right.fingerprint());
                let l = expect_arranged(arranged_map, left.fingerprint(), &l_base);
                let r = expect_arranged(arranged_map, right.fingerprint(), &r_base);
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                // Profiler hook (optional)
                with_profiler(profiler, |profiler| {
                    profiler.anti_join_operator(
                        transformation_name,
                        vec![l.to_string(), r.to_string()],
                        out.to_string(),
                        output.fingerprint(),
                    );
                });

                // Type inference
                self.verify_and_infer_global_type(
                    left.fingerprint(),
                    Some(right.fingerprint()),
                    output.fingerprint(),
                    flow,
                );

                let (pos_weight_concat, neg_weight_concat) = self.weight_concat_tokens();

                // Output expression
                let (anti_param_k, anti_param_v) =
                    compute_kv_param_tokens(flow.key(), flow.value(), flow.compares(), None);
                let out_map_value = build_key_val_from_kv_args(flow.value());
                let dedup_call = self.dedup_collection();

                quote! {
                    let #out =
                        #r
                            .flat_map_ref(|#anti_param_k, #anti_param_v| std::iter::once(( #anti_param_k.clone(), #anti_param_v.clone() )))
                            #dedup_call
                            #pos_weight_concat
                            .concat(
                                &{
                                    #l
                                    .join_core(&#r, |aj_k, _, aj_rv| {
                                        Some((aj_k.clone(), aj_rv.clone()))
                                    })
                                    #dedup_call
                                    #neg_weight_concat
                                }
                            )
                            .flat_map(|( #anti_param_k, #anti_param_v )| std::iter::once( #out_map_value ))
                            #dedup_call;
                }
            }

            // Antijoin: Key-only ¬ Key-only to key-value
            Transformation::NJnToKv {
                input,
                output,
                flow,
            } => {
                self.imports.mark_as_collection();
                self.imports.mark_timely_map();

                // Inputs / outputs
                let (left, right) = input;
                let l_base = find_local_ident(local_fp_to_ident, left.fingerprint());
                let r_base = find_local_ident(local_fp_to_ident, right.fingerprint());
                let l = expect_arranged(arranged_map, left.fingerprint(), &l_base);
                let r = expect_arranged(arranged_map, right.fingerprint(), &r_base);
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                // Profiler hook (optional)
                with_profiler(profiler, |profiler| {
                    profiler.anti_join_arrange_operator(
                        transformation_name,
                        vec![l.to_string(), r.to_string()],
                        format!("{}_arr", out),
                        output.fingerprint(),
                        output.is_k_only(),
                    );
                });

                // Type inference
                self.verify_and_infer_global_type(
                    left.fingerprint(),
                    Some(right.fingerprint()),
                    output.fingerprint(),
                    flow,
                );

                let (pos_weight_concat, neg_weight_concat) = self.weight_concat_tokens();

                // Output expression
                let (anti_param_k, anti_param_v) =
                    compute_kv_param_tokens(flow.key(), flow.value(), flow.compares(), None);
                let out_map_key = build_key_val_from_kv_args(flow.key());
                let out_map_value = build_key_val_from_kv_args(flow.value());
                let out_map_expr = if output.is_k_only() {
                    quote! { #out_map_key }
                } else {
                    quote! { ( #out_map_key, #out_map_value ) }
                };
                let dedup_call = self.dedup_collection();

                let transformation = quote! {
                    let #out =
                        #r
                            .flat_map_ref(|#anti_param_k, #anti_param_v | std::iter::once( ( #anti_param_k.clone(), #anti_param_v.clone() ) ))
                            #dedup_call
                            #pos_weight_concat
                            .concat(
                                &{
                                    #l
                                        .join_core(&#r, |aj_k, _, aj_rv| {
                                            Some((aj_k.clone(), aj_rv.clone()))
                                        })
                                        #dedup_call
                                        #neg_weight_concat
                                }
                            )
                            .flat_map(|( #anti_param_k, #anti_param_v )| std::iter::once( #out_map_expr ))
                            #dedup_call;
                };

                let arrange_stmt = self.register_arrangement(
                    arranged_map,
                    output.fingerprint(),
                    &out,
                    output.is_k_only(),
                );

                quote! {
                    #transformation
                    #arrange_stmt
                }
            }
        }
    }
}

// =========================================================================
// Arrangement Management Utilities
// =========================================================================
impl Compiler {
    /// Generate weight-handling token streams for antijoin (batch vs incremental).
    fn weight_concat_tokens(&self) -> (TokenStream, TokenStream) {
        let pos = if self.config.is_incremental() {
            quote! {}
        } else {
            quote! {
                .inner
                .flat_map(move |(x, t, _)| std::iter::once((x, t.clone(), 1i32)))
                .as_collection()
            }
        };
        let neg = if self.config.is_incremental() {
            quote! {
                .inner
                .flat_map(move |(x, t, d)| std::iter::once((x, t.clone(), -d)))
                .as_collection()
            }
        } else {
            quote! {
                .inner
                .flat_map(move |(x, t, _)| std::iter::once((x, t.clone(), -1i32)))
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
            self.imports.mark_arrange_by_self();
            quote! { let #arrangement_ident = #collection_ident.arrange_by_self(); }
        } else {
            self.imports.mark_arrange_by_key();
            quote! { let #arrangement_ident = #collection_ident.arrange_by_key(); }
        }
    }
}

fn expect_arranged(
    arranged_map: &HashMap<u64, Ident>,
    fingerprint: u64,
    base_ident: &Ident,
) -> Ident {
    arranged_map.get(&fingerprint).cloned().unwrap_or_else(|| {
        panic!(
            "collection '{}' (fingerprint {:016x}) must be arranged before use",
            base_ident, fingerprint
        )
    })
}
