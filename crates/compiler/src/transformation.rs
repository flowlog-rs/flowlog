//! Transformation code generation for FlowLog compiler.
//!
//! This module generates Rust code for various data transformations
//! such as Row-to-Row, Row-to-KV, KV-to-KV, KV-to-Row, Joins, and Antijoins
//! in the differential dataflow pipelines.

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use std::collections::HashMap;

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

/// Generate the non-recursive differential dataflow pipelines.
/// For non recursive transformations, we actually are using global fp to ident map,
/// while for recursive transformations, we have to using local fp to ident map.
/// To make unified interface, we pass in `local_fp_to_ident` here.
impl Compiler {
    pub(super) fn gen_transformation(
        &mut self,
        local_fp_to_ident: &HashMap<u64, Ident>,
        transformation: &Transformation,
        arranged_map: &mut HashMap<u64, Ident>,
    ) -> TokenStream {
        match transformation {
            // Row -> Row
            Transformation::RowToRow {
                input,
                output,
                flow,
            } => {
                let inp = find_local_ident(local_fp_to_ident, input.fingerprint());
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());
                let input_arity = input.arity().1;

                let (row_pat, row_fields) = row_pattern_and_fields(
                    input_arity,
                    flow.key(),
                    flow.value(),
                    flow.compares(),
                    flow.constraints(),
                );

                // Type inference
                self.verify_and_infer_global_type(
                    input.fingerprint(),
                    None,
                    output.fingerprint(),
                    flow,
                );
                let itype = self.find_global_type(input.fingerprint()).1.clone();

                let row_ty = type_tokens(&itype);
                let out_val = build_key_val_from_row_args(flow.value(), &row_fields);
                // Optional filter on comparisons and constraints
                let cmp_pred = build_row_compare_predicate(flow.compares(), &row_fields);
                let cst_pred = build_row_constraints_predicate(flow.constraints(), &row_fields);
                let pred = combine_predicates(cmp_pred, cst_pred);

                if let Some(pred) = pred {
                    quote! {
                        let #out = #inp
                            .flat_map(|#row_pat: #row_ty| {
                                if #pred { Some( #out_val ) } else { None }
                            });
                    }
                } else {
                    // For Row -> Row, use empty key () and put data in value
                    quote! {
                        let #out = #inp
                            .flat_map(|#row_pat: #row_ty| std::iter::once( #out_val ));
                    }
                }
            }

            // Row -> KV
            Transformation::RowToKv {
                input,
                output,
                flow,
            } => {
                let inp = find_local_ident(local_fp_to_ident, input.fingerprint());
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());
                let input_arity = input.arity().1;

                let (row_pat, row_fields) = row_pattern_and_fields(
                    input_arity,
                    flow.key(),
                    flow.value(),
                    flow.compares(),
                    flow.constraints(),
                );

                // Type inference
                self.verify_and_infer_global_type(
                    input.fingerprint(),
                    None,
                    output.fingerprint(),
                    flow,
                );
                let itype = self.find_global_type(input.fingerprint()).1.clone();

                let row_ty = type_tokens(&itype);
                let out_key = build_key_val_from_row_args(flow.key(), &row_fields);
                let out_val = build_key_val_from_row_args(flow.value(), &row_fields);
                let cmp_pred = build_row_compare_predicate(flow.compares(), &row_fields);
                let cst_pred = build_row_constraints_predicate(flow.constraints(), &row_fields);
                let pred = combine_predicates(cmp_pred, cst_pred);

                let transformation = if let Some(pred) = pred {
                    quote! {
                        let #out = #inp
                            .flat_map(|#row_pat: #row_ty| {
                                if #pred { Some( ( #out_key, #out_val ) ) } else { None }
                            });
                    }
                } else {
                    quote! {
                        let #out = #inp
                            .flat_map(|#row_pat: #row_ty| std::iter::once( ( #out_key, #out_val ) ));
                    }
                };

                let arrange_stmt =
                    self.register_arrangement(arranged_map, output.fingerprint(), &out);

                quote! {
                    #transformation
                    #arrange_stmt
                }
            }

            // KV -> KV
            Transformation::KvToKv {
                input,
                output,
                flow,
            } => {
                let inp = find_local_ident(local_fp_to_ident, input.fingerprint());
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                // Type inference
                self.verify_and_infer_global_type(
                    input.fingerprint(),
                    None,
                    output.fingerprint(),
                    flow,
                );

                let out_key = build_key_val_from_kv_args(flow.key());
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

                let transformation = if let Some(pred) = pred {
                    quote! {
                        let #out = #inp
                            .flat_map(|( #kv_param_k, #kv_param_v )| {
                                if #pred { Some( ( #out_key, #out_val ) ) } else { None }
                            });
                    }
                } else {
                    quote! {
                        let #out = #inp
                            .flat_map(|( #kv_param_k, #kv_param_v )| std::iter::once( ( #out_key, #out_val ) ));
                    }
                };

                let arrange_stmt =
                    self.register_arrangement(arranged_map, output.fingerprint(), &out);

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
                let inp = find_local_ident(local_fp_to_ident, input.fingerprint());
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                // Type inference
                self.verify_and_infer_global_type(
                    input.fingerprint(),
                    None,
                    output.fingerprint(),
                    flow,
                );

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

                if let Some(pred) = pred {
                    quote! {
                        let #out = #inp
                            .flat_map(|( #kv_param_k, #kv_param_v )| {
                                if #pred { Some( #out_val ) } else { None }
                            });
                    }
                } else {
                    quote! {
                        let #out = #inp
                            .flat_map(|( #kv_param_k, #kv_param_v )| std::iter::once( #out_val ));
                    }
                }
            }

            // Join: Key-value ⋈ Key-value -> Row
            Transformation::JnToRow {
                input,
                output,
                flow,
            } => {
                let (left, right) = input;
                let l_base = find_local_ident(local_fp_to_ident, left.fingerprint());
                let r_base = find_local_ident(local_fp_to_ident, right.fingerprint());

                // Type inference
                self.verify_and_infer_global_type(
                    left.fingerprint(),
                    Some(right.fingerprint()),
                    output.fingerprint(),
                    flow,
                );

                let l = expect_arranged(arranged_map, left.fingerprint(), &l_base);
                let r = expect_arranged(arranged_map, right.fingerprint(), &r_base);

                let out = find_local_ident(local_fp_to_ident, output.fingerprint());
                let out_val = build_key_val_from_join_args(flow.value());
                let (jn_k, jn_lv, jn_rv) =
                    compute_join_param_tokens(flow.key(), flow.value(), flow.compares());

                if let Some(pred) = build_join_compare_predicate(flow.compares()) {
                    quote! {
                        let #out =
                            #l
                                .join_core(&#r, |#jn_k, #jn_lv, #jn_rv| {
                                    if #pred { Some( #out_val ) } else { None }
                                });
                    }
                } else {
                    quote! {
                        let #out =
                            #l
                                .join_core(&#r, |#jn_k, #jn_lv, #jn_rv| {
                                    Some( #out_val )
                                });
                    }
                }
            }

            // Join: Key-value ⋈ Key-value -> key-value
            Transformation::JnToKv {
                input,
                output,
                flow,
            } => {
                let (left, right) = input;
                let l_base = find_local_ident(local_fp_to_ident, left.fingerprint());
                let r_base = find_local_ident(local_fp_to_ident, right.fingerprint());

                // Type inference
                self.verify_and_infer_global_type(
                    left.fingerprint(),
                    Some(right.fingerprint()),
                    output.fingerprint(),
                    flow,
                );

                let l = expect_arranged(arranged_map, left.fingerprint(), &l_base);
                let r = expect_arranged(arranged_map, right.fingerprint(), &r_base);

                let out = find_local_ident(local_fp_to_ident, output.fingerprint());
                let out_key = build_key_val_from_join_args(flow.key());
                let out_val = build_key_val_from_join_args(flow.value());
                let (jn_k, jn_lv, jn_rv) =
                    compute_join_param_tokens(flow.key(), flow.value(), flow.compares());

                let transformation =
                    if let Some(pred) = build_join_compare_predicate(flow.compares()) {
                        quote! {
                            let #out =
                                #l
                                    .join_core(&#r, |#jn_k, #jn_lv, #jn_rv| {
                                        if #pred { Some((#out_key, #out_val)) } else { None }
                                    });
                        }
                    } else {
                        quote! {
                            let #out =
                                #l
                                    .join_core(&#r, |#jn_k, #jn_lv, #jn_rv| {
                                        Some((#out_key, #out_val))
                                    });
                        }
                    };

                let arrange_stmt =
                    self.register_arrangement(arranged_map, output.fingerprint(), &out);

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
                let (left, right) = input;
                let l_base = find_local_ident(local_fp_to_ident, left.fingerprint());
                let r_base = find_local_ident(local_fp_to_ident, right.fingerprint());

                // Type inference
                self.verify_and_infer_global_type(
                    left.fingerprint(),
                    Some(right.fingerprint()),
                    output.fingerprint(),
                    flow,
                );

                let l = expect_arranged(arranged_map, left.fingerprint(), &l_base);
                let r = expect_arranged(arranged_map, right.fingerprint(), &r_base);

                let out = find_local_ident(local_fp_to_ident, output.fingerprint());
                let out_map_value = build_key_val_from_kv_args(flow.value());

                let (anti_param_k, anti_param_v) =
                    compute_kv_param_tokens(flow.key(), flow.value(), flow.compares(), None);
                let dedup_call = self.dedup_collection();

                quote! {
                    let #out =
                        #r
                            .flat_map_ref(|#anti_param_k, #anti_param_v| std::iter::once(( #anti_param_k.clone(), #anti_param_v.clone() )))
                            #dedup_call
                            .inner
                            .flat_map(move |(x, t, _)| std::iter::once((x, t.clone(), 1_i32)))
                            .as_collection()
                            .concat(
                                &{
                                    #l
                                    .join_core(&#r, |aj_k, _, aj_rv| {
                                        Some((aj_k.clone(), aj_rv.clone()))
                                    })
                                    #dedup_call
                                    .inner
                                    .flat_map(move |(x, t, _)| std::iter::once((x, t.clone(), -1_i32)))
                                    .as_collection()
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
                let (left, right) = input;
                let l_base = find_local_ident(local_fp_to_ident, left.fingerprint());
                let r_base = find_local_ident(local_fp_to_ident, right.fingerprint());

                // Type inference
                self.verify_and_infer_global_type(
                    left.fingerprint(),
                    Some(right.fingerprint()),
                    output.fingerprint(),
                    flow,
                );

                let l = expect_arranged(arranged_map, left.fingerprint(), &l_base);
                let r = expect_arranged(arranged_map, right.fingerprint(), &r_base);

                let out = find_local_ident(local_fp_to_ident, output.fingerprint());
                let out_map_key = build_key_val_from_kv_args(flow.key());
                let out_map_value = build_key_val_from_kv_args(flow.value());

                let (anti_param_k, anti_param_v) =
                    compute_kv_param_tokens(flow.key(), flow.value(), flow.compares(), None);
                let dedup_call = self.dedup_collection();

                let transformation = quote! {
                    let #out =
                        #r
                            .flat_map_ref(|#anti_param_k, #anti_param_v | std::iter::once( ( #anti_param_k.clone(), #anti_param_v.clone() ) ))
                            #dedup_call
                            .inner
                            .flat_map(move |(x, t, _)| std::iter::once((x, t.clone(), 1_i32)))
                            .as_collection()
                            .concat(
                                &{
                                    #l
                                        .join_core(&#r, |aj_k, _, aj_rv| {
                                            Some((aj_k.clone(), aj_rv.clone()))
                                        })
                                        #dedup_call
                                        .inner
                                        .flat_map(move |(x, t, _)| std::iter::once((x, t.clone(), -1_i32)))
                                        .as_collection()
                                }
                            )
                            .flat_map(|( #anti_param_k, #anti_param_v )| std::iter::once( ( #out_map_key, #out_map_value ) ))
                            #dedup_call;
                };

                let arrange_stmt =
                    self.register_arrangement(arranged_map, output.fingerprint(), &out);

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
    fn register_arrangement(
        &mut self,
        arranged_map: &mut HashMap<u64, Ident>,
        fingerprint: u64,
        collection_ident: &Ident,
    ) -> TokenStream {
        self.imports.mark_arrange_by_key();
        let arrangement_ident = format_ident!("{}_arr", collection_ident);
        arranged_map.insert(fingerprint, arrangement_ident.clone());
        quote! { let #arrangement_ident = #collection_ident.arrange_by_key(); }
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
