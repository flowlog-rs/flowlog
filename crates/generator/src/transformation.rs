use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use std::collections::HashMap;

use super::arg::{
    build_join_compare_predicate, build_key_val_from_join_args, build_key_val_from_kv_args,
    build_key_val_from_row_args, build_kv_compare_predicate, build_kv_constraints_predicate,
    build_row_compare_predicate, build_row_constraints_predicate, combine_predicates,
    compute_join_param_tokens, compute_kv_param_tokens,
};
use super::data_type::{find_type, insert_or_verify_type, row_pattern_and_fields, type_tokens};
use super::ident::find_ident;

use parser::DataType;
use planner::Transformation;

/// Generate the non-recursive differential dataflow pipelines.
pub(super) fn gen_transformation(
    fp_to_ident: &HashMap<u64, Ident>,
    fp_to_type: &mut HashMap<u64, DataType>,
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
            let inp = find_ident(fp_to_ident, input.fingerprint());
            let out = find_ident(fp_to_ident, output.fingerprint());
            let input_arity = input.arity().1;

            let (row_pat, row_fields) = row_pattern_and_fields(
                input_arity,
                flow.key(),
                flow.value(),
                flow.compares(),
                flow.constraints(),
            );
            let itype = find_type(fp_to_type, input.fingerprint());

            // Check if output type already exists, if so assert it matches, otherwise insert
            insert_or_verify_type(fp_to_type, output.fingerprint(), itype);

            let row_ty = type_tokens(itype, input_arity);
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
            let inp = find_ident(fp_to_ident, input.fingerprint());
            let out = find_ident(fp_to_ident, output.fingerprint());
            let input_arity = input.arity().1;

            let (row_pat, row_fields) = row_pattern_and_fields(
                input_arity,
                flow.key(),
                flow.value(),
                flow.compares(),
                flow.constraints(),
            );
            let itype = find_type(fp_to_type, input.fingerprint());

            // Check if output type already exists, if so assert it matches, otherwise insert
            insert_or_verify_type(fp_to_type, output.fingerprint(), itype);

            let row_ty = type_tokens(itype, input_arity);
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

            let arrange_stmt = register_arrangement(arranged_map, output.fingerprint(), &out);

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
            let inp = find_ident(fp_to_ident, input.fingerprint());
            let out = find_ident(fp_to_ident, output.fingerprint());

            let itype = find_type(fp_to_type, input.fingerprint());
            // Check if output type already exists, if so assert it matches, otherwise insert
            insert_or_verify_type(fp_to_type, output.fingerprint(), itype);

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

            let arrange_stmt = register_arrangement(arranged_map, output.fingerprint(), &out);

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
            let inp = find_ident(fp_to_ident, input.fingerprint());
            let out = find_ident(fp_to_ident, output.fingerprint());

            let itype = find_type(fp_to_type, input.fingerprint());
            // Check if output type already exists, if so assert it matches, otherwise insert
            insert_or_verify_type(fp_to_type, output.fingerprint(), itype);

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
            let l_base = find_ident(fp_to_ident, left.fingerprint());
            let r_base = find_ident(fp_to_ident, right.fingerprint());

            assert_eq!(
                find_type(fp_to_type, left.fingerprint()),
                find_type(fp_to_type, right.fingerprint())
            );
            let itype = find_type(fp_to_type, left.fingerprint());
            // Check if output type already exists, if so assert it matches, otherwise insert
            insert_or_verify_type(fp_to_type, output.fingerprint(), itype);

            let l = expect_arranged(arranged_map, left.fingerprint(), &l_base);
            let r = expect_arranged(arranged_map, right.fingerprint(), &r_base);

            let out = find_ident(fp_to_ident, output.fingerprint());

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
            let l_base = find_ident(fp_to_ident, left.fingerprint());
            let r_base = find_ident(fp_to_ident, right.fingerprint());

            assert_eq!(
                find_type(fp_to_type, left.fingerprint()),
                find_type(fp_to_type, right.fingerprint())
            );
            let itype = find_type(fp_to_type, left.fingerprint());
            // Check if output type already exists, if so assert it matches, otherwise insert
            insert_or_verify_type(fp_to_type, output.fingerprint(), itype);

            let l = expect_arranged(arranged_map, left.fingerprint(), &l_base);
            let r = expect_arranged(arranged_map, right.fingerprint(), &r_base);

            let out = find_ident(fp_to_ident, output.fingerprint());

            let out_key = build_key_val_from_join_args(flow.key());
            let out_val = build_key_val_from_join_args(flow.value());
            let (jn_k, jn_lv, jn_rv) =
                compute_join_param_tokens(flow.key(), flow.value(), flow.compares());

            let transformation = if let Some(pred) = build_join_compare_predicate(flow.compares()) {
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

            let arrange_stmt = register_arrangement(arranged_map, output.fingerprint(), &out);

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
            let (left, right) = input;
            let l_base = find_ident(fp_to_ident, left.fingerprint());
            let r_base = find_ident(fp_to_ident, right.fingerprint());

            assert_eq!(
                find_type(fp_to_type, left.fingerprint()),
                find_type(fp_to_type, right.fingerprint())
            );
            let itype = find_type(fp_to_type, left.fingerprint());
            // Check if output type already exists, if so assert it matches, otherwise insert
            insert_or_verify_type(fp_to_type, output.fingerprint(), itype);

            let l = expect_arranged(arranged_map, left.fingerprint(), &l_base);
            let r = expect_arranged(arranged_map, right.fingerprint(), &r_base);

            let out = find_ident(fp_to_ident, output.fingerprint());

            let out_map_value = build_key_val_from_kv_args(flow.value());

            let (anti_param_k, anti_param_v) =
                compute_kv_param_tokens(flow.key(), flow.value(), flow.compares(), None);

            let out_join_val = build_key_val_from_join_args(flow.value());

            let (antijoin_k, antijoin_lv, antijoin_rv) =
                compute_join_param_tokens(flow.key(), flow.value(), &[]);
            quote! {
                let #out =
                    #r
                        .flat_map_ref(|& #anti_param_k, & #anti_param_v| std::iter::once( #out_map_value ))
                        .inner
                        .flat_map(move |(x, t, _)| std::iter::once((x, t.clone(), 1 as i32)))
                        .as_collection()
                        .concat(
                            &{
                                #l
                                    .join_core(&#r, |#antijoin_k, #antijoin_lv, #antijoin_rv| {
                                        Some( #out_join_val )
                                    })
                                    .inner
                                    .flat_map(move |(x, t, _)| std::iter::once((x, t.clone(), -1 as i32)))
                                    .as_collection()
                            }
                        )
                        .threshold_semigroup(move |_, _, old| old.is_none().then_some(SEMIRING_ONE));
            }
        }

        // Antijoin: Key-only ¬ Key-only to key-value
        Transformation::NJnToKv {
            input,
            output,
            flow,
        } => {
            let (left, right) = input;
            let l_base = find_ident(fp_to_ident, left.fingerprint());
            let r_base = find_ident(fp_to_ident, right.fingerprint());

            assert_eq!(
                find_type(fp_to_type, left.fingerprint()),
                find_type(fp_to_type, right.fingerprint())
            );
            let itype = find_type(fp_to_type, left.fingerprint());
            // Check if output type already exists, if so assert it matches, otherwise insert
            insert_or_verify_type(fp_to_type, output.fingerprint(), itype);

            let l = expect_arranged(arranged_map, left.fingerprint(), &l_base);
            let r = expect_arranged(arranged_map, right.fingerprint(), &r_base);

            let out = find_ident(fp_to_ident, output.fingerprint());

            let out_map_key = build_key_val_from_kv_args(flow.key());
            let out_map_value = build_key_val_from_kv_args(flow.value());

            let (kv_param_k, kv_param_v) =
                compute_kv_param_tokens(flow.key(), flow.value(), flow.compares(), None);

            let out_join_key = build_key_val_from_join_args(flow.key());
            let out_join_val = build_key_val_from_join_args(flow.value());

            let (antijoin_k, antijoin_lv, antijoin_rv) =
                compute_join_param_tokens(flow.key(), flow.value(), &[]);

            let transformation = quote! {
                let #out =
                    #r
                        .flat_map_ref(|& #kv_param_k, & #kv_param_v | std::iter::once( ( #out_map_key, #out_map_value ) ))
                        .inner
                        .flat_map(move |(x, t, _)| std::iter::once((x, t.clone(), 1 as i32)))
                        .as_collection()
                        .concat(
                            &{
                                #l
                                    .join_core(&#r, |#antijoin_k, #antijoin_lv, #antijoin_rv| {
                                        Some((#out_join_key, #out_join_val))
                                    })
                                    .inner
                                    .flat_map(move |(x, t, _)| std::iter::once((x, t.clone(), -1 as i32)))
                                    .as_collection()
                            }
                        )
                        .threshold_semigroup(move |_, _, old| old.is_none().then_some(SEMIRING_ONE));
            };

            let arrange_stmt = register_arrangement(arranged_map, output.fingerprint(), &out);

            quote! {
                #transformation
                #arrange_stmt
            }
        }
    }
}

// =========================================================================
// Arrangement Management Utilities
// =========================================================================
fn register_arrangement(
    arranged_map: &mut HashMap<u64, Ident>,
    fingerprint: u64,
    collection_ident: &Ident,
) -> TokenStream {
    let arrangement_ident = format_ident!("{}_arr", collection_ident);
    arranged_map.insert(fingerprint, arrangement_ident.clone());
    quote! { let #arrangement_ident = #collection_ident.arrange_by_key(); }
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
