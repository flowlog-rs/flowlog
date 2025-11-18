use proc_macro2::{Ident, TokenStream};
use quote::quote;
use std::collections::HashMap;

use super::arg::{
    build_join_compare_predicate, build_key_val_from_join_args, build_key_val_from_kv_args,
    build_key_val_from_row_args, build_kv_compare_predicate, build_kv_constraints_predicate,
    build_row_compare_predicate, build_row_constraints_predicate, combine_predicates,
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

            let (row_pat, row_fields) = row_pattern_and_fields(input_arity);
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
                            if #pred { Some( ( (), #out_val ) ) } else { None }
                        });
                }
            } else {
                // For Row -> Row, use empty key () and put data in value
                quote! {
                    let #out = #inp
                        .flat_map(|#row_pat: #row_ty| std::iter::once( ( (), #out_val ) ));
                }
            }
        }

        // Row -> K
        Transformation::RowToK {
            input,
            output,
            flow,
        } => {
            let inp = find_ident(fp_to_ident, input.fingerprint());
            let out = find_ident(fp_to_ident, output.fingerprint());
            let input_arity = input.arity().1;

            let (row_pat, row_fields) = row_pattern_and_fields(input_arity);
            let itype = find_type(fp_to_type, input.fingerprint());

            // Check if output type already exists, if so assert it matches, otherwise insert
            insert_or_verify_type(fp_to_type, output.fingerprint(), itype);

            let row_ty = type_tokens(itype, input_arity);
            let out_key = build_key_val_from_row_args(flow.key(), &row_fields);
            let cmp_pred = build_row_compare_predicate(flow.compares(), &row_fields);
            let cst_pred = build_row_constraints_predicate(flow.constraints(), &row_fields);
            let pred = combine_predicates(cmp_pred, cst_pred);

            if let Some(pred) = pred {
                quote! {
                    let #out = #inp
                        .flat_map(|#row_pat: #row_ty| {
                            if #pred { Some( ( #out_key, () ) ) } else { None }
                        });
                }
            } else {
                // For Row -> K, put data in key and use empty value ()
                quote! {
                    let #out = #inp
                        .flat_map(|#row_pat: #row_ty| std::iter::once( ( #out_key, () ) ));
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

            let (row_pat, row_fields) = row_pattern_and_fields(input_arity);
            let itype = find_type(fp_to_type, input.fingerprint());

            // Check if output type already exists, if so assert it matches, otherwise insert
            insert_or_verify_type(fp_to_type, output.fingerprint(), itype);

            let row_ty = type_tokens(itype, input_arity);
            let out_key = build_key_val_from_row_args(flow.key(), &row_fields);
            let out_val = build_key_val_from_row_args(flow.value(), &row_fields);
            let cmp_pred = build_row_compare_predicate(flow.compares(), &row_fields);
            let cst_pred = build_row_constraints_predicate(flow.constraints(), &row_fields);
            let pred = combine_predicates(cmp_pred, cst_pred);

            if let Some(pred) = pred {
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

            if let Some(pred) = pred {
                quote! {
                    let #out = #inp
                        .flat_map(|(k, v)| {
                            if #pred { Some( ( #out_key, #out_val ) ) } else { None }
                        });
                }
            } else {
                quote! {
                    let #out = #inp
                        .flat_map(|(k, v)| std::iter::once( ( #out_key, #out_val ) ));
                }
            }
        }

        // KV -> K
        Transformation::KvToK {
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
            let cmp_pred = build_kv_compare_predicate(flow.compares());
            let cst_pred = build_kv_constraints_predicate(flow.constraints());
            let pred = combine_predicates(cmp_pred, cst_pred);

            if let Some(pred) = pred {
                quote! {
                    let #out = #inp
                        .flat_map(|(k, v)| {
                            if #pred { Some( ( #out_key, () ) ) } else { None }
                        });
                }
            } else {
                quote! {
                    let #out = #inp
                        .flat_map(|(k, v)| std::iter::once( ( #out_key, () ) ));
                }
            }
        }

        // KV -> Value-only
        Transformation::KvToV {
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

            if let Some(pred) = pred {
                quote! {
                    let #out = #inp
                        .flat_map(|(k, v)| {
                            if #pred { Some( ( (), #out_val ) ) } else { None }
                        });
                }
            } else {
                quote! {
                    let #out = #inp
                        .flat_map(|(k, v)| std::iter::once( ( (), #out_val ) ));
                }
            }
        }

        // All join operations: JnKK, JnKKv, JnKvKv, and Cartesian
        // They all use the same join_core logic, differing only in output expressions
        Transformation::JnKK {
            input,
            output,
            flow,
        }
        | Transformation::JnKKv {
            input,
            output,
            flow,
        }
        | Transformation::JnKvKv {
            input,
            output,
            flow,
        }
        | Transformation::Cartesian {
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

            // Collect arrangement statements if not already arranged.
            let mut arrange_tokens: Vec<TokenStream> = Vec::new();
            let l = if let Some(arr) = arranged_map.get(&left.fingerprint()).cloned() {
                arr
            } else {
                let arr = quote::format_ident!("{}_arr", l_base);
                arranged_map.insert(left.fingerprint(), arr.clone());
                arrange_tokens.push(quote! { let #arr = #l_base.arrange_by_key(); });
                arr
            };
            let r = if let Some(arr) = arranged_map.get(&right.fingerprint()).cloned() {
                arr
            } else {
                let arr = quote::format_ident!("{}_arr", r_base);
                arranged_map.insert(right.fingerprint(), arr.clone());
                arrange_tokens.push(quote! { let #arr = #r_base.arrange_by_key(); });
                arr
            };

            let out = find_ident(fp_to_ident, output.fingerprint());
            let op_name = match transformation {
                Transformation::JnKK { .. } => "JoinKK",
                Transformation::JnKKv { .. } => "JoinKKv",
                Transformation::JnKvKv { .. } => "JoinKvKv",
                Transformation::Cartesian { .. } => "Cartesian",
                _ => unreachable!(),
            };

            let out_key = build_key_val_from_join_args(flow.key());
            let out_val = build_key_val_from_join_args(flow.value());
            let (jn_k, jn_lv, jn_rv) =
                super::arg::compute_join_param_tokens(flow.key(), flow.value(), flow.compares());

            let arrangement = quote! { #( #arrange_tokens )* };
            if let Some(pred) = build_join_compare_predicate(flow.compares()) {
                quote! {
                    #arrangement
                    let #out =
                        #l
                            .join_core(&#r, |#jn_k, #jn_lv, #jn_rv| {
                                let out_key = #out_key;
                                let out_val = #out_val;
                                if #pred { Some((out_key, out_val)) } else { None }
                            });
                }
            } else {
                quote! {
                    #arrangement
                    let #out =
                        #l
                            .join_core(&#r, |#jn_k, #jn_lv, #jn_rv| {
                                let out_key = #out_key;
                                let out_val = #out_val;
                                Some((out_key, out_val))
                            });
                }
            }
        }

        // All antijoin operations: NjKK and NjKKv
        // They both use the same antijoin logic, differing only in output expressions
        Transformation::NjKK {
            input,
            output,
            flow,
        }
        | Transformation::NjKKv {
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

            let mut arrange_tokens: Vec<TokenStream> = Vec::new();
            let l = if let Some(arr) = arranged_map.get(&left.fingerprint()).cloned() {
                arr
            } else {
                let arr = quote::format_ident!("{}_arr", l_base);
                arranged_map.insert(left.fingerprint(), arr.clone());
                arrange_tokens.push(quote! { let #arr = #l_base.arrange_by_key(); });
                arr
            };
            let r = if let Some(arr) = arranged_map.get(&right.fingerprint()).cloned() {
                arr
            } else {
                let arr = quote::format_ident!("{}_arr", r_base);
                arranged_map.insert(right.fingerprint(), arr.clone());
                arrange_tokens.push(quote! { let #arr = #r_base.arrange_by_key(); });
                arr
            };

            let out = find_ident(fp_to_ident, output.fingerprint());
            let op_name = match transformation {
                Transformation::NjKK { .. } => "AntijoinKK",
                Transformation::NjKKv { .. } => "AntijoinKKv",
                _ => unreachable!(),
            };

            let out_key = build_key_val_from_join_args(flow.key());
            let out_val = build_key_val_from_join_args(flow.value());
            let (antijoin_k, antijoin_v) =
                super::arg::compute_anti_join_param_tokens(flow.key(), flow.value());

            let arrangement = quote! { #( #arrange_tokens )* };
            quote! {
                #arrangement
                let #out =
                    #l
                        .antijoin(&#r)
                        .map(|(#antijoin_k, #antijoin_v)| {
                            let out_key = #out_key;
                            let out_val = #out_val;
                            (out_key, out_val)
                        });
            }
        }
    }
}
