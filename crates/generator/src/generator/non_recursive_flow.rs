use proc_macro2::{Ident, TokenStream};
use quote::quote;
use std::collections::HashMap;
use tracing::trace;

use super::arg::{
    build_join_compare_predicate, build_key_val_from_join_args, build_key_val_from_kv_args,
    build_key_val_from_row_args, build_kv_compare_predicate, build_kv_constraints_predicate,
    build_row_compare_predicate, build_row_constraints_predicate, combine_predicates,
};
use super::comm::{
    find_fingerprint_type, fp_to_var, insert_or_verify_type, row_pattern_and_fields,
    row_type_tokens,
};

use parser::DataType;
use planner::Transformation;

/// Generate the non-recursive differential dataflow pipelines.
pub fn gen_non_recursive_flows(
    fp2ident: &HashMap<u64, Ident>,
    fp2type: &mut HashMap<u64, DataType>,
    transformations: &[Transformation],
) -> Vec<TokenStream> {
    let mut v = Vec::new();
    // Stratum-scoped cache of arrangements; emit arrange_by_key just before first use.
    let mut arranged_map: HashMap<u64, Ident> = HashMap::new();

    for f in transformations {
        match f {
            // Row -> Row
            Transformation::RowToRow {
                input,
                output,
                flow,
            } => {
                let inp = fp_to_var(fp2ident, input.fingerprint());
                let out = fp_to_var(fp2ident, output.fingerprint());
                let input_arity = input.arity().1;

                let (row_pat, row_fields) = row_pattern_and_fields(input_arity);
                let itype = find_fingerprint_type(fp2type, input.fingerprint());

                // Check if output type already exists, if so assert it matches, otherwise insert
                insert_or_verify_type(fp2type, output.fingerprint(), itype);

                let row_ty = row_type_tokens(itype, input_arity);
                let out_val = build_key_val_from_row_args(flow.value(), &row_fields);
                // Optional filter on comparisons and constraints
                let cmp_pred = build_row_compare_predicate(flow.compares(), &row_fields);
                let cst_pred = build_row_constraints_predicate(flow.constraints(), &row_fields);
                let pred = combine_predicates(cmp_pred, cst_pred);

                if let Some(pred) = pred {
                    v.push(quote! {
                        let #out = #inp
                            .flat_map(|#row_pat: #row_ty| {
                                if #pred { Some( ( (), #out_val ) ) } else { None }
                            })
                            .inspect(|rec| eprintln!("[op][RowToRow->{}] {:?}", stringify!(#out), rec));
                    });
                } else {
                    // For Row -> Row, use empty key () and put data in value
                    v.push(quote! {
                        let #out = #inp
                            .flat_map(|#row_pat: #row_ty| std::iter::once( ( (), #out_val ) ))
                            .inspect(|rec| eprintln!("[op][RowToRow->{}] {:?}", stringify!(#out), rec));
                    });
                }
            }

            // Row -> K
            Transformation::RowToK {
                input,
                output,
                flow,
            } => {
                let inp = fp_to_var(fp2ident, input.fingerprint());
                let out = fp_to_var(fp2ident, output.fingerprint());
                let input_arity = input.arity().1;

                let (row_pat, row_fields) = row_pattern_and_fields(input_arity);
                let itype = find_fingerprint_type(fp2type, input.fingerprint());

                // Check if output type already exists, if so assert it matches, otherwise insert
                insert_or_verify_type(fp2type, output.fingerprint(), itype);

                let row_ty = row_type_tokens(itype, input_arity);
                let out_key = build_key_val_from_row_args(flow.key(), &row_fields);
                let cmp_pred = build_row_compare_predicate(flow.compares(), &row_fields);
                let cst_pred = build_row_constraints_predicate(flow.constraints(), &row_fields);
                let pred = combine_predicates(cmp_pred, cst_pred);

                if let Some(pred) = pred {
                    v.push(quote! {
                        let #out = #inp
                            .flat_map(|#row_pat: #row_ty| {
                                if #pred { Some( ( #out_key, () ) ) } else { None }
                            })
                            .inspect(|rec| eprintln!("[op][RowToK->{}] {:?}", stringify!(#out), rec));
                    });
                } else {
                    // For Row -> K, put data in key and use empty value ()
                    v.push(quote! {
                        let #out = #inp
                            .flat_map(|#row_pat: #row_ty| std::iter::once( ( #out_key, () ) ))
                            .inspect(|rec| eprintln!("[op][RowToK->{}] {:?}", stringify!(#out), rec));
                    });
                }
            }

            // Row -> KV
            Transformation::RowToKv {
                input,
                output,
                flow,
            } => {
                let inp = fp_to_var(fp2ident, input.fingerprint());
                let out = fp_to_var(fp2ident, output.fingerprint());
                let input_arity = input.arity().1;

                let (row_pat, row_fields) = row_pattern_and_fields(input_arity);
                let itype = find_fingerprint_type(fp2type, input.fingerprint());

                // Check if output type already exists, if so assert it matches, otherwise insert
                insert_or_verify_type(fp2type, output.fingerprint(), itype);

                let row_ty = row_type_tokens(itype, input_arity);
                let out_key = build_key_val_from_row_args(flow.key(), &row_fields);
                let out_val = build_key_val_from_row_args(flow.value(), &row_fields);
                let cmp_pred = build_row_compare_predicate(flow.compares(), &row_fields);
                let cst_pred = build_row_constraints_predicate(flow.constraints(), &row_fields);
                let pred = combine_predicates(cmp_pred, cst_pred);

                if let Some(pred) = pred {
                    v.push(quote! {
                        let #out = #inp
                            .flat_map(|#row_pat: #row_ty| {
                                if #pred { Some( ( #out_key, #out_val ) ) } else { None }
                            })
                            .inspect(|rec| eprintln!("[op][RowToKv->{}] {:?}", stringify!(#out), rec));
                    });
                } else {
                    v.push(quote! {
                        let #out = #inp
                            .flat_map(|#row_pat: #row_ty| std::iter::once( ( #out_key, #out_val ) ))
                            .inspect(|rec| eprintln!("[op][RowToKv->{}] {:?}", stringify!(#out), rec));
                    });
                }
            }

            // KV -> KV
            Transformation::KvToKv {
                input,
                output,
                flow,
            } => {
                let inp = fp_to_var(fp2ident, input.fingerprint());
                let out = fp_to_var(fp2ident, output.fingerprint());

                let out_key = build_key_val_from_kv_args(flow.key());
                let out_val = build_key_val_from_kv_args(flow.value());
                let cmp_pred = build_kv_compare_predicate(flow.compares());
                let cst_pred = build_kv_constraints_predicate(flow.constraints());
                let pred = combine_predicates(cmp_pred, cst_pred);

                if let Some(pred) = pred {
                    v.push(quote! {
                        let #out = #inp
                            .flat_map(|(k, v)| {
                                if #pred { Some( ( #out_key, #out_val ) ) } else { None }
                            })
                            .inspect(|rec| eprintln!("[op][KvToKv->{}] {:?}", stringify!(#out), rec));
                    });
                } else {
                    v.push(quote! {
                        let #out = #inp
                            .flat_map(|(k, v)| std::iter::once( ( #out_key, #out_val ) ))
                            .inspect(|rec| eprintln!("[op][KvToKv->{}] {:?}", stringify!(#out), rec));
                    });
                }
            }

            // KV -> K
            Transformation::KvToK {
                input,
                output,
                flow,
            } => {
                let inp = fp_to_var(fp2ident, input.fingerprint());
                let out = fp_to_var(fp2ident, output.fingerprint());

                let out_key = build_key_val_from_kv_args(flow.key());
                let cmp_pred = build_kv_compare_predicate(flow.compares());
                let cst_pred = build_kv_constraints_predicate(flow.constraints());
                let pred = combine_predicates(cmp_pred, cst_pred);

                if let Some(pred) = pred {
                    v.push(quote! {
                        let #out = #inp
                            .flat_map(|(k, v)| {
                                if #pred { Some( ( #out_key, () ) ) } else { None }
                            })
                            .inspect(|rec| eprintln!("[op][KvToK->{}] {:?}", stringify!(#out), rec));
                    });
                } else {
                    v.push(quote! {
                        let #out = #inp
                            .flat_map(|(k, v)| std::iter::once( ( #out_key, () ) ))
                            .inspect(|rec| eprintln!("[op][KvToK->{}] {:?}", stringify!(#out), rec));
                    });
                }
            }

            // KV -> Row
            Transformation::KvToRow {
                input,
                output,
                flow,
            } => {
                let inp = fp_to_var(fp2ident, input.fingerprint());
                let out = fp_to_var(fp2ident, output.fingerprint());

                let out_val = build_key_val_from_kv_args(flow.value());
                let cmp_pred = build_kv_compare_predicate(flow.compares());
                let cst_pred = build_kv_constraints_predicate(flow.constraints());
                let pred = combine_predicates(cmp_pred, cst_pred);

                if let Some(pred) = pred {
                    v.push(quote! {
                        let #out = #inp
                            .flat_map(|(k, v)| {
                                if #pred { Some( ( (), #out_val ) ) } else { None }
                            })
                            .inspect(|rec| eprintln!("[op][KvToRow->{}] {:?}", stringify!(#out), rec));
                    });
                } else {
                    v.push(quote! {
                        let #out = #inp
                            .flat_map(|(k, v)| std::iter::once( ( (), #out_val ) ))
                            .inspect(|rec| eprintln!("[op][KvToRow->{}] {:?}", stringify!(#out), rec));
                    });
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
                let l_base = fp_to_var(fp2ident, left.fingerprint());
                let r_base = fp_to_var(fp2ident, right.fingerprint());

                let l = if let Some(arr) = arranged_map.get(&left.fingerprint()).cloned() {
                    arr
                } else {
                    let arr = quote::format_ident!("{}_arr", l_base);
                    v.push(quote! { let #arr = #l_base.arrange_by_key(); });
                    arranged_map.insert(left.fingerprint(), arr.clone());
                    arr
                };

                let r = if let Some(arr) = arranged_map.get(&right.fingerprint()).cloned() {
                    arr
                } else {
                    let arr = quote::format_ident!("{}_arr", r_base);
                    v.push(quote! { let #arr = #r_base.arrange_by_key(); });
                    arranged_map.insert(right.fingerprint(), arr.clone());
                    arr
                };
                let out = fp_to_var(fp2ident, output.fingerprint());

                // Determine operation name for logging
                let op_name = match f {
                    Transformation::JnKK { .. } => "JoinKK",
                    Transformation::JnKKv { .. } => "JoinKKv",
                    Transformation::JnKvKv { .. } => "JoinKvKv",
                    Transformation::Cartesian { .. } => "Cartesian",
                    _ => unreachable!(),
                };

                // Generate output expressions based on flow (handles all variations)
                let out_key = build_key_val_from_join_args(flow.key());
                let out_val = build_key_val_from_join_args(flow.value());

                let (jn_k, jn_lv, jn_rv) = super::arg::compute_join_param_tokens(
                    flow.key(),
                    flow.value(),
                    flow.compares(),
                );

                if let Some(pred) = build_join_compare_predicate(flow.compares()) {
                    v.push(quote! {
                        let #out =
                            #l
                                .join_core(&#r, |#jn_k, #jn_lv, #jn_rv| {
                                    let out_key = #out_key;
                                    let out_val = #out_val;
                                    if #pred { Some((out_key, out_val)) } else { None }
                                })
                                .inspect(|rec| eprintln!("[op][{}->{}] {:?}", #op_name, stringify!(#out), rec));
                    });
                } else {
                    v.push(quote! {
                        let #out =
                            #l
                                .join_core(&#r, |#jn_k, #jn_lv, #jn_rv| {
                                    let out_key = #out_key;
                                    let out_val = #out_val;
                                    Some((out_key, out_val))
                                })
                                .inspect(|rec| eprintln!("[op][{}->{}] {:?}", #op_name, stringify!(#out), rec));
                    });
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
                let l_base = fp_to_var(fp2ident, left.fingerprint());
                let r_base = fp_to_var(fp2ident, right.fingerprint());

                let l = if let Some(arr) = arranged_map.get(&left.fingerprint()).cloned() {
                    arr
                } else {
                    let arr = quote::format_ident!("{}_arr", l_base);
                    v.push(quote! { let #arr = #l_base.arrange_by_key(); });
                    arranged_map.insert(left.fingerprint(), arr.clone());
                    arr
                };

                let r = if let Some(arr) = arranged_map.get(&right.fingerprint()).cloned() {
                    arr
                } else {
                    let arr = quote::format_ident!("{}_arr", r_base);
                    v.push(quote! { let #arr = #r_base.arrange_by_key(); });
                    arranged_map.insert(right.fingerprint(), arr.clone());
                    arr
                };
                let out = fp_to_var(fp2ident, output.fingerprint());

                // Determine operation name for logging
                let op_name = match f {
                    Transformation::NjKK { .. } => "AntijoinKK",
                    Transformation::NjKKv { .. } => "AntijoinKKv",
                    _ => unreachable!(),
                };

                // Generate output expressions based on flow (handles all variations)
                let out_key = build_key_val_from_join_args(flow.key());
                let out_val = build_key_val_from_join_args(flow.value());

                let (antijoin_k, antijoin_v) =
                    super::arg::compute_anti_join_param_tokens(flow.key(), flow.value());

                v.push(quote! {
                    let #out =
                        #l
                            .antijoin(&#r)
                            .map(|(#antijoin_k, #antijoin_v)| {
                                let out_key = #out_key;
                                let out_val = #out_val;
                                (out_key, out_val)
                            })
                            .inspect(|rec| eprintln!("[op][{}->{}] {:?}", #op_name, stringify!(#out), rec));
                });
            }
        }
    }

    trace!("Generated static flows:\n{}\n", quote! { #(#v)* });
    v
}
