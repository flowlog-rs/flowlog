use proc_macro2::{Ident, TokenStream};
use quote::quote;
use std::collections::HashMap;

use super::arg::{
    build_key_val_from_join_args, build_key_val_from_kv_args, build_key_val_from_row_args,
};
use super::comm::{fp_to_var, row_pattern_and_fields, row_type_tokens};

use parser::DataType;
use planner::Transformation;

/// Generate the non-recursive (static) differential dataflow pipelines.
pub fn gen_static_flows(
    fp2ident: &HashMap<u64, Ident>,
    transformations: &[Transformation],
) -> Vec<TokenStream> {
    let mut v = Vec::new();
    // Stratum-scoped cache of arrangements; emit arrange_by_key just before first use.
    let mut arranged_map: HashMap<u64, Ident> = HashMap::new();

    for f in transformations {
        match f {
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
                let itype = s
                    .inputs()
                    .values()
                    .find_map(|t| if t.1 == *input { Some(t.3) } else { None })
                    .unwrap_or(InputType::Numeric);
                let row_ty = row_type_tokens(itype, input_arity);
                let out_key = build_key_val_from_row_args(flow.key(), &row_fields);
                let out_val = build_key_val_from_row_args(flow.value(), &row_fields);

                v.push(quote! {
                    let #out = #inp
                        .flat_map(|#row_pat: #row_ty| std::iter::once( ( #out_key, #out_val ) ))
                        .inspect(|rec| eprintln!("[op][RowToKv->{}] {:?}", stringify!(#out), rec));
                });
            }

            // KV -> KV
            Flow::KvToKv {
                input,
                input_key_arity: _,
                input_value_arity: _,
                output,
                key,
                value,
            } => {
                let inp = fp_to_var(id2ident, *input);
                let out = fp_to_var(id2ident, *output);

                let out_key = build_key_val_from_kv_args(key);
                let out_val = build_key_val_from_kv_args(value);
                v.push(quote! {
                    let #out = #inp
                        .flat_map(|(k, v)| std::iter::once( ( #out_key, #out_val ) ))
                        .inspect(|rec| eprintln!("[op][KvToKv->{}] {:?}", stringify!(#out), rec));
                });
            }

            // KV ⋈ KV on keys (annotate lv/rv value types)
            Flow::JoinKvKv {
                left,
                left_key_arity: _,
                left_value_arity: _,
                right,
                right_key_arity: _,
                right_value_arity: _,
                output,
                key,
                value,
            } => {
                // Arrange inputs on first use, then reuse the arranged names.
                let l_base = fp_to_var(id2ident, *left);
                let r_base = fp_to_var(id2ident, *right);

                let l = if let Some(arr) = arranged_map.get(left).cloned() {
                    arr
                } else {
                    let arr = quote::format_ident!("{}_arr", l_base);
                    v.push(quote! { let #arr = #l_base.arrange_by_key(); });
                    arranged_map.insert(*left, arr.clone());
                    arr
                };

                let r = if let Some(arr) = arranged_map.get(right).cloned() {
                    arr
                } else {
                    let arr = quote::format_ident!("{}_arr", r_base);
                    v.push(quote! { let #arr = #r_base.arrange_by_key(); });
                    arranged_map.insert(*right, arr.clone());
                    arr
                };
                let out = fp_to_var(id2ident, *output);

                let out_key = build_key_val_from_join_args(key);
                let out_val = build_key_val_from_join_args(value);

                v.push(quote! {
                    let #out =
                        #l
                            .join_core(&#r, |k, lv, rv| {
                                let out_key = #out_key;
                                let out_val = #out_val;
                                Some((out_key, out_val))
                            })
                            .inspect(|rec| eprintln!("[op][JoinKvKv/core->{}] {:?}", stringify!(#out), rec));
                });
            }
        }
    }

    print!("Generated static flows:\n{}\n", quote! { #(#v)* });
    v
}
