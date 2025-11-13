use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use std::collections::HashMap;

use super::arg::{
    build_key_val_from_join_args, build_key_val_from_kv_args, build_key_val_from_row_args,
};
use super::comm::{
    find_fingerprint_type, insert_or_verify_type, row_pattern_and_fields, row_type_tokens,
};

use parser::DataType;
use planner::{transformation, Transformation};

// With Stratum providing idb -> [rule_outputs], we can use it directly. Given the
// user guarantees flow order (producers before consumers), we can generate code
// in a single forward pass without recursive ensure logic.

/// Generate the iterative block for recursive rules (IDBs).
pub fn gen_iterative_block(
    fp2ident: &HashMap<u64, Ident>,
    fp2type: &mut HashMap<u64, DataType>,
    transformations: &[Transformation],
) -> TokenStream {
    // // Deterministic IDB groups from Stratum
    // let idb_groups_sorted = s.idb_groups_sorted();
    // if idb_groups_sorted.is_empty() {
    //     return quote! {};
    // }

    // let idb_list: Vec<u64> = idb_groups_sorted.iter().map(|(idb, _)| *idb).collect();

    // let v_names: Vec<_> = idb_list
    //     .iter()
    //     .map(|idb| format_ident!("v_{}", idb))
    //     .collect();
    // let n_names: Vec<_> = idb_list
    //     .iter()
    //     .map(|idb| format_ident!("n_{}", idb))
    //     .collect();

    // // Enter EDBs once
    // let enter_stmts: Vec<TokenStream> = s
    //     .inputs()
    //     .iter()
    //     .map(|(_name, (_arity, id, _path, _itype))| {
    //         let id = *id;
    //         let coll = id2ident[&id].clone();
    //         let entered = format_ident!("{}_in", coll);
    //         quote! { let #entered = #coll.enter(inner); }
    //     })
    //     .collect();

    // // Single-pass flow generation with simple caches
    // let mut arranged_map: HashMap<u64, Ident> = HashMap::new();
    // let mut current: HashMap<u64, Ident> = HashMap::new();

    // // Seed current with EDBs and IDBs
    // for (_name, (_arity, id, _path, _itype)) in s.inputs() {
    //     current.insert(*id, format_ident!("{}_in", id2ident[id]));
    // }
    // for idb in &idb_list {
    //     current.insert(*idb, format_ident!("v_{}", idb));
    // }

    // // Emit all flow pipelines in order; joins arrange just-in-time.
    // let mut flow_stmts: Vec<TokenStream> = Vec::new();
    // for f in s.flows() {
    //     match f {
    //         Transformation::RowToKv {
    //             input,
    //             output,
    //             flow,
    //         } => {
    //             let inp = current[input].clone();
    //             let bind = format_ident!("t_{}", output);
    //             let (row_pat, row_fields) = row_pattern_and_fields(*input_arity);
    //             let itype = find_fingerprint_type(fp2type, input.fingerprint());

    //             // Check if output type already exists, if so assert it matches, otherwise insert
    //             insert_or_verify_type(fp2type, output.fingerprint(), itype);
    //             let row_ty = row_type_tokens(itype, *input_arity);
    //             let out_key = build_key_val_from_row_args(key, &row_fields);
    //             let out_val = build_key_val_from_row_args(value, &row_fields);

    //             flow_stmts.push(quote! {
    //                 let #bind = #inp
    //                     .flat_map(|#row_pat: #row_ty| std::iter::once(( #out_key, #out_val )));
    //             });

    //             current.insert(*output, bind);
    //         }
    //     }
    // }

    // // Per-IDB: union all rules and reduce to set semantics
    // let mut union_stmts: Vec<TokenStream> = Vec::new();
    // for (idb_idx, (_idb, rs)) in idb_groups_sorted.iter().enumerate() {
    //     let next_name = &n_names[idb_idx];
    //     let mut outs: Vec<Ident> = Vec::new();
    //     for rid in rs {
    //         outs.push(format_ident!("t_{}", rid));
    //     }
    //     let concat_expr = if outs.is_empty() {
    //         quote! {
    //             differential_dataflow::collection::Collection::<_, (u64, ()), Diff>::empty(inner)
    //         }
    //     } else {
    //         let head = outs[0].clone();
    //         let tail = &outs[1..];
    //         let mut expr: TokenStream = quote! { #head };
    //         for t in tail {
    //             expr = quote! { #expr.concat(&#t) };
    //         }
    //         expr
    //     };

    //     union_stmts.push(quote! {
    //         let #next_name = #concat_expr
    //             .reduce(|_k, _in, out| out.push(((), 1)));
    //     });
    // }

    // let set_stmts: Vec<TokenStream> = v_names
    //     .iter()
    //     .zip(n_names.iter())
    //     .map(|(v, n)| quote! { #v.set(&#n); })
    //     .collect();

    // let vars_new: Vec<TokenStream> = v_names
    //     .iter()
    //     .map(|v| quote! { let #v = Variable::new(inner, timely::order::Product::new(Default::default(), 1)); })
    //     .collect();

    // let leaves: Vec<TokenStream> = n_names.iter().map(|n| quote! { #n.leave() }).collect();

    // quote! {
    //     let _rec_outputs = scope.iterative::<u64, _, _>(|inner| {
    //         #(#enter_stmts)*

    //         #(#vars_new)*

    //         // all flow pipelines
    //         #(#flow_stmts)*

    //         // unions per IDB
    //         #(#union_stmts)*

    //         // feedback
    //         #(#set_stmts)*

    //         ( #(#leaves),* )
    //     });
    // }

    todo!("Generate iterative block for recursive rules")
}
