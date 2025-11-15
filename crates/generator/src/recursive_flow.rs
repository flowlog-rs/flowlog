use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use std::collections::HashMap;

use super::ident::find_ident;
use super::transformation::gen_transformation;

use parser::DataType;
use planner::StratumPlanner;

/// =========================================================================
/// Recursive Flow Generation
/// =========================================================================

/// Generate the iterative block for recursive rules (IDBs).
pub fn gen_iterative_block(
    fp_to_ident: &HashMap<u64, Ident>,
    fp_to_type: &mut HashMap<u64, DataType>,
    stratum_planner: &StratumPlanner,
) -> TokenStream {
    // Deterministic leave relations for the stratum
    let leave_rels = stratum_planner.dynamic_leave_collections();
    let iterative_rels = stratum_planner.dynamic_iterative_collections();
    if leave_rels.is_empty() {
        return quote! {};
    }

    let iter_names: Vec<_> = iterative_rels
        .iter()
        .map(|idb| format_ident!("iter_{}", idb))
        .collect();
    let n_names: Vec<_> = iterative_rels
        .iter()
        .map(|idb| format_ident!("in_{}", *idb))
        .collect();

    // Enter EDBs once
    let enter_stmts: Vec<TokenStream> = stratum_planner
        .dynamic_enter_collections()
        .iter()
        .map(|fp| {
            let coll = find_ident(fp_to_ident, *fp);
            let entered = format_ident!("in_{}", *fp);
            quote! { let #entered = #coll.enter(inner); }
        })
        .collect();

    // Single-pass flow generation with simple caches
    let mut arranged_map: HashMap<u64, Ident> = HashMap::new();
    let mut current: HashMap<u64, Ident> = HashMap::new();

    // Seed current with entered EDBs and local variables
    for enter_fp in stratum_planner.dynamic_enter_collections() {
        current.insert(*enter_fp, format_ident!("in_{}", *enter_fp));
    }

    // Emit all flow pipelines in order; joins arrange just-in-time.
    let mut flow_stmts: Vec<TokenStream> = Vec::new();
    for transformation in stratum_planner.dynamic_transformations().iter() {
        flow_stmts.push(gen_transformation(
            &current,
            fp_to_type,
            transformation,
            &mut arranged_map,
        ));
    }

    // Per-IDB: union all rules and reduce to set semantics
    let mut union_stmts: Vec<TokenStream> = Vec::new();
    for (output_fp, idb_fps) in stratum_planner.output_to_idb_map() {
        let output = find_ident(&current, *output_fp);
        let mut outs: Vec<Ident> = Vec::new();
        for idb_fp in idb_fps {
            outs.push(format_ident!("t_{}", idb_fp));
        }

        let head = outs[0].clone();
        let tail = &outs[1..];
        let mut expr: TokenStream = quote! { #head };
        for t in tail {
            expr = quote! { #expr.concat(&#t) };
        }

        union_stmts.push(quote! {
            let #output = #expr.distinct().map(|(_, v)| v);
        });
    }

    let set_stmts: Vec<TokenStream> = iter_names
        .iter()
        .zip(n_names.iter())
        .map(|(v, n)| quote! { #v.set(&#n); })
        .collect();

    let vars_new: Vec<TokenStream> = iter_names
        .iter()
        .map(|v| quote! { let #v = Variable::new(inner, timely::order::Product::new(Default::default(), 1)); })
        .collect();

    let leaves: Vec<TokenStream> = n_names.iter().map(|n| quote! { #n.leave() }).collect();

    quote! {
        let _rec_outputs = scope.iterative::<u64, _, _>(|inner| {
            #(#enter_stmts)*

            #(#vars_new)*

            // all flow pipelines
            #(#flow_stmts)*

            // unions per IDB
            #(#union_stmts)*

            // feedback
            #(#set_stmts)*

            ( #(#leaves),* )
        });
    }
}
