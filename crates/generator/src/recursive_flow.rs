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
    // No recursive relations, no iterative block needed
    if leave_rels.is_empty() {
        return quote! {};
    }

    // Iterative relations for the stratum
    let iterative_rels = stratum_planner.dynamic_iterative_collections();
    let iter_names: Vec<_> = iterative_rels
        .iter()
        .map(|idb| format_ident!("iter_{}", idb))
        .collect();

    // Enter relations for the stratum
    let mut enter: HashMap<u64, Ident> = HashMap::new();
    let enter_rels = stratum_planner.dynamic_enter_collections();
    let enter_stmts: Vec<TokenStream> = enter_rels
        .iter()
        .map(|fp| {
            let coll = find_ident(fp_to_ident, *fp);
            let entered = format_ident!("in_{}", *fp);
            quote! { let #entered = #coll.enter(inner); }
        })
        .collect();
    for enter_fp in enter_rels {
        enter.insert(*enter_fp, format_ident!("in_{}", *enter_fp));
    }

    // Initialize arranged map and current/next iteration maps
    let mut arranged_map: HashMap<u64, Ident> = HashMap::new();
    let mut current: HashMap<u64, Ident> = HashMap::new();

    // Seed current with entered EDBs and local variables
    for enter_fp in enter_rels {
        current.insert(*enter_fp, format_ident!("in_{}", *enter_fp));
    }
    for iter_fp in iterative_rels {
        current.insert(*iter_fp, format_ident!("iter_{}", *iter_fp));
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
    let mut next: HashMap<u64, Ident> = HashMap::new();
    let mut union_stmts: Vec<TokenStream> = Vec::new();
    for (output_fp, idb_fps) in stratum_planner.output_to_idb_map() {
        let next_ident = format_ident!("next_{}", *output_fp);
        next.insert(*output_fp, next_ident.clone());
        let mut outs: Vec<Ident> = Vec::new();
        for idb_fp in idb_fps {
            outs.push(format_ident!("t_{}", idb_fp));
        }
        if enter.contains_key(output_fp) {
            outs.push(find_ident(&enter, *output_fp));
        }

        let head = outs[0].clone();
        let tail = &outs[1..];
        let mut expr: TokenStream = quote! { #head };
        for t in tail {
            expr = quote! { #expr.concat(&#t) };
        }

        union_stmts.push(quote! {
            let #next_ident = #expr.distinct();
        });
    }

    let set_stmts: Vec<TokenStream> = next
        .iter()
        .map(|(fp, next_ident)| {
            let iter_var = find_ident(&current, *fp);
            quote! { #iter_var.set(&#next_ident); }
        })
        .collect();

    let vars_new: Vec<TokenStream> = iter_names
        .iter()
        .map(|v| quote! { let #v = Variable::new(inner, timely::order::Product::new(Default::default(), 1)); })
        .collect();

    let leaves: Vec<TokenStream> = leave_rels
        .iter()
        .map(|fp| {
            let n = next.get(fp);
            quote! { #n.leave() }
        })
        .collect();

    let leave_stmts = match leaves.as_slice() {
        [leave_expr] => quote! { #leave_expr },
        _ => quote! { ( #(#leaves),* ) },
    };

    let leave_idents: Vec<Ident> = leave_rels
        .iter()
        .map(|fp| find_ident(fp_to_ident, *fp))
        .collect();

    let leave_pattern = match leave_idents.as_slice() {
        [ident] => quote! { #ident },
        _ => quote! { (#(#leave_idents),*) },
    };

    quote! {
        let #leave_pattern = scope.iterative::<u64, _, _>(|inner| {
            #(#enter_stmts)*

            #(#vars_new)*

            // all flow pipelines
            #(#flow_stmts)*

            // unions per IDB
            #(#union_stmts)*

            // feedback
            #(#set_stmts)*

            #leave_stmts
        });
    }
}
