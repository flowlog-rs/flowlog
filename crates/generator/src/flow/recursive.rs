use std::collections::HashMap;

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};

use crate::aggregation::{aggregation_merge_kv, aggregation_reduce, aggregation_row_chop};
use crate::ident::find_ident;
use crate::transformation::gen_transformation;

use parser::DataType;
use planner::StratumPlanner;

// =========================================================================
// Recursive Flow Generation
// =========================================================================

/// Generate the iterative block for recursive rules (IDBs).
pub(crate) fn gen_iterative_block(
    fp_to_ident: &HashMap<u64, Ident>,
    fp_to_type: &mut HashMap<u64, DataType>,
    non_recursive_arranged_map: &HashMap<u64, Ident>,
    stratum_planner: &StratumPlanner,
    output_to_aggregation_map: &HashMap<u64, (parser::AggregationOperator, usize, usize)>,
) -> TokenStream {
    let leave_fps = stratum_planner.recursion_leave_collections();
    if leave_fps.is_empty() {
        return quote! {};
    }

    let iterative_fps = stratum_planner.recursion_iterative_collections();
    let (iter_names, iter_bindings) = build_iterative_bindings(iterative_fps);

    let enter_fps = stratum_planner.recursion_enter_collections();
    let (enter_stmts, enter_bindings, mut recursive_arranged) =
        build_enter_bindings(fp_to_ident, non_recursive_arranged_map, enter_fps);

    let mut current: HashMap<u64, Ident> = enter_bindings.clone();
    current.extend(iter_bindings.clone());

    let flow_stmts: Vec<TokenStream> = stratum_planner
        .recursive_transformations()
        .iter()
        .map(|tx| gen_transformation(&current, fp_to_type, tx, &mut recursive_arranged))
        .collect();

    let (next_bindings, union_stmts) = collect_unions(
        stratum_planner.output_to_idb_map(),
        &enter_bindings,
        output_to_aggregation_map,
    );

    let set_stmts: Vec<TokenStream> = next_bindings
        .iter()
        .map(|(fp, next_ident)| {
            let iter_var = find_ident(&current, *fp);
            quote! { #iter_var.set(&#next_ident); }
        })
        .collect();

    let iter_var_inits: Vec<TokenStream> = iter_names
        .iter()
        .map(|name| {
            quote! { let #name = SemigroupVariable::new(inner, timely::order::Product::new(Default::default(), 1)); }
        })
        .collect();

    let (leave_pattern, leave_stmt) = build_leave_outputs(leave_fps, &next_bindings, fp_to_ident);

    quote! {
        let #leave_pattern = scope.iterative::<Iter, _, _>(|inner| {
            #(#enter_stmts)*
            #(#iter_var_inits)*

            // === Recursive rule pipelines ===
            #(#flow_stmts)*

            // === Union per IDB ===
            #(#union_stmts)*

            // === Feedback ===
            #(#set_stmts)*

            #leave_stmt
        });
    }
}

fn build_iterative_bindings(iterative_fps: &[u64]) -> (Vec<Ident>, HashMap<u64, Ident>) {
    let names: Vec<Ident> = iterative_fps
        .iter()
        .map(|fp| format_ident!("iter_{}", fp))
        .collect();

    let bindings = iterative_fps
        .iter()
        .zip(names.iter().cloned())
        .map(|(fp, ident)| (*fp, ident))
        .collect();

    (names, bindings)
}

fn build_enter_bindings(
    fp_to_ident: &HashMap<u64, Ident>,
    non_recursive_arranged_map: &HashMap<u64, Ident>,
    enter_fps: &[u64],
) -> (Vec<TokenStream>, HashMap<u64, Ident>, HashMap<u64, Ident>) {
    let mut bindings: HashMap<u64, Ident> = HashMap::new();
    let mut stmts: Vec<TokenStream> = Vec::new();
    let mut recursive_arranged: HashMap<u64, Ident> = HashMap::new();

    for fp in enter_fps {
        let source = non_recursive_arranged_map
            .get(fp)
            .cloned()
            .unwrap_or_else(|| find_ident(fp_to_ident, *fp));
        let entered = format_ident!("in_{}", source);
        bindings.insert(*fp, entered.clone());
        stmts.push(quote! { let #entered = #source.enter(inner); });

        if non_recursive_arranged_map.contains_key(fp) {
            let entered_arr = format_ident!("in_{}", non_recursive_arranged_map.get(fp).unwrap());
            recursive_arranged.insert(*fp, entered_arr);
        }
    }

    (stmts, bindings, recursive_arranged)
}

fn collect_unions(
    output_to_idb_map: &HashMap<u64, Vec<u64>>,
    enter_bindings: &HashMap<u64, Ident>,
    output_to_aggregation_map: &HashMap<u64, (parser::AggregationOperator, usize, usize)>,
) -> (HashMap<u64, Ident>, Vec<TokenStream>) {
    let mut next_bindings: HashMap<u64, Ident> = HashMap::new();
    let mut union_stmts = Vec::new();

    for (output_fp, idb_fps) in output_to_idb_map {
        let next_ident = format_ident!("next_{}", output_fp);
        next_bindings.insert(*output_fp, next_ident.clone());

        let mut sources: Vec<Ident> = idb_fps.iter().map(|fp| format_ident!("t_{}", fp)).collect();

        if let Some(entered) = enter_bindings.get(output_fp) {
            sources.push(entered.clone());
        }

        let (head, tail) = sources
            .split_first()
            .expect("at least one source collection for union");

        let union_expr = tail.iter().fold(quote! { #head }, |ts, ident| {
            quote! { #ts.concat(&#ident) }
        });

        let mut block = quote! {
            let #next_ident = #union_expr.threshold_semigroup(move |_, _, old| old.is_none().then_some(SEMIRING_ONE));
        };

        if let Some((agg_op, agg_pos, agg_arity)) = output_to_aggregation_map.get(output_fp) {
            let row_chop = aggregation_row_chop(*agg_arity, *agg_pos);
            let reduce_logic = aggregation_reduce(agg_op);
            let merge_kv = aggregation_merge_kv(*agg_arity, *agg_pos);
            block = quote! {
                #block
                let #next_ident = #next_ident
                    .map(#row_chop)
                    .reduce_core::<_,ValBuilder<_,_,_,_>,ValSpine<_,_,_,_>>(
                        "aggregation",
                        #reduce_logic
                    )
                    .as_collection(#merge_kv);
            };
        }
        union_stmts.push(block);
    }

    (next_bindings, union_stmts)
}

fn build_leave_outputs(
    leave_fps: &[u64],
    next: &HashMap<u64, Ident>,
    fp_to_ident: &HashMap<u64, Ident>,
) -> (TokenStream, TokenStream) {
    let leave_exprs: Vec<TokenStream> = leave_fps
        .iter()
        .map(|fp| {
            let next_ident = next
                .get(fp)
                .expect("leave relation missing from next bindings during recursion");
            quote! { #next_ident.leave() }
        })
        .collect();

    let leave_stmt = match leave_exprs.as_slice() {
        [expr] => quote! { #expr },
        _ => quote! { ( #(#leave_exprs),* ) },
    };

    let targets: Vec<Ident> = leave_fps
        .iter()
        .map(|fp| find_ident(fp_to_ident, *fp))
        .collect();

    let pattern = match targets.as_slice() {
        [ident] => quote! { #ident },
        _ => quote! { ( #(#targets),* ) },
    };

    (pattern, leave_stmt)
}
