//! Recursive flow generation for FlowLog compiler.
//!
//! This module handles the generation of recursive differential dataflow
//! pipelines within recursive strata.
//! It manages the iterative blocks, feedback loops, and unioning of IDB relations.
//! It is worth noticing that due to factoring optimization, we need to maintain a local
//! ident map derived from both non-recursive arrangements and global idents.

use std::collections::HashMap;

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};

use crate::aggregation::{aggregation_merge_kv, aggregation_reduce, aggregation_row_chop};
use crate::ident::find_local_ident;
use crate::Compiler;

use parser::AggregationOperator;
use planner::StratumPlanner;
use profiler::Profiler;

// =========================================================================
// Recursive Flow Generation
// =========================================================================
impl Compiler {
    /// Generate the iterative block for recursive rules (IDBs).
    pub(crate) fn gen_iterative_block(
        &mut self,
        non_recursive_arranged_map: &HashMap<u64, Ident>,
        stratum: &StratumPlanner,
        profiler: &mut Option<Profiler>,
    ) -> TokenStream {
        self.imports.mark_recursive();

        if let Some(profiler) = profiler.as_mut() {
            profiler.enter_scope();
        }

        let leave_fps = stratum.recursion_leave_collections();
        if leave_fps.is_empty() {
            return quote! {};
        }

        let enter_fps = stratum.recursion_enter_collections();
        let (enter_stmts, enter_bindings, mut recursive_arranged) =
            self.build_enter_bindings(non_recursive_arranged_map, enter_fps, profiler);

        let iterative_fps = stratum.recursion_iterative_collections();
        let (iter_names, iter_bindings) = self.build_iterative_bindings(iterative_fps);

        let iter_var_inits: Vec<TokenStream> = iter_names
            .iter()
            .map(|name| {
                if let Some(profiler) = profiler.as_mut() {
                    profiler.recursive_feedback_operator(
                        name.to_string(),
                        name.to_string(),
                        name.to_string(),
                    );
                }
                quote! { let #name = SemigroupVariable::new(inner, timely::order::Product::new(Default::default(), 1)); }
            })
            .collect();

        let mut current: HashMap<u64, Ident> = enter_bindings.clone();
        current.extend(iter_bindings.clone());

        let flow_stmts: Vec<TokenStream> = stratum
            .recursive_transformations()
            .iter()
            .map(|tx| self.gen_transformation(&current, tx, &mut recursive_arranged, profiler))
            .collect();

        let (next_bindings, union_stmts) = self.collect_unions(
            stratum.output_to_idb_map(),
            &enter_bindings,
            stratum.output_to_aggregation_map(),
            profiler,
        );

        let set_stmts: Vec<TokenStream> = next_bindings
            .iter()
            .map(|(fp, next_ident)| {
                let iter_var = find_local_ident(&current, *fp);
                if let Some(profiler) = profiler.as_mut() {
                    profiler.recursive_resultsin_operator(
                        iter_var.to_string(),
                        next_ident.to_string(),
                        next_ident.to_string(),
                    );
                }
                quote! { #iter_var.set(&#next_ident); }
            })
            .collect();

        if let Some(profiler) = profiler.as_mut() {
            profiler.leave_scope();
        }

        let (leave_pattern, leave_stmt) =
            self.build_leave_outputs(leave_fps, &next_bindings, profiler);

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

    /// Build the enter bindings for arrangements entering recursion.
    fn build_enter_bindings(
        &self,
        non_recursive_arranged_map: &HashMap<u64, Ident>,
        enter_fps: &[u64],
        profiler: &mut Option<Profiler>,
    ) -> (Vec<TokenStream>, HashMap<u64, Ident>, HashMap<u64, Ident>) {
        let mut bindings: HashMap<u64, Ident> = HashMap::new();
        let mut stmts: Vec<TokenStream> = Vec::new();
        let mut recursive_arranged: HashMap<u64, Ident> = HashMap::new();

        for fp in enter_fps {
            let source = non_recursive_arranged_map
                .get(fp)
                .cloned()
                .unwrap_or_else(|| self.find_global_ident(*fp));
            let entered = format_ident!("in_{}", source);
            bindings.insert(*fp, entered.clone());
            stmts.push(quote! { let #entered = #source.enter(inner); });

            // record profiling information if enabled
            if let Some(profiler) = profiler.as_mut() {
                profiler.recursive_enter_operator(
                    source.to_string(),
                    source.to_string(),
                    entered.to_string(),
                );
            }

            if non_recursive_arranged_map.contains_key(fp) {
                let entered_arr =
                    format_ident!("in_{}", non_recursive_arranged_map.get(fp).unwrap());
                recursive_arranged.insert(*fp, entered_arr);
            }
        }

        (stmts, bindings, recursive_arranged)
    }

    /// Collect union statements for all IDB relations in the stratum.
    fn collect_unions(
        &mut self,
        output_to_idb_map: &HashMap<u64, Vec<u64>>,
        enter_bindings: &HashMap<u64, Ident>,
        output_to_aggregation_map: &HashMap<u64, (AggregationOperator, usize, usize)>,
        profiler: &mut Option<Profiler>,
    ) -> (HashMap<u64, Ident>, Vec<TokenStream>) {
        let mut next_bindings: HashMap<u64, Ident> = HashMap::new();
        let mut union_stmts = Vec::new();

        for (output_fp, idb_fps) in output_to_idb_map {
            let next_ident = format_ident!("next_{}", output_fp);
            next_bindings.insert(*output_fp, next_ident.clone());

            let mut sources: Vec<Ident> =
                idb_fps.iter().map(|fp| format_ident!("t_{}", fp)).collect();

            if let Some(entered) = enter_bindings.get(output_fp) {
                sources.push(entered.clone());
            }

            let (head, tail) = sources
                .split_first()
                .expect("at least one source collection for union");

            let union_expr = tail.iter().fold(quote! { #head }, |ts, ident| {
                quote! { #ts.concat(&#ident) }
            });

            let dedup_call = self.dedup_collection();
            let mut block = quote! {
                let #next_ident = #union_expr #dedup_call;
            };

            // Record profiling information if enabled
            if let Some(profiler) = profiler.as_mut() {
                let output_name = self.find_global_ident(*output_fp).to_string();
                if sources.len() > 1 {
                    profiler.concat_operator(
                        output_name,
                        sources.iter().map(|id| id.to_string()).collect(),
                        next_ident.to_string(),
                        sources.len() as u32 - 1,
                    );
                } else {
                    profiler.input_dedup_operator(
                        output_name,
                        sources[0].to_string(),
                        next_ident.to_string(),
                    );
                }
            }

            if let Some((agg_op, agg_pos, agg_arity)) = output_to_aggregation_map.get(output_fp) {
                self.imports.mark_aggregation();
                self.imports.mark_as_collection();
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

    /// Build the leave collections for recursion.
    fn build_leave_outputs(
        &self,
        leave_fps: &[u64],
        next: &HashMap<u64, Ident>,
        profiler: &mut Option<Profiler>,
    ) -> (TokenStream, TokenStream) {
        let targets: Vec<Ident> = leave_fps
            .iter()
            .map(|fp| self.find_global_ident(*fp))
            .collect();

        let pattern = match targets.as_slice() {
            [ident] => quote! { #ident },
            _ => quote! { ( #(#targets),* ) },
        };

        let leave_exprs: Vec<TokenStream> = leave_fps
            .iter()
            .zip(targets.iter())
            .map(|(fp, target)| {
                let next_ident = next
                    .get(fp)
                    .expect("leave relation missing from next bindings during recursion");
                if let Some(profiler) = profiler.as_mut() {
                    profiler.recursive_leave_operator(
                        target.to_string(),
                        next_ident.to_string(),
                        target.to_string(),
                    );
                }
                quote! { #next_ident.leave() }
            })
            .collect();

        let leave_stmt = match leave_exprs.as_slice() {
            [expr] => quote! { #expr },
            _ => quote! { ( #(#leave_exprs),* ) },
        };

        (pattern, leave_stmt)
    }

    /// Build the iterative variable bindings for recursion.
    fn build_iterative_bindings(&self, iterative_fps: &[u64]) -> (Vec<Ident>, HashMap<u64, Ident>) {
        let names: Vec<Ident> = iterative_fps
            .iter()
            .map(|fp| {
                let name = self.find_global_ident(*fp);
                format_ident!("iter_{}", name)
            })
            .collect();

        let bindings = iterative_fps
            .iter()
            .zip(names.iter().cloned())
            .map(|(fp, ident)| (*fp, ident))
            .collect();

        (names, bindings)
    }
}
