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

use crate::aggregation::{
    aggregation_avg_optimize, aggregation_avg_post_leave, aggregation_avg_pre_leave,
    aggregation_count_optimize, aggregation_count_pre_leave, aggregation_max_optimize,
    aggregation_max_pre_leave, aggregation_merge_kv, aggregation_min_optimize,
    aggregation_min_pre_leave, aggregation_opt_post_leave, aggregation_reduce,
    aggregation_row_chop, aggregation_sum_optimize, aggregation_sum_pre_leave,
};
use crate::ident::find_local_ident;
use crate::udf::scalar::scalar_udf_map;
use crate::Compiler;

use common::ExecutionMode;
use parser::{AggregationOperator, Udf};
use planner::StratumPlanner;
use profiler::{with_profiler, Profiler};

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

        // Profiler: enter recursive scope (optional)
        with_profiler(profiler, |profiler| {
            profiler.enter_scope();
        });

        // Early exit if nothing leaves recursion.
        let leave_fps = stratum.recursion_leave_collections();
        if leave_fps.is_empty() {
            return quote! {};
        }

        // Build enter bindings.
        let enter_fps = stratum.recursion_enter_collections();
        let (enter_stmts, enter_bindings, mut recursive_arranged) =
            self.build_enter_bindings(non_recursive_arranged_map, enter_fps, profiler);

        // Build iterative variable bindings.
        let iterative_fps = stratum.recursion_iterative_collections();
        let (iter_names, iter_bindings) = self.build_iterative_bindings(iterative_fps);

        // Initialize iterative variables and record feedback operators.
        let iter_var_inits: Vec<TokenStream> = iter_names
            .iter()
            .map(|name| {
                // Profiler: feedback operator (optional)
                with_profiler(profiler, |profiler| {
                    profiler.recursive_feedback_operator(
                        name.to_string(),
                        name.to_string(),
                        name.to_string(),
                    );
                });
                match self.config.mode() {
                    ExecutionMode::Batch => {
                        quote! { let #name = SemigroupVariable::new(inner, timely::order::Product::new(Default::default(), 1)); }
                    }
                    ExecutionMode::Incremental => {
                        quote! { let #name = Variable::new(inner, timely::order::Product::new(Default::default(), 1)); }
                    }
                }
            })
            .collect();

        // Compose current binding map (enter + iterative).
        let mut current: HashMap<u64, Ident> = enter_bindings.clone();
        current.extend(iter_bindings.clone());

        // Generate recursive transformations.
        let flow_stmts: Vec<TokenStream> = stratum
            .recursive_transformations()
            .iter()
            .map(|tx| self.gen_transformation(&current, tx, &mut recursive_arranged, profiler))
            .collect();

        // Collect unions and (optional) aggregation/UDF for IDB outputs.
        let (next_bindings, union_stmts) = self.collect_unions(
            stratum.output_to_idb_map(),
            &enter_bindings,
            stratum.output_to_aggregation_map(),
            stratum.output_to_udf_map(),
            profiler,
        );

        // Feedback: set iterative variables.
        let set_stmts: Vec<TokenStream> = next_bindings
            .iter()
            .map(|(fp, next_ident)| {
                let iter_var = find_local_ident(&current, *fp);
                // Profiler: results-in operator (optional)
                with_profiler(profiler, |profiler| {
                    profiler.recursive_resultsin_operator(
                        iter_var.to_string(),
                        next_ident.to_string(),
                        next_ident.to_string(),
                    );
                });
                quote! { #iter_var.set(&#next_ident); }
            })
            .collect();

        // Build leave outputs for recursion.
        let (leave_pattern, leave_stmt, post_leave) = self.build_leave_outputs(
            leave_fps,
            &next_bindings,
            stratum.output_to_aggregation_map(),
            profiler,
        );

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
            #post_leave
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
            // Resolve source collection and create enter binding.
            let source = non_recursive_arranged_map
                .get(fp)
                .cloned()
                .unwrap_or_else(|| self.find_global_ident(*fp));
            let entered = format_ident!("in_{}", source);
            bindings.insert(*fp, entered.clone());
            stmts.push(quote! { let #entered = #source.enter(inner); });

            // Profiler: enter operator (optional)
            with_profiler(profiler, |profiler| {
                profiler.recursive_enter_operator(source.to_string(), entered.to_string());
            });

            // Preserve arranged bindings for recursive paths.
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
        output_to_udf_map: &HashMap<u64, (String, usize, usize, usize)>,
        profiler: &mut Option<Profiler>,
    ) -> (HashMap<u64, Ident>, Vec<TokenStream>) {
        let mut next_bindings: HashMap<u64, Ident> = HashMap::new();
        let mut union_stmts = Vec::new();

        for (output_fp, idb_fps) in output_to_idb_map {
            // Determine output binding name and union sources.
            let next_ident = format_ident!("next_{}", output_fp);
            next_bindings.insert(*output_fp, next_ident.clone());

            let mut sources: Vec<Ident> =
                idb_fps.iter().map(|fp| format_ident!("t_{}", fp)).collect();

            if let Some(entered) = enter_bindings.get(output_fp) {
                sources.push(entered.clone());
            }

            // Build concatenation expression for all sources.
            let (head, tail) = sources
                .split_first()
                .expect("Compiler error: at least one source collection for union");

            let union_expr = tail.iter().fold(quote! { #head }, |ts, ident| {
                quote! { #ts.concat(&#ident) }
            });

            // Apply dedup to merged collection.
            let dedup_call = self.dedup_collection();
            let mut block = quote! {
                let #next_ident = #union_expr #dedup_call;
            };

            // Profiler: union / dedup operator (optional)
            with_profiler(profiler, |profiler| {
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
            });

            if let Some((agg_op, agg_pos, agg_arity)) = output_to_aggregation_map.get(output_fp) {
                let output_name = self.find_global_ident(*output_fp).to_string();
                self.imports.mark_semiring_one();

                // Look up the aggregated column's data type.
                let (key_types, val_types) = self.find_global_type(*output_fp);
                let agg_type = *key_types
                    .iter()
                    .chain(val_types)
                    .nth(*agg_pos)
                    .expect("Compiler error: aggregation position out of bounds");

                // Semiring fast path: replace reduce_core with threshold_semigroup
                // using the appropriate semigroup, avoiding a second arrangement.
                if matches!(self.config.mode(), ExecutionMode::Batch) {
                    self.imports.mark_as_collection();
                    match agg_op {
                        AggregationOperator::Min => self.imports.mark_min_semiring(agg_type),
                        AggregationOperator::Max => self.imports.mark_max_semiring(agg_type),
                        AggregationOperator::Sum | AggregationOperator::Count => {
                            self.imports.mark_sum_semiring(agg_type)
                        }
                        AggregationOperator::Avg => self.imports.mark_avg_semiring(agg_type),
                    }
                    self.imports.mark_threshold_total();
                    self.imports.mark_timely_map();
                    let pipeline = match agg_op {
                        AggregationOperator::Min => {
                            aggregation_min_optimize(*agg_arity, *agg_pos, agg_type)
                        }
                        AggregationOperator::Max => {
                            aggregation_max_optimize(*agg_arity, *agg_pos, agg_type)
                        }
                        AggregationOperator::Sum => {
                            aggregation_sum_optimize(*agg_arity, *agg_pos, agg_type)
                        }
                        AggregationOperator::Count => {
                            aggregation_count_optimize(*agg_arity, *agg_pos, agg_type)
                        }
                        AggregationOperator::Avg => {
                            aggregation_avg_optimize(*agg_arity, *agg_pos, agg_type)
                        }
                    };
                    block = quote! {
                        #block
                        let #next_ident = #next_ident
                            #pipeline;
                    };

                    // Profiler: aggregation operator (optional)
                    with_profiler(profiler, |profiler| {
                        profiler.opt_aggregate_operator(
                            output_name,
                            next_ident.to_string(),
                            next_ident.to_string(),
                        );
                    });
                } else {
                    self.imports.mark_aggregation();
                    let row_chop = aggregation_row_chop(*agg_arity, *agg_pos);
                    let reduce_logic = aggregation_reduce(agg_op, agg_type);
                    let merge_kv = aggregation_merge_kv(*agg_arity, *agg_pos);
                    // Aggregate after union + dedup.
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

                    // Profiler: aggregation operator (optional)
                    with_profiler(profiler, |profiler| {
                        profiler.general_aggregate_operator(
                            output_name,
                            next_ident.to_string(),
                            next_ident.to_string(),
                        );
                    });
                }
            }

            // UDF logic (optional, mutually exclusive with aggregation)
            if let Some((fn_name, start, end, output_arity)) = output_to_udf_map.get(output_fp) {
                let udf = self
                    .program
                    .udfs()
                    .iter()
                    .find(|u| {
                        let (Udf::Scalar(e) | Udf::Aggregate(e)) = u;
                        e.name() == fn_name
                    })
                    .unwrap_or_else(|| panic!("Compiler error: UDF '{fn_name}' not found"));
                match udf {
                    Udf::Scalar(_) => {
                        for idb_fp in idb_fps {
                            self.verify_udf_types(*output_fp, *idb_fp, fn_name, *start);
                        }
                        self.imports.mark_udf();
                        let udf_pipeline = scalar_udf_map(fn_name, *start, *end, *output_arity);
                        block = quote! {
                            #block
                            let #next_ident = #next_ident
                                #udf_pipeline;
                        };
                    }
                    Udf::Aggregate(_) => {
                        unimplemented!(
                            "Compiler error: aggregate UDF '{fn_name}' is not yet supported"
                        )
                    }
                }
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
        output_to_aggregation_map: &HashMap<u64, (AggregationOperator, usize, usize)>,
        profiler: &mut Option<Profiler>,
    ) -> (TokenStream, TokenStream, TokenStream) {
        // Resolve target identifiers and construct pattern.
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
                let next_ident = next.get(fp).expect(
                    "Compiler error: leave relation missing from next bindings during recursion",
                );

                // For aggregated relations (min/max/sum/count/avg) in batch mode: convert to
                // semiring diff before leave() so cross-iteration aggregates are computed by
                // consolidation after leave.
                if let Some((agg_op, agg_pos, agg_arity)) = output_to_aggregation_map.get(fp) {
                    if matches!(self.config.mode(), ExecutionMode::Batch) {
                        let (key_types, val_types) = self.find_global_type(*fp);
                        let agg_type = *key_types
                            .iter()
                            .chain(val_types)
                            .nth(*agg_pos)
                            .expect("Compiler error: aggregation position out of bounds");
                        let pre_leave = match agg_op {
                            AggregationOperator::Min => {
                                aggregation_min_pre_leave(*agg_arity, *agg_pos, agg_type)
                            }
                            AggregationOperator::Max => {
                                aggregation_max_pre_leave(*agg_arity, *agg_pos, agg_type)
                            }
                            AggregationOperator::Sum => {
                                aggregation_sum_pre_leave(*agg_arity, *agg_pos, agg_type)
                            }
                            AggregationOperator::Count => {
                                aggregation_count_pre_leave(*agg_arity, *agg_pos, agg_type)
                            }
                            AggregationOperator::Avg => {
                                aggregation_avg_pre_leave(*agg_arity, *agg_pos, agg_type)
                            }
                        };

                        with_profiler(profiler, |profiler| {
                            profiler.recursive_pre_leave_opt_aggregate_operator(
                                target.to_string(),
                                next_ident.to_string(),
                                next_ident.to_string(),
                            );
                        });

                        return quote! { #next_ident #pre_leave .leave() };
                    }
                }

                quote! { #next_ident.leave() }
            })
            .collect();

        // Profiler: leave recursive scope (optional)
        with_profiler(profiler, |profiler| {
            profiler.leave_scope();
        });

        leave_fps
            .iter()
            .zip(targets.iter())
            .for_each(|(fp, target)| {
                let next_ident = next.get(fp).expect(
                    "Compiler error: leave relation missing from next bindings during recursion",
                );

                // Profiler: non-opt-aggregation leave operator (optional)
                with_profiler(profiler, |profiler| {
                    profiler.recursive_leave_operator(
                        target.to_string(),
                        next_ident.to_string(),
                        target.to_string(),
                    );
                });
            });

        let leave_stmt = match leave_exprs.as_slice() {
            [expr] => quote! { #expr },
            _ => quote! { ( #(#leave_exprs),* ) },
        };

        // Post-leave: for aggregated relations, consolidate + convert back to Present.
        let mut post_leave_stmts = Vec::new();
        for (fp, target) in leave_fps.iter().zip(targets.iter()) {
            if let Some((agg_op, agg_pos, agg_arity)) = output_to_aggregation_map.get(fp) {
                if matches!(self.config.mode(), ExecutionMode::Batch) {
                    let post_leave = match agg_op {
                        AggregationOperator::Avg => {
                            aggregation_avg_post_leave(*agg_arity, *agg_pos)
                        }
                        _ => aggregation_opt_post_leave(*agg_arity, *agg_pos),
                    };

                    with_profiler(profiler, |profiler| {
                        profiler.recursive_post_leave_opt_aggregate_operator(
                            target.to_string(),
                            target.to_string(),
                            target.to_string(),
                        );
                    });

                    post_leave_stmts.push(quote! {
                        let #target = #target #post_leave;
                    });
                }
            }
        }

        let post_leave = quote! { #(#post_leave_stmts)* };

        (pattern, leave_stmt, post_leave)
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
