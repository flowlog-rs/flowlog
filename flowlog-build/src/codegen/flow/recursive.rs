//! Recursive flow codegen: iterative scopes, feedback variables, and the
//! per-IDB `next_X / recursive_X` plumbing that makes DD's `Variable` loops
//! converge. Operates inside one stratum's worth of transformations, using
//! the non-recursive arrangement map built upstream as the entry environment.

use std::collections::HashMap;

use flowlog_common::ExecutionMode;
use flowlog_parser::AggregationOperator;
use flowlog_planner::planner::StratumPlanner;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::try_with_plan_graph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::codegen::CodeGen;
use crate::codegen::CodegenError;
use crate::codegen::aggregation::aggregation_kind;
use crate::codegen::aggregation::aggregation_merge;
use crate::codegen::aggregation::aggregation_split;

// =========================================================================
// Recursive Flow Generation
// =========================================================================
impl CodeGen {
    /// Emit the `scope.iterative(|inner| { ... })` block for one stratum,
    /// wiring up enter bindings, feedback `Variable`s, per-IDB unions with
    /// dedup/aggregation, and the leave expression.
    pub(crate) fn gen_recursive_block(
        &mut self,
        non_recursive_arranged_map: &HashMap<u64, Ident>,
        stratum: &StratumPlanner,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        self.features.mark_recursive();

        // Nothing leaves this recursion: legal but unobservable, so no
        // iterative scope is emitted -- and none recorded below, keeping
        // predicted addresses aligned with the dataflow.
        let leave_fps = stratum.recursion_leave_collections();
        if leave_fps.is_empty() {
            return Ok(quote! {});
        }

        with_plan_graph(plan_graph, |plan_graph| {
            plan_graph.enter_scope();
        });

        // --- Enter bindings ---
        let enter_fps = stratum.recursion_enter_collections();
        let (enter_stmts, enter_bindings, mut recursive_arranged) =
            self.build_enter_bindings(non_recursive_arranged_map, enter_fps, plan_graph);

        // --- Recursive variable bindings ---
        // Every feedback variable starts empty and grows monotonically, so
        // `Variable::new` covers all of them.
        let feedback_fps = stratum.recursion_feedback_collections();
        let (feedback_names, recursive_bindings) = self.build_recursive_bindings(feedback_fps);

        let step = quote! { timely::order::Product::new(Default::default(), 1) };
        let mut recursive_var_inits: Vec<TokenStream> = Vec::new();
        for (fp, name) in feedback_fps.iter().zip(&feedback_names) {
            with_plan_graph(plan_graph, |plan_graph| {
                plan_graph.recursive_feedback_operator(
                    self.display_name(*fp),
                    name.to_string(),
                    name.to_string(),
                );
            });
            let var_name = format_ident!("{}_var", name);
            recursive_var_inits.push(quote! {
                let (#var_name, #name) = Variable::new(inner, #step);
            });
        }

        // --- Combined environment for rule evaluation ---
        let mut current: HashMap<u64, Ident> = enter_bindings.clone();
        current.extend(recursive_bindings.clone());

        // --- Rule transformations ---
        let flow_stmts: Vec<TokenStream> = stratum
            .recursive_transformations()
            .iter()
            .map(|tx| {
                self.gen_transformation(&current, tx, &mut recursive_arranged, stratum, plan_graph)
            })
            .collect::<Result<_, _>>()?;

        // --- Union per IDB (delta_X) and aggregation ---
        let (next_bindings, union_stmts) = self.collect_unions(
            stratum.idb_to_heads_map(),
            &enter_bindings,
            stratum.idb_to_aggregation_map(),
            plan_graph,
        )?;

        // --- Feedback assignments (Variable::set) ---
        let set_stmts = self.gen_feedback_stmts(&next_bindings, &recursive_bindings, plan_graph)?;

        // --- Leave outputs ---
        let (leave_pattern, leave_stmt) = self.build_leave_outputs(
            leave_fps,
            &next_bindings,
            stratum.idb_to_aggregation_map(),
            plan_graph,
        )?;

        Ok(quote! {
            let #leave_pattern = scope.iterative::<Iter, _, _>(|inner| {
                #(#enter_stmts)*
                #(#recursive_var_inits)*

                // === Recursive rule pipelines ===
                #(#flow_stmts)*

                // === Union per IDB (next_X / delta_X) ===
                #(#union_stmts)*

                // === Feedback (Variable::set) ===
                #(#set_stmts)*

                #leave_stmt
            });
        })
    }

    /// Emit one `let in_X = X.enter(inner);` per entering collection, plus
    /// the `fp` to entered-ident map and a parallel map for arranged inputs.
    fn build_enter_bindings(
        &self,
        non_recursive_arranged_map: &HashMap<u64, Ident>,
        enter_fps: &[u64],
        plan_graph: &mut Option<PlanGraph>,
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
            // Clone before entering: when an outer-scope arrangement is
            // shared across strata (via program-wide `outer_arranged`),
            // multiple recursive blocks may each need to enter it.
            // TraceAgent is Rc-backed so the clone is cheap.
            stmts.push(quote! { let #entered = #source.clone().enter(inner); });

            with_plan_graph(plan_graph, |plan_graph| {
                plan_graph.recursive_enter_operator(source.to_string(), entered.to_string());
            });

            // Preserve arranged bindings for recursive paths.
            if let Some(arranged) = non_recursive_arranged_map.get(fp) {
                let entered_arr = format_ident!("in_{}", arranged);
                recursive_arranged.insert(*fp, entered_arr);
            }
        }

        (stmts, bindings, recursive_arranged)
    }

    /// For each recursive IDB: union its contributing heads, dedup, then
    /// optionally apply aggregation. Produces `next_X` bindings for feedback.
    fn collect_unions(
        &mut self,
        idb_to_heads_map: &HashMap<u64, Vec<u64>>,
        enter_bindings: &HashMap<u64, Ident>,
        idb_to_aggregation_map: &HashMap<u64, (AggregationOperator, usize, usize)>,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<(HashMap<u64, Ident>, Vec<TokenStream>), CodegenError> {
        let mut next_bindings: HashMap<u64, Ident> = HashMap::new();
        let mut union_stmts = Vec::new();

        let mut idb_heads: Vec<_> = idb_to_heads_map.iter().collect();
        idb_heads.sort_unstable_by_key(|(idb_fp, _)| **idb_fp);
        for (idb_fp, head_fps) in idb_heads {
            // Determine output binding name and union sources.
            let next_ident = format_ident!("next_{}", idb_fp);
            next_bindings.insert(*idb_fp, next_ident.clone());

            let mut sources: Vec<Ident> = head_fps
                .iter()
                .map(|fp| format_ident!("t_{}", fp))
                .collect();

            if let Some(entered) = enter_bindings.get(idb_fp) {
                sources.push(entered.clone());
            }

            // Build concatenation expression for all sources.
            let (head, tail) = sources.split_first().ok_or_else(|| {
                CodegenError::internal(format!(
                    "recursive IDB 0x{idb_fp:016x} has no source \
                     collections to union"
                ))
            })?;

            let union_expr = if tail.is_empty() {
                quote! { #head.clone() }
            } else {
                quote! { #head.clone().concatenate([ #( #tail.clone() ),* ]) }
            };

            // Feedback must not re-emit tuples across iterations, so the
            // merged collection takes the retained dedup.
            let mut block = quote! {
                let #next_ident =
                    ::flowlog_runtime::operators::flowlog_dedup_retained::<_, Diff>(#union_expr);
            };

            with_plan_graph(plan_graph, |plan_graph| {
                let source_names: Vec<String> = sources.iter().map(|id| id.to_string()).collect();
                let concat_count = if tail.is_empty() { 0 } else { 1 };
                plan_graph.concat_dedup_operator(
                    self.display_name(*idb_fp),
                    source_names,
                    next_ident.to_string(),
                    concat_count,
                    true,
                );
            });

            // ----------------------------------------------------------------
            // Aggregation
            // ----------------------------------------------------------------
            if let Some((agg_op, agg_pos, agg_arity)) = idb_to_aggregation_map.get(idb_fp) {
                let output_name = self.display_name(*idb_fp);
                let agg_type = self.agg_column_type(*idb_fp, *agg_pos)?;
                let kind = aggregation_kind(*agg_op);
                let split = aggregation_split(*agg_arity, *agg_pos);
                let merge = aggregation_merge(*agg_arity, *agg_pos, &agg_type);
                let op_name = format!("Reduce: {output_name}");
                block = quote! {
                    #block
                    let #next_ident = ::flowlog_runtime::operators::flowlog_reduce(
                        #next_ident, #op_name, #kind, #split, #merge,
                    );
                };

                // The runtime picks its strategy from the ambient difference,
                // and the two build different operators, so the plan graph
                // has to predict the same way.
                let binding = next_ident.to_string();
                with_plan_graph(plan_graph, |plan_graph| match self.config.mode() {
                    ExecutionMode::Batch => {
                        plan_graph.present_aggregate_operator(
                            output_name,
                            binding.clone(),
                            binding,
                        );
                    }
                    ExecutionMode::Inc => {
                        plan_graph.i32_aggregate_operator(output_name, binding.clone(), binding);
                    }
                });
            }

            union_stmts.push(block);
        }

        Ok((next_bindings, union_stmts))
    }

    /// Assemble the `.leave()` expression(s); aggregated relations in
    /// batch mode leave through `flowlog_reduce_leave`, which owns the
    /// boundary fold.
    fn build_leave_outputs(
        &self,
        leave_fps: &[u64],
        next: &HashMap<u64, Ident>,
        idb_to_aggregation_map: &HashMap<u64, (AggregationOperator, usize, usize)>,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<(TokenStream, TokenStream), CodegenError> {
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
            .map(|fp| -> Result<TokenStream, CodegenError> {
                let next_ident = next.get(fp).ok_or_else(|| {
                    CodegenError::internal(format!(
                        "leave relation fingerprint 0x{fp:016x} missing \
                         from next bindings during recursion"
                    ))
                })?;

                // Aggregated relations in batch mode complete across the
                // boundary: `flowlog_reduce_leave` lifts contributions into
                // the semiring diff, leaves, and folds every iteration once
                // at the outer timestamp.
                if let Some((agg_op, agg_pos, agg_arity)) = idb_to_aggregation_map.get(fp)
                    && self.config.mode() == ExecutionMode::Batch
                {
                    let kind = aggregation_kind(*agg_op);
                    let split = aggregation_split(*agg_arity, *agg_pos);
                    let agg_type = self.agg_column_type(*fp, *agg_pos)?;
                    let merge = aggregation_merge(*agg_arity, *agg_pos, &agg_type);

                    with_plan_graph(plan_graph, |plan_graph| {
                        plan_graph.recursive_pre_leave_present_aggregate_operator(
                            self.display_name(*fp),
                            next_ident.to_string(),
                            next_ident.to_string(),
                        );
                    });

                    let op_name = format!("ReduceLeave: {}", self.display_name(*fp));
                    return Ok(quote! {
                        ::flowlog_runtime::operators::flowlog_reduce_leave(
                            #next_ident, scope, #op_name, #kind, #split, #merge,
                        )
                    });
                }

                Ok(quote! { #next_ident.leave(scope) })
            })
            .collect::<Result<_, _>>()?;

        // An unbalanced leave is a codegen bug; surface it instead of
        // corrupting the addresses that follow.
        try_with_plan_graph(plan_graph, |plan_graph| plan_graph.leave_scope())
            .map_err(|e| CodegenError::internal(format!("recording recursive scope exit: {e}")))?;

        for (fp, target) in leave_fps.iter().zip(targets.iter()) {
            let next_ident = next.get(fp).ok_or_else(|| {
                CodegenError::internal(format!(
                    "leave relation fingerprint 0x{fp:016x} missing from \
                     next bindings during recursion"
                ))
            })?;

            with_plan_graph(plan_graph, |plan_graph| {
                plan_graph.recursive_leave_operator(
                    self.display_name(*fp),
                    next_ident.to_string(),
                    target.to_string(),
                );
            });
        }

        let leave_stmt = match leave_exprs.as_slice() {
            [expr] => quote! { #expr },
            _ => quote! { ( #(#leave_exprs),* ) },
        };

        // The boundary fold's outer-scope operators (consolidate + map) are
        // built by `flowlog_reduce_leave` at the leave site; only their
        // addresses are recorded here, after the scope exit.
        for (fp, target) in leave_fps.iter().zip(targets.iter()) {
            if idb_to_aggregation_map.contains_key(fp) && self.config.mode() == ExecutionMode::Batch
            {
                with_plan_graph(plan_graph, |plan_graph| {
                    plan_graph.recursive_post_leave_present_aggregate_operator(
                        self.display_name(*fp),
                        target.to_string(),
                        target.to_string(),
                    );
                });
            }
        }

        Ok((pattern, leave_stmt))
    }

    /// Build `recursive_X` identifier names and their `fp`-keyed binding map.
    fn build_recursive_bindings(&self, recursive_fps: &[u64]) -> (Vec<Ident>, HashMap<u64, Ident>) {
        let names: Vec<Ident> = recursive_fps
            .iter()
            .map(|fp| format_ident!("recursive_{}", self.find_global_ident(*fp)))
            .collect();

        let bindings = recursive_fps
            .iter()
            .copied()
            .zip(names.iter().cloned())
            .collect();

        (names, bindings)
    }

    /// Emit `Variable::set(next_X)` for each recursive relation.
    fn gen_feedback_stmts(
        &mut self,
        next_bindings: &HashMap<u64, Ident>,
        recursive_bindings: &HashMap<u64, Ident>,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Vec<TokenStream>, CodegenError> {
        let mut stmts = Vec::new();

        let mut recursive_entries: Vec<_> = recursive_bindings.iter().collect();
        recursive_entries.sort_unstable_by_key(|(fp, _)| **fp);
        for (fp, recursive_ident) in recursive_entries {
            let next_ident = next_bindings.get(fp).ok_or_else(|| {
                CodegenError::internal(format!(
                    "recursive relation fingerprint 0x{fp:016x} missing \
                     from next bindings"
                ))
            })?;
            with_plan_graph(plan_graph, |plan_graph| {
                plan_graph.recursive_resultsin_operator(
                    self.display_name(*fp),
                    next_ident.to_string(),
                    next_ident.to_string(),
                );
            });
            let var_name = format_ident!("{}_var", recursive_ident);
            stmts.push(quote! { #var_name.set(#next_ident.clone()); });
        }

        Ok(stmts)
    }
}
