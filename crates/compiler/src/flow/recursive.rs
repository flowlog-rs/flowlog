//! Recursive flow generation for FlowLog compiler.
//!
//! This module handles the generation of recursive differential dataflow
//! pipelines within recursive strata.
//! It manages the iterative scopes, feedback loops, and unioning of IDB relations.
//! It is worth noticing that due to factoring optimization, we need to maintain a local
//! ident map derived from both non-recursive arrangements and global idents.

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use std::collections::HashMap;

use crate::aggregation::{
    aggregation_avg_optimize, aggregation_avg_post_leave, aggregation_avg_pre_leave,
    aggregation_count_optimize, aggregation_count_pre_leave, aggregation_max_optimize,
    aggregation_max_pre_leave, aggregation_merge_kv, aggregation_min_optimize,
    aggregation_min_pre_leave, aggregation_opt_post_leave, aggregation_reduce_stmt,
    aggregation_row_chop, aggregation_sum_optimize, aggregation_sum_pre_leave,
};
use crate::ident::find_local_ident;
use crate::udf::head_udf_map;
use crate::Compiler;

use parser::{AggregationOperator, LoopCondition, LoopConnective};
use planner::StratumPlanner;
use profiler::{with_profiler, Profiler};

// =========================================================================
// Recursive Flow Generation
// =========================================================================
impl Compiler {
    /// Generate the iterative block for recursive rules.
    pub(crate) fn gen_recursive_block(
        &mut self,
        non_recursive_arranged_map: &HashMap<u64, Ident>,
        stratum: &StratumPlanner,
        profiler: &mut Option<Profiler>,
    ) -> TokenStream {
        self.imports.mark_recursive();

        // Profile: enter recursive scope (optional)
        with_profiler(profiler, |profiler| {
            profiler.enter_scope();
        });

        // Early exit if nothing leaves recursion.
        let leave_fps = stratum.recursion_leave_collections();
        if leave_fps.is_empty() {
            return quote! {};
        }

        // --- Enter bindings -------------------------------------------------
        let enter_fps = stratum.recursion_enter_collections();
        let (enter_stmts, enter_bindings, mut recursive_arranged) =
            self.build_enter_bindings(non_recursive_arranged_map, enter_fps, profiler);

        // --- Recursive variable bindings -------------------------
        let acc_fps = stratum.recursion_accumulate_recursive_collections();
        let itr_fps = stratum.recursion_iterative_recursive_collections();

        let (acc_names, acc_bindings) = self.build_recursive_bindings(acc_fps);
        let (itr_names, itr_bindings) = self.build_recursive_bindings(itr_fps);

        let mut recursive_bindings: HashMap<u64, Ident> = acc_bindings;
        recursive_bindings.extend(itr_bindings);

        // Initialize recursive variables and record feedback operators.
        let step = quote! { timely::order::Product::new(Default::default(), 1) };
        let mut recursive_var_inits: Vec<TokenStream> = Vec::new();

        // Accumulative: always Variable::new — starts empty and grows monotonically.
        for name in &acc_names {
            with_profiler(profiler, |profiler| {
                profiler.recursive_feedback_operator(
                    name.to_string(),
                    name.to_string(),
                    name.to_string(),
                );
            });
            let var_name = format_ident!("{}_var", name);
            recursive_var_inits.push(quote! {
                let (#var_name, #name) = Variable::new(inner, #step);
            });
        }

        // Iterative: always Variable::new_from for replacement semantics.
        // When an EDB base enters the scope, seed from it; otherwise seed from
        // an empty collection so that old values are still retracted each iteration.
        for (fp, name) in itr_fps.iter().zip(&itr_names) {
            with_profiler(profiler, |profiler| {
                profiler.recursive_feedback_operator(
                    name.to_string(),
                    name.to_string(),
                    name.to_string(),
                );
            });
            let var_name = format_ident!("{}_var", name);
            let init = if let Some(base) = enter_bindings.get(fp) {
                quote! { let (#var_name, #name) = Variable::new_from(#base.clone(), #step); }
            } else {
                quote! {
                    let (#var_name, #name) = Variable::new_from(
                        ::differential_dataflow::Collection::new(
                            ::timely::dataflow::operators::generic::operator::empty(inner)
                        ),
                        #step,
                    );
                }
            };
            recursive_var_inits.push(init);
        }

        // --- Combined environment for rule evaluation -----------------------
        let mut current: HashMap<u64, Ident> = enter_bindings.clone();
        current.extend(recursive_bindings.clone());

        // --- Rule transformations -------------------------------------------
        let flow_stmts: Vec<TokenStream> = stratum
            .recursive_transformations()
            .iter()
            .map(|tx| {
                self.gen_transformation(
                    &current,
                    tx,
                    &mut recursive_arranged,
                    stratum.head_to_idb_map(),
                    stratum.idb_to_aggregation_map(),
                    profiler,
                )
            })
            .collect();

        // --- Union per IDB (delta_X) and aggregation ------------------------
        let (next_bindings, union_stmts) = self.collect_unions(
            stratum.idb_to_heads_map(),
            &enter_bindings,
            itr_fps,
            stratum.idb_to_aggregation_map(),
            stratum.idb_to_udf_map(),
            profiler,
        );

        // --- Feedback assignments (Variable::set), optionally gated --------
        let set_stmts = self.gen_feedback_stmts(
            stratum.loop_condition(),
            &next_bindings,
            &recursive_bindings,
            itr_fps,
            profiler,
        );

        // --- Leave outputs --------------------------------------------------
        let (leave_pattern, leave_stmt, post_leave) = self.build_leave_outputs(
            leave_fps,
            &next_bindings,
            &recursive_bindings,
            itr_fps,
            stratum.idb_to_aggregation_map(),
            profiler,
        );

        quote! {
            let #leave_pattern = scope.iterative::<Iter, _, _>(|inner| {
                #(#enter_stmts)*
                #(#recursive_var_inits)*

                // === Recursive rule pipelines ===
                #(#flow_stmts)*

                // === Union per IDB (next_X / delta_X) ===
                #(#union_stmts)*

                // === Feedback (Variable::set, optionally gated) ===
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
        idb_to_heads_map: &HashMap<u64, Vec<u64>>,
        enter_bindings: &HashMap<u64, Ident>,
        iterative_fps: &[u64],
        idb_to_aggregation_map: &HashMap<u64, (AggregationOperator, usize, usize)>,
        idb_to_udf_map: &HashMap<u64, (String, usize, usize, usize)>,
        profiler: &mut Option<Profiler>,
    ) -> (HashMap<u64, Ident>, Vec<TokenStream>) {
        let mut next_bindings: HashMap<u64, Ident> = HashMap::new();
        let mut union_stmts = Vec::new();

        for (idb_fp, head_fps) in idb_to_heads_map {
            // Determine output binding name and union sources.
            let next_ident = format_ident!("next_{}", idb_fp);
            next_bindings.insert(*idb_fp, next_ident.clone());

            let mut sources: Vec<Ident> = head_fps
                .iter()
                .map(|fp| format_ident!("t_{}", fp))
                .collect();

            // Iterative relations use replacement semantics: the EDB base is
            // seeded via Variable::new_from, so we must not re-union it here.
            if !iterative_fps.contains(idb_fp) {
                if let Some(entered) = enter_bindings.get(idb_fp) {
                    sources.push(entered.clone());
                }
            }

            // Build concatenation expression for all sources.
            let (head, tail) = sources
                .split_first()
                .expect("Compiler error: at least one source collection for union");

            let union_expr = tail.iter().fold(quote! { #head.clone() }, |ts, ident| {
                quote! { #ts.concat(#ident.clone()) }
            });

            // Apply dedup to merged collection.
            // Inside a recursive scope we need a persistent trace to avoid
            // re-emitting tuples across iterations → use dedup_recursive.
            let dedup_call = self.dedup_recursive();
            let mut block = quote! {
                let #next_ident = #union_expr #dedup_call;
            };

            // Profiler: union / dedup operator (optional)
            with_profiler(profiler, |profiler| {
                let output_name = self.find_global_ident(*idb_fp).to_string();
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

            // ----------------------------------------------------------------
            // Aggregation
            // ----------------------------------------------------------------
            if let Some((agg_op, agg_pos, agg_arity)) = idb_to_aggregation_map.get(idb_fp) {
                let output_name = self.find_global_ident(*idb_fp).to_string();
                self.imports.mark_semiring_one();

                // Look up the aggregated column's data type.
                let (key_types, val_types) = self.find_global_type(*idb_fp);
                let agg_type = *key_types
                    .iter()
                    .chain(val_types)
                    .nth(*agg_pos)
                    .expect("Compiler error: aggregation position out of bounds");

                // Semiring fast path: replace reduce_core with threshold_semigroup
                // using the appropriate semigroup, avoiding a second arrangement.
                if self.config.is_datalog_batch() {
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
                    let merge_kv = aggregation_merge_kv(*agg_arity, *agg_pos);
                    // Aggregate after union + dedup.
                    let reduce_stmt =
                        aggregation_reduce_stmt(self.config.is_incremental(), agg_op, agg_type);
                    block = quote! {
                        #block
                        let #next_ident = #next_ident
                            .map(#row_chop)
                            .arrange_by_key()
                            #reduce_stmt
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

            // ----------------------------------------------------------------
            // UDF (mutually exclusive with aggregation)
            // ----------------------------------------------------------------
            if let Some((fn_name, start, end, output_arity)) = idb_to_udf_map.get(idb_fp) {
                for head_fp in head_fps {
                    self.verify_udf_types(*idb_fp, *head_fp, fn_name, *start);
                }
                self.imports.mark_udf();
                let udf_pipeline = head_udf_map(fn_name, *start, *end, *output_arity);
                block = quote! {
                    #block
                    let #next_ident = #next_ident
                        #udf_pipeline;
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
        recursive: &HashMap<u64, Ident>,
        iterative_fps: &[u64],
        idb_to_aggregation_map: &HashMap<u64, (AggregationOperator, usize, usize)>,
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

                // For aggregated relations (min/max/sum/count/avg) in datalog-batch mode:
                // convert to semiring diff before leave() so cross-iteration aggregates
                // are computed by consolidation after leave.
                if let Some((agg_op, agg_pos, agg_arity)) = idb_to_aggregation_map.get(fp) {
                    if self.config.is_datalog_batch() {
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

                // Mixed loop: all recursive relations leave via recursive_X, not next_X.
                //
                // - Iterative: var.set(derived_only) means recursive_X carries ±1 diffs
                //   across inner iterations.  Inside the scope they can't cancel (different
                //   inner timestamps); after leave() both project to the same outer time, so
                //   consolidate nets them out.
                // - Accumulative: feedback is var.set(derived ∪ self), so recursive_X is
                //   monotone (+1 only); no consolidate needed.  next_X is unsafe here because
                //   an iterative retraction can remove facts used in accumulative derivation,
                //   causing next_X to shrink and emit spurious −1 diffs on leave.
                //
                // Pure accumulative loops (iterative_fps empty) use next_X.leave() since
                // next_X == recursive_X at fixpoint with no retractions.
                if !iterative_fps.is_empty() {
                    if let Some(recursive_ident) = recursive.get(fp) {
                        if iterative_fps.contains(fp) {
                            return quote! { #recursive_ident.leave().consolidate() };
                        } else {
                            return quote! { #recursive_ident.leave() };
                        }
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

        // Post-leave: for batch-mode aggregated relations, consolidate and
        // convert the semiring diff back to a Present multiplicity.
        let mut post_leave_stmts = Vec::new();
        for (fp, target) in leave_fps.iter().zip(targets.iter()) {
            if let Some((agg_op, agg_pos, agg_arity)) = idb_to_aggregation_map.get(fp) {
                if self.config.is_datalog_batch() {
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

    /// Build `recursive_X` identifier names and the `fp → recursive_X` binding map.
    fn build_recursive_bindings(&self, recursive_fps: &[u64]) -> (Vec<Ident>, HashMap<u64, Ident>) {
        let names: Vec<Ident> = recursive_fps
            .iter()
            .map(|fp| {
                let name = self.find_global_ident(*fp);
                format_ident!("recursive_{}", name)
            })
            .collect();

        let bindings = recursive_fps
            .iter()
            .zip(names.iter().cloned())
            .map(|(fp, ident)| (*fp, ident))
            .collect();

        (names, bindings)
    }

    /// Translate `Option<&LoopCondition>` into a `ConditionPlan`, marking any
    /// required imports as a side effect.
    fn prepare_loop_condition(
        &mut self,
        condition: Option<&LoopCondition>,
        next_bindings: &HashMap<u64, Ident>,
    ) -> (Vec<TokenStream>, ConditionPlan) {
        let Some(cond) = condition else {
            return (Vec::new(), ConditionPlan::default());
        };

        let (prelude, boolean_until_conditions) = self.build_until_conditions(cond, next_bindings);

        // Preserve empty window lists as Some([]) rather than collapsing them to
        // None.  An empty list arises from contradictory bounds (e.g. `@it >= 5
        // and @it < 3`) and means "never continue" — blocking all feedback so
        // the loop performs zero iterations.
        //
        // This fix works in tandem with build_iter_conditions returning `false`
        // for an empty range list.  Both are required: collapsing Some([]) to
        // None here would make the empty case unreachable there, and the loop
        // would run forever instead of zero iterations.
        let iter_while_conditions = cond.while_part().map(|r| r.to_vec());
        // Mark imports whenever a `while` clause is present, even if the
        // resolved range list is empty (contradictory bounds like `@it >= 5
        // and @it < 3`). The generated feedback still goes through
        // `continue_stmt(...).flat_map(...).as_collection()`, which requires
        // these imports regardless of whether the range evaluates to `false`.
        if iter_while_conditions.is_some() {
            self.imports.mark_timely_map();
            self.imports.mark_as_collection();
        }

        let connective = cond.connective().cloned();
        (
            prelude,
            ConditionPlan {
                boolean_until_conditions,
                iter_while_conditions,
                connective,
            },
        )
    }

    /// Emit `Variable::set(feedback)` for each recursive relation, where
    /// `feedback` is `next_X` optionally filtered by the loop condition.
    fn gen_feedback_stmts(
        &mut self,
        condition: Option<&LoopCondition>,
        next_bindings: &HashMap<u64, Ident>,
        recursive_bindings: &HashMap<u64, Ident>,
        iterative_fps: &[u64],
        profiler: &mut Option<Profiler>,
    ) -> Vec<TokenStream> {
        let (mut stmts, plan) = self.prepare_loop_condition(condition, next_bindings);
        let has_iterative = !iterative_fps.is_empty();

        for (fp, recursive_ident) in recursive_bindings {
            let next_ident = next_bindings.get(fp).unwrap_or_else(|| {
                panic!("Compiler: recursive relation fp {fp} missing next binding")
            });
            let iter_var = find_local_ident(recursive_bindings, *fp);
            with_profiler(profiler, |profiler| {
                profiler.recursive_resultsin_operator(
                    iter_var.to_string(),
                    next_ident.to_string(),
                    next_ident.to_string(),
                );
            });
            let var_name = format_ident!("{}_var", recursive_ident);
            let dedup = self.dedup_recursive();
            // Only generate weight tokens and normalize when until conditions
            // exist — these require antijoin arithmetic (pos/neg/normalize).
            let (pos, neg, normalize) = if plan.boolean_until_conditions.is_some() {
                let (p, n) = self.weight_concat_tokens();
                (p, n, self.normalize_antijoin())
            } else {
                (quote! {}, quote! {}, quote! {})
            };

            let feedback = if iterative_fps.contains(fp) {
                // Iterative: replacement — feed back derived value only, no union with self.
                build_feedback_expr(
                    next_ident,
                    recursive_ident,
                    &plan,
                    &pos,
                    &neg,
                    &dedup,
                    &normalize,
                )
            } else if has_iterative {
                // Accumulative in a mixed loop: iterative relations can retract, so
                // explicitly union with the previous self to maintain monotonicity.
                let acc_next = format_ident!("acc_next_{}", fp);
                stmts.push(quote! {
                    let #acc_next = #next_ident.clone().concat(#recursive_ident.clone()) #dedup;
                });
                build_feedback_expr(
                    &acc_next,
                    recursive_ident,
                    &plan,
                    &pos,
                    &neg,
                    &dedup,
                    &normalize,
                )
            } else {
                // Purely accumulative loop: no self-union needed — all rules are monotone,
                // so every previously derived fact will be re-derived on the next iteration.
                build_feedback_expr(
                    next_ident,
                    recursive_ident,
                    &plan,
                    &pos,
                    &neg,
                    &dedup,
                    &normalize,
                )
            };

            stmts.push(quote! { #var_name.set(#feedback); });
        }

        stmts
    }

    /// Build prelude stmts and the arranged gate signal for the `until` clause.
    fn build_until_conditions(
        &mut self,
        cond: &LoopCondition,
        next_bindings: &HashMap<u64, Ident>,
    ) -> (Vec<TokenStream>, Option<Ident>) {
        let Some(until_group) = cond.until_part() else {
            return (Vec::new(), None);
        };
        self.imports.mark_as_collection();
        self.imports.mark_timely_map();

        let mut stmts: Vec<TokenStream> = Vec::new();

        // Initialise gate from the first until relation.
        let first = until_group.first();
        let first_next = next_bindings.get(&first.fp).unwrap_or_else(|| {
            panic!(
                "Compiler: until relation fp {} missing from next_bindings",
                first.fp
            )
        });
        let first_sig = format_ident!("rel_sig_{}", first.fp);
        let dedup = self.dedup_recursive();
        stmts.push(quote! { let #first_sig = #first_next.clone() #dedup; });
        let mut gate = first_sig;

        // Fold remaining until relations into the gate with explicit connectives.
        for (conn, rel) in until_group.rest() {
            let fp = rel.fp;
            let next_ident = next_bindings.get(&fp).unwrap_or_else(|| {
                panic!("Compiler: until relation fp {fp} missing from next_bindings")
            });
            let sig = format_ident!("rel_sig_{}", fp);
            stmts.push(quote! { let #sig = #next_ident.clone() #dedup; });

            let combined = format_ident!("rel_sig_comb_{}", fp);
            match conn {
                LoopConnective::And => stmts.push(quote! {
                    let #combined = #gate.arrange_by_self()
                        .join_core(#sig.arrange_by_self(), |(), _, _| std::iter::once(()));
                }),
                LoopConnective::Or => {
                    stmts.push(quote! { let #combined = #gate.concat(#sig.clone()) #dedup; });
                }
            }
            gate = combined;
        }

        let arr = format_ident!("{}_arr", gate);
        stmts.push(quote! { let #arr = #gate.clone().arrange_by_self(); });
        (stmts, Some(arr))
    }
}

// =========================================================================
// Loop condition plan
// =========================================================================

/// Analyzed loop condition, ready for feedback-expression code generation.
///
/// Built by `Compiler::prepare_loop_condition`; consumed by `build_feedback_expr`.
/// The gate-setup prelude is returned separately so the plan stays pure data.
#[derive(Default)]
struct ConditionPlan {
    /// Arranged until-gate (`<ident>_arr`) or `None` if there is no `until` clause.
    boolean_until_conditions: Option<Ident>,
    /// Iteration windows from the `while` clause, or `None` if absent.
    iter_while_conditions: Option<Vec<(u16, u16)>>,
    /// Connective joining the `until` and `while` clauses, if both are present.
    connective: Option<LoopConnective>,
}

// =========================================================================
// Token-building helpers
// =========================================================================

/// Boolean expression: `true` when `i` (DD inner counter as `u16`) is in any window.
fn build_iter_conditions(ranges: &[(u16, u16)]) -> TokenStream {
    ranges
        .iter()
        .map(|&(lo, hi)| match (lo, hi) {
            (0, u16::MAX) => quote! { true },
            (0, hi) => {
                let h = proc_macro2::Literal::u16_suffixed(hi);
                quote! { i <= #h }
            }
            (lo, u16::MAX) => {
                let l = proc_macro2::Literal::u16_suffixed(lo);
                quote! { i >= #l }
            }
            (lo, hi) => {
                let l = proc_macro2::Literal::u16_suffixed(lo);
                let h = proc_macro2::Literal::u16_suffixed(hi);
                quote! { (i >= #l && i <= #h) }
            }
        })
        .reduce(|acc, c| quote! { #acc || #c })
        .unwrap_or_else(|| quote! { false })
}

/// Build the feedback expression for one recursive variable given a `ConditionPlan`.
///
/// `dedup` — recursive-safe set-dedup (threshold_semigroup for batch, threshold for inc).
/// `normalize` — convert i32 antijoin arithmetic back to native diff type.
fn build_feedback_expr(
    next: &Ident,
    recursive: &Ident,
    plan: &ConditionPlan,
    pos: &TokenStream,
    neg: &TokenStream,
    dedup: &TokenStream,
    normalize: &TokenStream,
) -> TokenStream {
    match (
        plan.iter_while_conditions.as_deref(),
        plan.boolean_until_conditions.as_ref(),
    ) {
        (None, None) => quote! { #next.clone() },
        (Some(ranges), None) => continue_stmt(next, build_iter_conditions(ranges)),
        (None, Some(arr)) => {
            stop_stmt(quote! { #next.clone() }, recursive, arr, pos, neg, normalize)
        }
        (Some(ranges), Some(arr)) => {
            let range_cond = build_iter_conditions(ranges);
            if matches!(plan.connective, Some(LoopConnective::Or)) {
                // OR: rows in-range pass unconditionally; rows out-of-range are
                // still allowed unless the until gate fires.
                let allowed = continue_stmt(next, range_cond.clone());
                let blocked = stop_stmt(
                    continue_stmt(next, quote! { !(#range_cond) }),
                    recursive,
                    arr,
                    pos,
                    neg,
                    normalize,
                );
                quote! { { #allowed.concat(#blocked) #dedup } }
            } else {
                // AND: rows must be in-range AND not stopped.
                stop_stmt(
                    continue_stmt(next, range_cond),
                    recursive,
                    arr,
                    pos,
                    neg,
                    normalize,
                )
            }
        }
    }
}

/// Keep only tuples whose DD inner iteration counter satisfies `cond_expr`.
fn continue_stmt(next: &Ident, cond_expr: TokenStream) -> TokenStream {
    quote! {
        #next.clone()
            .inner
            .flat_map(|(data, time, diff)| {
                let i = time.inner;
                if #cond_expr { Some((data, time, diff)) } else { None }
            })
            .as_collection()
    }
}

/// Convergent stop-condition: `input - (input ⋈ gate) + (recursive ⋈ gate)`.
/// Returns `input` when gate is empty; converges to `recursive` when gate fires.
///
/// `normalize` converts the `i32` antijoin result back to the native diff type
/// (`Present` for batch, `i32` 0/1 for inc).
fn stop_stmt(
    input: TokenStream,
    recursive: &Ident,
    gate: &Ident,
    pos: &TokenStream,
    neg: &TokenStream,
    normalize: &TokenStream,
) -> TokenStream {
    quote! {
        {
            let keyed = (#input).map(|t| ((), t));
            let keyed_arr = keyed.clone().arrange_by_key();
            let keyed_rec = #recursive.clone().map(|t| ((), t));
            let keyed_rec_arr = keyed_rec.arrange_by_key();
            keyed
                #pos
                .concat({
                    keyed_arr
                        .join_core(#gate.clone(), |_, v, _| std::iter::once(((), v.clone())))
                        #neg
                })
                .concat({
                    keyed_rec_arr
                        .join_core(#gate.clone(), |_, v, _| std::iter::once(((), v.clone())))
                        #pos
                })
                .map(|((), t)| t)
                #normalize
        }
    }
}
