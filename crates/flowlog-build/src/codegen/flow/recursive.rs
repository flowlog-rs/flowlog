//! Recursive flow codegen — iterative scopes, feedback variables, and the
//! per-IDB `next_X / recursive_X` plumbing that makes DD's `Variable` loops
//! converge. Operates inside one stratum's worth of transformations, using
//! the non-recursive arrangement map built upstream as the entry environment.

use std::collections::HashMap;

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};

use crate::parser::{AggregationOperator, LoopCondition, LoopConnective};
use crate::planner::StratumPlanner;
use crate::profiler::{Profiler, with_profiler};

use crate::codegen::CodeGen;
use crate::codegen::CodegenError;
use crate::codegen::aggregation::{
    aggregation_avg_optimize, aggregation_avg_post_leave, aggregation_avg_pre_leave,
    aggregation_count_optimize, aggregation_count_pre_leave, aggregation_max_optimize,
    aggregation_max_pre_leave, aggregation_merge_kv, aggregation_min_optimize,
    aggregation_min_pre_leave, aggregation_opt_post_leave, aggregation_reduce_stmt,
    aggregation_row_chop, aggregation_sum_optimize, aggregation_sum_pre_leave,
};

// =========================================================================
// Recursive Flow Generation
// =========================================================================
impl CodeGen {
    /// Emit the `scope.iterative(|inner| { … })` block for one stratum,
    /// wiring up enter bindings, feedback `Variable`s, per-IDB unions with
    /// dedup/aggregation, optional gated feedback, and the leave expression.
    pub(crate) fn gen_recursive_block(
        &mut self,
        non_recursive_arranged_map: &HashMap<u64, Ident>,
        stratum: &StratumPlanner,
        profiler: &mut Option<Profiler>,
    ) -> Result<TokenStream, CodegenError> {
        self.features.mark_recursive();

        with_profiler(profiler, |profiler| {
            profiler.enter_scope();
        });

        // Early exit if nothing leaves recursion.
        let leave_fps = stratum.recursion_leave_collections();
        if leave_fps.is_empty() {
            return Ok(quote! {});
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
        for (fp, name) in acc_fps.iter().zip(&acc_names) {
            with_profiler(profiler, |profiler| {
                profiler.recursive_feedback_operator(
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

        // Iterative: always Variable::new_from for replacement semantics.
        // When an EDB base enters the scope, seed from it; otherwise seed from
        // an empty collection so that old values are still retracted each iteration.
        for (fp, name) in itr_fps.iter().zip(&itr_names) {
            with_profiler(profiler, |profiler| {
                profiler.recursive_feedback_operator(
                    self.display_name(*fp),
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
        // Per-block semantic arrangement cache: idents created here live inside
        // this `iterative(|inner| …)` closure, so sharing must be scoped to the
        // block (a fresh map per recursive scope, never shared across blocks).
        let mut recursive_sem_arranged: HashMap<u64, (Ident, Ident)> = HashMap::new();
        let flow_stmts: Vec<TokenStream> = stratum
            .recursive_transformations()
            .iter()
            .map(|tx| {
                self.gen_transformation(
                    &current,
                    tx,
                    &mut recursive_arranged,
                    &mut recursive_sem_arranged,
                    stratum,
                    profiler,
                )
            })
            .collect::<Result<_, _>>()?;

        // --- Union per IDB (delta_X) and aggregation ------------------------
        let (next_bindings, union_stmts) = self.collect_unions(
            stratum.idb_to_heads_map(),
            &enter_bindings,
            itr_fps,
            stratum.idb_to_aggregation_map(),
            profiler,
        )?;

        // --- Feedback assignments (Variable::set), optionally gated --------
        let set_stmts = self.gen_feedback_stmts(
            stratum.loop_condition(),
            &next_bindings,
            &recursive_bindings,
            itr_fps,
            profiler,
        )?;

        // --- Leave outputs --------------------------------------------------
        let (leave_pattern, leave_stmt, post_leave) = self.build_leave_outputs(
            leave_fps,
            &next_bindings,
            &recursive_bindings,
            itr_fps,
            stratum.idb_to_aggregation_map(),
            profiler,
        )?;

        Ok(quote! {
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
        })
    }

    /// Emit one `let in_X = X.enter(inner);` per entering collection, plus
    /// the `fp → entered_ident` map and a parallel map for arranged inputs.
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
            // Clone before entering: when an outer-scope arrangement is
            // shared across strata (via program-wide `outer_arranged`),
            // multiple recursive blocks may each need to enter it.
            // TraceAgent is Rc-backed so the clone is cheap.
            stmts.push(quote! { let #entered = #source.clone().enter(inner); });

            with_profiler(profiler, |profiler| {
                profiler.recursive_enter_operator(source.to_string(), entered.to_string());
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
        iterative_fps: &[u64],
        idb_to_aggregation_map: &HashMap<u64, (AggregationOperator, usize, usize)>,
        profiler: &mut Option<Profiler>,
    ) -> Result<(HashMap<u64, Ident>, Vec<TokenStream>), CodegenError> {
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
            if !iterative_fps.contains(idb_fp)
                && let Some(entered) = enter_bindings.get(idb_fp)
            {
                sources.push(entered.clone());
            }

            // Build concatenation expression for all sources.
            let (head, tail) = sources.split_first().ok_or_else(|| {
                CodegenError::internal(format!(
                    "recursive IDB 0x{idb_fp:016x} has no source \
                     collections to union"
                ))
            })?;

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

            with_profiler(profiler, |profiler| {
                let source_names: Vec<String> = sources.iter().map(|id| id.to_string()).collect();
                let concat_count = (sources.len() as u32).saturating_sub(1);
                profiler.concat_dedup_operator(
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

                // Semiring fast path: replace reduce_core with threshold_semigroup
                // using the appropriate semigroup, avoiding a second arrangement.
                if self.config.is_datalog_batch() {
                    self.features.mark_as_collection();
                    self.features.mark_agg_semiring(*agg_op, agg_type);
                    self.features.mark_threshold_total();
                    self.features.mark_timely_map();
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

                    with_profiler(profiler, |profiler| {
                        profiler.opt_aggregate_operator(
                            output_name,
                            next_ident.to_string(),
                            next_ident.to_string(),
                        );
                    });
                } else {
                    self.features.mark_aggregation();
                    let row_chop = aggregation_row_chop(*agg_arity, *agg_pos);
                    let merge_kv = aggregation_merge_kv(*agg_arity, *agg_pos);
                    // Aggregate after union + dedup.
                    let reduce_stmt =
                        aggregation_reduce_stmt(self.config.is_incremental(), agg_op, agg_type)?;
                    block = quote! {
                        #block
                        let #next_ident = #next_ident
                            .map(#row_chop)
                            .arrange_by_key()
                            #reduce_stmt
                            .as_collection(#merge_kv);
                    };

                    with_profiler(profiler, |profiler| {
                        profiler.general_aggregate_operator(
                            output_name,
                            next_ident.to_string(),
                            next_ident.to_string(),
                        );
                    });
                }
            }

            union_stmts.push(block);
        }

        Ok((next_bindings, union_stmts))
    }

    /// Assemble the `.leave()` expression(s) and any post-leave
    /// consolidation needed by batch-mode aggregated relations.
    fn build_leave_outputs(
        &self,
        leave_fps: &[u64],
        next: &HashMap<u64, Ident>,
        recursive: &HashMap<u64, Ident>,
        iterative_fps: &[u64],
        idb_to_aggregation_map: &HashMap<u64, (AggregationOperator, usize, usize)>,
        profiler: &mut Option<Profiler>,
    ) -> Result<(TokenStream, TokenStream, TokenStream), CodegenError> {
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

                // For aggregated relations (min/max/sum/count/avg) in datalog-batch mode:
                // convert to semiring diff before leave() so cross-iteration aggregates
                // are computed by consolidation after leave.
                if let Some((agg_op, agg_pos, agg_arity)) = idb_to_aggregation_map.get(fp)
                    && self.config.is_datalog_batch()
                {
                    let agg_type = self.agg_column_type(*fp, *agg_pos)?;
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
                            self.display_name(*fp),
                            next_ident.to_string(),
                            next_ident.to_string(),
                        );
                    });

                    return Ok(quote! { #next_ident #pre_leave .leave(scope) });
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
                if !iterative_fps.is_empty()
                    && let Some(recursive_ident) = recursive.get(fp)
                {
                    if iterative_fps.contains(fp) {
                        return Ok(quote! { #recursive_ident.leave(scope).consolidate() });
                    } else {
                        return Ok(quote! { #recursive_ident.leave(scope) });
                    }
                }

                Ok(quote! { #next_ident.leave(scope) })
            })
            .collect::<Result<_, _>>()?;

        with_profiler(profiler, |profiler| {
            profiler.leave_scope();
        });

        for (fp, target) in leave_fps.iter().zip(targets.iter()) {
            let next_ident = next.get(fp).ok_or_else(|| {
                CodegenError::internal(format!(
                    "leave relation fingerprint 0x{fp:016x} missing from \
                     next bindings during recursion"
                ))
            })?;

            with_profiler(profiler, |profiler| {
                profiler.recursive_leave_operator(
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

        // Post-leave: for batch-mode aggregated relations, consolidate and
        // convert the semiring diff back to a Present multiplicity.
        let mut post_leave_stmts = Vec::new();
        for (fp, target) in leave_fps.iter().zip(targets.iter()) {
            if let Some((agg_op, agg_pos, agg_arity)) = idb_to_aggregation_map.get(fp)
                && self.config.is_datalog_batch()
            {
                let post_leave = match agg_op {
                    AggregationOperator::Avg => aggregation_avg_post_leave(*agg_arity, *agg_pos),
                    _ => aggregation_opt_post_leave(*agg_arity, *agg_pos),
                };

                with_profiler(profiler, |profiler| {
                    profiler.recursive_post_leave_opt_aggregate_operator(
                        self.display_name(*fp),
                        target.to_string(),
                        target.to_string(),
                    );
                });

                post_leave_stmts.push(quote! {
                    let #target = #target #post_leave;
                });
            }
        }

        let post_leave = quote! { #(#post_leave_stmts)* };

        Ok((pattern, leave_stmt, post_leave))
    }

    /// Build `recursive_X` identifier names and the `fp → recursive_X` binding map.
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

    /// Translate `Option<&LoopCondition>` into a `ConditionPlan`, marking any
    /// required imports as a side effect.
    fn prepare_loop_condition(
        &mut self,
        condition: Option<&LoopCondition>,
        next_bindings: &HashMap<u64, Ident>,
    ) -> Result<(Vec<TokenStream>, ConditionPlan), CodegenError> {
        let Some(cond) = condition else {
            return Ok((Vec::new(), ConditionPlan::default()));
        };

        let (prelude, boolean_until_conditions) =
            self.build_until_conditions(cond, next_bindings)?;

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
            self.features.mark_timely_map();
            self.features.mark_as_collection();
        }

        let connective = cond.connective().cloned();
        Ok((
            prelude,
            ConditionPlan {
                boolean_until_conditions,
                iter_while_conditions,
                connective,
            },
        ))
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
    ) -> Result<Vec<TokenStream>, CodegenError> {
        let (mut stmts, plan) = self.prepare_loop_condition(condition, next_bindings)?;
        let has_iterative = !iterative_fps.is_empty();

        for (fp, recursive_ident) in recursive_bindings {
            let next_ident = next_bindings.get(fp).ok_or_else(|| {
                CodegenError::internal(format!(
                    "recursive relation fingerprint 0x{fp:016x} missing \
                     from next bindings"
                ))
            })?;
            with_profiler(profiler, |profiler| {
                profiler.recursive_resultsin_operator(
                    self.display_name(*fp),
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
                (p, n, self.dedup_recursive())
            } else {
                (quote! {}, quote! {}, quote! {})
            };

            // Choose the feedback source:
            //   - Iterative (replacement semantics): feed back the derived value only;
            //     no self-union, since iterative variables retract by overwriting.
            //   - Accumulative in a mixed loop: explicitly union with `recursive_X`,
            //     because iterative relations can retract and that retraction must
            //     not leak into the accumulative tally — we keep monotonicity by
            //     re-adding the previous self each iteration.
            //   - Purely accumulative loop: also feed back `next_X` directly. All
            //     rules are monotone, so every prior fact is re-derived and no
            //     self-union is required.
            let source = if !iterative_fps.contains(fp) && has_iterative {
                let acc_next = format_ident!("acc_next_{}", fp);
                stmts.push(quote! {
                    let #acc_next = #next_ident.clone().concat(#recursive_ident.clone()) #dedup;
                });
                acc_next
            } else {
                next_ident.clone()
            };
            let feedback = build_feedback_expr(
                &source,
                recursive_ident,
                &plan,
                &pos,
                &neg,
                &dedup,
                &normalize,
            );

            stmts.push(quote! { #var_name.set(#feedback); });
        }

        Ok(stmts)
    }

    /// Build prelude stmts and the arranged gate signal for the `until` clause.
    fn build_until_conditions(
        &mut self,
        cond: &LoopCondition,
        next_bindings: &HashMap<u64, Ident>,
    ) -> Result<(Vec<TokenStream>, Option<Ident>), CodegenError> {
        let Some(until_group) = cond.until_part() else {
            return Ok((Vec::new(), None));
        };
        self.features.mark_as_collection();
        self.features.mark_timely_map();

        let mut stmts: Vec<TokenStream> = Vec::new();

        // Initialise gate from the first until relation.
        let first_fp = until_group.first().fp();
        let first_next = next_bindings.get(&first_fp).ok_or_else(|| {
            CodegenError::internal(format!(
                "until relation fingerprint 0x{first_fp:016x} missing \
                 from next bindings"
            ))
        })?;
        let first_sig = format_ident!("rel_sig_{}", first_fp);
        let dedup = self.dedup_recursive();
        stmts.push(quote! { let #first_sig = #first_next.clone() #dedup; });
        let mut gate = first_sig;

        // Fold remaining until relations into the gate with explicit connectives.
        for (conn, rel) in until_group.rest() {
            let fp = rel.fp();
            let next_ident = next_bindings.get(&fp).ok_or_else(|| {
                CodegenError::internal(format!(
                    "until relation fingerprint 0x{fp:016x} missing from \
                     next bindings"
                ))
            })?;
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
        Ok((stmts, Some(arr)))
    }
}

// =========================================================================
// Loop condition plan
// =========================================================================

/// Analyzed loop condition, ready for feedback-expression code generation.
///
/// Built by `CodeGen::prepare_loop_condition`; consumed by `build_feedback_expr`.
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
        (None, Some(arr)) => stop_stmt(
            quote! { #next.clone() },
            recursive,
            arr,
            pos,
            neg,
            normalize,
        ),
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
