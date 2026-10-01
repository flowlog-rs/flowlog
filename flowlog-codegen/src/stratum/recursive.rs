//! A recursive stratum: its loop scope, the collections that enter it, the
//! feedback variables, the rule bodies and heads inside it, and the leave.
//! Each relation's head binds `next_<fp>` and feeds it back through its
//! variable `recursive_<name>`, which the next iteration reads; the
//! prelude's outer-scope arrangements are what enters the loop.

use std::collections::HashMap;

use flowlog_parser::AggregationOperator;
use flowlog_parser::Mutability;
use flowlog_planner::planner::StratumPlanner;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::try_with_plan_graph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;
use crate::expr::aggregation::aggregation_empty_key;
use crate::expr::aggregation::aggregation_kind;
use crate::expr::aggregation::aggregation_merge;
use crate::expr::aggregation::aggregation_split;

impl Codegen {
    /// Returns the loop of a recursive `stratum`, `let <outputs> =
    /// scope.scoped(|inner| { ... });`, or nothing when no relation leaves
    /// it. Inside, the loop enters its inputs, reading an arranged one from
    /// `outer_fp_to_arrangement`, declares its feedback variables, runs the rule
    /// bodies and each relation's head step, feeds the heads back, and
    /// leaves. Also records the weight of every relation that leaves.
    pub(super) fn gen_recursive(
        &mut self,
        outer_fp_to_arrangement: &HashMap<u64, Ident>,
        stratum: &StratumPlanner,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        self.features.mark_recursive();

        // Nothing leaves this recursion: legal but unobservable, so no loop
        // scope is emitted, and none is recorded below, which keeps the
        // predicted addresses aligned with the dataflow.
        let leave_fps = stratum.recursion_leave_collections();
        if leave_fps.is_empty() {
            return Ok(quote! {});
        }

        // Every head of a recursive stratum has the same mutability, which
        // with the engine picks the loop's time.
        let mutability = self.loop_mutability(stratum)?;
        let (loop_time, step) = self.inner_time_tokens(mutability)?;

        with_plan_graph(plan_graph, |plan_graph| {
            plan_graph.enter_scope();
        });

        // --- Enter bindings ---
        let enter_fps = stratum.recursion_enter_collections();
        let (enter_stmts, enter_bindings, mut recursive_arranged) =
            self.build_enter_bindings(outer_fp_to_arrangement, enter_fps, plan_graph);

        // --- Recursive variable bindings ---
        // Every feedback variable starts empty and grows monotonically, so
        // `Variable::new` covers all of them.
        let feedback_fps = stratum.recursion_feedback_collections();
        let (feedback_names, recursive_bindings) = self.build_recursive_bindings(feedback_fps);

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

        // --- Head step per IDB (next_X), as the loop's feedback ---
        let mut next_bindings: HashMap<u64, Ident> = HashMap::new();
        let mut union_stmts = Vec::new();
        for (idb_fp, head_fps) in stratum.idb_to_heads_map() {
            let entered = enter_bindings.get(idb_fp).cloned();
            let (code, binding, found) =
                self.gen_head(stratum, *idb_fp, head_fps, entered, true, plan_graph)?;
            debug_assert_eq!(
                found,
                mutability,
                "`{}` unions off the loop's weight",
                self.display_name(*idb_fp),
            );
            next_bindings.insert(*idb_fp, binding);
            union_stmts.push(code);
        }

        // --- Feedback assignments (Variable::set) ---
        let set_stmts = self.gen_feedback(
            feedback_fps,
            &next_bindings,
            &recursive_bindings,
            plan_graph,
        )?;

        // --- Leave outputs ---
        let (leave_pattern, leave_stmt) = self.build_leave_outputs(
            leave_fps,
            &next_bindings,
            stratum.idb_to_aggregation_map(),
            mutability,
            plan_graph,
        )?;
        for fp in leave_fps {
            self.global_fp_to_mutability.insert(*fp, mutability);
        }

        let body = quote! {
            |inner| {
                #(#enter_stmts)*
                #(#recursive_var_inits)*
                #(#flow_stmts)*
                #(#union_stmts)*
                #(#set_stmts)*
                #leave_stmt
            }
        };
        Ok(quote! {
            let #leave_pattern = scope.scoped::<#loop_time, _, _>("Iterative", #body);
        })
    }

    /// Returns the mutability every collection the loop body of `stratum`
    /// produces shares, which is its heads' too. Each such collection reads
    /// a feedback variable, directly or through another, and the planner
    /// factors everything else out as the stratum's prelude; so in a mutable
    /// loop only collections entered from outside can be static. A body
    /// whose collections disagree, or that has none, is a planner bug,
    /// reported as an internal error.
    fn loop_mutability(&self, stratum: &StratumPlanner) -> Result<Mutability, CodegenError> {
        let outputs: Vec<_> = stratum
            .recursive_transformations()
            .iter()
            .map(|tx| tx.output())
            .collect();
        let Some(mutability) = outputs.first().map(|output| output.mutability()) else {
            return Err(CodegenError::internal("recursive stratum has no loop body"));
        };
        if let Some(output) = outputs
            .iter()
            .find(|output| output.mutability() != mutability)
        {
            return Err(CodegenError::internal(format!(
                "loop body collection 0x{:016x} is {:?} in a {mutability:?} loop",
                output.fingerprint(),
                output.mutability(),
            )));
        }
        Ok(mutability)
    }

    /// Returns one `let in_X = X.enter(inner);` per entering collection, the
    /// map from each one's fingerprint to its entered binding, and the same
    /// map for the arranged ones, whose entered arrangement is `in_<X_arr>`.
    /// An arranged collection enters as its arrangement from `outer_fp_to_arrangement`.
    fn build_enter_bindings(
        &self,
        outer_fp_to_arrangement: &HashMap<u64, Ident>,
        enter_fps: &[u64],
        plan_graph: &mut Option<PlanGraph>,
    ) -> (Vec<TokenStream>, HashMap<u64, Ident>, HashMap<u64, Ident>) {
        let mut bindings: HashMap<u64, Ident> = HashMap::new();
        let mut stmts: Vec<TokenStream> = Vec::new();
        let mut recursive_arranged: HashMap<u64, Ident> = HashMap::new();

        for fp in enter_fps {
            let source = outer_fp_to_arrangement
                .get(fp)
                .cloned()
                .unwrap_or_else(|| self.find_global_ident(*fp));
            let entered = format_ident!("in_{}", source);
            bindings.insert(*fp, entered.clone());
            // Clone before entering: when an outer-scope arrangement is
            // shared across strata (via program-wide `outer_fp_to_arrangement`),
            // multiple recursive blocks may each need to enter it.
            // TraceAgent is Rc-backed so the clone is cheap.
            stmts.push(quote! { let #entered = #source.clone().enter(inner); });

            with_plan_graph(plan_graph, |plan_graph| {
                plan_graph.recursive_enter_operator(source.to_string(), entered.to_string());
            });

            if let Some(arranged) = outer_fp_to_arrangement.get(fp) {
                let entered_arr = format_ident!("in_{}", arranged);
                recursive_arranged.insert(*fp, entered_arr);
            }
        }

        (stmts, bindings, recursive_arranged)
    }

    /// Returns the pattern binding the relations that leave the loop, and
    /// the expression leaving each one's `next` binding. An aggregated
    /// relation of a static loop, whose weight is `mutability`, leaves
    /// through `flowlog_reduce_leave`, which owns the boundary fold. Records
    /// the scope exit and each leave in the plan graph.
    fn build_leave_outputs(
        &self,
        leave_fps: &[u64],
        next: &HashMap<u64, Ident>,
        idb_to_aggregation_map: &HashMap<u64, (AggregationOperator, usize, usize)>,
        mutability: Mutability,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<(TokenStream, TokenStream), CodegenError> {
        // A static aggregate cannot retract an earlier answer, so it folds
        // across the boundary instead of leaving answer by answer.
        let folds_at_leave = match mutability {
            Mutability::Static => true,
            Mutability::Mutable => false,
        };

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
                let next_ident = next_binding(next, *fp)?;

                // Static aggregated relations complete across the boundary:
                // `flowlog_reduce_leave` lifts contributions into the
                // semiring diff, leaves, and folds every iteration once at
                // the outer timestamp.
                if let Some((agg_op, agg_pos, agg_arity)) = idb_to_aggregation_map.get(fp)
                    && folds_at_leave
                {
                    let kind = aggregation_kind(*agg_op);
                    let split = aggregation_split(*agg_arity, *agg_pos);
                    let agg_type = self.agg_column_type(*fp, *agg_pos)?;
                    let merge = aggregation_merge(*agg_arity, *agg_pos, &agg_type);
                    // Min/max can fold their improving bounds: the final
                    // extreme is unchanged, and fewer rows cross the boundary.
                    // Count/sum/avg need original contributions. For example,
                    // summing running answers 2 and 5 would give 7, although
                    // the original inputs 2 and 3 sum to 5.
                    let input = match agg_op {
                        AggregationOperator::Min | AggregationOperator::Max => next_ident.clone(),
                        AggregationOperator::Count
                        | AggregationOperator::Sum
                        | AggregationOperator::Avg => format_ident!("next_{fp}"),
                    };
                    let empty_key = aggregation_empty_key(*agg_arity);

                    with_plan_graph(plan_graph, |plan_graph| {
                        plan_graph.recursive_pre_leave_static_aggregate_operator(
                            self.display_name(*fp),
                            input.to_string(),
                            next_ident.to_string(),
                        );
                    });

                    let op_name = format!("ReduceLeave: {}", self.display_name(*fp));
                    return Ok(quote! {
                        ::flowlog_runtime::operators::flowlog_reduce_leave(
                            #input, scope, #op_name, #kind, #empty_key, #split, #merge,
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
            let next_ident = next_binding(next, *fp)?;

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
            if idb_to_aggregation_map.contains_key(fp) && folds_at_leave {
                with_plan_graph(plan_graph, |plan_graph| {
                    plan_graph.recursive_post_leave_static_aggregate_operator(
                        self.display_name(*fp),
                        target.to_string(),
                        target.to_string(),
                    );
                });
            }
        }

        Ok((pattern, leave_stmt))
    }

    /// Returns the feedback variable `recursive_<name>` of each of
    /// `recursive_fps`, in order, and the same names by fingerprint.
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

    /// Returns `recursive_<name>_var.set(next);` for each of `feedback_fps`,
    /// in order, feeding its head's `next` binding back into the loop.
    fn gen_feedback(
        &self,
        feedback_fps: &[u64],
        next_bindings: &HashMap<u64, Ident>,
        recursive_bindings: &HashMap<u64, Ident>,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Vec<TokenStream>, CodegenError> {
        let mut stmts = Vec::new();

        for fp in feedback_fps {
            let next_ident = next_binding(next_bindings, *fp)?;
            let recursive_ident = recursive_bindings.get(fp).ok_or_else(|| {
                CodegenError::internal(format!(
                    "feedback relation fingerprint 0x{fp:016x} has no variable"
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

/// Returns relation `fp`'s `next` binding inside the loop, or an internal
/// error when it has none: every relation the loop feeds back or leaves is
/// one of its heads.
fn next_binding(next: &HashMap<u64, Ident>, fp: u64) -> Result<&Ident, CodegenError> {
    next.get(&fp).ok_or_else(|| {
        CodegenError::internal(format!(
            "relation fingerprint 0x{fp:016x} has no next binding in its loop"
        ))
    })
}
