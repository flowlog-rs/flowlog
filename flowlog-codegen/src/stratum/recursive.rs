//! A recursive stratum: its prelude, then the loop the planner's recursive
//! steps run in, emitted in six phases that read in the order of the code
//! they produce. Each phase leaves bindings in a [`Recursion`] for the
//! next: the collections that enter, the feedback variables, the bodies,
//! each relation's binding, the feedback, and the leave.

use std::collections::HashMap;

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
use crate::ident::find_local_ident;
use crate::internal_tuple_tokens;
use crate::ty::diff::weight_tokens;

// =============================================================================
// Recursion
// =============================================================================

/// A recursive stratum's loop as its phases build it: the bindings each
/// phase leaves for the next, by the fingerprint of the relation or
/// collection they hold.
struct Recursion {
    /// The weight every relation binding shares, which picks the loop's time.
    mutability: Mutability,
    /// `in_X` for each relation or arrangement `X` that enters the loop.
    entered: HashMap<u64, Ident>,
    /// The arrangement cache inside the loop: each entering arrangement as
    /// it entered, then what the bodies arrange.
    arranged: HashMap<u64, Ident>,
    /// `recursive_X` for each feedback relation `X`: the variable its rules
    /// read.
    variables: HashMap<u64, Ident>,
    /// Every binding a body reads: the entered ones, then the variables,
    /// which shadow an entered feedback relation.
    local_fp_to_ident: HashMap<u64, Ident>,
    /// `next_X` for each relation once its binding has been built: the
    /// union of its rule results with the entered seed, after the binding
    /// operation. The recursion feeds back and leaves this binding.
    bindings: HashMap<u64, Ident>,
}

impl Recursion {
    /// A recursion at `mutability` with nothing bound yet.
    fn new(mutability: Mutability) -> Self {
        Self {
            mutability,
            entered: HashMap::new(),
            arranged: HashMap::new(),
            variables: HashMap::new(),
            local_fp_to_ident: HashMap::new(),
            bindings: HashMap::new(),
        }
    }

    /// Returns relation `fp`'s completed binding, or an internal error
    /// when it has none. Every relation the recursion feeds back or leaves
    /// must have a completed binding.
    fn binding(&self, fp: u64) -> Result<&Ident, CodegenError> {
        self.bindings.get(&fp).ok_or_else(|| {
            CodegenError::internal(format!(
                "relation fingerprint 0x{fp:016x} has no completed binding in its loop"
            ))
        })
    }
}

// =============================================================================
// Phases
// =============================================================================

impl Codegen {
    /// Returns a recursive `stratum`: its prelude, then its loop, `let
    /// <outputs> = scope.scoped(|inner| { ... });`, or no loop when no
    /// relation leaves it. Also records the weight of every relation that
    /// leaves.
    pub(super) fn gen_recursive(
        &mut self,
        stratum: &StratumPlanner,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let prelude = self.gen_prelude(stratum, plan_graph)?;

        // Nothing leaves this recursion: legal but unobservable, so no loop
        // scope is emitted, and none is recorded below, which keeps the
        // predicted addresses aligned with the dataflow.
        let leave_fps = stratum.recursion_leave_collections();
        if leave_fps.is_empty() {
            return Ok(quote! { #(#prelude)* });
        }

        // Every relation binding in a recursive stratum has the same weight,
        // which with the engine picks the loop's time.
        let mutability = self.recursive_mutability(stratum)?;
        let (inner_time, step) = self.inner_time_tokens(mutability)?;
        let mut recursion = Recursion::new(mutability);

        // The plan graph mirrors the dataflow: the scope opens here and
        // closes in the leave, after the operators inside it.
        with_plan_graph(plan_graph, |plan_graph| {
            plan_graph.enter_scope();
        });

        // Phase 1: enter. An arranged input enters as the arrangement the
        // prelude left in the outer cache, so the loop never rebuilds it.
        let enter_stmts = self.gen_enter(stratum, &mut recursion, plan_graph);

        // Phase 2: feedback variables, after the entered bindings so that a
        // variable shadows the entered binding of its own relation.
        let variable_stmts = self.gen_variables(stratum, &mut recursion, &step, plan_graph)?;

        // Phase 3: the bodies, over the entered bindings and the variables.
        let body_stmts: Vec<TokenStream> = stratum
            .recursive_transformations()
            .iter()
            .map(|tx| {
                self.gen_operator(
                    &recursion.local_fp_to_ident,
                    tx,
                    &mut recursion.arranged,
                    stratum,
                    plan_graph,
                )
            })
            .collect::<Result<_, _>>()?;

        // Phase 4: each relation's binding, `next_X`, which the feedback and
        // the leave read.
        let binding_stmts = self.gen_relation_bindings(stratum, &mut recursion, plan_graph)?;

        // Phase 5: feedback, `recursive_X_var.set(next_X)`.
        let set_stmts = self.gen_feedback(stratum, &recursion, plan_graph)?;

        // Phase 6: leave. Last, as its operators are built last in the plan
        // graph.
        let (leave_pattern, leave_expr) = self.gen_leave(stratum, &recursion, plan_graph)?;
        for fp in leave_fps {
            self.global_fp_to_mutability.insert(*fp, mutability);
        }

        let body = quote! {
            |inner| {
                use ::flowlog_runtime::differential_dataflow::operators::iterate::Variable;
                #(#enter_stmts)*
                #(#variable_stmts)*
                #(#body_stmts)*
                #(#binding_stmts)*
                #(#set_stmts)*
                #leave_expr
            }
        };
        Ok(quote! {
            #(#prelude)*
            let #leave_pattern = scope.scoped::<#inner_time, _, _>("Iterative", #body);
        })
    }

    /// Returns `let in_X = X.enter(inner);` for each collection entering
    /// the loop of `stratum`, binding it in `recursion`, and binding an
    /// entering arrangement in the loop's arrangement cache as well.
    fn gen_enter(
        &self,
        stratum: &StratumPlanner,
        recursion: &mut Recursion,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Vec<TokenStream> {
        let mut stmts = Vec::new();
        for fp in stratum.recursion_enter_collections() {
            let arrangement = self.outer_fp_to_arrangement.get(fp);
            let source = arrangement
                .cloned()
                .unwrap_or_else(|| self.find_global_ident(*fp));
            let entered = format_ident!("in_{}", source);
            // Clone before entering: an outer-scope arrangement shared
            // across strata through the program-wide cache may be entered
            // by several recursive blocks. TraceAgent is Rc-backed, so the
            // clone is cheap.
            stmts.push(quote! { let #entered = #source.clone().enter(inner); });
            with_plan_graph(plan_graph, |plan_graph| {
                plan_graph.recursive_enter_operator(source.to_string(), entered.to_string());
            });
            if let Some(arrangement) = arrangement {
                recursion
                    .arranged
                    .insert(*fp, format_ident!("in_{}", arrangement));
            }
            recursion.entered.insert(*fp, entered);
        }
        stmts
    }

    /// Declares empty feedback variables with each relation's internal row
    /// type and the loop's weight, and registers their bindings in `recursion`.
    fn gen_variables(
        &self,
        stratum: &StratumPlanner,
        recursion: &mut Recursion,
        step: &TokenStream,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Vec<TokenStream>, CodegenError> {
        // Before `set` constrains the variable, closures may read its row
        // fields and arrangements dispatch on its weight. Both types must
        // be known at those uses.
        let weight = weight_tokens(recursion.mutability);
        let string_intern = self.config.str_intern_enabled();
        let mut stmts = Vec::new();
        for fp in stratum.recursion_feedback_collections() {
            let (_, row_types) = self.find_global_type(*fp)?;
            let row_type = internal_tuple_tokens(row_types, string_intern);
            let name = format_ident!("recursive_{}", self.find_global_ident(*fp));
            with_plan_graph(plan_graph, |plan_graph| {
                plan_graph.recursive_feedback_operator(
                    self.display_name(*fp),
                    name.to_string(),
                    name.to_string(),
                );
            });
            let var_name = format_ident!("{}_var", name);
            stmts.push(quote! {
                let (#var_name, #name) =
                    Variable::<_, Vec<(#row_type, _, #weight)>>::new(inner, #step);
            });
            recursion.variables.insert(*fp, name);
        }
        recursion.local_fp_to_ident = recursion.entered.clone();
        recursion
            .local_fp_to_ident
            .extend(recursion.variables.clone());
        Ok(stmts)
    }

    /// Returns each relation's binding in `stratum` as `next_<fp>`,
    /// applying its planned operation to the union of its rule results
    /// and entered seed. Records the completed bindings in `recursion`.
    fn gen_relation_bindings(
        &self,
        stratum: &StratumPlanner,
        recursion: &mut Recursion,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Vec<TokenStream>, CodegenError> {
        let mutability = recursion.mutability;
        let mut stmts = Vec::new();
        for (idb_fp, binding_plan) in stratum.relation_bindings() {
            let name = self.display_name(*idb_fp);
            let next = format_ident!("next_{idb_fp}");
            let parts =
                self.recursive_parts(recursion, binding_plan.seed(), binding_plan.inputs())?;
            let (rows, found) = self.gen_relation_binding(
                *idb_fp,
                &parts,
                binding_plan.operation(),
                &next,
                /*recursive*/ true,
                plan_graph,
            )?;
            // The stratifier assigns the relation the same value on its own
            // (Lemma 4 of `docs/design/mutability.md`), and every relation
            // binding of the loop shares the loop's weight.
            debug_assert_eq!(
                stratum.mutability(*idb_fp),
                Some(found),
                "`{name}` binds at a weight the stratifier does not give it",
            );
            debug_assert_eq!(found, mutability, "`{name}` binds off the loop's weight");
            stmts.push(quote! { let #next = #rows; });
            recursion.bindings.insert(*idb_fp, next);
        }
        Ok(stmts)
    }

    /// Returns `recursive_X_var.set(next_X);` for each feedback relation of
    /// `stratum`, in order, feeding its completed binding in `recursion`
    /// back into the loop.
    fn gen_feedback(
        &self,
        stratum: &StratumPlanner,
        recursion: &Recursion,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Vec<TokenStream>, CodegenError> {
        let mut stmts = Vec::new();
        for fp in stratum.recursion_feedback_collections() {
            let next = recursion.binding(*fp)?;
            let variable = recursion.variables.get(fp).ok_or_else(|| {
                CodegenError::internal(format!(
                    "feedback relation fingerprint 0x{fp:016x} has no variable"
                ))
            })?;
            with_plan_graph(plan_graph, |plan_graph| {
                plan_graph.recursive_resultsin_operator(
                    self.display_name(*fp),
                    next.to_string(),
                    next.to_string(),
                );
            });
            let var_name = format_ident!("{}_var", variable);
            stmts.push(quote! { #var_name.set(#next.clone()); });
        }
        Ok(stmts)
    }

    /// Returns the pattern binding the relations that leave the loop of
    /// `stratum`, and the expression leaving them: each one's `next`
    /// binding in `recursion`, or, for a lattice relation leaving a static
    /// loop, the fold of that binding outside the loop, since a static
    /// merge keeps every iteration's answer. Records the scope exit in the
    /// plan graph, between the folds' inner side and their outer side.
    fn gen_leave(
        &self,
        stratum: &StratumPlanner,
        recursion: &Recursion,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<(TokenStream, TokenStream), CodegenError> {
        let leave_fps = stratum.recursion_leave_collections();
        let targets: Vec<Ident> = leave_fps
            .iter()
            .map(|fp| self.find_global_ident(*fp))
            .collect();
        let pattern = match targets.as_slice() {
            [ident] => quote! { #ident },
            _ => quote! { ( #(#targets),* ) },
        };

        let mut folds = Vec::with_capacity(leave_fps.len());
        for fp in leave_fps {
            if recursion.mutability != Mutability::Static || !self.program.is_lattice(*fp) {
                folds.push(None);
                continue;
            }
            let folded = format_ident!("folded_{fp}");
            let fold = self.gen_lattice_fold(*fp, recursion.binding(*fp)?, &folded, plan_graph)?;
            folds.push(Some((folded, fold)));
        }

        // An unbalanced leave is a codegen bug; surface it instead of
        // corrupting the addresses that follow.
        try_with_plan_graph(plan_graph, |plan_graph| plan_graph.leave_scope())
            .map_err(|e| CodegenError::internal(format!("recording recursive scope exit: {e}")))?;

        let mut leave_exprs = Vec::with_capacity(leave_fps.len());
        for ((fp, target), fold) in leave_fps.iter().zip(&targets).zip(folds) {
            let name = self.display_name(*fp);
            let next = recursion.binding(*fp)?;
            leave_exprs.push(match fold {
                Some((folded, fold)) => {
                    with_plan_graph(plan_graph, |plan_graph| {
                        plan_graph.recursive_post_leave_lattice_operator(
                            name,
                            folded.to_string(),
                            folded.to_string(),
                        );
                    });
                    quote! {{ #fold #folded }}
                }
                None => {
                    with_plan_graph(plan_graph, |plan_graph| {
                        plan_graph.recursive_leave_operator(
                            name,
                            next.to_string(),
                            target.to_string(),
                        );
                    });
                    quote! { #next.leave(scope) }
                }
            });
        }
        let leave_expr = match leave_exprs.as_slice() {
            [expr] => quote! { #expr },
            _ => quote! { ( #(#leave_exprs),* ) },
        };
        Ok((pattern, leave_expr))
    }
}

// =============================================================================
// Pieces
// =============================================================================

impl Codegen {
    /// Returns the mutability shared by the loop body collections and
    /// relation bindings of `stratum`. Each body collection reads a
    /// feedback variable, directly or through another, and the planner
    /// factors everything else out as the stratum's prelude; so in a mutable
    /// loop only collections entered from outside can be static. A body
    /// whose collections disagree, or that has none, is a planner bug,
    /// reported as an internal error.
    fn recursive_mutability(&self, stratum: &StratumPlanner) -> Result<Mutability, CodegenError> {
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

    /// Returns a relation's seed followed by its rule results, with their
    /// scope's weights. An entered seed keeps its outer weight; feedback
    /// results use the recursion's weight, and other results keep their
    /// recorded weight.
    ///
    /// # Errors
    ///
    /// Returns an internal error if the seed has no entered binding or an
    /// input has no recorded weight.
    fn recursive_parts(
        &self,
        recursion: &Recursion,
        seed: Option<u64>,
        rule_outputs: &[u64],
    ) -> Result<Vec<(Ident, Mutability)>, CodegenError> {
        let entered = seed.map(|fp| {
            let binding = recursion.entered.get(&fp).ok_or_else(|| {
                CodegenError::internal(format!(
                    "planned relation seed 0x{fp:016x} has no entered binding"
                ))
            })?;
            Ok((binding.clone(), self.mutability(fp)?))
        });
        entered
            .into_iter()
            .chain(rule_outputs.iter().map(|fp| {
                let weight = if recursion.variables.contains_key(fp) {
                    recursion.mutability
                } else {
                    self.mutability(*fp)?
                };
                Ok((find_local_ident(&recursion.local_fp_to_ident, *fp), weight))
            }))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use proc_macro2::Span;

    use super::*;
    use crate::test_harness::codegen;

    /// A static input `R` and a static output `S` derived from it.
    const PROGRAM: &str =
        ".decl R(a: int32)\n.input R\n.decl S(a: int32)\nS(a) :- R(a).\n.output S\n";

    fn ident(name: &str) -> Ident {
        Ident::new(name, Span::call_site())
    }

    /// Seed absence is resolved before token emission; inspect bindings
    /// directly instead of reconstructing them from generated Rust.
    #[test]
    fn a_recursive_binding_without_a_seed_uses_its_rule_results() {
        let codegen = codegen(PROGRAM);
        let relation = codegen.program.idbs()[0].fingerprint();
        let mut recursion = Recursion::new(Mutability::Mutable);
        recursion.variables.insert(relation, ident("recursive_s"));
        recursion
            .local_fp_to_ident
            .insert(relation, ident("recursive_s"));
        let parts = codegen
            .recursive_parts(&recursion, None, &[relation])
            .expect("the rule reads feedback without an outer seed");
        assert_eq!(parts, vec![(ident("recursive_s"), Mutability::Mutable)]);
    }

    /// Completed plans cannot contain an unresolved seed, so exercise the
    /// resolver directly to verify rejection at the codegen boundary.
    #[test]
    fn a_recursive_seed_without_an_entered_binding_is_an_internal_error() {
        let codegen = codegen(PROGRAM);
        let relation = 0x7;
        let mut recursion = Recursion::new(Mutability::Mutable);
        recursion.variables.insert(relation, ident("recursive_s"));
        recursion
            .local_fp_to_ident
            .insert(relation, ident("recursive_s"));
        let error = codegen
            .recursive_parts(&recursion, Some(relation), &[relation])
            .expect_err("feedback cannot substitute for an entered seed");
        assert!(matches!(&error, CodegenError::Internal(_)));
        assert!(
            error
                .to_string()
                .contains("planned relation seed 0x0000000000000007 has no entered binding"),
            "{error}"
        );
    }

    /// Per-input weights are resolved before emission; inspect scope
    /// bindings directly to distinguish a static seed from feedback.
    #[test]
    fn recursive_parts_weigh_the_entered_binding_and_rule_results_apart() {
        let mut codegen = codegen(PROGRAM);
        let s = codegen.program.idbs()[0].fingerprint();
        let r = codegen.program.edbs()[0].fingerprint();
        codegen
            .global_fp_to_mutability
            .insert(s, Mutability::Static);
        codegen
            .global_fp_to_mutability
            .insert(0x7, Mutability::Mutable);
        let mut recursion = Recursion::new(Mutability::Mutable);
        recursion.entered.insert(s, ident("in_s"));
        recursion.variables.insert(s, ident("recursive_s"));
        recursion.local_fp_to_ident = [(s, ident("recursive_s")), (r, ident("in_r"))].into();

        let parts = codegen
            .recursive_parts(&recursion, Some(s), &[s, r, 0x7])
            .expect("every part has a weight");
        assert_eq!(
            parts,
            vec![
                (ident("in_s"), Mutability::Static),
                (ident("recursive_s"), Mutability::Mutable),
                (ident("in_r"), Mutability::Static),
                (ident("t_7"), Mutability::Mutable),
            ]
        );
    }
}
