//! A recursive stratum: its prelude, then the loop the planner's recursive
//! steps run in, emitted in six phases that read in the order of the code
//! they produce. Each phase leaves bindings in a [`Recursion`] for the
//! next: the collections that enter, the feedback variables, the bodies,
//! each relation's binding, the feedback, and the leave.

use std::collections::HashMap;
use std::sync::Arc;

use flowlog_parser::Mutability;
use flowlog_planner::planner::Collection;
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
    /// relation leaves it. Also records the weight every relation that
    /// leaves has after it.
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
        let inner_time = self.inner_time_tokens(mutability)?;
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
        // variable shadows the entered binding of its own relation. The
        // layouts change here, after the prelude read the earlier ones:
        // a variable holds rows in its binding's order from the first
        // iteration on.
        for (relation, binding) in stratum.relation_bindings() {
            self.record_binding_layout(*relation, binding.operation())?;
        }
        let variable_stmts = self.gen_variables(stratum, &mut recursion, plan_graph)?;

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
            let leaves = Self::leave_mutability(stratum, *fp)?;
            self.global_fp_to_mutability.insert(*fp, leaves);
        }

        let body = quote! {
            |inner| {
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
            if arrangement.is_some() {
                recursion.arranged.insert(*fp, entered.clone());
            }
            recursion.entered.insert(*fp, entered);
        }
        stmts
    }

    /// Declares empty feedback variables with each relation's internal row
    /// type and the loop's weight, and registers their bindings in `recursion`.
    /// A variable records a recursion still changing in the loop's last
    /// iteration in the run's `failure`, which the frontend has in scope.
    fn gen_variables(
        &self,
        stratum: &StratumPlanner,
        recursion: &mut Recursion,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Vec<TokenStream>, CodegenError> {
        // Before `set` constrains the variable, closures may read its row
        // fields and arrangements dispatch on its weight. Both types must
        // be known at those uses.
        let weight = weight_tokens(recursion.mutability);
        let string_intern = self.config.str_intern_enabled();
        let mut stmts = Vec::new();
        for fp in stratum.recursion_feedback_collections() {
            let row_types = self.row_types(*fp)?;
            let row_type = internal_tuple_tokens(&row_types, string_intern);
            let name = format_ident!("recursive_{}", self.find_global_ident(*fp));
            let relation = self.display_name(*fp);
            with_plan_graph(plan_graph, |plan_graph| {
                plan_graph.recursive_feedback_operator(
                    relation.clone(),
                    name.to_string(),
                    name.to_string(),
                );
            });
            let var_name = format_ident!("{}_var", name);
            stmts.push(quote! {
                let (#var_name, #name) = ::flowlog_runtime::operators::flowlog_variable::<
                    _,
                    #row_type,
                    #weight
                >(inner, #relation, &failure);
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
        &mut self,
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
                Self::recursive_parts(recursion, binding_plan.seed(), binding_plan.inputs())?;
            let answers = stratum.mutability(*idb_fp).ok_or_else(|| {
                CodegenError::internal(format!("relation `{name}` has no planned weight"))
            })?;
            // Every relation binding of the loop shares the loop's weight: a
            // lattice relation climbs inside the loop and pays as it leaves.
            debug_assert_eq!(answers, mutability, "`{name}` binds off the loop's weight");
            let rows = self.gen_relation_binding(
                *idb_fp,
                &parts,
                binding_plan.operation(),
                answers,
                &next,
                /*recursive*/ true,
                plan_graph,
            )?;
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
    /// binding in `recursion`, left, and, for a lattice relation leaving a
    /// presence loop, merged again outside it. Its answers climbed inside,
    /// keeping each beaten bound, so the merge answers them at the weight
    /// the relation leaves with: a static loop's climb again, to their
    /// final bounds; an append loop's pay. Records the scope exit in the
    /// plan graph before the operators outside it.
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

        // An unbalanced leave is a codegen bug; surface it instead of
        // corrupting the addresses that follow.
        try_with_plan_graph(plan_graph, |plan_graph| plan_graph.leave_scope())
            .map_err(|e| CodegenError::internal(format!("recording recursive scope exit: {e}")))?;

        let mut leave_exprs = Vec::with_capacity(leave_fps.len());
        for (fp, target) in leave_fps.iter().zip(&targets) {
            let next = recursion.binding(*fp)?;
            with_plan_graph(plan_graph, |plan_graph| {
                plan_graph.recursive_leave_operator(
                    self.display_name(*fp),
                    next.to_string(),
                    target.to_string(),
                );
            });
            let left = quote! { #next.leave(scope) };
            leave_exprs.push(
                if recursion.mutability == Mutability::Mutable || !self.program.is_lattice(*fp) {
                    left
                } else {
                    let leaves = Self::leave_mutability(stratum, *fp)?;
                    let path = self.merge_path(*fp, recursion.mutability, leaves)?;
                    self.gen_lattice_merge(*fp, left, path, None, target, plan_graph)?
                },
            );
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
    /// Returns the weight relation `fp` has once it leaves the loop of
    /// `stratum`, the planner's.
    ///
    /// # Errors
    ///
    /// Returns an internal error when the planner gives it none.
    fn leave_mutability(stratum: &StratumPlanner, fp: u64) -> Result<Mutability, CodegenError> {
        stratum.leave_mutability(fp).ok_or_else(|| {
            CodegenError::internal(format!(
                "relation fingerprint 0x{fp:016x} has no planned weight after its loop"
            ))
        })
    }

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

    /// Names a relation's seed, then its rule results, inside the loop:
    /// the seed by its entered binding, each result by its loop binding,
    /// which for a feedback relation is its variable. Every weight is the
    /// plan's, where a loop-body collection carries the loop's weight.
    ///
    /// # Errors
    ///
    /// Returns an internal error if the seed has no entered binding.
    fn recursive_parts(
        recursion: &Recursion,
        seed: Option<&Collection>,
        rule_outputs: &[Arc<Collection>],
    ) -> Result<Vec<(Ident, Mutability)>, CodegenError> {
        let entered = seed.map(|seed| {
            let fp = seed.fingerprint();
            let binding = recursion.entered.get(&fp).ok_or_else(|| {
                CodegenError::internal(format!(
                    "planned relation seed 0x{fp:016x} has no entered binding"
                ))
            })?;
            Ok((binding.clone(), seed.mutability()))
        });
        entered
            .into_iter()
            .chain(rule_outputs.iter().map(|output| {
                Ok((
                    find_local_ident(&recursion.local_fp_to_ident, output.fingerprint()),
                    output.mutability(),
                ))
            }))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use flowlog_common::compute_fp;
    use flowlog_planner::planner::BindingOperation;
    use flowlog_planner::planner::ProgramPlanner;
    use flowlog_planner::planner::RelationBindingPlan;
    use proc_macro2::Span;

    use super::*;
    use crate::test_harness::codegen;

    fn ident(name: &str) -> Ident {
        Ident::new(name, Span::call_site())
    }

    /// A static input `R` re-deriving itself: its recursive stratum binds
    /// `R` from the entered `R` as seed and one loop result.
    const SELF_RECURSIVE: &str = ".decl R(a:int32)\n.input R\n.output R\nR(a+1) :- R(a), a < 2.\n";

    /// Returns `R`'s binding in the recursive stratum of a
    /// [`SELF_RECURSIVE`] plan.
    fn self_recursive_binding(plan: &ProgramPlanner) -> &RelationBindingPlan {
        let stratum = plan
            .strata()
            .iter()
            .find(|stratum| stratum.is_recursive())
            .expect("R recurses");
        &stratum.relation_bindings()[&compute_fp("r")]
    }

    /// The plan's weights pass through: the seed at its outer weight, the
    /// result at the loop's.
    #[test]
    fn recursive_parts_name_the_entered_seed_and_the_loop_results() {
        let codegen = codegen(SELF_RECURSIVE);
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let binding = self_recursive_binding(&plan);
        let r = compute_fp("r");
        let head = binding.inputs()[0].fingerprint();
        let mut recursion = Recursion::new(Mutability::Static);
        recursion.entered.insert(r, ident("in_r"));
        recursion.variables.insert(r, ident("recursive_r"));
        recursion.local_fp_to_ident = [(r, ident("recursive_r")), (head, ident("t_head"))].into();

        let parts = Codegen::recursive_parts(&recursion, binding.seed(), binding.inputs())
            .expect("the seed entered");
        assert_eq!(
            parts,
            vec![
                (ident("in_r"), Mutability::Static),
                (ident("t_head"), Mutability::Static),
            ]
        );
    }

    #[test]
    fn a_recursive_binding_without_a_seed_uses_its_rule_results() {
        let codegen = codegen(SELF_RECURSIVE);
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let binding = self_recursive_binding(&plan);
        let head = binding.inputs()[0].fingerprint();
        let mut recursion = Recursion::new(Mutability::Static);
        recursion.local_fp_to_ident.insert(head, ident("t_head"));

        let parts =
            Codegen::recursive_parts(&recursion, None, binding.inputs()).expect("nothing to enter");
        assert_eq!(parts, vec![(ident("t_head"), Mutability::Static)]);
    }

    /// Completed plans cannot contain an unresolved seed, so exercise the
    /// resolver directly to verify rejection at the codegen boundary.
    #[test]
    fn a_recursive_seed_without_an_entered_binding_is_an_internal_error() {
        let codegen = codegen(SELF_RECURSIVE);
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let binding = self_recursive_binding(&plan);
        let recursion = Recursion::new(Mutability::Static);

        let error = Codegen::recursive_parts(&recursion, binding.seed(), binding.inputs())
            .expect_err("the seed was never entered");
        assert!(matches!(&error, CodegenError::Internal(_)));
        assert!(
            error.to_string().contains(&format!(
                "planned relation seed 0x{:016x} has no entered binding",
                compute_fp("r")
            )),
            "{error}"
        );
    }

    #[test]
    fn feedback_variables_keep_the_dedup_output_type() {
        let mut codegen = codegen(
            ".decl R(a:int32,b:symbol,c:int64)\n.input R\n.output R\n\
             R(a+1,b,c) :- R(a,b,c), a < 2.\n",
        );
        codegen.config.str_intern = false;
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).unwrap();
        let stratum = plan
            .strata()
            .iter()
            .find(|plan| plan.is_recursive())
            .unwrap();
        codegen
            .record_binding_layout(
                compute_fp("r"),
                &BindingOperation::Dedup {
                    order: vec![2, 0, 1],
                },
            )
            .unwrap();
        let mut recursion = Recursion::new(Mutability::Static);
        let statements = codegen
            .gen_variables(stratum, &mut recursion, &mut None)
            .unwrap();
        let expected = quote! {
            let (recursive_rel_0_r_var, recursive_rel_0_r) =
                ::flowlog_runtime::operators::flowlog_variable::<
                    _,
                    (i64, i32, String),
                    ::flowlog_runtime::diff::Static
                >(inner, "R", &failure);
        };
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0].to_string(), expected.to_string());
    }
}
