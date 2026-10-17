//! A recursive stratum: its prelude, then the loop the planner's recursive
//! steps run in, emitted in six phases that read in the order of the code
//! they produce. Each phase leaves bindings in a [`Recursion`] for the
//! next: the collections that enter, the feedback variables, the bodies,
//! each relation's head, the feedback, and the leave.

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
use crate::ty::diff::weight_tokens;

// =============================================================================
// Recursion
// =============================================================================

/// A recursive stratum's loop as its phases build it: the bindings each
/// phase leaves for the next, by the fingerprint of the relation or
/// collection they hold.
struct Recursion {
    /// The weight every head shares, which picks the loop's time.
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
    /// `next_X` for each relation once its head step has built it: the
    /// union of every head with the entered binding, which the recursion
    /// feeds back and leaves.
    heads: HashMap<u64, Ident>,
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
            heads: HashMap::new(),
        }
    }

    /// Returns relation `fp`'s bindings in `heads`, or an internal error
    /// when it has none: every relation the recursion feeds back or leaves
    /// is one of its heads.
    fn head(&self, fp: u64) -> Result<&Ident, CodegenError> {
        self.heads.get(&fp).ok_or_else(|| {
            CodegenError::internal(format!(
                "relation fingerprint 0x{fp:016x} has no head binding in its loop"
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

        // Every head of a recursive stratum has the same mutability, which
        // with the engine picks the loop's time.
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
        let variable_stmts = self.gen_variables(stratum, &mut recursion, &step, plan_graph);

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

        // Phase 4: each relation's head, `next_X`, which the feedback and
        // the leave read.
        let head_stmts = self.gen_heads(stratum, &mut recursion, plan_graph)?;

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
                #(#head_stmts)*
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

    /// Returns `let (recursive_X_var, recursive_X) = Variable::new(inner,
    /// step);` for each feedback relation of `stratum`, binding the
    /// variable in `recursion` and completing the bindings its bodies
    /// read. Every variable starts empty and grows monotonically, so
    /// `Variable::new` covers all of them.
    fn gen_variables(
        &self,
        stratum: &StratumPlanner,
        recursion: &mut Recursion,
        step: &TokenStream,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Vec<TokenStream> {
        // The weight is spelled out: a body arranges a variable before
        // `set` would fix its type, and the arrangement dispatches on it.
        let weight = weight_tokens(recursion.mutability);
        let mut stmts = Vec::new();
        for fp in stratum.recursion_feedback_collections() {
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
                let (#var_name, #name) = Variable::<_, Vec<(_, _, #weight)>>::new(inner, #step);
            });
            recursion.variables.insert(*fp, name);
        }
        recursion.local_fp_to_ident = recursion.entered.clone();
        recursion
            .local_fp_to_ident
            .extend(recursion.variables.clone());
        stmts
    }

    /// Returns the head step of every relation of `stratum`, binding each
    /// relation's head in `recursion`: `next_<fp>`, the union of all its
    /// heads with its entered binding, merged by its lattice column when
    /// it has one.
    fn gen_heads(
        &self,
        stratum: &StratumPlanner,
        recursion: &mut Recursion,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Vec<TokenStream>, CodegenError> {
        let mutability = recursion.mutability;
        let mut stmts = Vec::new();
        for (idb_fp, head_fps) in stratum.idb_to_heads_map() {
            let name = self.display_name(*idb_fp);
            let next = format_ident!("next_{idb_fp}");
            let parts = self.recursive_parts(recursion, *idb_fp, head_fps)?;
            let (rows, rows_mutability) =
                self.gen_union_dedup(*idb_fp, &parts, &next, true, plan_graph)?;
            let (head, found) =
                self.gen_lattice_merge(*idb_fp, rows, rows_mutability, &next, plan_graph)?;
            // The stratifier assigns the relation the same value on its own
            // (Lemma 4 of `docs/design/mutability.md`), and every head of
            // the loop shares the loop's weight.
            debug_assert_eq!(
                stratum.mutability(*idb_fp),
                Some(found),
                "`{name}` binds at a weight the stratifier does not give it",
            );
            debug_assert_eq!(found, mutability, "`{name}` binds off the loop's weight");
            stmts.push(quote! { let #next = #head; });
            recursion.heads.insert(*idb_fp, next);
        }
        Ok(stmts)
    }

    /// Returns `recursive_X_var.set(next_X);` for each feedback relation of
    /// `stratum`, in order, feeding its head in `recursion` back into the
    /// loop.
    fn gen_feedback(
        &self,
        stratum: &StratumPlanner,
        recursion: &Recursion,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Vec<TokenStream>, CodegenError> {
        let mut stmts = Vec::new();
        for fp in stratum.recursion_feedback_collections() {
            let next = recursion.head(*fp)?;
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
            let is_lattice = self
                .program
                .relation_by_fingerprint(*fp)
                .is_some_and(|relation| relation.lattice().is_some());
            if recursion.mutability != Mutability::Static || !is_lattice {
                folds.push(None);
                continue;
            }
            let folded = format_ident!("folded_{fp}");
            let fold = self.gen_lattice_fold(*fp, recursion.head(*fp)?, &folded, plan_graph)?;
            folds.push(Some((folded, fold)));
        }

        // An unbalanced leave is a codegen bug; surface it instead of
        // corrupting the addresses that follow.
        try_with_plan_graph(plan_graph, |plan_graph| plan_graph.leave_scope())
            .map_err(|e| CodegenError::internal(format!("recording recursive scope exit: {e}")))?;

        let mut leave_exprs = Vec::with_capacity(leave_fps.len());
        for ((fp, target), fold) in leave_fps.iter().zip(&targets).zip(folds) {
            let name = self.display_name(*fp);
            let next = recursion.head(*fp)?;
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
    /// Returns the mutability every collection the loop body of `stratum`
    /// produces shares, which is its heads' too. Each such collection reads
    /// a feedback variable, directly or through another, and the planner
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

    /// Returns the parts relation `idb_fp` unions inside `recursion`: its
    /// entered binding, when it has one, at the weight it entered with,
    /// then the heads `head_fps` as the recursion binds them. A head the
    /// recursion produces, or a feedback relation a rule reads as it is,
    /// is at the recursion's weight, whatever weight the relation's
    /// earlier binding recorded; any other head at the weight recorded
    /// for it.
    fn recursive_parts(
        &self,
        recursion: &Recursion,
        idb_fp: u64,
        head_fps: &[u64],
    ) -> Result<Vec<(Ident, Mutability)>, CodegenError> {
        let entered = recursion
            .entered
            .get(&idb_fp)
            .map(|binding| Ok((binding.clone(), self.mutability(idb_fp)?)));
        entered
            .into_iter()
            .chain(head_fps.iter().map(|fp| {
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

    /// Inside a mutable recursion, a relation's entered binding keeps the
    /// static weight it entered with, so the union lifts it, while a
    /// feedback relation read as a head is at the recursion's weight
    /// whatever its earlier binding recorded, and any other head at its
    /// own.
    #[test]
    fn recursive_parts_weigh_the_entered_binding_and_the_heads_apart() {
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
            .recursive_parts(&recursion, s, &[s, r, 0x7])
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
