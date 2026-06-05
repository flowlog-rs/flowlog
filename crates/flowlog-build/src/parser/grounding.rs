//! Variable grounding: assignment-style equalities.
//!
//! Soufflé lets a rule bind a variable by equality —
//! `calleeCtx = callerCtx`, `ctx = "<<immutable-context>>"`, `j = i - 1` —
//! where the left variable is *defined* by the right-hand expression rather
//! than range-restricted by a positive atom. FlowLog's core requires every
//! body variable to be range-restricted by a positive atom, so it rejects
//! such rules with an "unsafe variable" error. DOOP relies on the Soufflé
//! behaviour pervasively (`isModifier(x) :- x = "abstract"`, etc.).
//!
//! This pass desugars those equalities by substitution, reusing machinery the
//! engine already has (head arithmetic, plain column joins, inline facts) and
//! introducing no new runtime construct:
//!
//! * `v = w` (both variables, `w` bound) — rename `v` to `w` everywhere.
//! * `v = "c"` (constant) — inline the constant wherever `v` is used.
//! * `v = E` (compound, `v` used only in the head) — fold `E` into the head
//!   position, which already supports arithmetic.
//!
//! After [`ground_rule`] runs, a rule whose body collapses to nothing and
//! whose head is fully constant is a ground fact; the caller
//! (`Program::ground_assignment_bindings`) moves it into the inline-fact map.
//!
//! **Soundness.** Classification only fires on an equality whose lone-variable
//! side is *not* bound by any positive atom — precisely the rules the
//! range-restriction check already rejects, so no program FlowLog accepts
//! today changes meaning; the pass only widens what compiles. A compound
//! right-hand side used in a column position (e.g. inside a negative atom) is
//! left untouched and surfaces as the usual binding error.
//!
//! Runs after the body-aggregate and expression-atom-argument desugars, so a
//! rule body here only ever holds positive atoms, negative atoms, comparisons,
//! and fn-calls — never `Predicate::BodyAggregate`.

use std::collections::HashSet;

use super::logic::{
    Arithmetic, Atom, AtomArg, ComparisonOperator, Factor, FlowLogRule, HeadArg, Predicate,
};
use crate::parser::primitive::ConstType;

/// Replacement value an assignment binds its variable to.
enum Repl {
    /// A single variable or constant — substitutable in any position,
    /// including atom columns.
    Term(AtomArg),
    /// A compound expression — only foldable into head arithmetic positions.
    Expr(Arithmetic),
}

/// Ground assignment equalities in `rule` by substitution. Idempotent and
/// order-independent: chained assignments (`a = b, b = c`) resolve across
/// iterations.
pub(crate) fn ground_rule(rule: &mut FlowLogRule) {
    let mut body: Vec<Predicate> = rule.rhs().to_vec();
    let mut head_args: Vec<HeadArg> = rule.head().head_arguments().to_vec();
    let mut changed = false;

    while apply_one(&mut head_args, &mut body) {
        changed = true;
    }

    if changed {
        // Arity is preserved by substitution, so the head slot count is stable.
        for (dst, src) in rule
            .head_mut()
            .head_arguments_mut()
            .iter_mut()
            .zip(head_args)
        {
            *dst = src;
        }
        rule.set_rhs(body);
    }
}

/// Find and apply a single assignment, returning whether one fired.
fn apply_one(head_args: &mut [HeadArg], body: &mut Vec<Predicate>) -> bool {
    let bound = positive_bound(body);

    for i in 0..body.len() {
        let Predicate::Compare(cmp) = &body[i] else {
            continue;
        };
        if *cmp.operator() != ComparisonOperator::Equal {
            continue;
        }
        let Some((var, repl)) = classify(cmp.left(), cmp.right(), &bound) else {
            continue;
        };
        if !can_apply(&repl, &var, head_args, body, i) {
            continue;
        }

        body.remove(i);
        match repl {
            Repl::Term(term) => {
                for arg in head_args.iter_mut() {
                    subst_head_arg(arg, &var, &term);
                }
                for pred in body.iter_mut() {
                    subst_pred(pred, &var, &term);
                }
            }
            Repl::Expr(expr) => {
                for arg in head_args.iter_mut() {
                    if matches!(arg, HeadArg::Var(v) if *v == var) {
                        *arg = HeadArg::Arith(expr.clone());
                    }
                }
            }
        }
        return true;
    }
    false
}

/// Classify `left op right` (op already known to be `=`) as an assignment of a
/// lone, unbound variable to the fully-bound other side.
fn classify(
    left: &Arithmetic,
    right: &Arithmetic,
    bound: &HashSet<&String>,
) -> Option<(String, Repl)> {
    if let Some(asg) = assign_from(left, right, bound) {
        return Some(asg);
    }
    assign_from(right, left, bound)
}

/// If `target` is a lone variable not yet bound and every variable in `value`
/// is bound, return the assignment. Guards against self-reference (`x = x + 1`).
fn assign_from(
    target: &Arithmetic,
    value: &Arithmetic,
    bound: &HashSet<&String>,
) -> Option<(String, Repl)> {
    let var = lone_var(target)?;
    if bound.contains(&var) {
        return None;
    }
    if value.vars().iter().any(|v| **v == var) {
        return None;
    }
    if !value.vars().iter().all(|v| bound.contains(v)) {
        return None;
    }
    Some((var, repl_of(value)))
}

/// The variable name when `a` is exactly a single variable.
fn lone_var(a: &Arithmetic) -> Option<String> {
    match a.init() {
        Factor::Var(v) if a.rest().is_empty() => Some(v.clone()),
        _ => None,
    }
}

fn repl_of(value: &Arithmetic) -> Repl {
    if value.rest().is_empty() {
        match value.init() {
            Factor::Var(v) => return Repl::Term(AtomArg::Var(v.clone())),
            Factor::Const(c) => return Repl::Term(AtomArg::Const(c.clone())),
            _ => {}
        }
    }
    Repl::Expr(value.clone())
}

/// Whether substituting `var` is fully representable. `Term` replacements work
/// anywhere; `Expr` replacements only when `var` is used solely as a direct
/// head variable (so it can become head arithmetic). Aggregation uses are
/// always declined to avoid altering grouping semantics.
fn can_apply(
    repl: &Repl,
    var: &str,
    head_args: &[HeadArg],
    body: &[Predicate],
    skip: usize,
) -> bool {
    if head_args
        .iter()
        .any(|a| matches!(a, HeadArg::Aggregation(agg) if agg.vars().iter().any(|v| *v == var)))
    {
        return false;
    }

    match repl {
        Repl::Term(_) => true,
        Repl::Expr(_) => {
            // Every head use must be a bare `HeadArg::Var(var)`.
            let head_ok = head_args.iter().all(|a| match a {
                HeadArg::Var(_) => true,
                HeadArg::Arith(ar) => ar.vars().iter().all(|v| **v != var),
                HeadArg::Aggregation(agg) => agg.vars().iter().all(|v| **v != var),
            });
            // A compound expression cannot occupy a column or nest inside
            // another expression, so `var` must be absent from the body.
            let body_free = body
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != skip)
                .all(|(_, p)| !pred_mentions(p, var));
            head_ok && body_free
        }
    }
}

/// Variables range-restricted by positive atoms.
fn positive_bound(body: &[Predicate]) -> HashSet<&String> {
    let mut bound = HashSet::new();
    for pred in body {
        if let Predicate::PositiveAtom(atom) = pred {
            for arg in atom.arguments() {
                if let AtomArg::Var(v) = arg {
                    bound.insert(v);
                }
            }
        }
    }
    bound
}

fn pred_mentions(pred: &Predicate, var: &str) -> bool {
    match pred {
        Predicate::PositiveAtom(a) | Predicate::NegativeAtom(a) => a
            .arguments()
            .iter()
            .any(|arg| matches!(arg, AtomArg::Var(v) if v == var)),
        Predicate::Compare(c) => c.vars_set().iter().any(|v| **v == var),
        Predicate::FnCall(fc) => fc.vars().iter().any(|v| **v == var),
        // Body aggregates are desugared before this pass runs.
        Predicate::BodyAggregate(_) => unreachable!("body aggregate survived desugar"),
    }
}

fn subst_head_arg(arg: &mut HeadArg, var: &str, term: &AtomArg) {
    match arg {
        HeadArg::Var(v) if v == var => {
            *arg = match term {
                AtomArg::Var(w) => HeadArg::Var(w.clone()),
                AtomArg::Const(c) => HeadArg::Arith(const_arith(c.clone())),
                AtomArg::Placeholder => HeadArg::Var(var.to_string()),
                AtomArg::Expr(_) => {
                    unreachable!("expression atom arg desugared before grounding")
                }
            };
        }
        HeadArg::Arith(a) => subst_arith(a, var, term),
        // Aggregations are excluded by `can_apply`; nothing to do.
        HeadArg::Var(_) | HeadArg::Aggregation(_) => {}
    }
}

fn subst_pred(pred: &mut Predicate, var: &str, term: &AtomArg) {
    match pred {
        Predicate::PositiveAtom(a) | Predicate::NegativeAtom(a) => subst_atom(a, var, term),
        Predicate::Compare(c) => {
            subst_arith(c.left_mut(), var, term);
            subst_arith(c.right_mut(), var, term);
        }
        Predicate::FnCall(fc) => {
            for a in fc.args_mut() {
                subst_arith(a, var, term);
            }
        }
        // Body aggregates are desugared before this pass runs.
        Predicate::BodyAggregate(_) => unreachable!("body aggregate survived desugar"),
    }
}

fn subst_atom(atom: &mut Atom, var: &str, term: &AtomArg) {
    for arg in atom.arguments_mut() {
        if matches!(arg, AtomArg::Var(v) if v == var) {
            *arg = term.clone();
        }
    }
}

fn subst_arith(arith: &mut Arithmetic, var: &str, term: &AtomArg) {
    subst_factor(arith.init_mut(), var, term);
    for (_, f) in arith.rest_mut() {
        subst_factor(f, var, term);
    }
}

fn subst_factor(factor: &mut Factor, var: &str, term: &AtomArg) {
    match factor {
        Factor::Var(v) if v == var => *factor = term_factor(term),
        Factor::FnCall(fc) => {
            for a in fc.args_mut() {
                subst_arith(a, var, term);
            }
        }
        Factor::Builtin(bc) => {
            for a in bc.args_mut() {
                subst_arith(a, var, term);
            }
        }
        Factor::Cast(c) => subst_factor(c.inner_mut(), var, term),
        Factor::Var(_) | Factor::Const(_) => {}
    }
}

fn term_factor(term: &AtomArg) -> Factor {
    match term {
        AtomArg::Var(w) => Factor::Var(w.clone()),
        AtomArg::Const(c) => Factor::Const(c.clone()),
        AtomArg::Placeholder => Factor::Var("_".to_string()),
        AtomArg::Expr(_) => unreachable!("expression atom arg desugared before grounding"),
    }
}

fn const_arith(c: ConstType) -> Arithmetic {
    Arithmetic::new(Factor::Const(c), Vec::new())
}
