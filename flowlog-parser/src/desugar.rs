//! Equality-as-assignment desugaring.
//!
//! Soufflé and standard Datalog treat a body literal `v = expr` as an
//! *assignment* that grounds `v` whenever `v` is otherwise unbound and every
//! variable in `expr` is already bound. FlowLog's planner grounds variables
//! only through positive atoms, so this pass eliminates assignment literals up
//! front by substituting the bound variable into the head and the remaining
//! body, before catalog construction and planning ever see the rule:
//!
//! ```text
//! R(t)        :- A(x), t = x + 1.        =>  R(x + 1) :- A(x).
//! D(d)        :- M(rt), d = cat(rt,"()").=>  D(cat(rt, "()")) :- M(rt).
//! R(y)        :- A(x), y = x.            =>  R(x) :- A(x).
//! isModifier(x) :- x = "abstract".       =>  fact  isModifier("abstract").
//! ```
//!
//! A `v = expr` whose `v` already occurs in a positive atom is a genuine
//! equality *filter* (both sides are bound), and is left untouched — only an
//! otherwise-unbound `v` is treated as an assignment.
//!
//! Chained assignments (`a = x + 1, b = a + 2`) are resolved to a fixpoint and
//! back-substituted so each bound variable is expressed purely over
//! positive-atom variables and constants.
//!
//! Rules whose body becomes empty and whose head is fully ground are emitted as
//! inline facts (reusing the existing fact path) rather than being handed to the
//! planner, which requires at least one positive atom.

use std::collections::HashMap;
use std::collections::HashSet;
use std::mem;

use flowlog_common::Span;

use super::segment::Segment;
use crate::Arithmetic;
use crate::ArithmeticOperator;
use crate::AtomArg;
use crate::ComparisonExpr;
use crate::ComparisonOperator;
use crate::ConstType;
use crate::Factor;
use crate::FlowLogRule;
use crate::HeadArg;
use crate::Predicate;
use crate::TupleElem;
use crate::TupleLit;
use crate::error::ParseError;

/// Rewrite every rule in `segments`, removing equality assignments by
/// substitution. Rules that reduce to a fully-ground fact are appended to
/// `raw_facts` instead of being kept in their segment.
pub(crate) fn desugar_equality_assignments(
    segments: &mut [Segment],
    raw_facts: &mut Vec<FlowLogRule>,
) -> Result<(), ParseError> {
    for seg in segments.iter_mut() {
        let rules: &mut Vec<FlowLogRule> = match seg {
            Segment::Plain(rules) => rules,
            Segment::Loop(block) | Segment::Fixpoint(block) => block.rules_mut(),
        };

        let mut kept = Vec::with_capacity(rules.len());
        for mut rule in mem::take(rules) {
            if desugar_rule(&mut rule)? {
                raw_facts.push(rule);
            } else {
                kept.push(rule);
            }
        }
        *rules = kept;
    }
    Ok(())
}

/// Desugar a single rule in place. Returns `true` when the rule became a
/// fully-ground fact and should be moved to the fact set.
fn desugar_rule(rule: &mut FlowLogRule) -> Result<bool, ParseError> {
    // Variables grounded by a positive atom — never treated as assignments.
    let mut bound: HashSet<String> = HashSet::new();
    for pred in rule.rhs() {
        if let Predicate::PositiveAtom(atom) = pred {
            for arg in atom.arguments() {
                if let AtomArg::Var(v) = arg {
                    bound.insert(v.clone());
                }
            }
        }
    }

    // Discover assignment comparisons to a fixpoint. `assignment_idx` records
    // which `rhs` slots are consumed (assignments and destructures, dropped
    // later); `order` preserves discovery order so chains resolve correctly.
    // `destructure_filters` holds synthesized `proj(x,i) = comp` predicates for
    // tuple destructure components that were already bound.
    let mut assignment_idx: HashSet<usize> = HashSet::new();
    let mut order: Vec<(String, Arithmetic)> = Vec::new();
    let mut destructure_filters: Vec<Predicate> = Vec::new();
    loop {
        let mut progressed = false;
        for (i, pred) in rule.rhs().iter().enumerate() {
            if assignment_idx.contains(&i) {
                continue;
            }
            let Predicate::Compare(expr) = pred else {
                continue;
            };
            if *expr.operator() != ComparisonOperator::Equal {
                continue;
            }
            if let Some((var, value)) = as_assignment(expr.left(), expr.right(), &bound) {
                bound.insert(var.clone());
                assignment_idx.insert(i);
                order.push((var, value));
                progressed = true;
            } else if let Some((rec_var, lit)) = as_destructure(expr.left(), expr.right(), &bound) {
                expand_destructure(
                    &rec_var,
                    &lit,
                    expr.span(),
                    &mut bound,
                    &mut order,
                    &mut destructure_filters,
                )?;
                assignment_idx.insert(i);
                progressed = true;
            }
        }
        if !progressed {
            break;
        }
    }

    if order.is_empty() && assignment_idx.is_empty() {
        return Ok(false);
    }

    // Back-substitute so each binding is expressed only over positive-atom
    // variables and constants, then freeze into a resolved map.
    let mut resolved: HashMap<String, Arithmetic> = HashMap::new();
    let mut resolved_order: Vec<String> = Vec::new();
    for (var, mut value) in order {
        for prior in &resolved_order {
            subst_arith(&mut value, prior, &resolved[prior]);
        }
        resolved_order.push(var.clone());
        resolved.insert(var, value);
    }

    // Substitute into the head.
    for var in &resolved_order {
        subst_head(rule, var, &resolved[var]);
    }

    // Substitute into every non-consumed body predicate.
    let mut new_rhs: Vec<Predicate> = Vec::with_capacity(rule.rhs().len());
    for (i, pred) in rule.rhs_mut().iter_mut().enumerate() {
        if assignment_idx.contains(&i) {
            continue;
        }
        for var in &resolved_order {
            subst_predicate(pred, var, &resolved[var])?;
        }
        new_rhs.push(pred.clone());
    }
    // Append destructure filters (i.e., `proj(x, i) = comp` for already-bound
    // components), substituting any resolved variables first.
    for mut filt in destructure_filters {
        for var in &resolved_order {
            subst_predicate(&mut filt, var, &resolved[var])?;
        }
        new_rhs.push(filt);
    }
    rule.set_rhs(new_rhs);

    // An emptied body must leave the segment (the planner needs ≥1 positive
    // atom): fold ground integer arithmetic (`x = 1 + 2` → fact `P(3)`),
    // reject anything non-constant (unbound head var, float/string/builtin)
    // instead of panicking the planner downstream.
    if !rule.rhs().is_empty() {
        return Ok(false);
    }
    let span = rule.head().span();
    for arg in rule.head_mut().head_arguments_mut() {
        match arg {
            HeadArg::Arith(a) if a.is_const() => {}
            HeadArg::Arith(a) => {
                let folded = fold_const_int(a).ok_or(ParseError::GroundRuleNotConst { span })?;
                *a = Arithmetic::new(Factor::Const(ConstType::Int(folded)), vec![]);
            }
            HeadArg::Var(_) | HeadArg::Aggregation(_) => {
                return Err(ParseError::GroundRuleNotConst { span });
            }
        }
    }
    Ok(true)
}

/// Fold a ground arithmetic expression over polymorphic integer literals
/// (`1 + 2` → `3`). Returns `None` for any non-integer factor (variable,
/// float, string, builtin, UDF call) or on overflow / division by zero —
/// the caller rejects those.
fn fold_const_int(a: &Arithmetic) -> Option<i64> {
    fn factor_value(f: &Factor) -> Option<i64> {
        match f {
            Factor::Const(ConstType::Int(v)) => Some(*v),
            Factor::Group(inner) => fold_const_int(inner),
            _ => None,
        }
    }
    let mut acc = factor_value(a.init())?;
    for (op, f) in a.rest() {
        let v = factor_value(f)?;
        acc = match op {
            ArithmeticOperator::Plus => acc.checked_add(v)?,
            ArithmeticOperator::Minus => acc.checked_sub(v)?,
            ArithmeticOperator::Multiply => acc.checked_mul(v)?,
            ArithmeticOperator::Divide => acc.checked_div(v)?,
            ArithmeticOperator::Modulo => acc.checked_rem(v)?,
        };
    }
    Some(acc)
}

/// Recognise `lhs = rhs` as an assignment that grounds a fresh variable.
///
/// Returns `(var, value)` when exactly one side is a single still-unbound
/// variable and the other side's variables are all bound. The variable must not
/// also appear on the value side (`v = v + 1` is not groundable).
fn as_assignment(
    lhs: &Arithmetic,
    rhs: &Arithmetic,
    bound: &HashSet<String>,
) -> Option<(String, Arithmetic)> {
    let try_side =
        |var_side: &Arithmetic, value_side: &Arithmetic| -> Option<(String, Arithmetic)> {
            if !var_side.is_var() {
                return None;
            }
            let var = var_side.vars().into_iter().next()?.clone();
            if bound.contains(&var) {
                return None;
            }
            let value_vars = value_side.vars();
            if value_vars.iter().any(|v| **v == var) {
                return None;
            }
            value_vars
                .iter()
                .all(|v| bound.contains(*v))
                .then(move || (var, value_side.clone()))
        };

    try_side(lhs, rhs).or_else(|| try_side(rhs, lhs))
}

/// Recognise a tuple **destructure**: `x = (a, b, c)` (or `(a, b, c) = x`) where `x`
/// is a single **bound** variable and the other side is a bare tuple literal.
/// Returns the bound tuple variable and a clone of the literal.
fn as_destructure(
    lhs: &Arithmetic,
    rhs: &Arithmetic,
    bound: &HashSet<String>,
) -> Option<(String, TupleLit)> {
    let try_side = |var_side: &Arithmetic, rec_side: &Arithmetic| -> Option<(String, TupleLit)> {
        if !var_side.is_var() {
            return None;
        }
        let var = var_side.vars().into_iter().next()?.clone();
        if !bound.contains(&var) {
            return None;
        }
        if !rec_side.rest().is_empty() {
            return None;
        }
        match rec_side.init() {
            Factor::Tuple(r) => Some((var, r.clone())),
            _ => None,
        }
    };
    try_side(lhs, rhs).or_else(|| try_side(rhs, lhs))
}

/// Expand a destructure of bound tuple `rec_var` against `lit`. Each component:
/// a placeholder is ignored; a fresh variable becomes an assignment
/// `comp := proj(rec_var, i)`; an already-bound variable or constant becomes a
/// filter `proj(rec_var, i) = component`.
fn expand_destructure(
    rec_var: &str,
    lit: &TupleLit,
    span: Span,
    bound: &mut HashSet<String>,
    order: &mut Vec<(String, Arithmetic)>,
    filters: &mut Vec<Predicate>,
) -> Result<(), ParseError> {
    for (idx, elem) in lit.fields().iter().enumerate() {
        let proj = Arithmetic::new(
            Factor::TupleProj {
                tuple: Box::new(Arithmetic::var(rec_var)),
                index: idx,
            },
            vec![],
        );
        // A fresh variable component binds to the projection; everything else
        // (already-bound var, constant, or placeholder) becomes an equality
        // filter. The placeholder's filter is `proj = proj` — trivially true at
        // runtime, but it makes the type-checker witness that the tuple really
        // has a field at this index (so `[_]` on a non-tuple, or a trailing
        // `_` past the tuple's arity, is rejected rather than silently dropped).
        let (lhs, rhs) = match elem {
            TupleElem::Expr(component)
                if component.is_var() && !bound.contains(component.vars()[0]) =>
            {
                let comp_var = component.vars()[0].clone();
                bound.insert(comp_var.clone());
                order.push((comp_var, proj));
                continue;
            }
            TupleElem::Expr(component) => (proj, component.clone()),
            TupleElem::Placeholder => (proj.clone(), proj),
        };
        filters.push(Predicate::Compare(ComparisonExpr::new(
            lhs,
            ComparisonOperator::Equal,
            rhs,
            span,
        )));
    }
    Ok(())
}

/// `Factor` form of a resolved value: a lone factor is inlined directly; a
/// multi-term expression is wrapped in a group so left-to-right folding keeps
/// its meaning when spliced into a larger expression.
fn value_factor(value: &Arithmetic) -> Factor {
    if value.rest().is_empty() {
        value.init().clone()
    } else {
        Factor::Group(Box::new(value.clone()))
    }
}

fn subst_head(rule: &mut FlowLogRule, var: &str, value: &Arithmetic) {
    for arg in rule.head_mut().head_arguments_mut() {
        match arg {
            HeadArg::Var(v) if v == var => {
                *arg = if value.is_var() {
                    HeadArg::Var(value.vars()[0].clone())
                } else {
                    HeadArg::Arith(value.clone())
                };
            }
            HeadArg::Var(_) => {}
            HeadArg::Arith(a) => subst_arith(a, var, value),
            HeadArg::Aggregation(agg) => subst_arith(agg.arithmetic_mut(), var, value),
        }
    }
}

fn subst_predicate(pred: &mut Predicate, var: &str, value: &Arithmetic) -> Result<(), ParseError> {
    match pred {
        // The assignment variable is never grounded by a positive atom, so it
        // cannot appear in one.
        Predicate::PositiveAtom(_) => {}
        Predicate::NegativeAtom(atom) => {
            let span = atom.span();
            for arg in atom.arguments_mut() {
                if let AtomArg::Var(v) = arg
                    && v == var
                {
                    *arg = atom_arg_from_value(value).ok_or_else(|| {
                        ParseError::AssignmentVarInNegation {
                            span,
                            var: var.to_string(),
                        }
                    })?;
                }
            }
        }
        Predicate::Compare(expr) => {
            subst_arith(expr.left_mut(), var, value);
            subst_arith(expr.right_mut(), var, value);
        }
    }
    Ok(())
}

/// A negated atom argument can only receive a bare variable or constant — not a
/// computed expression. Returns `None` for anything else.
fn atom_arg_from_value(value: &Arithmetic) -> Option<AtomArg> {
    if !value.rest().is_empty() {
        return None;
    }
    match value.init() {
        Factor::Var(v) => Some(AtomArg::Var(v.clone())),
        Factor::Const(c) => Some(AtomArg::Const(c.clone())),
        _ => None,
    }
}

fn subst_arith(arith: &mut Arithmetic, var: &str, value: &Arithmetic) {
    subst_factor(arith.init_mut(), var, value);
    for (_, factor) in arith.rest_mut() {
        subst_factor(factor, var, value);
    }
}

fn subst_factor(factor: &mut Factor, var: &str, value: &Arithmetic) {
    match factor {
        Factor::Var(v) if v == var => *factor = value_factor(value),
        Factor::Var(_) | Factor::Const(_) => {}
        Factor::FnCall(fc) => {
            for arg in fc.args_mut() {
                subst_arith(arg, var, value);
            }
        }
        Factor::Builtin(bc) => {
            for arg in bc.args_mut() {
                subst_arith(arg, var, value);
            }
        }
        Factor::Cast(c) => subst_factor(c.inner_mut(), var, value),
        Factor::Group(a) => subst_arith(a, var, value),
        Factor::Tuple(r) => {
            for a in r.exprs_mut() {
                subst_arith(a, var, value);
            }
        }
        Factor::TupleProj { tuple, .. } => subst_arith(tuple, var, value),
    }
}
