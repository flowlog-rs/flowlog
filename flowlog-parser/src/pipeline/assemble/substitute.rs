//! Substitute away assignment literals. When a body `t = expr` is the only
//! thing that defines `t`, every use of `t` is replaced by `expr` and the
//! literal is dropped; later stages read a variable's value from body atoms
//! and aggregates, never from `=`. A `t = expr` whose `t` is already defined
//! by an atom is an ordinary equality test, kept.
//!
//! An atom's argument is a variable or a constant, so an atom that reads `t`
//! keeps it when `expr` is computed, and `t = expr` stays beside the atom:
//!
//! - in an aggregate's braces, a positive atom binds `t`, so the braces gain
//!   `t = expr` as an ordinary test;
//! - in the rule body, a negated atom binds nothing, so the body keeps
//!   `t = expr`, the one `=` past this pass that defines a variable. The
//!   planner computes `t` as a column for the antijoin to read.
//!
//! ```text
//! R(t) :- A(x), t = x + 1.                =>  R(x + 1) :- A(x).
//! R(t) :- A(x), t = x + 1, t < 10.        =>  R(x + 1) :- A(x), x + 1 < 10.
//! R(a, b) :- A(x), a = x + 1, b = a + 2.  =>  R(x + 1, (x + 1) + 2) :- A(x).
//! R(t) :- A(t), B(x), t = x + 1.          =>  unchanged: t is bound by A(t)
//! R(x) :- A(x), t = x + 1, !B(t).         =>  R(x) :- A(x), !B(t), t = x + 1.
//! R(c) :- A(x), t = x + 1, c = count : B(t, y).
//!     =>  R(c) :- A(x), c = count : { B(t, y), t = x + 1 }.
//! P(x) :- x = 1 + 2.                      =>  body empties; fold makes P(3)
//! ```
//!
//! [`substitute_assignments`] drives [`substitute_rule`] per rule; a rule left
//! with an empty body becomes a fact in the fold stage.

use std::collections::HashSet;

use flowlog_common::Span;

use crate::Arithmetic;
use crate::Atom;
use crate::AtomArg;
use crate::ComparisonExpr;
use crate::ComparisonOperator;
use crate::Factor;
use crate::FlowLogRule;
use crate::HeadArg;
use crate::Predicate;
use crate::Variable;
use crate::error::ParseError;
use crate::error::grammar_bug;

/// Rewrites every rule in `rules`, removing equality assignments by
/// substitution. A rule that reduces to an empty body (all its literals were
/// assignments) is left in place for the fold stage to convert to a fact.
pub(crate) fn substitute_assignments(rules: &mut [FlowLogRule]) -> Result<(), ParseError> {
    for rule in rules.iter_mut() {
        substitute_rule(rule)?;
    }
    Ok(())
}

/// Eliminates a single rule's equality assignments by substitution, in place.
/// A rule left with an empty body was all assignments and is a ground fact;
/// the fold stage materializes it.
fn substitute_rule(rule: &mut FlowLogRule) -> Result<(), ParseError> {
    let bound: HashSet<Variable> = rule.bound_vars().into_iter().cloned().collect();
    let mut rhs = rule.rhs().to_vec();
    let resolved = substitute_body(&mut rhs, bound)?;
    for (var, value) in &resolved {
        subst_head(rule, var, value);
    }
    rule.set_rhs(rhs);
    Ok(())
}

/// Eliminates the assignments of one body, given the variables `bound`
/// before it: the rule's binders for the rule body, the enclosing bodies'
/// plus its own for an aggregate's. Returns the substitutions made, in
/// order, for the caller to apply to what reads the body's variables: the
/// head, or the aggregate's target. An assignment of a computed value to a
/// variable that a negated atom of the body reads is substituted everywhere
/// but in those atoms, and kept, resolved, at the end of the body; an atom
/// in braces that reads it keeps it too ([`subst_predicate`]). Each
/// aggregate of the body then gets the same treatment, so an assignment
/// inside braces is substituted within them.
fn substitute_body(
    body: &mut Vec<Predicate>,
    mut bound: HashSet<Variable>,
) -> Result<Vec<(Variable, Arithmetic)>, ParseError> {
    // Discover assignment comparisons to a fixpoint. `assignment_idx` records
    // which `body` slots are consumed (assignments and destructures, dropped
    // later); `order` preserves discovery order so chains resolve correctly,
    // each binding with the span of the literal it came from.
    // `destructure_filters` holds synthesized `proj(x,i) = field` predicates
    // for tuple destructure fields that were already bound.
    let mut assignment_idx: HashSet<usize> = HashSet::new();
    let mut order: Vec<(Variable, Arithmetic, Span)> = Vec::new();
    let mut destructure_filters: Vec<Predicate> = Vec::new();
    loop {
        let mut progressed = false;
        for (i, pred) in body.iter().enumerate() {
            if assignment_idx.contains(&i) {
                continue;
            }
            let Predicate::Compare(expr) = pred else {
                continue;
            };
            if *expr.operator() != ComparisonOperator::Equal {
                continue;
            }
            if let Some((var, value)) = expr.as_assignment(|v| bound.contains(v)) {
                let (var, value) = (var.clone(), value.clone());
                bound.insert(var.clone());
                assignment_idx.insert(i);
                order.push((var, value, expr.span()));
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

    // Back-substitute so each binding is expressed only over variables that
    // positive atoms or aggregates bind, and constants.
    let mut resolved: Vec<(Variable, Arithmetic, Span)> = Vec::with_capacity(order.len());
    for (var, mut value, span) in order {
        for (prior, prior_value, _) in &resolved {
            subst_arith(&mut value, prior, prior_value);
        }
        resolved.push((var, value, span));
    }

    // The variables a negated atom reads a computed value of: they stay in
    // the atom, and their assignments stay in the body.
    let kept: HashSet<Variable> = {
        let negated: HashSet<&Variable> = body
            .iter()
            .filter_map(|pred| match pred {
                Predicate::NegativeAtom(atom) => Some(atom),
                Predicate::PositiveAtom(_) | Predicate::Compare(_) | Predicate::Aggregate(_) => {
                    None
                }
            })
            .flat_map(Atom::vars)
            .collect();
        resolved
            .iter()
            .filter(|(var, value, _)| negated.contains(var) && atom_arg_from_value(value).is_none())
            .map(|(var, ..)| var.clone())
            .collect()
    };

    if !resolved.is_empty() || !assignment_idx.is_empty() {
        let mut new_body: Vec<Predicate> = Vec::with_capacity(body.len());
        for (i, mut pred) in std::mem::take(body).into_iter().enumerate() {
            if assignment_idx.contains(&i) {
                continue;
            }
            for (var, value, span) in &resolved {
                let keeps_var = match pred {
                    Predicate::NegativeAtom(_) => kept.contains(var),
                    Predicate::PositiveAtom(_)
                    | Predicate::Compare(_)
                    | Predicate::Aggregate(_) => false,
                };
                if !keeps_var {
                    subst_predicate(&mut pred, var, value, *span)?;
                }
            }
            new_body.push(pred);
        }
        // Append destructure filters (i.e., `proj(x, i) = field` for
        // already-bound fields), substituting any resolved variables first.
        for mut filt in destructure_filters {
            for (var, value, span) in &resolved {
                subst_predicate(&mut filt, var, value, *span)?;
            }
            new_body.push(filt);
        }
        // Each kept variable keeps its assignment, its value resolved over
        // the binders like every other use.
        for (var, value, span) in resolved.iter().filter(|(var, ..)| kept.contains(var)) {
            new_body.push(Predicate::Compare(ComparisonExpr::new(
                Arithmetic::var(var.clone()),
                ComparisonOperator::Equal,
                value.clone(),
                *span,
            )));
        }
        *body = new_body;
    }

    for pred in body.iter_mut() {
        let Predicate::Aggregate(aggregate) = pred else {
            continue;
        };
        let mut inner_bound = bound.clone();
        inner_bound.extend(aggregate.bound_vars().into_iter().cloned());
        let mut inner = std::mem::take(aggregate.body_mut());
        let inner_resolved = substitute_body(&mut inner, inner_bound)?;
        *aggregate.body_mut() = inner;
        if let Some(target) = aggregate.target_mut() {
            for (var, value) in &inner_resolved {
                subst_arith(target, var, value);
            }
        }
    }
    Ok(resolved
        .into_iter()
        .map(|(var, value, _)| (var, value))
        .collect())
}

/// Recognizes a tuple **destructure**: `x = (a, b, c)` (or `(a, b, c) = x`)
/// where `x` is a single **bound** variable and the other side is a bare
/// tuple literal. Returns the bound tuple variable and the literal's fields.
fn as_destructure(
    lhs: &Arithmetic,
    rhs: &Arithmetic,
    bound: &HashSet<Variable>,
) -> Option<(Variable, Vec<Arithmetic>)> {
    let try_side = |var_side: &Arithmetic, lit_side: &Arithmetic| {
        if !var_side.is_var() {
            return None;
        }
        let var = var_side.vars().into_iter().next()?.clone();
        if !bound.contains(&var) {
            return None;
        }
        Some((var, lit_side.as_tuple()?.to_vec()))
    };
    try_side(lhs, rhs).or_else(|| try_side(rhs, lhs))
}

/// Expands a destructure of bound tuple `rec_var` against `fields`. Each
/// field: a fresh variable becomes an assignment `field := proj(rec_var, i)`;
/// an already-bound variable, a constant, or an expression becomes a filter
/// `proj(rec_var, i) = field`.
fn expand_destructure(
    rec_var: &Variable,
    fields: &[Arithmetic],
    span: Span,
    bound: &mut HashSet<Variable>,
    order: &mut Vec<(Variable, Arithmetic, Span)>,
    filters: &mut Vec<Predicate>,
) -> Result<(), ParseError> {
    for (idx, field) in fields.iter().enumerate() {
        let proj = Arithmetic::new(
            Factor::TupleProj {
                tuple: Box::new(Arithmetic::var(rec_var.clone())),
                index: idx,
            },
            vec![],
        );
        let fresh: Option<&Variable> = field
            .is_var()
            .then(|| field.vars()[0])
            .filter(|var| !bound.contains(*var));
        let (lhs, rhs) = match fresh {
            // An anonymous variable (a `_`) is read by nothing, so its
            // assignment would be dropped and the type-checker, which runs
            // after this pass, would never see the field. The filter
            // `proj = proj` is trivially true at runtime but keeps the field
            // in view, so a `_` past the tuple's width is rejected rather
            // than silently dropped.
            // TODO: remove once type checking runs before this pass, which
            // then sees the pattern as written.
            Some(Variable::Anonymous(_)) => (proj.clone(), proj),
            Some(var) => {
                bound.insert(var.clone());
                order.push((var.clone(), proj, span));
                continue;
            }
            None => (proj, field.clone()),
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

fn subst_head(rule: &mut FlowLogRule, var: &Variable, value: &Arithmetic) {
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
        }
    }
}

/// Replaces `var` by `value` in `pred`. `span` is the assignment's, for an
/// equality the substitution adds.
fn subst_predicate(
    pred: &mut Predicate,
    var: &Variable,
    value: &Arithmetic,
    span: Span,
) -> Result<(), ParseError> {
    match pred {
        // The assignment variable is never grounded by a positive atom, so it
        // cannot appear in one.
        Predicate::PositiveAtom(_) => {}
        // The body's negated atoms keep a variable assigned a computed
        // value (see `substitute_body`), and braces hold no negated atom, so
        // only a variable or a constant reaches one here.
        Predicate::NegativeAtom(atom) => {
            if !subst_atom(atom, var, value) {
                return Err(grammar_bug(format!(
                    "computed value of `{var}` reached negated atom `!{atom}`"
                )));
            }
        }
        Predicate::Compare(expr) => {
            subst_arith(expr.left_mut(), var, value);
            subst_arith(expr.right_mut(), var, value);
        }
        // The aggregate's variable is bound, so never assigned. The assigned
        // variable is shared with the aggregate when it occurs inside. A
        // positive atom there binds what it reads, so one that cannot take a
        // computed value keeps the variable, and the braces gain
        // `var = value`, a test of what the atom binds.
        Predicate::Aggregate(aggregate) => {
            if let Some(target) = aggregate.target_mut() {
                subst_arith(target, var, value);
            }
            let braces = aggregate.body_mut();
            let mut keeps_var = false;
            for inner in braces.iter_mut() {
                match inner {
                    Predicate::PositiveAtom(atom) => keeps_var |= !subst_atom(atom, var, value),
                    Predicate::NegativeAtom(_)
                    | Predicate::Compare(_)
                    | Predicate::Aggregate(_) => subst_predicate(inner, var, value, span)?,
                }
            }
            if keeps_var {
                braces.push(Predicate::Compare(ComparisonExpr::new(
                    Arithmetic::var(var.clone()),
                    ComparisonOperator::Equal,
                    value.clone(),
                    span,
                )));
            }
        }
    }
    Ok(())
}

/// Replaces `var` in `atom`'s arguments by `value`. Returns `false`, leaving
/// `atom` as is, when `atom` reads `var` and `value` is computed, which no
/// atom argument can hold.
fn subst_atom(atom: &mut Atom, var: &Variable, value: &Arithmetic) -> bool {
    if !atom.vars().any(|v| v == var) {
        return true;
    }
    let Some(replacement) = atom_arg_from_value(value) else {
        return false;
    };
    for arg in atom.arguments_mut() {
        if let AtomArg::Var(v) = arg
            && v == var
        {
            *arg = replacement.clone();
        }
    }
    true
}

/// An atom argument can only receive a bare variable or constant: not a
/// computed expression. Returns `None` for anything else.
fn atom_arg_from_value(value: &Arithmetic) -> Option<AtomArg> {
    if !value.rest().is_empty() {
        return None;
    }
    match value.init() {
        Factor::Var(v) => Some(AtomArg::Var(v.clone())),
        Factor::Const(c) => Some(AtomArg::Const(c.clone())),
        Factor::FnCall(_)
        | Factor::Builtin(_)
        | Factor::Cast(_)
        | Factor::Group(_)
        | Factor::Tuple(_)
        | Factor::Placeholder
        | Factor::TupleProj { .. }
        | Factor::Aggregate(_) => None,
    }
}

fn subst_arith(arith: &mut Arithmetic, var: &Variable, value: &Arithmetic) {
    for factor in arith.factors_mut() {
        subst_factor(factor, var, value);
    }
}

fn subst_factor(factor: &mut Factor, var: &Variable, value: &Arithmetic) {
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
        Factor::Tuple(fields) => {
            for a in fields {
                subst_arith(a, var, value);
            }
        }
        Factor::TupleProj { tuple, .. } => subst_arith(tuple, var, value),
        // Rule construction hoisted every aggregate into a binding and
        // named every placeholder.
        Factor::Placeholder | Factor::Aggregate(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::test_harness::parse_rule;

    /// A negated atom's argument is a variable or a constant, so a negated
    /// atom keeps a variable assigned a computed value, and the body keeps
    /// the assignment, resolved, after its other predicates. Every other use
    /// of the variable takes the value, and a variable or constant value
    /// reaches the negated atom too.
    // Cases: rule, substituted.
    #[rstest]
    #[case::computed("R(x) :- A(x), !B(t), t = x + 1.", "r(x) :- A(x), !B(t), t = x + 1.")]
    #[case::read_by_the_head_and_a_test(
        "R(t) :- A(x), t = x + 1, t < 9, !B(t).",
        "r(x + 1) :- A(x), (x + 1) < 9, !B(t), t = x + 1."
    )]
    #[case::chained(
        "R(x) :- A(x), u = t * 2, t = x + 1, !B(u).",
        "r(x) :- A(x), !B(u), u = (x + 1) * 2."
    )]
    #[case::destructured(
        "R(x) :- A(p), p = (x, y), !B(y).",
        "r((p).0) :- A(p), !B(y), y = (p).1."
    )]
    #[case::variable("R(x) :- A(x), t = x, !B(t).", "r(x) :- A(x), !B(x).")]
    #[case::constant("R(x) :- A(x), t = 1, !B(t).", "r(x) :- A(x), !B(1).")]
    fn a_negated_atom_keeps_a_variable_assigned_a_computed_value(
        #[case] rule: &str,
        #[case] expected: &str,
    ) {
        let mut rule = parse_rule(rule);
        substitute_rule(&mut rule).expect("substitutes");
        assert_eq!(rule.to_string(), expected);
    }

    /// Chained assignments resolve to a fixpoint regardless of source order
    /// (`b = a + 2` is discovered only after `a = x + 1` grounds `a`), and the
    /// resolved values substitute into the remaining comparison filter.
    #[test]
    fn chained_assignments_resolve_and_substitute_into_filters() {
        let mut rule = parse_rule("R(a, b) :- A(x), b = a + 2, a = x + 1, b < 10.");
        substitute_rule(&mut rule).expect("substitutes");
        // Both assignments eliminated; only the `< 10` filter survives, and it
        // no longer mentions the assignment variables.
        let mut compare_vars = Vec::new();
        let mut compares = 0;
        for pred in rule.rhs() {
            if let Predicate::Compare(e) = pred {
                compares += 1;
                compare_vars.extend(e.left().vars().into_iter().cloned());
                compare_vars.extend(e.right().vars().into_iter().cloned());
            }
        }
        assert_eq!(compares, 1, "only the filter comparison remains");
        assert!(
            compare_vars.iter().all(|v| v == "x"),
            "assignment vars must be fully substituted away: {rule}"
        );
    }

    /// A variable equated to an aggregate's variable is replaced by it, as
    /// for any assignment.
    #[test]
    fn a_variable_equated_to_an_aggregate_takes_its_variable() {
        let mut rule = parse_rule("R(x, c) :- A(x), c = count : B(x, _).");
        substitute_rule(&mut rule).expect("substitutes");
        assert_eq!(
            rule.to_string(),
            "r(x, @agg_0) :- A(x), @agg_0 = count : { B(x, _) }."
        );
    }

    /// An assigned variable shared with an aggregate reaches its atoms and
    /// comparisons, so they filter on the assigned value.
    #[test]
    fn an_assignment_reaches_inside_an_aggregate() {
        let mut rule = parse_rule("R(t, c) :- A(x), t = x, c = count : { B(t, y), y > t }.");
        substitute_rule(&mut rule).expect("substitutes");
        assert_eq!(
            rule.to_string(),
            "r(x, @agg_0) :- A(x), @agg_0 = count : { B(x, y), y > x }."
        );
    }

    /// An assignment inside braces is substituted within them, the target
    /// included, as the rule's own are in the head.
    // Cases: rule, substituted.
    #[rstest]
    #[case(
        "R(s) :- s = sum t : { D(d), t = d + 1 }.",
        "r(@agg_0) :- @agg_0 = sum (d + 1) : { D(d) }."
    )]
    #[case(
        "R(c) :- c = count : { D(d), s = sum w : E(d, w), s > 2 }.",
        "r(@agg_0) :- @agg_0 = count : { D(d), @agg_1 = sum w : { E(d, w) }, @agg_1 > 2 }."
    )]
    fn an_assignment_inside_an_aggregate_is_substituted_there(
        #[case] rule: &str,
        #[case] expected: &str,
    ) {
        let mut rule = parse_rule(rule);
        substitute_rule(&mut rule).expect("substitutes");
        assert_eq!(rule.to_string(), expected);
    }

    /// An atom in braces binds what it reads, so it keeps a variable
    /// assigned a computed value, and its braces gain the assignment as a
    /// test; a test there takes the value as before. A variable value is
    /// substituted into the atom.
    // Cases: rule, substituted.
    #[rstest]
    #[case::computed(
        "R(t, c) :- A(x), t = x + 1, c = count : B(t, _).",
        "r(x + 1, @agg_0) :- A(x), @agg_0 = count : { B(t, _), t = x + 1 }."
    )]
    #[case::read_by_a_test(
        "R(x, c) :- A(x), t = x + 1, c = count : { B(t, y), t > 3 }.",
        "r(x, @agg_0) :- A(x), @agg_0 = count : { B(t, y), (x + 1) > 3, t = x + 1 }."
    )]
    #[case::nested(
        "R(x, c) :- A(x), t = x + 1, c = count : { B(x, y), d = count : { C(t) } }.",
        "r(x, @agg_0) :- A(x), @agg_0 = count : { B(x, y), @agg_1 = count : { C(t), t = x + 1 } }."
    )]
    #[case::kept_for_a_negated_atom(
        "R(x, c) :- A(x), t = x + 1, !N(t), c = count : { B(t, y) }.",
        "r(x, @agg_0) :- A(x), !N(t), @agg_0 = count : { B(t, y), t = x + 1 }, t = x + 1."
    )]
    #[case::variable(
        "R(x, c) :- A(x), t = x, c = count : { B(t, y) }.",
        "r(x, @agg_0) :- A(x), @agg_0 = count : { B(x, y) }."
    )]
    fn an_atom_in_braces_keeps_a_variable_assigned_a_computed_value(
        #[case] rule: &str,
        #[case] expected: &str,
    ) {
        let mut rule = parse_rule(rule);
        substitute_rule(&mut rule).expect("substitutes");
        assert_eq!(rule.to_string(), expected);
    }
}
