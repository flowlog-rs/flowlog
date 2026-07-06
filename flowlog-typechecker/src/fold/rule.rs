//! Fold and classify one rule: value-fold its head and body, drop always-true
//! constant comparisons, then decide whether it survives, is eliminated, or
//! becomes a fact. Rests on `expr` (folding) and `eval` (comparison).

use flowlog_parser::Factor;
use flowlog_parser::FlowLogRule;
use flowlog_parser::HeadArg;
use flowlog_parser::InlineFact;
use flowlog_parser::Predicate;

use crate::fold::eval::eval_compare;
use crate::fold::expr::fold_arith;

/// Value-fold every expression in the head and body, then drop always-true
/// constant comparisons while a positive atom remains. Dropping compares never
/// removes an atom, so the planner's >=1-positive-atom invariant is preserved;
/// an always-true compare is a no-op filter, so this is output-preserving.
/// (Always-FALSE compares are left in place for [`classify`] to eliminate.)
pub(super) fn fold_rule(rule: &mut FlowLogRule) {
    for arg in rule.head_mut().head_arguments_mut() {
        match arg {
            HeadArg::Var(_) => {}
            HeadArg::Arith(a) => fold_arith(a),
            HeadArg::Aggregation(agg) => fold_arith(agg.arithmetic_mut()),
        }
    }
    for predicate in rule.rhs_mut() {
        match predicate {
            Predicate::PositiveAtom(_) | Predicate::NegativeAtom(_) => {}
            Predicate::Compare(cmp) => {
                fold_arith(cmp.left_mut());
                fold_arith(cmp.right_mut());
            }
        }
    }

    let has_positive_atom = rule
        .rhs()
        .iter()
        .any(|p| matches!(p, Predicate::PositiveAtom(_)));
    if has_positive_atom && rule.rhs().iter().any(is_always_true_compare) {
        let kept: Vec<Predicate> = rule
            .rhs()
            .iter()
            .filter(|p| !is_always_true_compare(p))
            .cloned()
            .collect();
        rule.set_rhs(kept);
    }
}

/// What to do with a plain-segment rule after [`fold_rule`] has run.
pub(super) enum Disposition {
    /// Keep the (possibly rewritten) rule.
    Keep,
    /// Drop the rule — an always-false constant comparison makes its body
    /// unsatisfiable, so its contribution is the empty set.
    Remove,
    /// Replace the rule with the equivalent inline fact — the body is
    /// unconditionally true and the head is all-constant.
    ToFact(String, InlineFact),
}

/// Classify a plain-segment rule after [`fold_rule`] has run.
pub(super) fn classify(rule: &FlowLogRule) -> Disposition {
    let rhs = rule.rhs();

    if rhs.iter().any(|p| matches!(const_compare(p), Some(false))) {
        return Disposition::Remove;
    }

    // Always-true sole: no atoms, a non-empty body of only always-true constant
    // comparisons, and an all-constant head — the rule derives its head tuple
    // unconditionally, identical to asserting it as a fact. (A head with a
    // variable fails `to_inline_fact`; a negative atom is a real runtime
    // condition, so `has_atom` blocks it.)
    let has_atom = rhs
        .iter()
        .any(|p| matches!(p, Predicate::PositiveAtom(_) | Predicate::NegativeAtom(_)));
    if !has_atom
        && !rhs.is_empty()
        && rhs.iter().all(|p| matches!(const_compare(p), Some(true)))
        && let Ok((name, fact)) = rule.to_inline_fact()
    {
        return Disposition::ToFact(name, fact);
    }

    Disposition::Keep
}

/// `Some(true/false)` if `p` is a comparison between two folded constant
/// operands; `None` otherwise (variable operand, string constraint,
/// unfoldable float, mismatched types).
fn const_compare(p: &Predicate) -> Option<bool> {
    let Predicate::Compare(cmp) = p else {
        return None;
    };
    if !cmp.left().is_const() || !cmp.right().is_const() {
        return None;
    }
    let (Factor::Const(l), Factor::Const(r)) = (cmp.left().init(), cmp.right().init()) else {
        return None;
    };
    eval_compare(cmp.operator(), l, r)
}

fn is_always_true_compare(p: &Predicate) -> bool {
    matches!(const_compare(p), Some(true))
}
