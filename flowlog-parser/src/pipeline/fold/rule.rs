//! Fold and classify one rule: value-fold its head and body, drop always-true
//! constant comparisons, then decide whether it survives, is eliminated, or
//! becomes a fact.

use crate::Factor;
use crate::FlowLogRule;
use crate::HeadArg;
use crate::InlineFact;
use crate::Predicate;
use crate::error::ParseError;
use crate::pipeline::fold::eval::eval_compare;
use crate::pipeline::fold::expr::fold_arith;

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

/// What to do with a rule after [`fold_rule`] has run.
pub(super) enum Disposition {
    /// Keep the (possibly rewritten) rule.
    Keep,
    /// Drop the rule: an always-false constant comparison makes its body
    /// unsatisfiable, so its contribution is the empty set.
    Remove,
    /// Replace the rule with the equivalent inline fact: its body holds
    /// unconditionally (or is empty) and its head is all-constant.
    ToFact(String, InlineFact),
}

/// Classify a rule after [`fold_rule`] has run.
///
/// A rule whose body substitution emptied (an all-assignment rule) must become
/// a ground fact; a head that did not fold to a constant (`1 / 0`, a string
/// builtin) has no value and is rejected with [`ParseError::GroundRuleNotConst`].
pub(super) fn classify(rule: &FlowLogRule) -> Result<Disposition, ParseError> {
    let rhs = rule.rhs();

    // Body substitution emptied this rule: it is a ground fact, or its head has
    // no constant value and cannot be one.
    if rhs.is_empty() {
        return match InlineFact::from_rule(rule) {
            Ok((name, fact)) => Ok(Disposition::ToFact(name, fact)),
            Err(_) => Err(ParseError::GroundRuleNotConst {
                span: rule.head().span(),
            }),
        };
    }

    if rhs.iter().any(|p| matches!(const_compare(p), Some(false))) {
        return Ok(Disposition::Remove);
    }

    // Always-true sole: no atoms, a non-empty body of only always-true constant
    // comparisons, and an all-constant head. The rule derives its head tuple
    // unconditionally, identical to asserting it as a fact. (A head with a
    // variable fails `InlineFact::from_rule`; a negative atom is a real runtime
    // condition, so `has_atom` blocks it.)
    let has_atom = rhs
        .iter()
        .any(|p| matches!(p, Predicate::PositiveAtom(_) | Predicate::NegativeAtom(_)));
    if !has_atom
        && rhs.iter().all(|p| matches!(const_compare(p), Some(true)))
        && let Ok((name, fact)) = InlineFact::from_rule(rule)
    {
        return Ok(Disposition::ToFact(name, fact));
    }

    Ok(Disposition::Keep)
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
