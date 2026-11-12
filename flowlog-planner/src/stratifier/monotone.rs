//! The climb: every rule of a recursion is monotone in the relations the
//! recursion derives.
//!
//! A recursion repeats its rules until nothing changes. When each rule,
//! given more rows or better bounds, derives no fewer rows and no worse
//! bounds, that fixpoint is unique and every evaluation reaches it: in
//! whatever order the rows arrive, and whether or not the evaluation
//! retracts the bounds that later ones beat. [`check`] proves this per
//! rule from its syntax and rejects each read that could break it:
//!
//! - negating or aggregating a relation the recursion derives;
//! - using a bound, the value of a lattice column, other than as a bound:
//!   copying it into a plain column, matching it exactly, testing it with
//!   a comparison a better bound may fail, or deriving a worse bound from
//!   a better one.
//!
//! The check follows a bound through `+` and `-` with values that do not
//! depend on it, and treats any other operation on it as opaque. It
//! assumes those two keep order, which holds while a sum stays in range:
//! integers wrap on overflow, so a beaten bound pushed past its type's
//! limit can come back around and beat the true answer.

use std::collections::HashMap;

use flowlog_common::Span;
use flowlog_parser::Arithmetic;
use flowlog_parser::ArithmeticOperator;
use flowlog_parser::Atom;
use flowlog_parser::AtomArg;
use flowlog_parser::ComparisonExpr;
use flowlog_parser::ComparisonOperator;
use flowlog_parser::Factor;
use flowlog_parser::FlowLogRule;
use flowlog_parser::HeadArg;
use flowlog_parser::Lattice;
use flowlog_parser::Predicate;
use flowlog_parser::Program;
use flowlog_parser::Relation;
use flowlog_parser::Variable;

use crate::planner::NonMonotone;
use crate::planner::PlanError;

/// The relations a recursion derives, each with its lattice column and
/// merge, or `None` for a plain relation.
type Derived = HashMap<u64, Option<(usize, Lattice)>>;

/// Checks that every rule of the recursive stratum of `rule_ids` is
/// monotone in the relations the stratum derives, its rules' heads.
///
/// # Errors
///
/// Returns [`PlanError::NotMonotone`] for the first read, in rule order,
/// that a later round could undo, and an internal [`PlanError`] for an
/// atom without its relation's lattice column, which type checking rules
/// out.
pub(super) fn check(program: &Program, rule_ids: &[usize]) -> Result<(), PlanError> {
    let rules = program.rules();
    let derived: Derived = rule_ids
        .iter()
        .map(|&rule_id| {
            let fp = rules[rule_id].head().head_fingerprint();
            let lattice = program
                .relation_by_fingerprint(fp)
                .and_then(Relation::lattice);
            (fp, lattice)
        })
        .collect();
    rule_ids
        .iter()
        .try_for_each(|&rule_id| check_rule(&rules[rule_id], &derived))
}

/// Checks one rule of the recursion deriving `derived`.
fn check_rule(rule: &FlowLogRule, derived: &Derived) -> Result<(), PlanError> {
    let reject = |read, atom: &Atom, use_span| PlanError::NotMonotone {
        read,
        relation: atom.raw_name().to_string(),
        read_span: atom.span(),
        use_span,
        rule_span: rule.span(),
    };
    for predicate in rule.rhs() {
        match predicate {
            Predicate::NegativeAtom(atom) if derived.contains_key(&atom.fingerprint()) => {
                return Err(reject(NonMonotone::Negation, atom, atom.span()));
            }
            Predicate::Aggregate(aggregate) => {
                if let Some(atom) = aggregate
                    .body_atoms()
                    .into_iter()
                    .find(|atom| derived.contains_key(&atom.fingerprint()))
                {
                    return Err(reject(NonMonotone::Aggregate, atom, atom.span()));
                }
            }
            Predicate::PositiveAtom(_) | Predicate::NegativeAtom(_) | Predicate::Compare(_) => {}
        }
    }
    for (index, predicate) in rule.rhs().iter().enumerate() {
        let Predicate::PositiveAtom(atom) = predicate else {
            continue;
        };
        let Some(&Some((column, lattice))) = derived.get(&atom.fingerprint()) else {
            continue;
        };
        let read = match atom.arguments().get(column) {
            Some(AtomArg::Placeholder) => None,
            Some(AtomArg::Const(_)) => Some((NonMonotone::Matched(lattice), atom.span())),
            Some(AtomArg::Var(bound)) => misuse(rule, derived, (index, column), bound, lattice),
            None => {
                return Err(PlanError::internal(format!(
                    "atom `{}` has no lattice column {column}",
                    atom.raw_name()
                )));
            }
        };
        if let Some((read, use_span)) = read {
            return Err(reject(read, atom, use_span));
        }
    }
    Ok(())
}

/// The first use of `bound`, the `lattice` bound the body atom at `read`'s
/// predicate index reads at `read`'s column, that a better bound could
/// undo, and where it is.
fn misuse(
    rule: &FlowLogRule,
    derived: &Derived,
    read: (usize, usize),
    bound: &Variable,
    lattice: Lattice,
) -> Option<(NonMonotone, Span)> {
    let head = rule.head();
    let head_lattice = derived.get(&head.head_fingerprint()).copied().flatten();
    for (column, arg) in head.head_arguments().iter().enumerate() {
        let (slope, span) = match arg {
            HeadArg::Var(var) => (Slope::of_var(var, bound), head.span()),
            HeadArg::Arith(expr) if expr.span().is_dummy() => (Slope::of(expr, bound), head.span()),
            HeadArg::Arith(expr) => (Slope::of(expr, bound), expr.span()),
        };
        let used = match head_lattice {
            Some((lattice_column, merge)) if lattice_column == column => {
                match slope.as_bound_improves(lattice) {
                    Slope::Flat => None,
                    Slope::Unknown => Some(NonMonotone::Opaque(lattice)),
                    moved if moved == Slope::better(merge) => None,
                    Slope::Up | Slope::Down => Some(NonMonotone::Reversed(lattice)),
                }
            }
            _ => (slope != Slope::Flat).then_some(NonMonotone::Copied(lattice)),
        };
        if let Some(used) = used {
            return Some((used, span));
        }
    }
    for (index, predicate) in rule.rhs().iter().enumerate() {
        let used = match predicate {
            Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) => {
                let matched = atom.arguments().iter().enumerate().any(|(column, arg)| {
                    (index, column) != read && matches!(arg, AtomArg::Var(var) if var == bound)
                });
                matched.then_some((NonMonotone::Matched(lattice), atom.span()))
            }
            Predicate::Compare(comparison) => {
                compare(comparison, bound, lattice).map(|used| (used, comparison.span()))
            }
            Predicate::Aggregate(aggregate) => aggregate
                .vars()
                .contains(bound)
                .then_some((NonMonotone::Matched(lattice), aggregate.span())),
        };
        if used.is_some() {
            return used;
        }
    }
    None
}

/// How `comparison` may turn false as `bound`, a `lattice` bound,
/// improves, or `None` when it cannot.
fn compare(comparison: &ComparisonExpr, bound: &Variable, lattice: Lattice) -> Option<NonMonotone> {
    let (low, high) = match comparison.operator() {
        ComparisonOperator::LessThan | ComparisonOperator::LessEqualThan => {
            (comparison.left(), comparison.right())
        }
        ComparisonOperator::GreaterThan | ComparisonOperator::GreaterEqualThan => {
            (comparison.right(), comparison.left())
        }
        ComparisonOperator::Equal
        | ComparisonOperator::NotEqual
        | ComparisonOperator::Match { .. }
        | ComparisonOperator::Contains { .. } => {
            return comparison
                .vars_set()
                .contains(bound)
                .then_some(NonMonotone::Matched(lattice));
        }
    };
    // `low < high` stays true while `low` falls and `high` rises.
    let low = Slope::of(low, bound).as_bound_improves(lattice);
    let high = Slope::of(high, bound).as_bound_improves(lattice);
    if low == Slope::Unknown || high == Slope::Unknown {
        Some(NonMonotone::Opaque(lattice))
    } else if low == Slope::Up || high == Slope::Down {
        Some(NonMonotone::Filtered(lattice))
    } else {
        None
    }
}

/// Which way an expression moves as one variable in it grows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Slope {
    /// It does not mention the variable.
    Flat,
    Up,
    Down,
    /// It may move either way.
    Unknown,
}

impl Slope {
    /// The way a `lattice` bound moves as it improves.
    fn better(lattice: Lattice) -> Self {
        match lattice {
            Lattice::Min => Self::Down,
            Lattice::Max => Self::Up,
        }
    }

    /// The way an expression of this slope in a `lattice` bound moves as
    /// the bound improves.
    fn as_bound_improves(self, lattice: Lattice) -> Self {
        match lattice {
            Lattice::Min => self.flip(),
            Lattice::Max => self,
        }
    }

    fn flip(self) -> Self {
        match self {
            Self::Up => Self::Down,
            Self::Down => Self::Up,
            Self::Flat | Self::Unknown => self,
        }
    }

    /// The slope of the sum of two terms of slopes `self` and `other`.
    fn plus(self, other: Self) -> Self {
        match (self, other) {
            (Self::Flat, slope) | (slope, Self::Flat) => slope,
            (Self::Up, Self::Up) => Self::Up,
            (Self::Down, Self::Down) => Self::Down,
            (Self::Up | Self::Down | Self::Unknown, _) => Self::Unknown,
        }
    }

    /// The slope of `expr` in `var`.
    fn of(expr: &Arithmetic, var: &Variable) -> Self {
        let init = Self::of_factor(expr.init(), var);
        expr.rest().iter().fold(init, |slope, (operator, factor)| {
            let term = Self::of_factor(factor, var);
            match operator {
                ArithmeticOperator::Plus => slope.plus(term),
                ArithmeticOperator::Minus => slope.plus(term.flip()),
                ArithmeticOperator::Multiply
                | ArithmeticOperator::Divide
                | ArithmeticOperator::Modulo
                | ArithmeticOperator::Power
                | ArithmeticOperator::BitAnd
                | ArithmeticOperator::BitOr
                | ArithmeticOperator::BitXor
                | ArithmeticOperator::ShiftLeft
                | ArithmeticOperator::ShiftRight
                | ArithmeticOperator::ShiftRightUnsigned => {
                    if slope == Self::Flat && term == Self::Flat {
                        Self::Flat
                    } else {
                        Self::Unknown
                    }
                }
            }
        })
    }

    fn of_factor(factor: &Factor, var: &Variable) -> Self {
        match factor {
            Factor::Var(other) => Self::of_var(other, var),
            Factor::Group(expr) => Self::of(expr, var),
            Factor::Const(_)
            | Factor::FnCall(_)
            | Factor::Builtin(_)
            | Factor::Cast(_)
            | Factor::Tuple(_)
            | Factor::TupleProj { .. }
            | Factor::Placeholder
            | Factor::Aggregate(_) => {
                if factor.vars().contains(&var) {
                    Self::Unknown
                } else {
                    Self::Flat
                }
            }
        }
    }

    fn of_var(other: &Variable, var: &Variable) -> Self {
        if other == var { Self::Up } else { Self::Flat }
    }
}

#[cfg(test)]
mod tests {
    use flowlog_parser::test_harness::program;
    use rstest::rstest;

    use super::*;
    use crate::stratifier::Stratifier;

    /// A graph, a set of blocked nodes, a `min` and a `max` lattice seeded
    /// at node 1, and two plain relations, for the rules each case adds.
    const DECLS: &str = "\
        .decl Edge(x: int32, y: int32, w: int32)\n\
        .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
        .decl Block(x: int32)\n\
        .input Block(IO=\"file\", filename=\"Block.csv\", delimiter=\",\")\n\
        .decl Near(n: int32, d: min<int32>)\n.output Near\n\
        .decl Far(n: int32, d: max<int32>)\n.output Far\n\
        .decl Seen(n: int32, d: int32)\n.output Seen\n\
        .decl Reached(n: int32)\n.output Reached\n\
        Near(1, 0).\nFar(1, 0).\n";

    /// Stratifies [`DECLS`] with `rules`: `Ok` when every recursion only
    /// climbs, or the first read that could undo one and the relation it
    /// reads.
    fn verdict(rules: &str) -> Result<(), (NonMonotone, String)> {
        match Stratifier::from_program(&program(&format!("{DECLS}{rules}"))) {
            Ok(_) => Ok(()),
            Err(PlanError::NotMonotone { read, relation, .. }) => Err((read, relation)),
            Err(err) => panic!("unexpected error: {err:?}"),
        }
    }

    /// A recursion that negates or aggregates what it derives, or uses a
    /// bound other than as a bound, is rejected at the first such read.
    // Cases: rules, the read rejected, the relation it reads.
    #[rstest]
    #[case::self_negation(
        "Seen(x, w) :- Edge(x, _, w), !Seen(w, x).\n",
        NonMonotone::Negation,
        "Seen"
    )]
    #[case::negation_through_a_cycle(
        "Reached(y) :- Edge(_, y, _), !Seen(y, 0).\nSeen(y, 0) :- Reached(y).\n",
        NonMonotone::Negation,
        "Seen"
    )]
    #[case::aggregate(
        "Seen(y, s) :- Seen(x, _), Edge(x, y, _), s = sum d : Seen(y, d).\n",
        NonMonotone::Aggregate,
        "Seen"
    )]
    #[case::aggregate_of_a_lattice(
        "Near(y, m) :- Near(x, _), Edge(x, y, _), m = min d : Near(y, d).\n",
        NonMonotone::Aggregate,
        "Near"
    )]
    #[case::nested_aggregate(
        "Seen(y, s) :- Seen(x, _), Edge(x, y, _), \
         s = sum c : { Edge(y, z, _), c = count : Seen(z, _) }.\n",
        NonMonotone::Aggregate,
        "Seen"
    )]
    #[case::bound_copied_into_a_plain_column(
        "Seen(n, d) :- Near(n, d).\nNear(y, d + w) :- Seen(x, d), Edge(x, y, w).\n",
        NonMonotone::Copied(Lattice::Min),
        "Near"
    )]
    #[case::bound_copied_into_a_key(
        "Near(d, 0) :- Near(_, d).\n",
        NonMonotone::Copied(Lattice::Min),
        "Near"
    )]
    #[case::bound_reversed(
        "Near(y, 10 - d) :- Near(x, d), Edge(x, y, _).\n",
        NonMonotone::Reversed(Lattice::Min),
        "Near"
    )]
    #[case::bound_into_the_dual_lattice(
        "Far(y, d) :- Near(y, d).\nNear(y, d) :- Far(y, d).\n",
        NonMonotone::Reversed(Lattice::Min),
        "Near"
    )]
    #[case::min_bound_tested_from_below(
        "Near(y, d + w) :- Near(x, d), Edge(x, y, w), d > 3.\n",
        NonMonotone::Filtered(Lattice::Min),
        "Near"
    )]
    #[case::max_bound_tested_from_above(
        "Far(y, d + w) :- Far(x, d), Edge(x, y, w), d < 3.\n",
        NonMonotone::Filtered(Lattice::Max),
        "Far"
    )]
    #[case::bound_joined(
        "Near(y, d) :- Near(x, d), Edge(x, y, d).\n",
        NonMonotone::Matched(Lattice::Min),
        "Near"
    )]
    #[case::bound_constant(
        "Near(y, 1) :- Near(x, 0), Edge(x, y, _).\n",
        NonMonotone::Matched(Lattice::Min),
        "Near"
    )]
    #[case::bound_unequal(
        "Near(y, d + w) :- Near(x, d), Edge(x, y, w), d != w.\n",
        NonMonotone::Matched(Lattice::Min),
        "Near"
    )]
    #[case::bound_negated(
        "Near(y, d + w) :- Near(x, d), Edge(x, y, w), !Block(d).\n",
        NonMonotone::Matched(Lattice::Min),
        "Near"
    )]
    #[case::bound_aggregated(
        "Near(y, d + c) :- Near(x, d), Edge(x, y, _), c = count : Edge(d, _, _).\n",
        NonMonotone::Matched(Lattice::Min),
        "Near"
    )]
    #[case::bound_multiplied(
        "Near(y, d * w) :- Near(x, d), Edge(x, y, w).\n",
        NonMonotone::Opaque(Lattice::Min),
        "Near"
    )]
    #[case::bound_multiplied_in_a_test(
        "Near(y, d + w) :- Near(x, d), Edge(x, y, w), d * 2 < w.\n",
        NonMonotone::Opaque(Lattice::Min),
        "Near"
    )]
    fn a_read_a_later_round_could_undo_is_rejected(
        #[case] rules: &str,
        #[case] read: NonMonotone,
        #[case] relation: &str,
    ) {
        assert_eq!(verdict(rules), Err((read, relation.to_string())));
    }

    /// A recursion that only adds rows and improves bounds is accepted,
    /// and so is any read of a relation computed before it.
    // Cases: rules.
    #[rstest]
    #[case::bound_plus_a_weight("Near(y, d + w) :- Near(x, d), Edge(x, y, w).\n")]
    #[case::bound_minus_a_weight("Near(y, d - w) :- Near(x, d), Edge(x, y, w).\n")]
    #[case::bound_passed_through("Near(y, d) :- Near(x, d), Edge(x, y, _).\n")]
    #[case::max_bound_plus_a_weight("Far(y, d + w) :- Far(x, d), Edge(x, y, w).\n")]
    #[case::bounds_reversed_into_the_dual_lattice(
        "Far(y, 10 - d) :- Near(y, d).\nNear(y, 10 - d) :- Far(y, d).\n"
    )]
    #[case::min_bound_tested_from_above("Near(y, d + w) :- Near(x, d), Edge(x, y, w), d < 100.\n")]
    #[case::min_bound_sum_tested_from_above(
        "Near(y, d + w) :- Near(x, d), Edge(x, y, w), d + w <= 100.\n"
    )]
    #[case::max_bound_tested_from_below("Far(y, d + w) :- Far(x, d), Edge(x, y, w), 3 < d.\n")]
    #[case::existence_of_a_bound(
        "Reached(y) :- Near(y, _).\nNear(y, d + w) :- Near(x, d), Edge(x, y, w), Reached(x).\n"
    )]
    #[case::bound_copied_after_its_recursion(
        "Near(y, d + w) :- Near(x, d), Edge(x, y, w).\nSeen(n, d) :- Near(n, d).\n"
    )]
    #[case::negation_of_an_earlier_stratum(
        "Near(y, d + w) :- Near(x, d), Edge(x, y, w), !Block(y).\n"
    )]
    #[case::aggregate_of_an_earlier_stratum(
        "Near(y, d + c) :- Near(x, d), Edge(x, y, _), c = count : Edge(y, _, _).\n"
    )]
    #[case::nested_aggregate_of_an_earlier_stratum(
        "Near(y, d + n) :- Near(x, d), Edge(x, y, _), \
         n = sum c : { Edge(y, z, _), c = count : Edge(z, _, _) }.\n"
    )]
    fn a_recursion_that_only_climbs_is_accepted(#[case] rules: &str) {
        assert_eq!(verdict(rules), Ok(()));
    }
}
