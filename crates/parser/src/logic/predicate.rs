//! Predicate types for Macaron Datalog programs.
//!
//! - Positive atoms: `edge(X, Y)`
//! - Negated atoms: `!edge(X, Y)`
//! - Comparisons: `X > 5`, `Age ≥ 18`
//! - Boolean literals: `true`, `false`
//!
//! Predicates form the antecedent of rules: `head(...) :- p1, !p2, X > Y, true.`

use super::{Atom, AtomArg, ComparisonExpr};
use crate::{Lexeme, Rule};
use pest::iterators::Pair;
use std::fmt;

/// A predicate in a rule body.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Predicate {
    /// Positive atom, e.g. `edge(X, Y)`.
    PositiveAtomPredicate(Atom),
    /// Negated atom (negation as failure), e.g. `!edge(X, Y)`.
    NegatedAtomPredicate(Atom),
    /// Comparison expression, e.g. `X > 5`.
    ComparePredicate(ComparisonExpr),
    /// Boolean literal.
    BoolPredicate(bool),
}

impl Predicate {
    /// Arguments of the (negated) atom.
    ///
    /// # Panics
    /// Panics if called on non-atom predicates.
    #[inline]
    pub fn arguments(&self) -> Vec<&AtomArg> {
        match self {
            Self::PositiveAtomPredicate(atom) | Self::NegatedAtomPredicate(atom) => {
                atom.arguments().iter().collect()
            }
            Self::ComparePredicate(_) => {
                unreachable!("Cannot get arguments from a comparison predicate")
            }
            Self::BoolPredicate(_) => {
                unreachable!("Cannot get arguments from a boolean predicate")
            }
        }
    }

    /// Relation name (for atom / negated atom).
    ///
    /// # Panics
    /// Panics if called on non-atom predicates.
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        match self {
            Self::PositiveAtomPredicate(atom) | Self::NegatedAtomPredicate(atom) => atom.name(),
            Self::ComparePredicate(_) => {
                unreachable!("Cannot get name from a comparison predicate")
            }
            Self::BoolPredicate(_) => unreachable!("Cannot get name from a boolean predicate"),
        }
    }

    /// Is this a boolean literal?
    #[inline]
    pub fn is_boolean(&self) -> bool {
        matches!(self, Self::BoolPredicate(_))
    }
}

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PositiveAtomPredicate(atom) => write!(f, "{atom}"),
            Self::NegatedAtomPredicate(atom) => write!(f, "!{atom}"),
            Self::ComparePredicate(expr) => write!(f, "{expr}"),
            Self::BoolPredicate(b) => write!(f, "{b}"),
        }
    }
}

impl fmt::Debug for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Delegate Debug to Display
        fmt::Display::fmt(self, f)
    }
}

impl Lexeme for Predicate {
    /// Parse:
    /// - `atom` → positive atom
    /// - `neg_atom` → `!atom`
    /// - `compare_expr` → comparison
    /// - `boolean` → `True` | `False`
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let inner = parsed_rule
            .into_inner()
            .next()
            .expect("Parser error: expected inner rule for predicate");

        match inner.as_rule() {
            Rule::atom => Self::PositiveAtomPredicate(Atom::from_parsed_rule(inner)),
            Rule::neg_atom => {
                let atom_rule = inner
                    .into_inner()
                    .next()
                    .expect("Parser error: neg_atom missing inner atom");
                Self::NegatedAtomPredicate(Atom::from_parsed_rule(atom_rule))
            }
            Rule::compare_expr => Self::ComparePredicate(ComparisonExpr::from_parsed_rule(inner)),
            Rule::boolean => match inner.as_str() {
                "True" => Self::BoolPredicate(true),
                "False" => Self::BoolPredicate(false),
                other => unreachable!("Parser error: invalid boolean literal: {other}"),
            },
            other => unreachable!("Parser error: invalid predicate type: {:?}", other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logic::{Arithmetic, ArithmeticOperator, ComparisonOperator, Factor};
    use crate::primitive::ConstType;

    // Helpers
    fn atom(name: &str, args: Vec<AtomArg>) -> Atom {
        Atom::new(name, args)
    }
    fn var(name: &str) -> AtomArg {
        AtomArg::Var(name.into())
    }
    fn int(v: i32) -> AtomArg {
        AtomArg::Const(ConstType::Integer(v))
    }
    fn txt(s: &str) -> AtomArg {
        AtomArg::Const(ConstType::Text(s.into()))
    }
    fn cmp_expr_gt_x_5() -> ComparisonExpr {
        let l = Arithmetic::new(Factor::Var("X".into()), vec![]);
        let r = Arithmetic::new(Factor::Const(ConstType::Integer(5)), vec![]);
        ComparisonExpr::new(l, ComparisonOperator::GreaterThan, r)
    }

    #[test]
    fn positive_atom_predicate() {
        let p = Predicate::PositiveAtomPredicate(atom("edge", vec![var("X"), var("Y")]));
        assert_eq!(p.name(), "edge");
        assert!(!p.is_boolean());
        assert!(p.to_string().starts_with("edge(X, Y)"));
        let args = p.arguments();
        assert_eq!(args.len(), 2);
        assert_eq!(args[0], &var("X"));
        assert_eq!(args[1], &var("Y"));
    }

    #[test]
    fn negated_atom_predicate() {
        let p = Predicate::NegatedAtomPredicate(atom("blocked", vec![var("User")]));
        assert_eq!(p.name(), "blocked");
        assert!(p.to_string().starts_with("!blocked(User)"));
        let args = p.arguments();
        assert_eq!(args.len(), 1);
        assert_eq!(args[0], &var("User"));
    }

    #[test]
    fn comparison_predicate_display() {
        let p = Predicate::ComparePredicate(cmp_expr_gt_x_5());
        assert!(!p.is_boolean());
        assert_eq!(p.to_string(), "X > 5");
    }

    #[test]
    fn boolean_predicates() {
        let t = Predicate::BoolPredicate(true);
        let f = Predicate::BoolPredicate(false);
        assert!(t.is_boolean() && f.is_boolean());
        assert_eq!(t.to_string(), "true");
        assert_eq!(f.to_string(), "false");
    }

    #[test]
    #[should_panic(expected = "Cannot get arguments from a comparison predicate")]
    fn compare_arguments_panics() {
        let p = Predicate::ComparePredicate(cmp_expr_gt_x_5());
        let _ = p.arguments();
    }

    #[test]
    #[should_panic(expected = "Cannot get arguments from a boolean predicate")]
    fn boolean_arguments_panics() {
        let p = Predicate::BoolPredicate(true);
        let _ = p.arguments();
    }

    #[test]
    #[should_panic(expected = "Cannot get name from a comparison predicate")]
    fn compare_name_panics() {
        let p = Predicate::ComparePredicate(cmp_expr_gt_x_5());
        let _ = p.name();
    }

    #[test]
    #[should_panic(expected = "Cannot get name from a boolean predicate")]
    fn boolean_name_panics() {
        let p = Predicate::BoolPredicate(false);
        let _ = p.name();
    }

    #[test]
    fn display_matrix_and_complex_examples() {
        // Mixed atom
        let pa = Predicate::PositiveAtomPredicate(atom("person", vec![txt("Alice"), int(30)]));
        assert!(pa.to_string().starts_with("person(\"Alice\", 30)"));

        // All comparison ops
        let l = Arithmetic::new(Factor::Var("x".into()), vec![]);
        let r = Arithmetic::new(Factor::Const(ConstType::Integer(10)), vec![]);
        let cases = [
            (ComparisonOperator::Equal, "x == 10"),
            (ComparisonOperator::NotEqual, "x ≠ 10"),
            (ComparisonOperator::GreaterThan, "x > 10"),
            (ComparisonOperator::GreaterEqualThan, "x ≥ 10"),
            (ComparisonOperator::LessThan, "x < 10"),
            (ComparisonOperator::LessEqualThan, "x ≤ 10"),
        ];
        for (op, expected) in cases {
            let p = Predicate::ComparePredicate(ComparisonExpr::new(l.clone(), op, r.clone()));
            assert_eq!(p.to_string(), expected);
        }

        // Complex comparison: salary * 12 > 100000
        let left = Arithmetic::new(
            Factor::Var("salary".into()),
            vec![(
                ArithmeticOperator::Multiply,
                Factor::Const(ConstType::Integer(12)),
            )],
        );
        let right = Arithmetic::new(Factor::Const(ConstType::Integer(100000)), vec![]);
        let complex = Predicate::ComparePredicate(ComparisonExpr::new(
            left,
            ComparisonOperator::GreaterThan,
            right,
        ));
        assert_eq!(complex.to_string(), "salary * 12 > 100000");
    }

    #[test]
    fn nullary_atom_predicate() {
        let p = Predicate::PositiveAtomPredicate(atom("flag", vec![]));
        assert_eq!(p.name(), "flag");
        assert!(p.to_string().starts_with("flag()"));
        assert!(p.arguments().is_empty());
    }

    #[test]
    fn clone_hash_eq() {
        let a = Predicate::PositiveAtomPredicate(atom("t", vec![var("X")]));
        let b = a.clone();
        assert_eq!(a, b);

        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(a);
        set.insert(b);
        set.insert(Predicate::BoolPredicate(true));
        assert_eq!(set.len(), 2);
    }
}
