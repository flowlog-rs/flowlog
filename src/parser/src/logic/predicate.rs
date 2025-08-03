//! Predicate types for FlowLog rule bodies.

use crate::{Lexeme, Rule};
use super::{Atom, AtomArg, ComparisonExpr};
use pest::iterators::Pair;
use std::fmt;

/// Represents a predicate in a rule body.
///
/// Predicates define the conditions that must be satisfied for a rule to fire.
/// They can be positive atoms (facts that must be true), negated atoms (facts
/// that must be false), comparison expressions, or boolean literals.
///
/// # Examples
///
/// ```rust
/// use parser::logic::Predicate;
/// use parser::logic::{Atom, AtomArg};
///
/// // Positive atom: edge(X, Y)
/// let atom = Atom::new("edge", vec![
///     AtomArg::Var("X".to_string()),
///     AtomArg::Var("Y".to_string())
/// ]);
/// let positive_pred = Predicate::PositiveAtomPredicate(atom.clone());
///
/// // Negated atom: !edge(X, Y)
/// let negated_pred = Predicate::NegatedAtomPredicate(atom);
///
/// // Boolean literal
/// let bool_pred = Predicate::BoolPredicate(true);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Predicate {
    /// A positive atom (e.g., edge(X, Y))
    PositiveAtomPredicate(Atom),
    /// A negated atom (e.g., !edge(X, Y))
    NegatedAtomPredicate(Atom),
    /// A comparison expression (e.g., X > Y)
    ComparePredicate(ComparisonExpr),
    /// A boolean literal (True or False)
    BoolPredicate(bool),
}

impl Predicate {
    /// Returns the arguments of this predicate if it's an atom or negated atom
    pub fn arguments(&self) -> Vec<&AtomArg> {
        match self {
            Self::PositiveAtomPredicate(atom) => atom.arguments().iter().collect(),
            Self::NegatedAtomPredicate(atom) => atom.arguments().iter().collect(),
            Self::ComparePredicate(_) => {
                unreachable!("Cannot get arguments from a comparison predicate")
            }
            Self::BoolPredicate(_) => unreachable!("Cannot get arguments from a true predicate"),
        }
    }

    /// Returns the name of this predicate if it's an atom or negated atom.
    ///
    /// # Panics
    ///
    /// Panics if called on a comparison or boolean predicate.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::PositiveAtomPredicate(atom) | Self::NegatedAtomPredicate(atom) => atom.name(),
            Self::ComparePredicate(_) => {
                unreachable!("Cannot get name from a comparison predicate")
            }
            Self::BoolPredicate(_) => unreachable!("Cannot get name from a boolean predicate"),
        }
    }

    pub fn is_boolean(&self) -> bool {
        matches!(&self, Predicate::BoolPredicate(_))
    }
}

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PositiveAtomPredicate(atom) => write!(f, "{atom}"),
            Self::NegatedAtomPredicate(atom) => write!(f, "!{atom}"),
            Self::ComparePredicate(expr) => write!(f, "{expr}"),
            Self::BoolPredicate(boolean) => write!(f, "{boolean}"),
        }
    }
}

impl Lexeme for Predicate {
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        match parsed_rule.as_rule() {
            Rule::predicate => {
                // predicate is a wrapper that contains the actual predicate type
                let inner_rule = parsed_rule.into_inner().next().unwrap();
                Self::from_parsed_rule(inner_rule)
            }
            Rule::atom => {
                let atom = Atom::from_parsed_rule(parsed_rule);
                Self::PositiveAtomPredicate(atom)
            }
            Rule::neg_atom => {
                // The negated atom rule has an extra layer: neg_atom >> { "!" ~ atom }
                let inner_rule = parsed_rule.into_inner().next().unwrap();
                let negated_atom = Atom::from_parsed_rule(inner_rule);
                Self::NegatedAtomPredicate(negated_atom)
            }
            Rule::compare_expr => {
                let compare_expr = ComparisonExpr::from_parsed_rule(parsed_rule);
                Self::ComparePredicate(compare_expr)
            }
            Rule::boolean => {
                let value = parsed_rule.as_str();
                match value {
                    "True" => Self::BoolPredicate(true),
                    "False" => Self::BoolPredicate(false),
                    _ => unreachable!("Invalid boolean literal: {}", value),
                }
            }
            _ => unreachable!("Invalid predicate type: {:?}", parsed_rule.as_rule()),
        }
    }
}
