//! Predicate types for FlowLog Datalog programs.
//!
//! - Positive atoms: `edge(X, Y)`
//! - Negative atoms: `!edge(X, Y)`
//! - Comparisons: `X > 5`, `Age ≥ 18`
//!
//! Predicates form the antecedent of rules: `head(...) :- p1, !p2, X > Y.`

use std::fmt;

use pest::iterators::Pair;

use crate::common::FileId;
use crate::parser::error::{grammar_bug, ParseError};
use crate::parser::{Lexeme, Rule};

use super::{Atom, ComparisonExpr, FnCall};

/// A predicate in a rule body.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) enum Predicate {
    /// Positive atom, e.g. `edge(X, Y)`.
    PositiveAtom(Atom),
    /// Negative atom (negation as failure), e.g. `!edge(X, Y)`.
    NegativeAtom(Atom),
    /// Comparison expression, e.g. `X > 5`.
    Compare(ComparisonExpr),
    /// UDF predicate call, e.g. `is_valid(X, Y + 1)`.
    FnCall(FnCall),
}

#[cfg(test)]
impl Predicate {
    /// Relation name for atom / negative-atom predicates. Tests only;
    /// production code pattern-matches the variant.
    pub(crate) fn name(&self) -> &str {
        match self {
            Self::PositiveAtom(atom) | Self::NegativeAtom(atom) => atom.name(),
            Self::Compare(_) => unreachable!("no name on Compare"),
            Self::FnCall(_) => unreachable!("no name on FnCall"),
        }
    }
}

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PositiveAtom(atom) => write!(f, "{atom}"),
            Self::NegativeAtom(atom) => write!(f, "!{atom}"),
            Self::Compare(expr) => write!(f, "{expr}"),
            Self::FnCall(fc) => write!(f, "{fc}"),
        }
    }
}

impl fmt::Debug for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PositiveAtom(atom) => write!(f, "{atom:?}"),
            Self::NegativeAtom(atom) => write!(f, "!{atom:?}"),
            Self::Compare(expr) => write!(f, "{expr}"),
            Self::FnCall(fc) => write!(f, "{fc}"),
        }
    }
}

impl Lexeme for Predicate {
    /// Parse:
    /// - `atom` → positive atom
    /// - `negative_atom` → `!atom`
    /// - `compare_expr` → comparison
    /// - `boolean` → `True` | `False`
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        let inner = parsed_rule
            .into_inner()
            .next()
            .ok_or_else(|| grammar_bug("expected inner rule for predicate"))?;

        Ok(match inner.as_rule() {
            Rule::atom => Self::PositiveAtom(Atom::from_parsed_rule(inner, file)?),
            Rule::negative_atom => {
                let atom_rule = inner
                    .into_inner()
                    .next()
                    .ok_or_else(|| grammar_bug("negative_atom missing inner atom"))?;
                Self::NegativeAtom(Atom::from_parsed_rule(atom_rule, file)?)
            }
            Rule::compare_expr => Self::Compare(ComparisonExpr::from_parsed_rule(inner, file)?),
            Rule::fn_call_expr => Self::FnCall(FnCall::from_parsed_rule(inner, file)?),
            other => return Err(grammar_bug(format!("invalid predicate type: {other:?}"))),
        })
    }
}
