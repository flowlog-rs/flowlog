//! Predicate types for FlowLog Datalog programs.
//!
//! - Positive atoms: `edge(X, Y)`
//! - Negative atoms: `!edge(X, Y)`
//! - Comparisons: `X > 5`, `Age ≥ 18`
//!
//! Predicates form the antecedent of rules: `head(...) :- p1, !p2, X > Y.`

use std::fmt;

use flowlog_common::FileId;
use pest::iterators::Pair;

use super::Atom;
use super::ComparisonExpr;
use crate::Lexeme;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;

/// A predicate in a rule body.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Predicate {
    /// Positive atom, e.g. `edge(X, Y)`.
    PositiveAtom(Atom),
    /// Negative atom (negation as failure), e.g. `!edge(X, Y)`.
    NegativeAtom(Atom),
    /// Comparison expression. Covers arithmetic comparisons (`X > 5`), UDF
    /// filters (`f(X) == True`, since UDFs are value-only), and the string
    /// constraints `match`/`contains` (whose operator carries the `!`).
    Compare(ComparisonExpr),
}

#[cfg(test)]
impl Predicate {
    /// Relation name for atom / negative-atom predicates. Tests only;
    /// production code pattern-matches the variant.
    pub fn name(&self) -> &str {
        match self {
            Self::PositiveAtom(atom) | Self::NegativeAtom(atom) => atom.name(),
            Self::Compare(_) => unreachable!("no name on Compare"),
        }
    }
}

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PositiveAtom(atom) => write!(f, "{atom}"),
            Self::NegativeAtom(atom) => write!(f, "!{atom}"),
            Self::Compare(expr) => write!(f, "{expr}"),
        }
    }
}

impl fmt::Debug for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PositiveAtom(atom) => write!(f, "{atom:?}"),
            Self::NegativeAtom(atom) => write!(f, "!{atom:?}"),
            Self::Compare(expr) => write!(f, "{expr}"),
        }
    }
}

impl Lexeme for Predicate {
    /// Parse a `Rule::predicate` pair (the grammar's wrapper that
    /// chooses one of `atom | negative_atom | compare_expr | …`).
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        let inner = parsed_rule
            .into_inner()
            .next()
            .ok_or_else(|| grammar_bug("expected inner rule for predicate"))?;
        Self::from_inner(inner, file)
    }
}

impl Predicate {
    /// Dispatch on the already-unwrapped inner of a `Rule::predicate`.
    /// Callers that need to inspect the inner kind before deciding
    /// (e.g. routing `disjunction_group` elsewhere) use this directly.
    pub fn from_inner(inner: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
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
            // `match`/`contains` are binary boolean operators — a comparison
            // whose operator is `Match`/`Contains` (the `!` folded in).
            Rule::string_constraint => {
                Self::Compare(ComparisonExpr::from_string_constraint(inner, file)?)
            }
            other => return Err(grammar_bug(format!("invalid predicate type: {other:?}"))),
        })
    }
}
