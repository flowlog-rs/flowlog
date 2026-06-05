//! Predicate types for FlowLog Datalog programs.
//!
//! - Positive atoms: `edge(X, Y)`
//! - Negative atoms: `!edge(X, Y)`
//! - Comparisons: `X > 5`, `Age ≥ 18`
//!
//! Predicates form the antecedent of rules: `head(...) :- p1, !p2, X > Y.`

use std::fmt;

use pest::iterators::Pair;

use crate::common::{FileId, Span};
use crate::parser::error::{ParseError, grammar_bug};
use crate::parser::primitive::ConstType;
use crate::parser::{Lexeme, Rule, span_of};

use super::{
    Arithmetic, Atom, BodyAggregate, BuiltinCall, ComparisonExpr, ComparisonOperator, Factor,
    FnCall,
};

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
    /// Soufflé body-position aggregate, e.g. `x = min ord(h) : Body(g, h)`.
    /// Always lowered to a positive auxiliary atom by the desugar pass
    /// before typechecking, so later stages never observe this variant.
    BodyAggregate(BodyAggregate),
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
            Self::BodyAggregate(_) => unreachable!("no name on BodyAggregate"),
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
            Self::BodyAggregate(agg) => write!(f, "{agg}"),
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
            Self::BodyAggregate(agg) => write!(f, "{agg}"),
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
    pub(crate) fn from_inner(inner: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        Ok(match inner.as_rule() {
            Rule::atom => Self::PositiveAtom(Atom::from_parsed_rule(inner, file)?),
            Rule::negative_atom => parse_negative(inner, file)?,
            Rule::compare_expr => Self::Compare(ComparisonExpr::from_parsed_rule(inner, file)?),
            Rule::fn_call_expr => Self::FnCall(FnCall::from_parsed_rule(inner, file)?),
            Rule::builtin_fn_call => {
                let span = span_of(&inner, file);
                let call = BuiltinCall::from_parsed_rule(inner, file)?;
                Self::Compare(boolean_builtin_as_compare(call, true, span))
            }
            Rule::body_aggregate => {
                Self::BodyAggregate(BodyAggregate::from_parsed_rule(inner, file)?)
            }
            other => return Err(grammar_bug(format!("invalid predicate type: {other:?}"))),
        })
    }
}

/// Parse a `negative_atom` parse-tree node. The grammar permits two
/// flavours:
///
/// 1. `!atom` — a real negated atom. Lowered to [`Predicate::NegativeAtom`].
/// 2. `!builtin_fn_call` — negation of a boolean built-in (e.g.
///    `!match("java.*", class)`). Lowered to a comparison against the
///    `false` literal, so downstream stages see a plain
///    [`Predicate::Compare`] and need no special-casing.
fn parse_negative(inner: Pair<Rule>, file: FileId) -> Result<Predicate, ParseError> {
    let body = inner
        .into_inner()
        .next()
        .ok_or_else(|| grammar_bug("negative_atom missing inner term"))?;

    // Boundary parens around the negated term are pure grouping.
    let payload = match body.as_rule() {
        Rule::neg_inner => body
            .into_inner()
            .next()
            .ok_or_else(|| grammar_bug("neg_inner missing payload"))?,
        _ => body,
    };

    match payload.as_rule() {
        Rule::atom => Ok(Predicate::NegativeAtom(Atom::from_parsed_rule(
            payload, file,
        )?)),
        Rule::builtin_fn_call => {
            let span = span_of(&payload, file);
            let call = BuiltinCall::from_parsed_rule(payload, file)?;
            Ok(Predicate::Compare(boolean_builtin_as_compare(
                call, false, span,
            )))
        }
        other => Err(grammar_bug(format!(
            "negative_atom: unexpected payload {other:?}"
        ))),
    }
}

/// Wrap a boolean-returning built-in call as a comparison against a
/// boolean literal, e.g. `match(re, s)` → `match(re, s) = true` and
/// `!match(re, s)` → `match(re, s) = false`.
///
/// Down-stream stages already know how to evaluate the LHS factor (it's
/// a normal `Factor::Builtin`) and how to compare booleans, so the
/// desugaring lands without any new bookkeeping.
fn boolean_builtin_as_compare(
    call: BuiltinCall,
    expect_true: bool,
    span: Span,
) -> ComparisonExpr {
    let lhs = Arithmetic::new(Factor::Builtin(call), Vec::new());
    let rhs = Arithmetic::new(Factor::Const(ConstType::Bool(expect_true)), Vec::new());
    ComparisonExpr::synth(lhs, ComparisonOperator::Equal, rhs, span)
}
