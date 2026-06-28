//! Tuple literals — the value/pattern form of FlowLog's fixed tuples.
//!
//! A [`TupleLit`] is `( e0, e1, … )` in a rule body or head: it constructs a
//! tuple, or destructures one when matched against a bound variable. Each
//! [`TupleElem`] is an expression or a `_` placeholder (the latter only
//! meaningful when destructuring — it discards the matched component).
//!
//! This is the term-level literal; the tuple *type* (`.type T = ( … )`) lives in
//! the type registry, and the dual projection node is `Factor::TupleProj`.

use std::fmt;

use pest::iterators::Pair;

use educe::Educe;

use super::Arithmetic;
use crate::error::{ParseError, grammar_bug};
use crate::{Lexeme, Rule, span_of};
use flowlog_common::{FileId, Span};

/// A tuple literal `( e0, e1, … )` (value/pattern position). Each element is
/// either an expression or a `_` placeholder (only meaningful when
/// destructuring — it discards the matched component).
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct TupleLit {
    fields: Vec<TupleElem>,
    #[educe(PartialEq(ignore), Hash(ignore))]
    span: Span,
}

/// One element of a [`TupleLit`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TupleElem {
    Expr(Arithmetic),
    Placeholder,
}

impl TupleLit {
    #[must_use]
    pub fn new(fields: Vec<TupleElem>, span: Span) -> Self {
        Self { fields, span }
    }

    #[must_use]
    pub fn fields(&self) -> &[TupleElem] {
        &self.fields
    }

    #[must_use]
    pub fn fields_mut(&mut self) -> &mut [TupleElem] {
        &mut self.fields
    }

    #[must_use]
    #[inline]
    pub fn span(&self) -> Span {
        self.span
    }

    /// The element expressions, skipping `_` placeholders, in order.
    pub fn exprs(&self) -> impl Iterator<Item = &Arithmetic> {
        self.fields.iter().filter_map(|e| match e {
            TupleElem::Expr(a) => Some(a),
            TupleElem::Placeholder => None,
        })
    }

    /// Mutable view of the element expressions, skipping `_` placeholders.
    pub fn exprs_mut(&mut self) -> impl Iterator<Item = &mut Arithmetic> {
        self.fields.iter_mut().filter_map(|e| match e {
            TupleElem::Expr(a) => Some(a),
            TupleElem::Placeholder => None,
        })
    }

    /// Variables appearing in the element expressions (placeholders contribute
    /// none), in order.
    #[must_use]
    pub fn vars(&self) -> Vec<&String> {
        self.exprs().flat_map(Arithmetic::vars).collect()
    }
}

impl fmt::Display for TupleLit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self
            .fields
            .iter()
            .map(|e| match e {
                TupleElem::Expr(a) => a.to_string(),
                TupleElem::Placeholder => "_".to_string(),
            })
            .collect::<Vec<_>>()
            .join(", ");
        // Source form. A 1-tuple needs the trailing comma (`(x,)`) to stay
        // distinct from plain grouping (`(x)`), mirroring the grammar.
        if self.fields.len() == 1 {
            write!(f, "({inner},)")
        } else {
            write!(f, "({inner})")
        }
    }
}

impl Lexeme for TupleLit {
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        let span = span_of(&parsed_rule, file);
        let mut fields = Vec::new();
        for elem in parsed_rule.into_inner() {
            if elem.as_rule() != Rule::tuple_elem {
                return Err(grammar_bug(format!(
                    "unexpected child of tuple_lit: {:?}",
                    elem.as_rule()
                )));
            }
            let inner = elem
                .into_inner()
                .next()
                .ok_or_else(|| grammar_bug("tuple_elem missing inner token"))?;
            let parsed = match inner.as_rule() {
                Rule::arithmetic_expr => {
                    TupleElem::Expr(Arithmetic::from_parsed_rule(inner, file)?)
                }
                Rule::placeholder => TupleElem::Placeholder,
                other => {
                    return Err(grammar_bug(format!("invalid tuple element: {other:?}")));
                }
            };
            fields.push(parsed);
        }
        Ok(Self::new(fields, span))
    }
}
