//! Tuple literals: the value/pattern form of FlowLog's fixed tuples.
//!
//! A [`TupleLit`] is `( e0, e1, ... )` in a rule body or head: it constructs a
//! tuple, or destructures one when matched against a bound variable. Each
//! [`TupleElem`] is an expression or a `_` placeholder (the latter only
//! meaningful when destructuring: it discards the matched component).
//!
//! This is the term-level literal; the tuple *type* (`.type T = ( ... )`) lives in
//! the type registry, and the dual projection node is `Factor::TupleProj`.

use std::fmt;

use educe::Educe;
use flowlog_common::Span;

use super::Arithmetic;
use crate::Lexeme;
use crate::Node;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;

/// A tuple literal `( e0, e1, ... )` (value/pattern position). Each element is
/// either an expression or a `_` placeholder (only meaningful when
/// destructuring: it discards the matched component).
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
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let span = node.span();
        let mut fields = Vec::new();
        for elem in node.children() {
            if elem.rule() != Rule::tuple_elem {
                return Err(grammar_bug(format!(
                    "unexpected child of tuple_lit: {:?}",
                    elem.rule()
                )));
            }
            let inner = elem.children().next_any("tuple element value")?;
            let parsed = match inner.rule() {
                Rule::arithmetic_expr => TupleElem::Expr(inner.lower()?),
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

#[cfg(test)]
mod tests {
    use flowlog_common::FileId;

    use super::*;

    /// A `( x, _ )` literal: one expression element, one placeholder.
    fn expr_and_placeholder() -> TupleLit {
        TupleLit::new(
            vec![
                TupleElem::Expr(Arithmetic::var("x")),
                TupleElem::Placeholder,
            ],
            Span::DUMMY,
        )
    }

    /// `fields` exposes the real element slice, not an empty one.
    #[test]
    fn fields_returns_all_elements() {
        let t = expr_and_placeholder();
        assert_eq!(t.fields().len(), 2);
        assert!(matches!(t.fields()[0], TupleElem::Expr(_)));
        assert!(matches!(t.fields()[1], TupleElem::Placeholder));
    }

    /// `fields_mut` exposes the real element slice, not an empty one.
    #[test]
    fn fields_mut_returns_all_elements() {
        let mut t = expr_and_placeholder();
        assert_eq!(t.fields_mut().len(), 2);
    }

    /// `exprs` yields the expression elements (skipping placeholders), not an
    /// empty iterator: `( x, _ )` has exactly one expression, `x`.
    #[test]
    fn exprs_skips_placeholders_and_yields_expressions() {
        let t = expr_and_placeholder();
        let got: Vec<String> = t.exprs().map(|a| a.to_string()).collect();
        assert_eq!(got, vec!["x".to_string()]);
    }

    /// `exprs_mut` yields the expression elements, not an empty iterator.
    #[test]
    fn exprs_mut_yields_expressions() {
        let mut t = expr_and_placeholder();
        assert_eq!(t.exprs_mut().count(), 1);
    }

    /// `vars` returns the variables of the expression elements, not a constant
    /// or empty vector (placeholders contribute none).
    #[test]
    fn vars_returns_expression_variables() {
        let t = expr_and_placeholder();
        let x = "x".to_string();
        assert_eq!(t.vars(), vec![&x]);
    }

    /// `Display` renders a multi-element tuple as `(x, _)`, and a 1-tuple
    /// keeps the disambiguating trailing comma (`(x,)`). The `len == 1` guard
    /// picks between the two; both sides of the boundary are pinned, and an
    /// empty (default) rendering is caught.
    #[test]
    fn display_renders_tuple_forms() {
        assert_eq!(expr_and_placeholder().to_string(), "(x, _)");

        let one = TupleLit::new(vec![TupleElem::Expr(Arithmetic::var("x"))], Span::DUMMY);
        assert_eq!(one.to_string(), "(x,)");
    }

    /// A well-formed `tuple_lit` parses successfully: every child is a
    /// `tuple_elem`, so the `!= tuple_elem` reject guard must NOT fire. If the
    /// guard's comparison were flipped it would reject valid tuples.
    #[test]
    fn from_parsed_rule_accepts_valid_tuple() {
        use pest::Parser;

        use crate::FlowLogParser;

        let mut pairs = FlowLogParser::parse(Rule::tuple_lit, "(x, y)").unwrap();
        let tup = TupleLit::from_parsed_rule(Node::new(pairs.next().unwrap(), FileId::new(0)))
            .expect("valid tuple_lit must parse");
        assert_eq!(tup.fields().len(), 2);
        assert_eq!(tup.to_string(), "(x, y)");
    }
}
