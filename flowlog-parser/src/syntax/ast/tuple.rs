//! Tuple literals: the value/pattern form of FlowLog's fixed tuples.
//!
//! A [`TupleLit`] is `( e0, e1, ... )` in a rule body or head: it constructs a
//! tuple, or destructures one when matched against a bound variable. Each
//! [`TupleElem`] is an expression or a `_` placeholder (the latter only
//! meaningful when destructuring: it discards the matched component).
//!
//! Tuple types (`.type T = (...)`) live in the type registry.
//! `Factor::TupleProj` represents a projection of one tuple component.

use std::fmt;

use educe::Educe;
use flowlog_common::Span;

use super::Arithmetic;
use crate::Lexeme;
use crate::Node;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;

// =============================================================================
// TupleLit
// =============================================================================

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

// =============================================================================
// TupleElem
// =============================================================================

/// One tuple field: a value expression or a destructuring placeholder.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TupleElem {
    Expr(Arithmetic),
    Placeholder,
}

impl Lexeme for TupleElem {
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        // A `paren_item` can contain a condition because value parentheses
        // share their recursive grammar with rule bodies. In a tuple it must
        // be one expression or placeholder, with no comparison suffix.
        let node = if node.rule() == Rule::paren_item {
            let span = node.span();
            let mut parts = node.children();
            let value = parts.next_any("tuple element")?;
            if !matches!(value.rule(), Rule::arithmetic_expr | Rule::placeholder)
                || parts.next().is_some()
            {
                return Err(ParseError::Syntax {
                    span,
                    message: "expected a value expression".into(),
                });
            }
            value
        } else {
            node
        };
        Ok(match node.rule() {
            Rule::arithmetic_expr => Self::Expr(node.lower()?),
            Rule::placeholder => Self::Placeholder,
            other => return Err(grammar_bug(format!("invalid tuple element: {other:?}"))),
        })
    }
}

#[cfg(test)]
mod tests {
    use flowlog_common::FileId;
    use rstest::rstest;

    use super::*;
    use crate::assert_err;
    use crate::test_util::parse_node;
    use crate::test_util::parse_pair;

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

    #[rstest]
    fn expression_elements_accept_direct_and_parenthesized_items(
        #[values(Rule::arithmetic_expr, Rule::paren_item)] start: Rule,
    ) {
        let field: TupleElem = parse_node(start, "x + 1");
        assert!(matches!(field, TupleElem::Expr(expr) if expr.to_string() == "x + 1"));
    }

    #[rstest]
    fn placeholder_elements_accept_direct_and_parenthesized_items(
        #[values(Rule::placeholder, Rule::paren_item)] start: Rule,
    ) {
        let hole: TupleElem = parse_node(start, "_");
        assert!(matches!(hole, TupleElem::Placeholder));
    }

    #[rstest]
    #[case("x > 0")]
    #[case("!R(x)")]
    #[case("!match(x, y)")]
    fn tuple_elements_reject_conditions(#[case] source: &str) {
        let node = Node::new(parse_pair(Rule::paren_item, source), FileId::new(0));
        assert_err!(
            node.lower::<TupleElem>(),
            ParseError::Syntax { span, message }
                if &source[span.range()] == source && message == "expected a value expression"
        );
    }
}
