//! Body-position aggregate predicate (Soufflé compatibility).
//!
//! Surface syntax:
//!
//! ```text
//!   <result> = <op> [<expr>] : <atom>
//!   <result> = <op> [<expr>] : { <pred1>, <pred2>, ... }
//! ```
//!
//! Semantics: `result` is bound to `agg(expr)` taken over every binding
//! of the inner body that is consistent with the outer rule. Grouping
//! variables are implicit — every body var that is referenced *outside*
//! the aggregate in the same rule acts as a group-by key.
//!
//! Lowering happens in [`super::super::desugar`]: each body-position
//! aggregate becomes a fresh auxiliary IDB relation plus a positive
//! atom in the outer rule, so the typechecker / stratifier / planner
//! never see this variant.
//!
//! `expr` is optional only for `count`, matching Soufflé.
//!
//! Examples:
//!
//! ```text
//!   minLen = min len : Atom(_, _, len)
//!   minLen = min len : { Atom1(...), Atom2(...) }
//!   minOrd = min ord(h) : Body(g, h)
//!   color  = max (n)  : StringColorGreaterThan(n, heap)
//!   n      = count    : MethodAndTypeToHeap(_, type, meth)
//! ```

use std::fmt;

use pest::iterators::Pair;

use super::{AggregationOperator, Arithmetic, Predicate};
use crate::common::{FileId, Ignored, Span};
use crate::parser::error::{ParseError, grammar_bug};
use crate::parser::{Lexeme, Rule, span_of};

/// `x = AGG [expr] : body` parsed AST node.
///
/// `expr` is `None` only for `count`. `body` is one or more predicates
/// (a single bare atom in source produces a one-element vector).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BodyAggregate {
    result: String,
    operator: AggregationOperator,
    expr: Option<Arithmetic>,
    body: Vec<Predicate>,
    span: Ignored<Span>,
}

impl BodyAggregate {
    /// Result variable bound by the aggregate.
    #[must_use]
    #[inline]
    pub(crate) fn result(&self) -> &str {
        &self.result
    }

    /// Aggregation operator (`min`, `max`, `count`, `sum`, `avg`).
    #[must_use]
    #[inline]
    pub(crate) fn operator(&self) -> AggregationOperator {
        self.operator
    }

    /// Aggregated expression; `None` only for `count`.
    #[must_use]
    #[inline]
    pub(crate) fn expr(&self) -> Option<&Arithmetic> {
        self.expr.as_ref()
    }

    /// Body predicates (the `: ...` half).
    #[must_use]
    #[inline]
    pub(crate) fn body(&self) -> &[Predicate] {
        &self.body
    }

    /// Mutable view of body predicates. Used by name-rewriting passes
    /// (component inliner, dot normalization) that must descend into
    /// the inner body atoms just like they descend into the rule's
    /// top-level `rhs`.
    #[must_use]
    #[inline]
    pub(crate) fn body_mut(&mut self) -> &mut [Predicate] {
        &mut self.body
    }

    /// Replace the body predicates wholesale. Needed by desugaring passes
    /// that may change the body length (e.g. lifting an expression atom
    /// argument inside the aggregate to a fresh variable + equality).
    #[inline]
    pub(crate) fn set_body(&mut self, body: Vec<Predicate>) {
        self.body = body;
    }

    /// Source location.
    #[must_use]
    #[inline]
    pub(crate) fn span(&self) -> Span {
        self.span.0
    }
}

impl fmt::Display for BodyAggregate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} = {}", self.result, self.operator)?;
        if let Some(expr) = &self.expr {
            write!(f, " {expr}")?;
        }
        write!(f, " : ")?;
        if self.body.len() == 1 {
            write!(f, "{}", self.body[0])
        } else {
            f.write_str("{ ")?;
            for (i, p) in self.body.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                write!(f, "{p}")?;
            }
            f.write_str(" }")
        }
    }
}

impl Lexeme for BodyAggregate {
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        let span = span_of(&parsed_rule, file);
        let mut inner = parsed_rule.into_inner();

        let result_pair = inner
            .next()
            .ok_or_else(|| grammar_bug("body_aggregate missing result variable"))?;
        let result = result_pair.as_str().to_string();

        let op_pair = inner
            .next()
            .ok_or_else(|| grammar_bug("body_aggregate missing aggregate_op"))?;
        let operator = AggregationOperator::from_parsed_rule(op_pair, file)?;

        let mut expr: Option<Arithmetic> = None;
        let mut body: Vec<Predicate> = Vec::new();

        for child in inner {
            match child.as_rule() {
                Rule::body_agg_expr => {
                    let inner_expr = child
                        .into_inner()
                        .next()
                        .ok_or_else(|| grammar_bug("body_agg_expr missing inner expression"))?;
                    expr = Some(Arithmetic::from_parsed_rule(inner_expr, file)?);
                }
                Rule::body_agg_body => {
                    let body_inner = child
                        .into_inner()
                        .next()
                        .ok_or_else(|| grammar_bug("body_agg_body missing inner"))?;
                    match body_inner.as_rule() {
                        Rule::atom => {
                            // Bare single-atom body — wrap as a one-element predicate vector.
                            body.push(Predicate::from_inner(body_inner, file)?);
                        }
                        Rule::predicates => {
                            // `{ predicates }` form — every comma-separated
                            // predicate becomes a conjunct. We deliberately
                            // reject nested disjunction groups here so the
                            // lowering stays a single positive-conjunction
                            // body; the desugar pass enforces this.
                            for pred_node in body_inner.into_inner() {
                                let inner_pred = pred_node.into_inner().next().ok_or_else(|| {
                                    grammar_bug("predicate missing inner inside body_agg_body")
                                })?;
                                body.push(Predicate::from_inner(inner_pred, file)?);
                            }
                        }
                        other => {
                            return Err(grammar_bug(format!(
                                "unexpected body_agg_body inner rule: {other:?}"
                            )));
                        }
                    }
                }
                other => {
                    return Err(grammar_bug(format!(
                        "unexpected child of body_aggregate: {other:?}"
                    )));
                }
            }
        }

        if body.is_empty() {
            return Err(grammar_bug("body_aggregate missing body"));
        }

        Ok(Self {
            result,
            operator,
            expr,
            body,
            span: Ignored(span),
        })
    }
}
