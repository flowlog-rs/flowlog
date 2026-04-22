//! Rule structures for FlowLog Datalog programs.
//!
//! A rule is `head :- p1, p2, ..., pn.`
//! - Head: a single derived relation
//! - Body: predicates that must all be satisfied

use super::{Factor, Head, HeadArg, Predicate};
use crate::common::{FileId, Ignored, Span};
use crate::parser::error::{grammar_bug, ParseError};
use crate::parser::primitive::ConstType;
use crate::parser::{span_of, Lexeme, Rule};
use pest::iterators::Pair;
use std::fmt;

/// A complete FlowLog rule.
///
/// Declared `pub` (not `pub(crate)`) because it leaks through
/// `Catalog::from_rule` / `Program::rules` signatures that external
/// callers (flowlog-compiler, integration tests) consume. The rule's
/// inherent methods stay `pub(crate)` — external code only passes rules
/// by reference without inspecting fields.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct FlowLogRule {
    head: Head,
    rhs: Vec<Predicate>,
    span: Ignored<Span>,
}

impl fmt::Display for FlowLogRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} :- {}.",
            self.head,
            self.rhs
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl fmt::Debug for FlowLogRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} :- {}.",
            self.head,
            self.rhs
                .iter()
                .map(|p| format!("{p:?}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl FlowLogRule {
    /// Construct a rule.
    #[must_use]
    pub(crate) fn new(head: Head, rhs: Vec<Predicate>) -> Self {
        Self {
            head,
            rhs,
            span: Ignored(Span::DUMMY),
        }
    }

    /// Source location this rule was parsed from.
    #[must_use]
    #[inline]
    pub(crate) fn span(&self) -> Span {
        self.span.0
    }

    /// Rule head.
    #[must_use]
    #[inline]
    pub(crate) fn head(&self) -> &Head {
        &self.head
    }

    /// Rule body (right-hand side predicates).
    #[must_use]
    #[inline]
    pub(crate) fn rhs(&self) -> &[Predicate] {
        &self.rhs
    }

    #[inline]
    pub(crate) fn head_mut(&mut self) -> &mut Head {
        &mut self.head
    }

    #[inline]
    pub(crate) fn rhs_mut(&mut self) -> &mut [Predicate] {
        &mut self.rhs
    }

    /// Extract constants from a fact's head.
    ///
    /// Panics if any head argument is not a simple constant.
    #[must_use]
    pub(crate) fn extract_constants_from_head(&self) -> Vec<ConstType> {
        let mut out = Vec::new();
        for arg in self.head.head_arguments() {
            match arg {
                HeadArg::Var(_) | HeadArg::Aggregation(_) => {
                    panic!("Fact head must contain only constants: {self}")
                }
                HeadArg::Arith(arith) => {
                    if arith.is_const() {
                        if let Factor::Const(c) = arith.init() {
                            out.push(c.clone());
                        } else {
                            // Defensive (shouldn't happen if is_const() is true).
                            panic!("Fact head must contain only constants: {self}");
                        }
                    } else {
                        panic!("Fact head must contain only constants: {self}");
                    }
                }
            }
        }
        out
    }

    /// Parse a (possibly multi-head / multi-body) rule and expand into
    /// one `FlowLogRule` per (head, body) pair.
    ///
    /// Souffle-style semicolons separate alternative heads and bodies:
    ///   `h1; h2 :- b1; b2.` → `h1:-b1. h1:-b2. h2:-b1. h2:-b2.`
    pub(crate) fn expand_from_parsed_rule(
        parsed_rule: Pair<Rule>,
        file: FileId,
    ) -> Result<Vec<Self>, ParseError> {
        let rule_span = span_of(&parsed_rule, file);
        let mut inner = parsed_rule.into_inner();

        let rule_heads_node = inner
            .next()
            .ok_or_else(|| grammar_bug("rule missing heads"))?;
        let mut heads: Vec<Head> = Vec::new();
        for h in rule_heads_node.into_inner() {
            heads.push(Head::from_parsed_rule(h, file)?);
        }

        let rule_bodies_node = inner
            .next()
            .ok_or_else(|| grammar_bug("rule missing bodies"))?;
        let mut bodies: Vec<Vec<Predicate>> = Vec::new();
        for predicates_node in rule_bodies_node.into_inner() {
            let mut body = Vec::new();
            for p in predicates_node.into_inner() {
                body.push(Predicate::from_parsed_rule(p, file)?);
            }
            bodies.push(body);
        }

        let mut out = Vec::with_capacity(heads.len() * bodies.len());
        for head in &heads {
            for body in &bodies {
                out.push(Self {
                    head: head.clone(),
                    rhs: body.clone(),
                    span: Ignored(rule_span),
                });
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::logic::{Aggregation, Arithmetic, Factor};
    use crate::parser::primitive::ConstType;
    use crate::parser::AggregationOperator;

    fn head_const(v: ConstType) -> HeadArg {
        HeadArg::Arith(Arithmetic::new(Factor::Const(v), vec![]))
    }
    fn head_named(name: &str, args: Vec<HeadArg>) -> Head {
        Head::new(name.into(), args)
    }

    #[test]
    fn extract_constants() {
        let head = head_named(
            "facts",
            vec![
                head_const(ConstType::Int(42)),
                head_const(ConstType::Text("hello".into())),
            ],
        );
        let r = FlowLogRule::new(head, vec![]);
        let c = r.extract_constants_from_head();
        assert_eq!(c, vec![ConstType::Int(42), ConstType::Text("hello".into())]);
    }

    #[test]
    #[should_panic(expected = "Fact head must contain only constants")]
    fn extract_constants_panics_on_var() {
        let head = head_named(
            "invalid",
            vec![head_const(ConstType::Int(1)), HeadArg::Var("X".into())],
        );
        let _ = FlowLogRule::new(head, vec![]).extract_constants_from_head();
    }

    #[test]
    #[should_panic(expected = "Fact head must contain only constants")]
    fn extract_constants_panics_on_aggregation() {
        let agg = Aggregation::new(
            AggregationOperator::Sum,
            Arithmetic::new(Factor::Var("X".into()), vec![]),
        );
        let head = head_named(
            "invalid",
            vec![head_const(ConstType::Int(1)), HeadArg::Aggregation(agg)],
        );
        let _ = FlowLogRule::new(head, vec![]).extract_constants_from_head();
    }
}
