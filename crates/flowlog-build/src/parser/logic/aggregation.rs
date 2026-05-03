//! Aggregation expressions for FlowLog Datalog programs.
//!
//! - [`AggregationOperator`]: `min | max | count | sum | average`
//! - [`Aggregation`]: `op(expr)` (e.g., `sum(price * qty)`)

use super::Arithmetic;
use crate::common::{FileId, Ignored, Span};
use crate::parser::error::{grammar_bug, ParseError};
use crate::parser::{span_of, Lexeme, Rule};
use pest::iterators::Pair;
use std::fmt;

/// Supported aggregation operators.
///
/// Declared `pub` because `Features::agg_semirings()` exposes an
/// `AggSemiringNeeds` whose entries `flowlog-compiler` iterates over,
/// calling [`Self::semiring_mod`] / [`Self::semiring_prefix`] on each.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregationOperator {
    Min,
    Max,
    Count,
    Sum,
    Avg,
}

impl AggregationOperator {
    /// Normalize Count → Sum (they share the same semiring).
    #[must_use]
    pub(crate) fn semiring_canonical(self) -> Self {
        match self {
            Self::Count => Self::Sum,
            other => other,
        }
    }

    /// Lowercase name for semiring module paths (e.g. `"min"`, `"sum"`).
    #[must_use]
    pub fn semiring_mod(self) -> &'static str {
        match self.semiring_canonical() {
            Self::Min => "min",
            Self::Max => "max",
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Count => unreachable!(),
        }
    }

    /// Title-case prefix for semiring type names (e.g. `"Min"`, `"Sum"`).
    #[must_use]
    pub fn semiring_prefix(self) -> &'static str {
        match self.semiring_canonical() {
            Self::Min => "Min",
            Self::Max => "Max",
            Self::Sum => "Sum",
            Self::Avg => "Avg",
            Self::Count => unreachable!(),
        }
    }
}

impl fmt::Display for AggregationOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Min => write!(f, "min"),
            Self::Max => write!(f, "max"),
            Self::Count => write!(f, "count"),
            Self::Sum => write!(f, "sum"),
            Self::Avg => write!(f, "average"),
        }
    }
}

impl Lexeme for AggregationOperator {
    /// Parse an aggregation operator from the grammar.
    fn from_parsed_rule(parsed_rule: Pair<Rule>, _file: FileId) -> Result<Self, ParseError> {
        let op = parsed_rule
            .into_inner()
            .next()
            .ok_or_else(|| grammar_bug("aggregation operator missing inner token"))?;

        Ok(match op.as_rule() {
            Rule::min => Self::Min,
            Rule::max => Self::Max,
            Rule::count => Self::Count,
            Rule::sum => Self::Sum,
            Rule::average => Self::Avg,
            other => {
                return Err(grammar_bug(format!(
                    "unexpected aggregation operator rule: {other:?}"
                )));
            }
        })
    }
}

/// `op(expr)` aggregation (e.g., `sum(price * qty)`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Aggregation {
    operator: AggregationOperator,
    arithmetic: Arithmetic,
    span: Ignored<Span>,
}

impl Aggregation {
    #[cfg(test)]
    pub(crate) fn new(operator: AggregationOperator, arithmetic: Arithmetic) -> Self {
        Self {
            operator,
            arithmetic,
            span: Ignored(Span::DUMMY),
        }
    }

    /// Source location this aggregation was parsed from.
    #[must_use]
    #[inline]
    pub(crate) fn span(&self) -> Span {
        self.span.0
    }

    /// Variables referenced by the arithmetic expression.
    #[must_use]
    pub(crate) fn vars(&self) -> Vec<&String> {
        self.arithmetic.vars()
    }

    /// Underlying arithmetic expression.
    #[must_use]
    #[inline]
    pub(crate) fn arithmetic(&self) -> &Arithmetic {
        &self.arithmetic
    }

    #[inline]
    pub(crate) fn arithmetic_mut(&mut self) -> &mut Arithmetic {
        &mut self.arithmetic
    }

    /// Aggregation operator.
    #[must_use]
    #[inline]
    pub(crate) fn operator(&self) -> &AggregationOperator {
        &self.operator
    }
}

impl fmt::Display for Aggregation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", self.operator, self.arithmetic)
    }
}

impl Lexeme for Aggregation {
    /// Parse an aggregation from the grammar.
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        let span = span_of(&parsed_rule, file);
        let mut inner = parsed_rule.into_inner();

        let op_pair = inner
            .next()
            .ok_or_else(|| grammar_bug("aggregation missing operator"))?;
        let operator = AggregationOperator::from_parsed_rule(op_pair, file)?;

        let expr_pair = inner
            .next()
            .ok_or_else(|| grammar_bug("aggregation missing arithmetic expression"))?;
        let arithmetic = Arithmetic::from_parsed_rule(expr_pair, file)?;

        Ok(Self {
            operator,
            arithmetic,
            span: Ignored(span),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::FileId;
    use crate::parser::{FlowLogParser, Lexeme, Rule};
    use pest::Parser;

    #[test]
    fn parse_aggregate_expr() {
        let mut pairs = FlowLogParser::parse(Rule::aggregate_expr, "sum(price * qty)").unwrap();
        let agg = Aggregation::from_parsed_rule(pairs.next().unwrap(), FileId(0)).unwrap();
        assert_eq!(*agg.operator(), AggregationOperator::Sum);
        assert_eq!(agg.vars().len(), 2);
    }
}
