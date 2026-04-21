//! Aggregation expressions for FlowLog Datalog programs.
//!
//! - [`AggregationOperator`]: `min | max | count | sum | average`
//! - [`Aggregation`]: `op(expr)` (e.g., `sum(price * qty)`)
//!
//! # Example
//! ```rust
//! use flowlog_build::parser::logic::{Aggregation, AggregationOperator, Arithmetic, Factor};
//! let expr = Arithmetic::new(Factor::Var("x".into()), vec![]);
//! let agg = Aggregation::new(AggregationOperator::Sum, expr);
//! assert_eq!(agg.to_string(), "sum(x)");
//! ```

use super::Arithmetic;
use crate::parser::error::{grammar_bug, ParseError};
use crate::parser::{span_of, Lexeme, Rule};
use crate::common::source::{FileId, Ignored, Span};
use pest::iterators::Pair;
use std::fmt;

/// Supported aggregation operators.
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
    pub fn semiring_canonical(self) -> Self {
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
pub struct Aggregation {
    operator: AggregationOperator,
    arithmetic: Arithmetic,
    span: Ignored<Span>,
}

impl Aggregation {
    /// Create a new aggregation.
    #[must_use]
    #[inline]
    pub fn new(operator: AggregationOperator, arithmetic: Arithmetic) -> Self {
        Self {
            operator,
            arithmetic,
            span: Ignored(Span::DUMMY),
        }
    }

    /// Source location this aggregation was parsed from.
    #[must_use]
    #[inline]
    pub fn span(&self) -> Span {
        self.span.0
    }

    /// Variables referenced by the arithmetic expression.
    #[must_use]
    pub fn vars(&self) -> Vec<&String> {
        self.arithmetic.vars()
    }

    /// Underlying arithmetic expression.
    #[must_use]
    #[inline]
    pub fn arithmetic(&self) -> &Arithmetic {
        &self.arithmetic
    }

    #[inline]
    pub fn arithmetic_mut(&mut self) -> &mut Arithmetic {
        &mut self.arithmetic
    }

    /// Aggregation operator.
    #[must_use]
    #[inline]
    pub fn operator(&self) -> &AggregationOperator {
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
    use crate::parser::logic::{ArithmeticOperator, Factor};
    use crate::parser::primitive::ConstType;
    use AggregationOperator::*;

    #[test]
    fn operator_display() {
        assert_eq!(Min.to_string(), "min");
        assert_eq!(Max.to_string(), "max");
        assert_eq!(Count.to_string(), "count");
        assert_eq!(Sum.to_string(), "sum");
    }

    #[test]
    fn aggregation_display_golden() {
        let salary = Aggregation::new(Sum, Arithmetic::new(Factor::Var("salary".into()), vec![]));
        assert_eq!(salary.to_string(), "sum(salary)");

        let count1 = Aggregation::new(
            Count,
            Arithmetic::new(Factor::Const(ConstType::Int(1)), vec![]),
        );
        assert_eq!(count1.to_string(), "count(1)");

        let expr = Arithmetic::new(
            Factor::Var("price".into()),
            vec![(ArithmeticOperator::Multiply, Factor::Var("qty".into()))],
        );
        let maxp = Aggregation::new(Max, expr);
        assert_eq!(maxp.to_string(), "max(price * qty)");
    }

    #[test]
    fn vars_extraction() {
        let expr = Arithmetic::new(
            Factor::Var("base".into()),
            vec![
                (ArithmeticOperator::Plus, Factor::Var("bonus".into())),
                (ArithmeticOperator::Multiply, Factor::Var("rate".into())),
            ],
        );
        let agg = Aggregation::new(Sum, expr);
        let v = agg.vars();
        assert_eq!(v.len(), 3);
        assert!(v.iter().any(|s| *s == "base"));
        assert!(v.iter().any(|s| *s == "bonus"));
        assert!(v.iter().any(|s| *s == "rate"));

        let c = Aggregation::new(
            Count,
            Arithmetic::new(Factor::Const(ConstType::Int(0)), vec![]),
        );
        assert!(c.vars().is_empty());
    }
}
