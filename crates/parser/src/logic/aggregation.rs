//! Aggregation expressions for FlowLog Datalog programs.
//!
//! - [`AggregationOperator`]: `min | max | count | sum`
//! - [`Aggregation`]: `op(expr)` (e.g., `sum(price * qty)`)
//!
//! # Example
//! ```rust
//! use parser::logic::{Aggregation, AggregationOperator, Arithmetic, Factor};
//! let expr = Arithmetic::new(Factor::Var("x".into()), vec![]);
//! let agg = Aggregation::new(AggregationOperator::Sum, expr);
//! assert_eq!(agg.to_string(), "sum(x)");
//! ```

use super::Arithmetic;
use crate::{Lexeme, Rule};
use pest::iterators::Pair;
use std::fmt;

/// Supported aggregation operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregationOperator {
    Min,
    Max,
    Count,
    Sum,
}

impl fmt::Display for AggregationOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Min => write!(f, "min"),
            Self::Max => write!(f, "max"),
            Self::Count => write!(f, "count"),
            Self::Sum => write!(f, "sum"),
        }
    }
}

impl Lexeme for AggregationOperator {
    /// Parse an aggregation operator from the grammar.
    ///
    /// # Panics
    /// Panics if the rule is not one of `min|max|count|sum`.
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let op = parsed_rule
            .into_inner()
            .next()
            .expect("Parser error: aggregation operator missing inner token");

        match op.as_rule() {
            Rule::min => Self::Min,
            Rule::max => Self::Max,
            Rule::count => Self::Count,
            Rule::sum => Self::Sum,
            other => panic!(
                "Parser error: unexpected aggregation operator rule: {:?}",
                other
            ),
        }
    }
}

/// `op(expr)` aggregation (e.g., `sum(price * qty)`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Aggregation {
    operator: AggregationOperator,
    arithmetic: Arithmetic,
}

impl Aggregation {
    /// Create a new aggregation.
    #[must_use]
    #[inline]
    pub fn new(operator: AggregationOperator, arithmetic: Arithmetic) -> Self {
        Self {
            operator,
            arithmetic,
        }
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
    ///
    /// # Panics
    /// Panics if the inner structure is malformed (missing operator or expression).
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let mut inner = parsed_rule.into_inner();

        let op_pair = inner
            .next()
            .expect("Parser error: aggregation missing operator");
        let operator = AggregationOperator::from_parsed_rule(op_pair);

        let expr_pair = inner
            .next()
            .expect("Parser error: aggregation missing arithmetic expression");
        let arithmetic = Arithmetic::from_parsed_rule(expr_pair);

        Self::new(operator, arithmetic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logic::{ArithmeticOperator, Factor};
    use crate::primitive::ConstType;
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
            Arithmetic::new(Factor::Const(ConstType::Int32(1)), vec![]),
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
            Arithmetic::new(Factor::Const(ConstType::Int32(0)), vec![]),
        );
        assert!(c.vars().is_empty());
    }
}
