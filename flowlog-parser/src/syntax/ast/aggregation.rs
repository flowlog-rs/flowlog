//! Aggregation expressions for FlowLog Datalog programs.
//!
//! - [`AggregationOperator`]: `min | max | count | sum | average`
//! - [`Aggregation`]: `op(expr)` (e.g., `sum(price * qty)`)

use std::fmt;

use educe::Educe;
use flowlog_common::Span;

use super::Arithmetic;
use crate::Lexeme;
use crate::Node;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;

/// Supported aggregation operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregationOperator {
    Min,
    Max,
    Count,
    Sum,
    Avg,
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
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let op = node.children().next_any("operator keyword")?;
        Ok(match op.rule() {
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
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct Aggregation {
    operator: AggregationOperator,
    arithmetic: Arithmetic,
    #[educe(PartialEq(ignore), Hash(ignore))]
    span: Span,
}

impl Aggregation {
    pub(super) fn new(operator: AggregationOperator, arithmetic: Arithmetic, span: Span) -> Self {
        Self {
            operator,
            arithmetic,
            span,
        }
    }

    /// Source location this aggregation was parsed from.
    #[must_use]
    #[inline]
    pub fn span(&self) -> Span {
        self.span
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
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let span = node.span();
        let mut children = node.children();
        let operator = children.lower_next("aggregate operator")?;
        let arithmetic = children.lower_next("aggregate operand")?;
        Ok(Self::new(operator, arithmetic, span))
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::test_util::parse_node;

    #[rstest]
    #[case("min", AggregationOperator::Min)]
    #[case("MIN", AggregationOperator::Min)]
    #[case("max", AggregationOperator::Max)]
    #[case("MAX", AggregationOperator::Max)]
    #[case("count", AggregationOperator::Count)]
    #[case("COUNT", AggregationOperator::Count)]
    #[case("sum", AggregationOperator::Sum)]
    #[case("SUM", AggregationOperator::Sum)]
    #[case("average", AggregationOperator::Avg)]
    #[case("AVG", AggregationOperator::Avg)]
    fn aggregate_operator_spellings_lower_to_their_variant(
        #[case] source: &str,
        #[case] expected: AggregationOperator,
    ) {
        assert_eq!(
            parse_node::<AggregationOperator>(Rule::aggregate_op, source),
            expected
        );
    }

    #[test]
    fn aggregate_preserves_operator_and_operand() {
        let agg: Aggregation = parse_node(Rule::aggregate_expr, "sum(price * qty)");
        assert_eq!(*agg.operator(), AggregationOperator::Sum);
        assert_eq!(agg.arithmetic().to_string(), "price * qty");
        assert_eq!(agg.vars(), vec!["price", "qty"]);
    }

    #[test]
    fn aggregate_preserves_source_spans() {
        let agg: Aggregation = parse_node(Rule::aggregate_expr, "sum(price * qty)");
        assert_eq!((agg.span().start(), agg.span().end()), (0, 16));
        let operand_span = agg.arithmetic().span();
        assert_eq!((operand_span.start(), operand_span.end()), (4, 15));
    }

    /// `Display` for `AggregationOperator` renders the surface keyword:
    /// `Count` prints `count` and `Avg` prints `average`. Pins the exact
    /// strings so a no-op `fmt` (empty output) is caught.
    #[test]
    fn operator_display_renders_surface_keyword() {
        let cases = [
            (AggregationOperator::Min, "min"),
            (AggregationOperator::Max, "max"),
            (AggregationOperator::Count, "count"),
            (AggregationOperator::Sum, "sum"),
            (AggregationOperator::Avg, "average"),
        ];
        for (op, expected) in cases {
            assert_eq!(op.to_string(), expected, "{op:?}");
        }
    }

    /// `Display` for `Aggregation` renders `op(expr)`. Pins the exact string
    /// so a no-op `fmt` (empty output) is caught.
    #[test]
    fn aggregation_display_renders_op_and_expr() {
        let agg = Aggregation::new(AggregationOperator::Sum, Arithmetic::var("x"), Span::DUMMY);
        assert_eq!(agg.to_string(), "sum(x)");
    }
}
