//! Aggregation expressions for FlowLog Datalog programs.
//!
//! - [`AggregationOperator`]: `min | max | count | sum | average`
//! - [`Aggregation`]: `op(expr)` (e.g., `sum(price * qty)`)

use std::fmt;

use educe::Educe;
use flowlog_error::Span;

use super::Arithmetic;
use crate::Lexeme;
use crate::Node;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;

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
    /// Normalize Count to Sum (they share the same semiring).
    #[must_use]
    pub fn semiring_canonical(self) -> Self {
        match self {
            Self::Count => Self::Sum,
            other => other,
        }
    }

    /// Lowercase name for semiring module paths (e.g. `"min"`, `"sum"`).
    ///
    /// `Count` shares its semiring with `Sum`.
    #[must_use]
    pub fn semiring_mod(self) -> &'static str {
        match self {
            Self::Min => "min",
            Self::Max => "max",
            Self::Sum | Self::Count => "sum",
            Self::Avg => "avg",
        }
    }

    /// Title-case prefix for semiring type names (e.g. `"Min"`, `"Sum"`).
    ///
    /// `Count` shares its semiring with `Sum`.
    #[must_use]
    pub fn semiring_prefix(self) -> &'static str {
        match self {
            Self::Min => "Min",
            Self::Max => "Max",
            Self::Sum | Self::Count => "Sum",
            Self::Avg => "Avg",
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
    #[cfg(test)]
    pub fn new(operator: AggregationOperator, arithmetic: Arithmetic) -> Self {
        Self {
            operator,
            arithmetic,
            span: Span::DUMMY,
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
    /// Parse an aggregation from the grammar.
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let span = node.span();
        let mut children = node.children();
        let operator = children.lower_next::<AggregationOperator>("operator")?;
        let arithmetic = children.lower_next::<Arithmetic>("arithmetic expression")?;
        Ok(Self {
            operator,
            arithmetic,
            span,
        })
    }
}

#[cfg(test)]
mod tests {
    use flowlog_error::FileId;
    use pest::Parser;

    use super::*;
    use crate::FlowLogParser;
    use crate::Lexeme;
    use crate::Rule;

    #[test]
    fn parse_aggregate_expr() {
        let mut pairs = FlowLogParser::parse(Rule::aggregate_expr, "sum(price * qty)").unwrap();
        let agg = Aggregation::from_parsed_rule(Node::new(pairs.next().unwrap(), FileId::new(0)))
            .unwrap();
        assert_eq!(*agg.operator(), AggregationOperator::Sum);
        assert_eq!(agg.vars().len(), 2);
    }

    /// `semiring_mod` maps every operator to its lowercase semiring module
    /// name, with `Count` sharing `Sum`'s. Pins the exact strings so a body
    /// replaced by a constant (`""` / `"xyzzy"`) is caught.
    #[test]
    fn semiring_mod_maps_every_operator() {
        let cases = [
            (AggregationOperator::Min, "min"),
            (AggregationOperator::Max, "max"),
            (AggregationOperator::Count, "sum"),
            (AggregationOperator::Sum, "sum"),
            (AggregationOperator::Avg, "avg"),
        ];
        for (op, expected) in cases {
            assert_eq!(op.semiring_mod(), expected, "{op:?}");
        }
    }

    /// `semiring_prefix` maps every operator to its title-case semiring type
    /// prefix, with `Count` sharing `Sum`'s. Pins the exact strings so a body
    /// replaced by a constant is caught.
    #[test]
    fn semiring_prefix_maps_every_operator() {
        let cases = [
            (AggregationOperator::Min, "Min"),
            (AggregationOperator::Max, "Max"),
            (AggregationOperator::Count, "Sum"),
            (AggregationOperator::Sum, "Sum"),
            (AggregationOperator::Avg, "Avg"),
        ];
        for (op, expected) in cases {
            assert_eq!(op.semiring_prefix(), expected, "{op:?}");
        }
    }

    /// `Display` for `AggregationOperator` renders the surface keyword:
    /// `Count` prints `count` and `Avg` prints `average`, distinct from the
    /// semiring names.
    /// Pins the exact strings so a no-op `fmt` (empty output) is caught.
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
        let agg = Aggregation::new(AggregationOperator::Sum, Arithmetic::var("x"));
        assert_eq!(agg.to_string(), "sum(x)");
    }
}
