//! Comparison expressions for FlowLog Datalog programs.
//!
//! - [`ComparisonOperator`]: `== | ≠ | > | ≥ | < | ≤`
//! - [`ComparisonExpr`]: `{left} {op} {right}`

use std::collections::HashSet;
use std::fmt;

use educe::Educe;
use pest::iterators::Pair;

use super::Arithmetic;
use crate::parser::error::{ParseError, grammar_bug};
use crate::parser::{Lexeme, Rule, span_of};
use flowlog_common::{FileId, Span};

/// Comparison operator.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ComparisonOperator {
    Equal,            // ==
    NotEqual,         // ≠
    GreaterThan,      // >
    GreaterEqualThan, // ≥
    LessThan,         // <
    LessEqualThan,    // ≤
}

impl ComparisonOperator {
    #[must_use]
    #[inline]
    pub(crate) fn is_inequality(&self) -> bool {
        matches!(
            self,
            Self::LessThan | Self::LessEqualThan | Self::GreaterThan | Self::GreaterEqualThan
        )
    }
}

impl fmt::Display for ComparisonOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sym = match self {
            Self::Equal => "==",
            Self::NotEqual => "≠",
            Self::GreaterThan => ">",
            Self::GreaterEqualThan => "≥",
            Self::LessThan => "<",
            Self::LessEqualThan => "≤",
        };
        write!(f, "{sym}")
    }
}

impl Lexeme for ComparisonOperator {
    /// Parse a comparison operator from the grammar.
    fn from_parsed_rule(parsed_rule: Pair<Rule>, _file: FileId) -> Result<Self, ParseError> {
        let op = parsed_rule
            .into_inner()
            .next()
            .ok_or_else(|| grammar_bug("comparison operator missing inner token"))?;
        Ok(match op.as_rule() {
            Rule::equal => Self::Equal,
            Rule::not_equal => Self::NotEqual,
            Rule::greater_than => Self::GreaterThan,
            Rule::greater_equal_than => Self::GreaterEqualThan,
            Rule::less_than => Self::LessThan,
            Rule::less_equal_than => Self::LessEqualThan,
            other => {
                return Err(grammar_bug(format!(
                    "unknown comparison operator: {other:?}"
                )));
            }
        })
    }
}

/// `{left} {op} {right}` boolean comparison.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub(crate) struct ComparisonExpr {
    left: Arithmetic,
    operator: ComparisonOperator,
    right: Arithmetic,
    #[educe(PartialEq(ignore), Hash(ignore))]
    span: Span,
}

impl ComparisonExpr {
    /// Build a comparison directly.
    #[must_use]
    pub(crate) fn new(
        left: Arithmetic,
        operator: ComparisonOperator,
        right: Arithmetic,
        span: Span,
    ) -> Self {
        Self {
            left,
            operator,
            right,
            span,
        }
    }

    /// Source location this comparison was parsed from.
    #[must_use]
    #[inline]
    pub(crate) fn span(&self) -> Span {
        self.span
    }

    /// Left-hand expression.
    #[must_use]
    #[inline]
    pub(crate) fn left(&self) -> &Arithmetic {
        &self.left
    }

    /// Operator.
    #[must_use]
    #[inline]
    pub(crate) fn operator(&self) -> &ComparisonOperator {
        &self.operator
    }

    /// Right-hand expression.
    #[must_use]
    #[inline]
    pub(crate) fn right(&self) -> &Arithmetic {
        &self.right
    }

    #[inline]
    pub(crate) fn left_mut(&mut self) -> &mut Arithmetic {
        &mut self.left
    }

    #[inline]
    pub(crate) fn right_mut(&mut self) -> &mut Arithmetic {
        &mut self.right
    }

    /// Unique variables referenced on either side (deduplicated).
    #[must_use]
    pub(crate) fn vars_set(&self) -> HashSet<&String> {
        let mut vars = self.left.vars_set();
        vars.extend(self.right.vars_set());
        vars
    }
}

impl fmt::Display for ComparisonExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.operator, self.right)
    }
}

impl Lexeme for ComparisonExpr {
    /// Parse `arithmetic ~ comparison_operator ~ arithmetic`.
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        let span = span_of(&parsed_rule, file);
        let mut inner = parsed_rule.into_inner();

        let left_pair = inner
            .next()
            .ok_or_else(|| grammar_bug("comparison missing left expression"))?;
        let op_pair = inner
            .next()
            .ok_or_else(|| grammar_bug("comparison missing operator"))?;
        let right_pair = inner
            .next()
            .ok_or_else(|| grammar_bug("comparison missing right expression"))?;

        let left = Arithmetic::from_parsed_rule(left_pair, file)?;
        let operator = ComparisonOperator::from_parsed_rule(op_pair, file)?;
        let right = Arithmetic::from_parsed_rule(right_pair, file)?;

        Ok(Self {
            left,
            operator,
            right,
            span,
        })
    }
}
