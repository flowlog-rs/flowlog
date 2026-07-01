//! Comparison expressions for FlowLog Datalog programs.
//!
//! - [`ComparisonOperator`]: `== | ≠ | > | ≥ | < | ≤`
//! - [`ComparisonExpr`]: `{left} {op} {right}`

use std::collections::HashSet;
use std::fmt;

use educe::Educe;
use flowlog_common::FileId;
use flowlog_common::Span;
use pest::iterators::Pair;

use super::Arithmetic;
use crate::Lexeme;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;
use crate::span_of;

/// Comparison operator. The arithmetic comparisons (`==`, `<`, …) are
/// symmetric value tests; `Match`/`Contains` are the string constraints
/// (`match(pat, s)`, `contains(sub, s)`) — binary boolean operators over two
/// string operands, with the surface `!` negation folded into the operator.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ComparisonOperator {
    Equal,            // ==
    NotEqual,         // ≠
    GreaterThan,      // >
    GreaterEqualThan, // ≥
    LessThan,         // <
    LessEqualThan,    // ≤
    /// match(pat, s).
    Match {
        negated: bool,
    },
    /// contains(sub, s).
    Contains {
        negated: bool,
    },
}

impl ComparisonOperator {
    #[must_use]
    #[inline]
    pub fn is_inequality(&self) -> bool {
        matches!(
            self,
            Self::LessThan | Self::LessEqualThan | Self::GreaterThan | Self::GreaterEqualThan
        )
    }

    /// Whether this is a string constraint (`match`/`contains`) rather than
    /// an arithmetic comparison.
    #[must_use]
    #[inline]
    pub fn is_string_constraint(&self) -> bool {
        matches!(self, Self::Match { .. } | Self::Contains { .. })
    }
}

impl fmt::Display for ComparisonOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // String constraints render in their surface call form via
        // `ComparisonExpr`'s Display; standalone we show the keyword.
        let sym = match self {
            Self::Equal => "==",
            Self::NotEqual => "≠",
            Self::GreaterThan => ">",
            Self::GreaterEqualThan => "≥",
            Self::LessThan => "<",
            Self::LessEqualThan => "≤",
            Self::Match { negated } => {
                return write!(f, "{}match", if *negated { "!" } else { "" });
            }
            Self::Contains { negated } => {
                return write!(f, "{}contains", if *negated { "!" } else { "" });
            }
        };
        write!(f, "{sym}")
    }
}

/// Map a `string_constraint_op` node (`match` | `contains`) to its
/// [`ComparisonOperator`], folding in `negated`.
fn string_constraint_op(
    node: &Pair<Rule>,
    negated: bool,
) -> Result<ComparisonOperator, ParseError> {
    let kw = node
        .clone()
        .into_inner()
        .next()
        .ok_or_else(|| grammar_bug("string_constraint_op missing keyword"))?;
    Ok(match kw.as_rule() {
        Rule::match_op => ComparisonOperator::Match { negated },
        Rule::contains_op => ComparisonOperator::Contains { negated },
        other => return Err(grammar_bug(format!("unknown string constraint: {other:?}"))),
    })
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
pub struct ComparisonExpr {
    left: Arithmetic,
    operator: ComparisonOperator,
    right: Arithmetic,
    #[educe(PartialEq(ignore), Hash(ignore))]
    span: Span,
}

impl ComparisonExpr {
    /// Build a comparison directly.
    #[must_use]
    pub fn new(
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

    /// Parse a `string_constraint` node (`match`/`contains`, optionally `!`-negated)
    /// into a comparison whose operator is [`ComparisonOperator::Match`] /
    /// [`ComparisonOperator::Contains`]. `left` is the first argument (pattern /
    /// substring), `right` the subject string.
    pub fn from_string_constraint(
        parsed_rule: Pair<Rule>,
        file: FileId,
    ) -> Result<Self, ParseError> {
        if parsed_rule.as_rule() != Rule::string_constraint {
            return Err(grammar_bug(format!(
                "expected string_constraint, got {:?}",
                parsed_rule.as_rule()
            )));
        }
        let span = span_of(&parsed_rule, file);

        // Grammar is `not_op? ~ string_constraint_op ~ …`, so `not_op` (if any)
        // precedes the operator — `negated` is known when we reach it.
        let mut negated = false;
        let mut operator = None;
        let mut args: Vec<Arithmetic> = Vec::with_capacity(2);
        for node in parsed_rule.into_inner() {
            match node.as_rule() {
                Rule::not_op => negated = true,
                Rule::string_constraint_op => {
                    operator = Some(string_constraint_op(&node, negated)?)
                }
                Rule::arithmetic_expr => args.push(Arithmetic::from_parsed_rule(node, file)?),
                other => {
                    return Err(grammar_bug(format!(
                        "unexpected node in string_constraint: {other:?}"
                    )));
                }
            }
        }
        let operator = operator.ok_or_else(|| grammar_bug("string_constraint missing operator"))?;
        let mut args = args.into_iter();
        let left = args
            .next()
            .ok_or_else(|| grammar_bug("string_constraint missing first argument"))?;
        let right = args
            .next()
            .ok_or_else(|| grammar_bug("string_constraint missing second argument"))?;
        Ok(Self::new(left, operator, right, span))
    }

    /// Source location this comparison was parsed from.
    #[must_use]
    #[inline]
    pub fn span(&self) -> Span {
        self.span
    }

    /// Left-hand expression.
    #[must_use]
    #[inline]
    pub fn left(&self) -> &Arithmetic {
        &self.left
    }

    /// Operator.
    #[must_use]
    #[inline]
    pub fn operator(&self) -> &ComparisonOperator {
        &self.operator
    }

    /// Right-hand expression.
    #[must_use]
    #[inline]
    pub fn right(&self) -> &Arithmetic {
        &self.right
    }

    #[inline]
    pub fn left_mut(&mut self) -> &mut Arithmetic {
        &mut self.left
    }

    #[inline]
    pub fn right_mut(&mut self) -> &mut Arithmetic {
        &mut self.right
    }

    /// Unique variables referenced on either side (deduplicated).
    #[must_use]
    pub fn vars_set(&self) -> HashSet<&String> {
        let mut vars = self.left.vars_set();
        vars.extend(self.right.vars_set());
        vars
    }
}

impl fmt::Display for ComparisonExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.operator.is_string_constraint() {
            write!(f, "{}({}, {})", self.operator, self.left, self.right)
        } else {
            write!(f, "{} {} {}", self.left, self.operator, self.right)
        }
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
