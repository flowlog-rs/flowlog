//! Comparison expressions for FlowLog Datalog programs.
//!
//! - [`ComparisonOperator`]: `== | != | > | >= | < | <=`
//! - [`ComparisonExpr`]: `{left} {op} {right}`

use std::collections::HashSet;
use std::fmt;

use educe::Educe;
use flowlog_common::Span;

use super::Arithmetic;
use crate::Lexeme;
use crate::Node;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;

/// Comparison operator. The arithmetic comparisons (`==`, `<`, ...) are
/// symmetric value tests; `Match`/`Contains` are the string constraints
/// (`match(pat, s)`, `contains(sub, s)`): binary boolean operators over two
/// string operands, with the surface `!` negation folded into the operator.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ComparisonOperator {
    Equal,            // ==
    NotEqual,         // !=
    GreaterThan,      // >
    GreaterEqualThan, // >=
    LessThan,         // <
    LessEqualThan,    // <=
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
    pub fn is_ordering(&self) -> bool {
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
            Self::NotEqual => "!=",
            Self::GreaterThan => ">",
            Self::GreaterEqualThan => ">=",
            Self::LessThan => "<",
            Self::LessEqualThan => "<=",
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
fn string_constraint_op(node: Node, negated: bool) -> Result<ComparisonOperator, ParseError> {
    let kw = node.children().next_any("constraint keyword")?;
    Ok(match kw.rule() {
        Rule::match_op => ComparisonOperator::Match { negated },
        Rule::contains_op => ComparisonOperator::Contains { negated },
        other => return Err(grammar_bug(format!("unknown string constraint: {other:?}"))),
    })
}

impl Lexeme for ComparisonOperator {
    /// Parse a comparison operator from the grammar.
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let op = node.children().next_any("operator symbol")?;
        Ok(match op.rule() {
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
    pub(crate) fn from_string_constraint(node: Node) -> Result<Self, ParseError> {
        let span = node.span();

        // Grammar is `not_op? ~ string_constraint_op ~ ...`, so `not_op` (if any)
        // precedes the operator, so `negated` is known when we reach it.
        let mut negated = false;
        let mut operator = None;
        let mut args: Vec<Arithmetic> = Vec::with_capacity(2);
        for child in node.children() {
            match child.rule() {
                Rule::not_op => negated = true,
                Rule::string_constraint_op => {
                    operator = Some(string_constraint_op(child, negated)?)
                }
                Rule::arithmetic_expr => args.push(child.lower::<Arithmetic>()?),
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
    pub(crate) fn left_mut(&mut self) -> &mut Arithmetic {
        &mut self.left
    }

    #[inline]
    pub(crate) fn right_mut(&mut self) -> &mut Arithmetic {
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
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let span = node.span();
        let mut children = node.children();
        let left = children.lower_next::<Arithmetic>("left operand")?;
        let operator = children.lower_next::<ComparisonOperator>("operator")?;
        let right = children.lower_next::<Arithmetic>("right operand")?;
        Ok(Self {
            left,
            operator,
            right,
            span,
        })
    }
}

#[cfg(test)]
mod tests {
    use flowlog_common::FileId;
    use rstest::rstest;

    use super::*;
    use crate::test_util::parse_node;
    use crate::test_util::parse_pair;

    #[rstest]
    #[case::equal(ComparisonOperator::Equal, false, false)]
    #[case::not_equal(ComparisonOperator::NotEqual, false, false)]
    #[case::less(ComparisonOperator::LessThan, true, false)]
    #[case::less_equal(ComparisonOperator::LessEqualThan, true, false)]
    #[case::greater(ComparisonOperator::GreaterThan, true, false)]
    #[case::greater_equal(ComparisonOperator::GreaterEqualThan, true, false)]
    #[case::match_op(ComparisonOperator::Match { negated: false }, false, true)]
    #[case::contains(ComparisonOperator::Contains { negated: true }, false, true)]
    fn operator_classification(
        #[case] op: ComparisonOperator,
        #[case] is_ordering: bool,
        #[case] is_string_constraint: bool,
    ) {
        assert_eq!(op.is_ordering(), is_ordering);
        assert_eq!(op.is_string_constraint(), is_string_constraint);
    }

    #[rstest]
    #[case::equal("x = y", ComparisonOperator::Equal)]
    #[case::not_equal("x != y", ComparisonOperator::NotEqual)]
    #[case::less("x < y", ComparisonOperator::LessThan)]
    #[case::less_equal("x <= y", ComparisonOperator::LessEqualThan)]
    #[case::greater("x > y", ComparisonOperator::GreaterThan)]
    #[case::greater_equal("x >= y", ComparisonOperator::GreaterEqualThan)]
    fn compare_expr_parses_its_operator(#[case] src: &str, #[case] op: ComparisonOperator) {
        assert_eq!(
            parse_node::<ComparisonExpr>(Rule::compare_expr, src).operator(),
            &op
        );
    }

    #[test]
    fn string_constraint_folds_negation_into_the_operator() {
        let m = ComparisonExpr::from_string_constraint(Node::new(
            parse_pair(Rule::string_constraint, "match(\"a\", x)"),
            FileId::new(0),
        ))
        .unwrap();
        assert_eq!(m.operator(), &ComparisonOperator::Match { negated: false });

        let c = ComparisonExpr::from_string_constraint(Node::new(
            parse_pair(Rule::string_constraint, "!contains(\"a\", x)"),
            FileId::new(0),
        ))
        .unwrap();
        assert_eq!(
            c.operator(),
            &ComparisonOperator::Contains { negated: true }
        );
    }
}
