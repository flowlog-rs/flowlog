//! Value comparisons and string constraints, including their source locations.
//!
//! [`ComparisonOperator`] identifies the operation and string negation;
//! [`ComparisonExpr`] owns both operands and their surface rendering.

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

// =============================================================================
// ComparisonOperator
// =============================================================================

/// Equality, ordering, or a string constraint over two operands.
/// String constraints carry their own negation; value comparisons do not
/// introduce a unary `!` operator.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ComparisonOperator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterEqualThan,
    LessThan,
    LessEqualThan,
    /// Tests the right operand against the left operand's regular expression.
    Match {
        negated: bool,
    },
    /// Tests whether the right operand contains the left operand's substring.
    Contains {
        negated: bool,
    },
}

impl ComparisonOperator {
    /// Returns `true` for strict or inclusive ordering comparisons.
    #[must_use]
    #[inline]
    pub fn is_ordering(&self) -> bool {
        match self {
            Self::LessThan | Self::LessEqualThan | Self::GreaterThan | Self::GreaterEqualThan => {
                true
            }
            Self::Equal | Self::NotEqual | Self::Match { .. } | Self::Contains { .. } => false,
        }
    }

    /// Returns `true` for a string constraint with either polarity.
    #[must_use]
    #[inline]
    pub fn is_string_constraint(&self) -> bool {
        match self {
            Self::Match { .. } | Self::Contains { .. } => true,
            Self::Equal
            | Self::NotEqual
            | Self::LessThan
            | Self::LessEqualThan
            | Self::GreaterThan
            | Self::GreaterEqualThan => false,
        }
    }

    /// Parses a `string_constraint_op`, including the enclosing constraint's
    /// negation.
    fn from_string_constraint(node: Node, negated: bool) -> Result<Self, ParseError> {
        let keyword = node.children().next_any("string constraint keyword")?;
        Ok(match keyword.rule() {
            Rule::match_op => Self::Match { negated },
            Rule::contains_op => Self::Contains { negated },
            other => return Err(grammar_bug(format!("unknown string constraint: {other:?}"))),
        })
    }
}

impl fmt::Display for ComparisonOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::GreaterThan => ">",
            Self::GreaterEqualThan => ">=",
            Self::LessThan => "<",
            Self::LessEqualThan => "<=",
            Self::Match { negated: false } => "match",
            Self::Match { negated: true } => "!match",
            Self::Contains { negated: false } => "contains",
            Self::Contains { negated: true } => "!contains",
        })
    }
}

impl Lexeme for ComparisonOperator {
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        if node.rule() == Rule::string_constraint_op {
            return Self::from_string_constraint(node, false);
        }
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

// =============================================================================
// ComparisonExpr
// =============================================================================

/// An infix value comparison or a string constraint in call notation.
/// Equality and hashing ignore source locations.
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
    /// Recognizes a bare `call_expr` as a string constraint. Returns `None`
    /// for another name, a different arity, or a placeholder argument.
    /// Once recognized, invalid value operands produce a parse error.
    pub(super) fn from_parenthesized_call(node: Node) -> Result<Option<Self>, ParseError> {
        let span = node.span();
        let mut children = node.children();
        let name = children.next_any("call name")?;
        if name.rule() != Rule::string_constraint_op {
            return Ok(None);
        }
        let operator = name.lower()?;
        let (Some(left), Some(right)) = (children.next(), children.next()) else {
            return Ok(None);
        };
        if children.next().is_some()
            || left.rule() == Rule::placeholder
            || right.rule() == Rule::placeholder
        {
            return Ok(None);
        }
        Ok(Some(Self::new(
            left.lower()?,
            operator,
            right.lower()?,
            span,
        )))
    }

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
    pub fn span(&self) -> Span {
        self.span
    }

    #[must_use]
    #[inline]
    pub fn left(&self) -> &Arithmetic {
        &self.left
    }

    #[must_use]
    #[inline]
    pub fn operator(&self) -> &ComparisonOperator {
        &self.operator
    }

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

    /// Unique variables referenced across both operands.
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
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let span = node.span();
        match node.rule() {
            Rule::compare_expr | Rule::paren_item => {
                // A comparison inside shared parentheses has the same three
                // children as `compare_expr`; only its enclosing rule differs.
                let mut children = node.children();
                let left = children.lower_next("left operand")?;
                let operator = children.lower_next("comparison operator")?;
                let right = children.lower_next("right operand")?;
                Ok(Self::new(left, operator, right, span))
            }
            Rule::string_constraint | Rule::negative_string_constraint => {
                let negated = node.rule() == Rule::negative_string_constraint;
                let mut children = node.children();
                if negated {
                    children.require(Rule::not_op)?;
                    children = children.require(Rule::string_constraint)?.children();
                }
                let operator = ComparisonOperator::from_string_constraint(
                    children.require(Rule::string_constraint_op)?,
                    negated,
                )?;
                let left = children.lower_next("left operand")?;
                let right = children.lower_next("right operand")?;
                Ok(Self::new(left, operator, right, span))
            }
            other => Err(grammar_bug(format!("invalid comparison rule: {other:?}"))),
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use flowlog_common::FileId;
    use pest::Parser as _;
    use pest::error::ErrorVariant;
    use rstest::rstest;

    use super::*;
    use crate::FlowLogParser;
    use crate::assert_err;
    use crate::test_util::parse_node;
    use crate::test_util::parse_pair;

    #[rstest]
    #[case("match(")]
    #[case("contains(")]
    fn nested_string_constraint_syntax_is_fully_consumed(#[case] open: &str) {
        let source = format!("{}x{}", open.repeat(256), ", y)".repeat(256));
        let pairs = FlowLogParser::parse(Rule::string_constraint, &source).unwrap();
        assert_eq!(pairs.as_str(), source);
    }

    #[rstest]
    #[case("match(")]
    #[case("contains(")]
    fn unclosed_nested_string_constraints_are_rejected(#[case] open: &str) {
        let source = format!("{}x", open.repeat(256));
        let err = FlowLogParser::parse(Rule::string_constraint, &source).unwrap_err();
        assert!(matches!(err.variant, ErrorVariant::ParsingError { .. }));
    }

    #[rstest]
    #[case("match(cfg.Type, x)")]
    #[case("contains(x, cfg.Type)")]
    #[case("match((field: number), x)")]
    fn string_constraint_syntax_rejects_type_operands(#[case] source: &str) {
        let err = FlowLogParser::parse(Rule::string_constraint, source).unwrap_err();
        assert!(matches!(err.variant, ErrorVariant::ParsingError { .. }));
    }

    #[rstest]
    #[case("match((x > 0), y)", "x > 0")]
    #[case("contains(x, (R(x); S(x)))", "(R(x); S(x))")]
    fn bare_string_constraints_reject_condition_operands(
        #[case] source: &str,
        #[case] invalid: &str,
    ) {
        let node = Node::new(parse_pair(Rule::call_expr, source), FileId::new(0));
        assert_err!(
            ComparisonExpr::from_parenthesized_call(node),
            ParseError::Syntax { span, .. } if &source[span.range()] == invalid
        );
    }

    #[rstest]
    #[case::equal(ComparisonOperator::Equal, false, false)]
    #[case::not_equal(ComparisonOperator::NotEqual, false, false)]
    #[case::less(ComparisonOperator::LessThan, true, false)]
    #[case::less_equal(ComparisonOperator::LessEqualThan, true, false)]
    #[case::greater(ComparisonOperator::GreaterThan, true, false)]
    #[case::greater_equal(ComparisonOperator::GreaterEqualThan, true, false)]
    #[case::match_op(ComparisonOperator::Match { negated: false }, false, true)]
    #[case::negated_match(ComparisonOperator::Match { negated: true }, false, true)]
    #[case::contains(ComparisonOperator::Contains { negated: false }, false, true)]
    #[case::negated_contains(ComparisonOperator::Contains { negated: true }, false, true)]
    fn operators_distinguish_ordering_and_string_constraints(
        #[case] op: ComparisonOperator,
        #[case] is_ordering: bool,
        #[case] is_string_constraint: bool,
    ) {
        assert_eq!(op.is_ordering(), is_ordering);
        assert_eq!(op.is_string_constraint(), is_string_constraint);
    }

    #[rstest]
    #[case(Rule::compare_op, "=", ComparisonOperator::Equal)]
    #[case(Rule::compare_op, "!=", ComparisonOperator::NotEqual)]
    #[case(Rule::compare_op, "<", ComparisonOperator::LessThan)]
    #[case(Rule::compare_op, "<=", ComparisonOperator::LessEqualThan)]
    #[case(Rule::compare_op, ">", ComparisonOperator::GreaterThan)]
    #[case(Rule::compare_op, ">=", ComparisonOperator::GreaterEqualThan)]
    #[case(Rule::string_constraint_op, "match", ComparisonOperator::Match { negated: false })]
    #[case(Rule::string_constraint_op, "contains", ComparisonOperator::Contains { negated: false })]
    fn operator_spellings_round_trip(
        #[case] start_rule: Rule,
        #[case] src: &str,
        #[case] expected: ComparisonOperator,
    ) {
        let operator: ComparisonOperator = parse_node(start_rule, src);
        assert_eq!(operator, expected);
        assert_eq!(operator.to_string(), src);
        assert_eq!(
            parse_node::<ComparisonOperator>(start_rule, &operator.to_string()),
            operator
        );
    }

    #[rstest]
    #[case(Rule::compare_expr, "x = y")]
    #[case(Rule::compare_expr, "x != y")]
    #[case(Rule::compare_expr, "x < y")]
    #[case(Rule::compare_expr, "x <= y")]
    #[case(Rule::compare_expr, "x > y")]
    #[case(Rule::compare_expr, "x >= y")]
    #[case(Rule::string_constraint, "match(x, y)")]
    #[case(Rule::string_constraint, "contains(x, y)")]
    #[case(Rule::negative_string_constraint, "!match(x, y)")]
    #[case(Rule::negative_string_constraint, "!contains(x, y)")]
    fn comparisons_round_trip_in_source_notation(#[case] start_rule: Rule, #[case] src: &str) {
        let expr: ComparisonExpr = parse_node(start_rule, src);
        assert_eq!(expr.to_string(), src);
        assert_eq!(
            parse_node::<ComparisonExpr>(start_rule, &expr.to_string()),
            expr
        );
    }

    #[rstest]
    fn comparisons_preserve_operand_order_and_spans(
        #[values(Rule::compare_expr, Rule::paren_item)] start_rule: Rule,
    ) {
        let source = "x + x > y";
        let expr: ComparisonExpr = parse_node(start_rule, source);
        assert_eq!(&source[expr.span().range()], source);
        assert_eq!(&source[expr.left().span().range()], "x + x");
        assert_eq!(&source[expr.right().span().range()], "y");
        assert_eq!(expr.operator(), &ComparisonOperator::GreaterThan);
    }

    #[rstest]
    #[case(Rule::string_constraint, "match(x, y)", ComparisonOperator::Match { negated: false })]
    #[case(Rule::string_constraint, "contains(x, y)", ComparisonOperator::Contains { negated: false })]
    #[case(Rule::negative_string_constraint, "!match(x, y)", ComparisonOperator::Match { negated: true })]
    #[case(Rule::negative_string_constraint, "!contains(x, y)", ComparisonOperator::Contains { negated: true })]
    fn string_constraints_preserve_negation_and_source_spans(
        #[case] start_rule: Rule,
        #[case] source: &str,
        #[case] expected: ComparisonOperator,
    ) {
        let expr: ComparisonExpr = parse_node(start_rule, source);
        assert_eq!(expr.operator(), &expected);
        assert_eq!(&source[expr.span().range()], source);
        assert_eq!(&source[expr.left().span().range()], "x");
        assert_eq!(&source[expr.right().span().range()], "y");
    }

    #[rstest]
    #[case("match(x, y)", ComparisonOperator::Match { negated: false })]
    #[case("contains(x, y)", ComparisonOperator::Contains { negated: false })]
    fn bare_calls_recognize_string_constraints(
        #[case] source: &str,
        #[case] expected: ComparisonOperator,
    ) {
        let node = Node::new(parse_pair(Rule::call_expr, source), FileId::new(0));
        let expr = ComparisonExpr::from_parenthesized_call(node)
            .unwrap()
            .unwrap();
        assert_eq!(expr.operator(), &expected);
        assert_eq!(expr.to_string(), source);
        assert_eq!(&source[expr.span().range()], source);
    }

    #[rstest]
    #[case("f(x, y)")]
    #[case("Match(x, y)")]
    #[case("match.Edge(x, y)")]
    #[case("match()")]
    #[case("contains(x)")]
    #[case("match(x, y, z)")]
    #[case("match(_, y)")]
    #[case("contains(x, _)")]
    fn other_bare_calls_remain_available_as_relations(#[case] source: &str) {
        let node = Node::new(parse_pair(Rule::call_expr, source), FileId::new(0));
        assert!(
            ComparisonExpr::from_parenthesized_call(node)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn vars_set_deduplicates_across_both_operands() {
        let expr: ComparisonExpr = parse_node(Rule::compare_expr, "x + x > x + y");
        let vars: HashSet<_> = expr.vars_set().into_iter().map(String::as_str).collect();
        assert_eq!(vars, HashSet::from(["x", "y"]));
    }

    #[test]
    fn comparison_identity_ignores_source_locations() {
        let first = ComparisonExpr::from_parsed_rule(Node::new(
            parse_pair(Rule::compare_expr, "x > y"),
            FileId::new(0),
        ))
        .unwrap();
        let second = ComparisonExpr::from_parsed_rule(Node::new(
            parse_pair(Rule::compare_expr, "x > y"),
            FileId::new(1),
        ))
        .unwrap();
        assert_ne!(first.span(), second.span());
        assert_eq!(first, second);
        assert_eq!(HashSet::from([first, second]).len(), 1);
    }
}
