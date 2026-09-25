//! Arithmetic expressions for FlowLog Datalog programs.
//!
//! - [`ArithmeticOperator`]: `+ - * / % ^` and the bitwise
//!   `band bor bxor bshl bshr bshru`
//! - [`Factor`]: atomic operands (variables, constants, calls, casts,
//!   groups, and tuples)
//! - [`Arithmetic`]: a left-to-right fold with precedence encoded as groups

use std::collections::HashSet;
use std::fmt;

use educe::Educe;
use flowlog_common::Span;

use super::BuiltinCall;
use super::BuiltinOperator;
use super::Cast;
use super::Constant;
use super::FnCall;
use super::tuple::TupleLit;
use crate::Lexeme;
use crate::Node;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;

// =============================================================================
// ArithmeticOperator
// =============================================================================

/// Binary arithmetic operators in value expressions.
///
/// The bitwise operators and `^` follow Souffle: the shifts mask their
/// count to the operand width, `bshr` extends the sign and `bshru` fills
/// with zeros, and integer `^` wraps on overflow and yields `0` for a
/// negative exponent. Those rules live in the runtime's `arith` module.
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum ArithmeticOperator {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    /// `^`, exponentiation; the only right-associative operator.
    Power,
    /// `band`
    BitAnd,
    /// `bor`
    BitOr,
    /// `bxor`
    BitXor,
    /// `bshl`
    ShiftLeft,
    /// `bshr`, arithmetic on signed operands.
    ShiftRight,
    /// `bshru`, logical on signed operands.
    ShiftRightUnsigned,
}

impl ArithmeticOperator {
    /// Returns `true` for the bitwise operators, which accept integer
    /// operands only.
    #[must_use]
    pub fn is_bitwise(&self) -> bool {
        match self {
            Self::BitAnd
            | Self::BitOr
            | Self::BitXor
            | Self::ShiftLeft
            | Self::ShiftRight
            | Self::ShiftRightUnsigned => true,
            Self::Plus
            | Self::Minus
            | Self::Multiply
            | Self::Divide
            | Self::Modulo
            | Self::Power => false,
        }
    }

    /// Binding strength, weakest first: bitwise or, xor, and; shifts;
    /// additive; multiplicative; power. The same ladder as C, with `^`
    /// on top.
    fn precedence(&self) -> u8 {
        match self {
            Self::BitOr => 0,
            Self::BitXor => 1,
            Self::BitAnd => 2,
            Self::ShiftLeft | Self::ShiftRight | Self::ShiftRightUnsigned => 3,
            Self::Plus | Self::Minus => 4,
            Self::Multiply | Self::Divide | Self::Modulo => 5,
            Self::Power => 6,
        }
    }

    /// Returns `true` if a run of this operator groups from the right:
    /// `a ^ b ^ c` is `a ^ (b ^ c)`.
    fn is_right_associative(&self) -> bool {
        match self {
            Self::Power => true,
            Self::Plus
            | Self::Minus
            | Self::Multiply
            | Self::Divide
            | Self::Modulo
            | Self::BitAnd
            | Self::BitOr
            | Self::BitXor
            | Self::ShiftLeft
            | Self::ShiftRight
            | Self::ShiftRightUnsigned => false,
        }
    }
}

impl fmt::Display for ArithmeticOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Plus => "+",
            Self::Minus => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::Modulo => "%",
            Self::Power => "^",
            Self::BitAnd => "band",
            Self::BitOr => "bor",
            Self::BitXor => "bxor",
            Self::ShiftLeft => "bshl",
            Self::ShiftRight => "bshr",
            Self::ShiftRightUnsigned => "bshru",
        })
    }
}

impl Lexeme for ArithmeticOperator {
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let op = node.children().next_any("operator symbol")?;
        Ok(match op.rule() {
            Rule::plus => Self::Plus,
            Rule::minus => Self::Minus,
            Rule::times => Self::Multiply,
            Rule::divide => Self::Divide,
            Rule::modulo => Self::Modulo,
            Rule::power => Self::Power,
            Rule::band => Self::BitAnd,
            Rule::bor => Self::BitOr,
            Rule::bxor => Self::BitXor,
            Rule::bshl => Self::ShiftLeft,
            Rule::bshr => Self::ShiftRight,
            Rule::bshru => Self::ShiftRightUnsigned,
            other => {
                return Err(grammar_bug(format!(
                    "unknown arithmetic operator: {other:?}"
                )));
            }
        })
    }
}

// =============================================================================
// Factor
// =============================================================================

/// Atomic operand for arithmetic. `FnCall` and `Builtin` are kept
/// distinct so downstream stages match on the node type, not on a name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Factor {
    Var(String),
    Const(Constant),
    /// User `.extern fn` call.
    FnCall(FnCall),
    /// Engine built-in (Souffle-style intrinsic).
    Builtin(BuiltinCall),
    /// `as(factor, T)` cast.
    Cast(Box<Cast>),
    /// Evaluation boundary from parentheses or operator precedence.
    /// Always multi-term at parse time: single-factor expressions like
    /// `(x)` collapse to the bare factor.
    Group(Box<Arithmetic>),
    /// `(e0, e1, ...)` tuple literal.
    Tuple(TupleLit),
    /// Projection of component `index` out of `tuple`. Synthesized by the
    /// destructure desugar; it has no surface syntax and appears only
    /// after desugaring.
    TupleProj {
        tuple: Box<Arithmetic>,
        index: usize,
    },
}

impl Factor {
    /// Returns `true` for a bare variable.
    #[must_use]
    pub fn is_var(&self) -> bool {
        match self {
            Self::Var(_) => true,
            Self::Const(_)
            | Self::FnCall(_)
            | Self::Builtin(_)
            | Self::Cast(_)
            | Self::Group(_)
            | Self::Tuple(_)
            | Self::TupleProj { .. } => false,
        }
    }

    /// Returns `true` for a bare constant.
    #[must_use]
    pub fn is_const(&self) -> bool {
        match self {
            Self::Const(_) => true,
            Self::Var(_)
            | Self::FnCall(_)
            | Self::Builtin(_)
            | Self::Cast(_)
            | Self::Group(_)
            | Self::Tuple(_)
            | Self::TupleProj { .. } => false,
        }
    }

    /// Variables in order of appearance, including repeated occurrences.
    #[must_use]
    pub fn vars(&self) -> Vec<&String> {
        match self {
            Self::Var(v) => vec![v],
            Self::Const(_) => vec![],
            Self::FnCall(fc) => fc.vars(),
            Self::Builtin(bc) => bc.vars(),
            Self::Cast(c) => c.inner().vars(),
            Self::Group(a) => a.vars(),
            Self::Tuple(r) => r.vars(),
            Self::TupleProj { tuple, .. } => tuple.vars(),
        }
    }
}

impl fmt::Display for Factor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Var(v) => write!(f, "{v}"),
            Self::Const(c) => write!(f, "{c}"),
            Self::FnCall(fc) => write!(f, "{fc}"),
            Self::Builtin(bc) => write!(f, "{bc}"),
            Self::Cast(c) => write!(f, "{c}"),
            Self::Group(a) => write!(f, "({a})"),
            Self::Tuple(r) => write!(f, "{r}"),
            Self::TupleProj { tuple, index } => write!(f, "({tuple}).{index}"),
        }
    }
}

impl Lexeme for Factor {
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let inner = node.children().next_any("factor value")?;
        Ok(match inner.rule() {
            Rule::as_cast => Self::Cast(Box::new(inner.lower()?)),
            Rule::call_expr => parse_call_expr(inner)?,
            Rule::variable => Self::Var(inner.text().to_string()),
            Rule::constant => Self::Const(inner.lower()?),
            Rule::paren_factor => parse_paren_factor(inner)?,
            other => return Err(grammar_bug(format!("invalid factor rule: {other:?}"))),
        })
    }
}

/// Resolves a value-position call into a built-in or external call.
/// Built-ins are arity-checked here, while external signatures are checked
/// against the UDF registry later by the typechecker.
fn parse_call_expr(node: Node) -> Result<Factor, ParseError> {
    let span = node.span();
    let mut arguments = node.children();
    let name = arguments.next_any("call name")?;
    let name = name.text();
    // Shared call syntax admits qualified relation names. A value function
    // must still use the unqualified name declared by `.extern fn`.
    if name.contains('.') {
        return Err(ParseError::Syntax {
            span,
            message: "value function names cannot be qualified".into(),
        });
    }
    let args = arguments
        .map(Node::lower)
        .collect::<Result<Vec<Arithmetic>, _>>()?;

    if let Some(op) = BuiltinOperator::from_keyword(name) {
        Ok(Factor::Builtin(BuiltinCall::new(op, args, span)?))
    } else {
        Ok(Factor::FnCall(FnCall::new(name.to_string(), args, span)))
    }
}

/// Lowers value parentheses: a comma selects a tuple; otherwise the contents
/// are one expression. Conditions are rejected, and redundant groups collapse
/// without adding a lowering stack frame for each enclosing pair.
fn parse_paren_factor(mut node: Node) -> Result<Factor, ParseError> {
    loop {
        let span = node.span();
        let mut children = node.children();
        let mut alternatives = children.require(Rule::paren_bodies)?.children();
        let conjunction = alternatives.require(Rule::paren_items)?;
        if alternatives.next().is_some() {
            return Err(ParseError::Syntax {
                span,
                message: "expected a value expression, found a disjunction".into(),
            });
        }
        let mut elements = conjunction.children();
        let first = elements.require(Rule::paren_item)?;
        let mut elements = elements.peekable();
        let trailing_comma = children.take_if(Rule::trailing_comma).is_some();
        if trailing_comma || elements.peek().is_some() {
            // A trailing comma makes `(x,)` a tuple even with one field.
            // Tuple elements own their expression-or-placeholder validation.
            let fields = std::iter::once(first)
                .chain(elements)
                .map(Node::lower)
                .collect::<Result<_, _>>()?;
            return Ok(Factor::Tuple(TupleLit::new(fields, span)));
        }

        // A comma-free item must be a value. The shared grammar also admits
        // conditions, so neither a negation nor a comparison suffix may be
        // discarded when the item is interpreted as a grouped expression.
        let item_span = first.span();
        let mut parts = first.children();
        let value = parts.next_any("grouped operand")?;
        if value.rule() == Rule::placeholder {
            return Err(ParseError::GroupedPlaceholder { span: value.span() });
        }
        if value.rule() != Rule::arithmetic_expr || parts.next().is_some() {
            return Err(ParseError::Syntax {
                span: item_span,
                message: "expected a value expression".into(),
            });
        }

        // Peel redundant parentheses before lowering the expression. Lowering
        // first would recurse once per wrapper even though all wrappers vanish.
        let mut operands = value.clone().children();
        let factor = operands.require(Rule::factor)?;
        if operands.next().is_none() {
            let inner = factor.children().next_any("factor value")?;
            if inner.rule() == Rule::paren_factor {
                node = inner;
                continue;
            }
        }
        let expr: Arithmetic = value.lower()?;
        return Ok(expr.into_factor());
    }
}

// =============================================================================
// Arithmetic
// =============================================================================

/// A left-to-right fold over factors. Every operator in `rest` has the
/// same precedence, so the fold order is the evaluation order; tighter
/// operators are grouped into [`Factor::Group`] operands by parsing.
/// Operators at the same precedence associate left, except `^`, which
/// associates right. Explicit parentheses preserve their own evaluation
/// boundaries.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct Arithmetic {
    init: Factor,
    rest: Vec<(ArithmeticOperator, Factor)>,
    #[educe(PartialEq(ignore), Hash(ignore))]
    span: Span,
}

impl Arithmetic {
    /// Creates a synthesized expression with no source location
    /// (`Span::DUMMY`).
    #[must_use]
    pub(crate) fn new(init: Factor, rest: Vec<(ArithmeticOperator, Factor)>) -> Self {
        Self {
            init,
            rest,
            span: Span::DUMMY,
        }
    }

    /// Preserves evaluation order when embedding an expression as an operand.
    fn into_factor(self) -> Factor {
        if self.rest.is_empty() {
            self.init
        } else {
            Factor::Group(Box::new(self))
        }
    }

    /// Groups a flat operator run by precedence, preserving source order
    /// and operand spans.
    fn with_precedence(
        span: Span,
        init: Factor,
        init_span: Span,
        steps: Vec<(ArithmeticOperator, Factor, Span)>,
    ) -> Self {
        let mut expr = group_by_precedence((init, init_span), steps);
        expr.span = span;
        expr
    }

    /// A bare variable as an expression: `Factor::Var(name)` with no operators.
    #[must_use]
    pub(crate) fn var(name: &str) -> Self {
        Self::new(Factor::Var(name.to_string()), vec![])
    }

    /// Source location this expression was parsed from (`Span::DUMMY` for
    /// nodes synthesized without a concrete source range).
    #[must_use]
    #[inline]
    pub fn span(&self) -> Span {
        self.span
    }

    /// First term.
    #[must_use]
    pub fn init(&self) -> &Factor {
        &self.init
    }

    /// Remaining `(op, factor)` pairs.
    #[must_use]
    pub fn rest(&self) -> &[(ArithmeticOperator, Factor)] {
        &self.rest
    }

    pub(crate) fn init_mut(&mut self) -> &mut Factor {
        &mut self.init
    }

    pub(crate) fn rest_mut(&mut self) -> &mut [(ArithmeticOperator, Factor)] {
        &mut self.rest
    }

    /// Variables in order of appearance (duplicates preserved).
    #[must_use]
    pub fn vars(&self) -> Vec<&String> {
        let mut out = self.init.vars();
        for (_, f) in &self.rest {
            out.extend(f.vars());
        }
        out
    }

    /// Unique variables (deduplicated).
    #[must_use]
    pub fn vars_set(&self) -> HashSet<&String> {
        self.vars().into_iter().collect()
    }

    /// Returns `true` for a single constant with no operators.
    #[must_use]
    pub fn is_const(&self) -> bool {
        self.rest.is_empty() && self.init.is_const()
    }

    /// Returns `true` for a single variable with no operators.
    #[must_use]
    pub fn is_var(&self) -> bool {
        self.rest.is_empty() && self.init.is_var()
    }
}

impl fmt::Display for Arithmetic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.init)?;
        for (op, factor) in &self.rest {
            write!(f, " {op} {factor}")?;
        }
        Ok(())
    }
}

impl Lexeme for Arithmetic {
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        if node.rule() != Rule::arithmetic_expr {
            return Err(ParseError::Syntax {
                span: node.span(),
                message: "expected a value expression".into(),
            });
        }
        let span = node.span();
        let mut children = node.children();
        let initial = children.next_any("initial factor")?;
        let init_span = initial.span();
        let init = initial.lower()?;

        let mut steps = Vec::new();
        while let Some(op_node) = children.next() {
            let op = op_node.lower::<ArithmeticOperator>()?;
            let factor_node = children.next_any("factor after operator")?;
            let factor_span = factor_node.span();
            let factor = factor_node.lower::<Factor>()?;
            steps.push((op, factor, factor_span));
        }

        // Group after lowering nested factors so normalization state does not
        // accumulate on the stack while descending through parentheses.
        Ok(Self::with_precedence(span, init, init_span, steps))
    }
}

/// Builds the expression for `first` followed by `steps`, splitting at the
/// weakest operators present so each level of the result holds operators
/// of one precedence. A run of the weakest operator stays flat when it
/// associates left; a right-associative run nests its tail as one operand.
/// Fresh groups keep explicit parentheses opaque, so `a / (b * c)` cannot
/// become `a / b * c`. Recursion depth is bounded by the number of
/// precedence levels, not by the expression length.
fn group_by_precedence(
    first: (Factor, Span),
    steps: Vec<(ArithmeticOperator, Factor, Span)>,
) -> Arithmetic {
    let span = steps
        .last()
        .map_or(first.1, |(_, _, last)| first.1.merge(*last));
    let Some(weakest) = steps.iter().map(|(op, ..)| op.precedence()).min() else {
        return Arithmetic {
            init: first.0,
            rest: Vec::new(),
            span,
        };
    };

    // Cut the run at every weakest operator: the steps before the first
    // cut belong to the leading segment, and each cut starts a segment
    // headed by the operand after it.
    let mut lead_steps = Vec::new();
    let mut joins: Vec<(ArithmeticOperator, (Factor, Span), Vec<_>)> = Vec::new();
    for (op, factor, factor_span) in steps {
        if op.precedence() == weakest {
            joins.push((op, (factor, factor_span), Vec::new()));
        } else if let Some((_, _, tail)) = joins.last_mut() {
            tail.push((op, factor, factor_span));
        } else {
            lead_steps.push((op, factor, factor_span));
        }
    }
    let lead = group_by_precedence(first, lead_steps);

    if joins
        .first()
        .is_some_and(|(op, ..)| op.is_right_associative())
    {
        // Hang each segment on the one after it, from the right, so
        // `a ^ b ^ c` is `a ^ (b ^ c)`.
        let mut tail: Option<(ArithmeticOperator, Factor, Span)> = None;
        for (op, head, tail_steps) in joins.into_iter().rev() {
            let mut segment = group_by_precedence(head, tail_steps);
            if let Some((next_op, next, next_span)) = tail.take() {
                segment = Arithmetic {
                    span: segment.span.merge(next_span),
                    init: segment.into_factor(),
                    rest: vec![(next_op, next)],
                };
            }
            let segment_span = segment.span;
            tail = Some((op, segment.into_factor(), segment_span));
        }
        return Arithmetic {
            init: lead.into_factor(),
            rest: tail
                .map(|(op, factor, _)| (op, factor))
                .into_iter()
                .collect(),
            span,
        };
    }
    Arithmetic {
        init: lead.into_factor(),
        rest: joins
            .into_iter()
            .map(|(op, head, tail_steps)| (op, group_by_precedence(head, tail_steps).into_factor()))
            .collect(),
        span,
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
    use crate::types::DataType;

    #[rstest]
    #[case("x")]
    #[case("x, y")]
    fn nested_value_parentheses_are_fully_consumed(#[case] inner: &str) {
        let source = format!("{}{inner}{}", "(".repeat(256), ")".repeat(256));
        let pairs = FlowLogParser::parse(Rule::arithmetic_expr, &source).unwrap();
        assert_eq!(pairs.as_str(), source);
    }

    #[rstest]
    #[case::unclosed("x", "")]
    #[case::missing_operand("x +", ")")]
    fn malformed_nested_parentheses_are_rejected(#[case] inner: &str, #[case] close: &str) {
        let source = format!("{}{inner}{}", "(".repeat(256), close.repeat(256));
        let err = FlowLogParser::parse(Rule::paren_factor, &source).unwrap_err();
        assert!(matches!(err.variant, ErrorVariant::ParsingError { .. }));
    }

    #[test]
    fn empty_parentheses_are_rejected() {
        let err = FlowLogParser::parse(Rule::paren_factor, "()").unwrap_err();
        assert!(matches!(err.variant, ErrorVariant::ParsingError { .. }));
    }

    #[rstest]
    #[case("?x")]
    #[case("?as")]
    #[case("?True")]
    fn prefixed_factor_preserves_variable_name(#[case] src: &str) {
        assert_eq!(
            parse_node::<Factor>(Rule::factor, src),
            Factor::Var(src.to_string())
        );
    }

    #[rstest]
    #[case("?x + x")]
    #[case("(?x + ?y) * ?z")]
    #[case("strlen(?text)")]
    #[case("as(?x, number)")]
    #[case("(?x, ?y)")]
    fn prefixed_variables_round_trip_in_expressions(#[case] src: &str) {
        assert_eq!(
            parse_node::<Arithmetic>(Rule::arithmetic_expr, src).to_string(),
            src
        );
    }

    #[rstest]
    #[case("x + x + y", vec!["x", "x", "y"])]
    #[case("a * (b + c)", vec!["a", "b", "c"])]
    #[case("f(x, x + y)", vec!["x", "x", "y"])]
    #[case("(x, y, x)", vec!["x", "y", "x"])]
    fn vars_preserve_source_order_and_occurrences(
        #[case] source: &str,
        #[case] expected: Vec<&str>,
    ) {
        let expr: Arithmetic = parse_node(Rule::arithmetic_expr, source);
        assert_eq!(expr.vars(), expected);
    }

    #[test]
    fn vars_set_contains_each_variable_once() {
        let expr: Arithmetic = parse_node(Rule::arithmetic_expr, "x + x + y");
        let vars: HashSet<_> = expr.vars_set().into_iter().map(String::as_str).collect();
        assert_eq!(vars, HashSet::from(["x", "y"]));
    }

    #[rstest]
    #[case("+", ArithmeticOperator::Plus)]
    #[case("-", ArithmeticOperator::Minus)]
    #[case("*", ArithmeticOperator::Multiply)]
    #[case("/", ArithmeticOperator::Divide)]
    #[case("%", ArithmeticOperator::Modulo)]
    #[case("^", ArithmeticOperator::Power)]
    #[case("band", ArithmeticOperator::BitAnd)]
    #[case("bor", ArithmeticOperator::BitOr)]
    #[case("bxor", ArithmeticOperator::BitXor)]
    #[case("bshl", ArithmeticOperator::ShiftLeft)]
    #[case("bshr", ArithmeticOperator::ShiftRight)]
    #[case("bshru", ArithmeticOperator::ShiftRightUnsigned)]
    fn operators_parse_and_display_their_surface_spelling(
        #[case] source: &str,
        #[case] expected: ArithmeticOperator,
    ) {
        let op: ArithmeticOperator = parse_node(Rule::arithmetic_op, source);
        assert_eq!(op, expected);
        assert_eq!(op.to_string(), source);
    }

    #[rstest]
    #[case("a + b * c", "a + (b * c)")]
    #[case("a + b / c", "a + (b / c)")]
    #[case("a + b % c", "a + (b % c)")]
    #[case("a - b * c", "a - (b * c)")]
    #[case("a - b / c", "a - (b / c)")]
    #[case("a - b % c", "a - (b % c)")]
    #[case("a * b + c", "(a * b) + c")]
    #[case("a / b - c", "(a / b) - c")]
    #[case("a % b + c", "(a % b) + c")]
    #[case("a * b + c / d - e % f", "(a * b) + (c / d) - (e % f)")]
    #[case("a - b + c - d", "a - b + c - d")]
    #[case("a / b * c % d", "a / b * c % d")]
    #[case("a + b / c * d % e", "a + (b / c * d % e)")]
    #[case("(a + b) * c", "(a + b) * c")]
    #[case("a / (b * c)", "a / (b * c)")]
    #[case("a - (b - c)", "a - (b - c)")]
    #[case("a + (b / c) * d", "a + ((b / c) * d)")]
    #[case("a + b / (c * d)", "a + (b / (c * d))")]
    #[case("-3 + 2 * -4", "-3 + (2 * -4)")]
    #[case("f(a + b * c)", "f(a + (b * c))")]
    #[case("(a + b * c, d)", "(a + (b * c), d)")]
    // Bitwise operators bind looser than arithmetic, in C's order.
    #[case("a bor b bxor c band d", "a bor (b bxor (c band d))")]
    #[case("a band b bor c", "(a band b) bor c")]
    #[case("a bshl b + c", "a bshl (b + c)")]
    #[case("a + b bshl c", "(a + b) bshl c")]
    #[case("a bshl b bshr c bshru d", "a bshl b bshr c bshru d")]
    #[case("a band b bshl c * d", "a band (b bshl (c * d))")]
    // `^` binds tightest and groups from the right.
    #[case("a * b ^ c", "a * (b ^ c)")]
    #[case("a ^ b * c", "(a ^ b) * c")]
    #[case("a ^ b ^ c", "a ^ (b ^ c)")]
    #[case("a ^ b ^ c ^ d", "a ^ (b ^ (c ^ d))")]
    #[case("(a ^ b) ^ c", "(a ^ b) ^ c")]
    #[case("a + b ^ c ^ d * e", "a + ((b ^ (c ^ d)) * e)")]
    fn precedence_groups_round_trip(#[case] src: &str, #[case] rendered: &str) {
        let expr: Arithmetic = parse_node(Rule::arithmetic_expr, src);
        assert_eq!(expr.to_string(), rendered);
        assert_eq!(
            parse_node::<Arithmetic>(Rule::arithmetic_expr, rendered),
            expr
        );
    }

    #[test]
    fn implicit_groups_keep_their_operand_spans() {
        let src = "a * b + c / d - e % f";
        let expr: Arithmetic = parse_node(Rule::arithmetic_expr, src);
        assert_eq!(&src[expr.span().range()], src);
        for (factor, expected) in [
            (expr.init(), "a * b"),
            (&expr.rest()[0].1, "c / d"),
            (&expr.rest()[1].1, "e % f"),
        ] {
            let Factor::Group(group) = factor else {
                panic!("expected a precedence group");
            };
            assert_eq!(&src[group.span().range()], expected);
        }
    }

    /// A bitwise word is an operator only as a whole word, so a variable
    /// that starts with one is still an operand.
    #[rstest]
    #[case("x bandy", "x")]
    #[case("x borrow", "x")]
    fn bitwise_word_prefix_does_not_split_a_variable(#[case] src: &str, #[case] consumed: &str) {
        let pairs = FlowLogParser::parse(Rule::arithmetic_expr, src).unwrap();
        assert_eq!(pairs.as_str().trim_end(), consumed);
    }

    /// Dropping this group would turn `a * (b + c)` into `a * b + c`.
    #[test]
    fn multi_term_parens_parse_as_group() {
        let arith: Arithmetic = parse_node(Rule::arithmetic_expr, "a * (b + c)");

        assert!(matches!(arith.init(), Factor::Var(v) if v == "a"));
        let (op, factor) = &arith.rest()[0];
        assert!(matches!(op, ArithmeticOperator::Multiply));
        assert!(matches!(factor, Factor::Group(_)));

        assert_eq!(arith.to_string(), "a * (b + c)");
        assert_eq!(arith, parse_node(Rule::arithmetic_expr, &arith.to_string()));
    }

    /// A single factor keeps its identity when parentheses are removed.
    #[rstest]
    #[case("(x)", Factor::Var("x".into()))]
    #[case("(((x)))", Factor::Var("x".into()))]
    #[case("(42)", Factor::Const(Constant::new(DataType::IntLit, "42")))]
    #[case("(f(x))", Factor::FnCall(FnCall::new("f".into(), vec![Arithmetic::var("x")], Span::DUMMY)))]
    fn single_factor_parens_collapse_to_the_factor(#[case] source: &str, #[case] expected: Factor) {
        assert_eq!(parse_node::<Factor>(Rule::factor, source), expected);
    }

    /// The comma must distinguish `(a,)` from grouping, including after
    /// canonical rendering removes a trailing comma from a longer tuple.
    #[rstest]
    #[case("(a,)", "(a,)")]
    #[case("(_, b)", "(_, b)")]
    #[case("(a + 1, b)", "(a + 1, b)")]
    #[case("(a, b,)", "(a, b)")]
    #[case("(_,)", "(_,)")]
    fn comma_commits_parens_to_a_tuple(#[case] src: &str, #[case] rendered: &str) {
        let factor: Factor = parse_node(Rule::factor, src);
        assert!(matches!(factor, Factor::Tuple(_)), "src={src}");
        assert_eq!(factor.to_string(), rendered);
        assert_eq!(factor, parse_node(Rule::factor, rendered));
    }

    #[rstest]
    #[case("(_)")]
    #[case("(((_)))")]
    fn grouped_placeholder_is_rejected(#[case] src: &str) {
        let node = Node::new(parse_pair(Rule::factor, src), FileId::new(0));
        assert_err!(
            node.lower::<Factor>(),
            ParseError::GroupedPlaceholder { span } if &src[span.range()] == "_"
        );
    }

    /// Removing redundant parentheses must preserve the expression, including
    /// the distinction between a grouped arithmetic operation and a tuple.
    #[rstest]
    #[case("x", "x")]
    #[case("a+b*c", "(a + (b * c))")]
    #[case("a,b", "(a, b)")]
    #[case("x,", "(x,)")]
    fn redundant_parentheses_preserve_expression_shape(
        #[case] inner: &str,
        #[case] rendered: &str,
    ) {
        let src = format!("{}{inner}{}", "(".repeat(200), ")".repeat(200));
        let arith: Arithmetic = parse_node(Rule::arithmetic_expr, &src);
        assert_eq!(arith.to_string(), rendered);
        assert_eq!(arith, parse_node(Rule::arithmetic_expr, rendered));
    }

    #[test]
    fn redundant_parens_preserve_inner_group_span() {
        let src = "(((a+b*c)))";
        let arith: Arithmetic = parse_node(Rule::arithmetic_expr, src);
        assert_eq!(&src[arith.span().range()], src);
        let Factor::Group(group) = arith.init() else {
            panic!("expected a group");
        };
        assert_eq!(&src[group.span().range()], "a+b*c");
    }

    #[test]
    fn malformed_cast_is_not_accepted_as_a_value_call() {
        let err = FlowLogParser::parse(Rule::arithmetic_expr, "as(x, 5)").unwrap_err();
        assert!(matches!(err.variant, ErrorVariant::ParsingError { .. }));
    }

    #[rstest]
    #[case("(x > 1)", "x > 1", "expected a value expression")]
    #[case(
        "(Edge(x); Other(x))",
        "(Edge(x); Other(x))",
        "expected a value expression, found a disjunction"
    )]
    #[case("(!Edge(x))", "!Edge(x)", "expected a value expression")]
    #[case("f(_)", "_", "expected a value expression")]
    #[case("c.f(x)", "c.f(x)", "value function names cannot be qualified")]
    fn predicate_syntax_is_rejected_in_value_context(
        #[case] src: &str,
        #[case] invalid: &str,
        #[case] expected_message: &str,
    ) {
        let node = Node::new(parse_pair(Rule::factor, src), FileId::new(0));
        assert_err!(
            node.lower::<Factor>(),
            ParseError::Syntax { span, message }
                if &src[span.range()] == invalid && message == expected_message
        );
    }

    #[test]
    fn udf_whose_name_starts_with_as_still_parses() {
        let factor: Factor = parse_node(Rule::factor, "assert(x)");
        assert!(matches!(factor, Factor::FnCall(c) if c.name() == "assert"));
    }

    /// Each grammar factor kind parses to its variant. Contents are tested
    /// in each variant's own module (`cast.rs`, `fn_call.rs`, `builtin.rs`,
    /// `tuple.rs`); `TupleProj` is absent because it has no surface syntax.
    #[rstest]
    #[case("x", "var")]
    #[case("1", "const")]
    #[case("f(x)", "fncall")]
    #[case("ord(x)", "builtin")]
    #[case("as(x, uint32)", "cast")]
    #[case("(a, b)", "tuple")]
    #[case("(a + b)", "group")]
    fn factor_parses_each_variant(#[case] src: &str, #[case] expected: &str) {
        let kind = match parse_node::<Factor>(Rule::factor, src) {
            Factor::Var(_) => "var",
            Factor::Const(_) => "const",
            Factor::FnCall(_) => "fncall",
            Factor::Builtin(_) => "builtin",
            Factor::Cast(_) => "cast",
            Factor::Group(_) => "group",
            Factor::Tuple(_) => "tuple",
            Factor::TupleProj { .. } => "tupleproj",
        };
        assert_eq!(kind, expected, "src={src}");
    }

    #[rstest]
    #[case("x", true, false)]
    #[case("1", false, true)]
    #[case("f(x)", false, false)]
    #[case("ord(x)", false, false)]
    #[case("as(x, T)", false, false)]
    #[case("(x + 1)", false, false)]
    #[case("(x, y)", false, false)]
    fn factor_classification_requires_a_bare_variable_or_constant(
        #[case] source: &str,
        #[case] is_var: bool,
        #[case] is_const: bool,
    ) {
        let factor: Factor = parse_node(Rule::factor, source);
        assert_eq!(factor.is_var(), is_var);
        assert_eq!(factor.is_const(), is_const);
    }

    #[test]
    fn synthesized_projection_is_neither_a_variable_nor_a_constant() {
        let factor = Factor::TupleProj {
            tuple: Box::new(Arithmetic::var("x")),
            index: 0,
        };
        assert!(!factor.is_var());
        assert!(!factor.is_const());
    }

    /// The single-term predicates require both "no operators" and the
    /// matching factor kind.
    #[rstest]
    #[case("x", false, true)]
    #[case("1", true, false)]
    #[case("x + 1", false, false)]
    fn is_const_and_is_var_require_a_single_term(
        #[case] src: &str,
        #[case] is_const: bool,
        #[case] is_var: bool,
    ) {
        let a: Arithmetic = parse_node(Rule::arithmetic_expr, src);
        assert_eq!(a.is_const(), is_const);
        assert_eq!(a.is_var(), is_var);
    }

    #[test]
    fn infix_cat_is_rejected() {
        // The head's closing delimiter prevents accepting only the `x` prefix.
        let err = FlowLogParser::parse(Rule::head, "C(x cat y)").unwrap_err();
        assert!(matches!(err.variant, ErrorVariant::ParsingError { .. }));
    }
}
