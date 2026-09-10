//! Arithmetic expressions for FlowLog Datalog programs.
//!
//! - [`ArithmeticOperator`]: `+ | - | * | / | %`
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

/// Arithmetic operator.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum ArithmeticOperator {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
}

impl fmt::Display for ArithmeticOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sym = match self {
            Self::Plus => "+",
            Self::Minus => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::Modulo => "%",
        };
        write!(f, "{sym}")
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
    #[must_use]
    pub fn is_var(&self) -> bool {
        matches!(self, Self::Var(_))
    }

    #[must_use]
    pub fn is_const(&self) -> bool {
        matches!(self, Self::Const(_))
    }

    /// Variables appearing in this factor.
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

/// Resolves a unified `call_expr` (`name(args...)`) into a [`Factor`]. The
/// name is matched against the reserved value built-ins
/// ([`BuiltinOperator::from_keyword`]); a hit yields a [`Factor::Builtin`]
/// (arity-checked), otherwise a [`Factor::FnCall`] (a `.extern fn` call,
/// validated against the UDF registry later by the typechecker).
fn parse_call_expr(node: Node) -> Result<Factor, ParseError> {
    let span = node.span();
    let mut children = node.children();
    let name = children.next_any("function name")?.text().to_string();
    let args = children
        .filter(|c| c.rule() == Rule::arithmetic_expr)
        .map(|c| c.lower::<Arithmetic>())
        .collect::<Result<Vec<_>, _>>()?;

    if let Some(op) = BuiltinOperator::from_keyword(&name) {
        Ok(Factor::Builtin(BuiltinCall::new(op, args, span)?))
    } else {
        Ok(Factor::FnCall(FnCall::new(name, args, span)))
    }
}

/// Resolves a `paren_factor` (`(`-headed operand) into a [`Factor`]: any
/// comma (a second element or the trailing-comma marker) commits it to a
/// [`Factor::Tuple`]; otherwise the interior is grouping and becomes a
/// [`Factor::Group`], or, single-factor, collapses to the bare factor.
fn parse_paren_factor(node: Node) -> Result<Factor, ParseError> {
    let span = node.span();
    let mut children = node.children();
    let first = children.next_any("parenthesised operand")?;
    let mut rest = children.peekable();

    if rest.peek().is_none() {
        // A single comma-free element: plain grouping.
        if first.rule() == Rule::placeholder {
            return Err(ParseError::GroupedPlaceholder { span: first.span() });
        }
        let expr: Arithmetic = first.lower()?;
        return Ok(expr.into_factor());
    }

    let fields = std::iter::once(first)
        .chain(rest)
        .filter(|c| c.rule() != Rule::trailing_comma)
        .map(|c| c.lower())
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Factor::Tuple(TupleLit::new(fields, span)))
}

// =============================================================================
// Arithmetic
// =============================================================================

/// A left-to-right fold over factors. Parsing groups `*`, `/`, and `%`
/// before `+` and `-`; operators at the same precedence associate left.
/// Explicit parentheses preserve their own evaluation boundaries.
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

    /// Groups multiplicative runs, preserving source order and operand spans.
    fn with_precedence(
        span: Span,
        init: Factor,
        init_span: Span,
        steps: Vec<(ArithmeticOperator, Factor, Span)>,
    ) -> Self {
        let mut term = Self {
            init,
            rest: Vec::new(),
            span: init_span,
        };
        // Each completed multiplicative term keeps its following additive
        // operator. Fresh terms keep explicit groups opaque, so a / (b * c)
        // cannot become a / b * c. Flat runs avoid nesting per operator.
        let mut terms = Vec::new();
        for (op, factor, factor_span) in steps {
            match op {
                ArithmeticOperator::Plus | ArithmeticOperator::Minus => {
                    terms.push((term, op));
                    term = Self {
                        init: factor,
                        rest: Vec::new(),
                        span: factor_span,
                    };
                }
                ArithmeticOperator::Multiply
                | ArithmeticOperator::Divide
                | ArithmeticOperator::Modulo => {
                    term.rest.push((op, factor));
                    term.span = term.span.merge(factor_span);
                }
            }
        }

        let mut terms = terms.into_iter();
        let Some((first, mut op)) = terms.next() else {
            term.span = span;
            return term;
        };
        let init = first.into_factor();
        let mut rest = Vec::new();
        for (next, following_op) in terms {
            rest.push((op, next.into_factor()));
            op = following_op;
        }
        rest.push((op, term.into_factor()));
        Self { init, rest, span }
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

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use ArithmeticOperator::Plus;
    use Factor::Var;
    use rstest::rstest;

    use super::*;
    use crate::test_util::parse_node;

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

    /// `vars()` preserves order and duplicates; `vars_set()` dedups. The
    /// two accessors exist because downstream passes need both: variable
    /// binding passes count occurrences (repeat = join predicate), while
    /// scope analysis needs the unique set. Collapsing either one into
    /// the other would silently break one of those callers.
    #[test]
    fn vars_preserves_dups_vars_set_dedups() {
        // x + x + y: vars = [x, x, y], vars_set = {x, y}
        let a = Arithmetic::new(
            Var("x".into()),
            vec![(Plus, Var("x".into())), (Plus, Var("y".into()))],
        );
        let x = "x".to_string();
        let y = "y".to_string();
        assert_eq!(a.vars(), vec![&x, &x, &y]);
        assert_eq!(a.vars_set().len(), 2);
    }

    /// Every operator spelling round-trips through parse and Display.
    #[rstest]
    #[case("x + y")]
    #[case("x - y")]
    #[case("x * y")]
    #[case("x / y")]
    #[case("x % y")]
    fn display_round_trips_each_operator(#[case] src: &str) {
        assert_eq!(
            parse_node::<Arithmetic>(Rule::arithmetic_expr, src).to_string(),
            src
        );
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

    /// A parenthesised sub-expression parses into `Factor::Group`,
    /// preserves its inner variables (in order), and round-trips through
    /// `Display` with its parentheses intact: without the preserved group,
    /// `a * (b + c)` would fold as `a * b + c`.
    #[test]
    fn multi_term_parens_parse_as_group() {
        let arith: Arithmetic = parse_node(Rule::arithmetic_expr, "a * (b + c)");

        // init = `a`; rest = [(*, Group(b + c))].
        assert!(matches!(arith.init(), Factor::Var(v) if v == "a"));
        let (op, factor) = &arith.rest()[0];
        assert!(matches!(op, ArithmeticOperator::Multiply));
        assert!(matches!(factor, Factor::Group(_)));

        // Variables recurse through the group, preserving order.
        let a = "a".to_string();
        let b = "b".to_string();
        let c = "c".to_string();
        assert_eq!(arith.vars(), vec![&a, &b, &c]);

        // Parentheses survive the round-trip.
        assert_eq!(arith.to_string(), "a * (b + c)");
    }

    /// Parentheses around a single factor are semantically transparent and
    /// collapse to the bare factor at parse time: `(x)`, `("c")`, and
    /// `(f(x))` must behave exactly like their unparenthesised forms in
    /// fact detection, subtype narrowing, and assignment recognition.
    /// Nested parens around a multi-term expression collapse to one `Group`.
    #[test]
    fn single_factor_parens_collapse_to_the_factor() {
        let parse = |src: &str| -> Factor {
            parse_node::<Arithmetic>(Rule::arithmetic_expr, src)
                .init()
                .clone()
        };

        assert!(matches!(parse("(x)"), Factor::Var(v) if v == "x"));
        assert!(matches!(parse("(((x)))"), Factor::Var(v) if v == "x"));
        assert!(matches!(parse("(\"boolean\")"), Factor::Const(_)));
        // Nested parens: `((b + c))` is one Group around the expression.
        let Factor::Group(inner) = parse("((b + c))") else {
            panic!("expected Group");
        };
        assert!(matches!(inner.init(), Factor::Var(v) if v == "b"));
        assert!(!inner.rest().is_empty());
    }

    /// A comma is what commits parens to a tuple literal: a lone trailing
    /// comma is a 1-tuple, a `_` element stays a placeholder, and a
    /// trailing comma after multiple elements is accepted but renders
    /// canonically without it. Comma-free parens are grouping, pinned by
    /// `single_factor_parens_collapse_to_the_factor`.
    #[rstest]
    #[case("(a,)", "(a,)")]
    #[case("(_, b)", "(_, b)")]
    #[case("(a + 1, b)", "(a + 1, b)")]
    #[case("(a, b,)", "(a, b)")]
    #[case("(_,)", "(_,)")]
    fn comma_commits_parens_to_a_tuple(#[case] src: &str, #[case] rendered: &str) {
        let arith: Arithmetic = parse_node(Rule::arithmetic_expr, src);
        assert!(matches!(arith.init(), Factor::Tuple(_)), "src={src}");
        assert_eq!(arith.to_string(), rendered);
    }

    /// A lone parenthesised `_` is rejected with its dedicated error.
    #[test]
    fn grouped_placeholder_is_rejected() {
        use flowlog_common::FileId;

        use crate::assert_err;
        use crate::test_util::parse_pair;

        let pair = parse_pair(Rule::arithmetic_expr, "(_)");
        let result = Arithmetic::from_parsed_rule(Node::new(pair, FileId::new(0)));
        assert_err!(result, ParseError::GroupedPlaceholder { .. });
    }

    /// A 200-deep paren nest parses and collapses to the bare factor.
    /// Under split `(`-headed rules this input would hang the suite; the
    /// mechanism is documented at `paren_factor` in grammar.pest
    /// (issue #289).
    #[test]
    fn deeply_nested_parens_parse_without_backtracking_blowup() {
        let src = format!("{}x{}", "(".repeat(200), ")".repeat(200));
        let arith: Arithmetic = parse_node(Rule::arithmetic_expr, &src);
        assert!(matches!(arith.init(), Factor::Var(v) if v == "x"));
    }

    /// A 200-deep unclosed paren nest is rejected; the failure path must
    /// stay free of the re-parsing blowup too (issue #289).
    #[test]
    fn unclosed_paren_nest_is_rejected_without_backtracking_blowup() {
        use pest::Parser as _;

        use crate::FlowLogParser;

        assert!(FlowLogParser::parse(Rule::arithmetic_expr, &"(".repeat(200)).is_err());
    }

    /// Empty parens are rejected by the grammar: `paren_factor` requires
    /// at least one element.
    #[test]
    fn empty_parens_are_rejected() {
        use pest::Parser as _;

        use crate::FlowLogParser;

        assert!(FlowLogParser::parse(Rule::arithmetic_expr, "()").is_err());
    }

    /// A malformed cast stops the expression at the bare `as`, leaving
    /// `(x, 5)` for the enclosing rule, rather than being taken as a call
    /// named `as` (issue #298).
    #[test]
    fn malformed_cast_is_not_accepted_as_a_call() {
        use crate::test_util::parse_pair;

        // `5` is not a type, so `as_cast` fails at its type argument.
        assert_eq!(parse_pair(Rule::arithmetic_expr, "as(x, 5)").as_str(), "as");
    }

    /// The reserved-name guard keys on a whole word, so a UDF whose name
    /// only starts with `as` is still a call.
    #[test]
    fn udf_whose_name_starts_with_as_still_parses() {
        let arith: Arithmetic = parse_node(Rule::arithmetic_expr, "assert(x)");
        assert!(matches!(arith.init(), Factor::FnCall(c) if c.name() == "assert"));
    }

    /// A 200-deep unterminated cast nest stops at the bare `as` instead of
    /// re-parsing every level; the mechanism is documented at `call_expr`
    /// in grammar.pest (issue #298). A *valid* nest cannot pin this: pest
    /// never backtracks out of a successful alternative, so only a failing
    /// nest exercises the retry.
    #[test]
    fn unclosed_cast_nest_is_refused_without_backtracking_blowup() {
        use crate::test_util::parse_pair;

        let src = "as(".repeat(200);
        assert_eq!(parse_pair(Rule::arithmetic_expr, &src).as_str(), "as");
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
        let kind = match parse_node::<Arithmetic>(Rule::arithmetic_expr, src).init() {
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
}
