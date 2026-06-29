//! Arithmetic expressions for FlowLog Datalog Programs.
//!
//! - [`ArithmeticOperator`]: `+ | - | * | / | %`
//! - [`Factor`]: variables or constants
//! - [`Arithmetic`]: `factor (op, factor)*`

use std::collections::HashSet;
use std::fmt;

use educe::Educe;
use flowlog_common::FileId;
use flowlog_common::Span;
use pest::iterators::Pair;

use super::BuiltinCall;
use super::BuiltinOperator;
use super::FnCall;
use super::tuple::TupleLit;
use crate::Lexeme;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;
use crate::primitive::ConstType;
use crate::span_of;
use crate::type_ref_name;

/// Arithmetic operator.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum ArithmeticOperator {
    Plus,     // +
    Minus,    // -
    Multiply, // *
    Divide,   // /
    Modulo,   // %
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
    /// Parse an operator from the grammar.
    fn from_parsed_rule(parsed_rule: Pair<Rule>, _file: FileId) -> Result<Self, ParseError> {
        let op = parsed_rule
            .into_inner()
            .next()
            .ok_or_else(|| grammar_bug("operator missing inner token"))?;
        Ok(match op.as_rule() {
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

/// Atomic operand for arithmetic. `FnCall` and `Builtin` are kept
/// distinct so downstream stages match on the node type, not on a name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Factor {
    Var(String),
    Const(ConstType),
    /// User `.extern fn` call.
    FnCall(FnCall),
    /// Engine built-in (Soufflé-style intrinsic).
    Builtin(BuiltinCall),
    /// `as(factor, T)`. Runtime no-op; the typechecker lowers it away
    /// after validating the cast.
    Cast(Box<Cast>),
    /// Parenthesised sub-expression `(expr)`. Preserves grouping because
    /// arithmetic folds left-to-right with no operator precedence. The
    /// grammar enforces multi-term content (`group_expr` requires at
    /// least one operator; single-factor parens like `(x)` are silent
    /// and parse as the bare factor), so a `Group` exists only when the
    /// grouping affects the fold.
    Group(Box<Arithmetic>),
    /// Tuple literal `(e0, e1, …)` — constructs a tuple from its (bound)
    /// components, or, matched against a bound variable, destructures one.
    Tuple(TupleLit),
    /// Projection of tuple component `index` out of `tuple`. **Synthesized
    /// by the destructure desugar only** — there is no surface syntax for it.
    /// `(a, b) = x` (with `x` bound) lowers to `a = TupleProj{Var(x), 0}`,
    /// `b = TupleProj{Var(x), 1}`; codegen emits a Rust tuple field access
    /// (`x.0`, `x.1`).
    TupleProj {
        tuple: Box<Arithmetic>,
        index: usize,
    },
}

/// `as(factor, target_type)`. `inner` is a single [`Factor`] (not a
/// full [`Arithmetic`]) so the typechecker can lower `Cast(inner)` to
/// `inner` after subtype validation — downstream never sees a cast.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct Cast {
    inner: Box<Factor>,
    /// User-written target type name; resolved by the typechecker.
    target_type: String,
    #[educe(PartialEq(ignore), Hash(ignore))]
    span: Span,
}

impl Cast {
    #[must_use]
    pub fn new(inner: Factor, target_type: String, span: Span) -> Self {
        Self {
            inner: Box::new(inner),
            target_type,
            span,
        }
    }

    #[must_use]
    #[inline]
    pub fn inner(&self) -> &Factor {
        &self.inner
    }

    #[inline]
    pub fn inner_mut(&mut self) -> &mut Factor {
        &mut self.inner
    }

    #[must_use]
    #[inline]
    pub fn target_type(&self) -> &str {
        &self.target_type
    }

    #[must_use]
    #[inline]
    pub fn span(&self) -> Span {
        self.span
    }
}

impl fmt::Display for Cast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "as({}, {})", self.inner, self.target_type)
    }
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
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        let inner = parsed_rule
            .into_inner()
            .next()
            .ok_or_else(|| grammar_bug("factor missing inner token"))?;
        Ok(match inner.as_rule() {
            Rule::as_cast => Self::Cast(Box::new(Cast::from_parsed_rule(inner, file)?)),
            Rule::call_expr => parse_call_expr(inner, file)?,
            Rule::variable => Self::Var(inner.as_str().to_string()),
            Rule::constant => Self::Const(ConstType::from_parsed_rule(inner, file)?),
            Rule::tuple_lit => Self::Tuple(TupleLit::from_parsed_rule(inner, file)?),
            // Multi-term parenthesised sub-expression — the grammar's
            // `group_expr` requires at least one operator, so `Group` is
            // constructed only when grouping affects the fold.
            Rule::group_expr => Self::Group(Box::new(Arithmetic::from_parsed_rule(inner, file)?)),
            // Single-factor parens `(x)`: the paren wrappers are silent in
            // the grammar, so the bare inner factor pair surfaces here.
            Rule::factor => Self::from_parsed_rule(inner, file)?,
            other => return Err(grammar_bug(format!("invalid factor rule: {other:?}"))),
        })
    }
}

/// Resolve a unified `call_expr` (`name(args...)`) into a [`Factor`]. The
/// name is matched against the reserved value built-ins
/// ([`BuiltinOperator::from_keyword`]); a hit yields a [`Factor::Builtin`]
/// (arity-checked), otherwise a [`Factor::FnCall`] (a `.extern fn` call,
/// validated against the UDF registry later by the typechecker).
fn parse_call_expr(node: Pair<Rule>, file: FileId) -> Result<Factor, ParseError> {
    let span = span_of(&node, file);
    let mut children = node.into_inner();
    let name = children
        .next()
        .ok_or_else(|| grammar_bug("call_expr missing function name"))?
        .as_str()
        .to_string();
    let args = children
        .filter(|p| p.as_rule() == Rule::arithmetic_expr)
        .map(|p| Arithmetic::from_parsed_rule(p, file))
        .collect::<Result<Vec<_>, _>>()?;

    if let Some(op) = BuiltinOperator::from_keyword(&name) {
        Ok(Factor::Builtin(BuiltinCall::new(op, args, span)?))
    } else {
        Ok(Factor::FnCall(FnCall::new(name, args, span)))
    }
}

impl Lexeme for Cast {
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        if parsed_rule.as_rule() != Rule::as_cast {
            return Err(grammar_bug(format!(
                "expected as_cast, got {:?}",
                parsed_rule.as_rule()
            )));
        }
        let span = span_of(&parsed_rule, file);
        let mut inner: Option<Factor> = None;
        let mut target: Option<String> = None;
        for child in parsed_rule.into_inner() {
            match child.as_rule() {
                Rule::factor => inner = Some(Factor::from_parsed_rule(child, file)?),
                Rule::type_ref => target = Some(type_ref_name(&child)),
                other => {
                    return Err(grammar_bug(format!(
                        "unexpected child of as_cast: {other:?}"
                    )));
                }
            }
        }
        let inner = inner.ok_or_else(|| grammar_bug("as_cast missing inner factor"))?;
        let target = target.ok_or_else(|| grammar_bug("as_cast missing target type"))?;
        Ok(Self::new(inner, target, span))
    }
}

/// `factor (op, factor)*` expression (left-associative pretty print).
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct Arithmetic {
    init: Factor,
    rest: Vec<(ArithmeticOperator, Factor)>,
    #[educe(PartialEq(ignore), Hash(ignore))]
    span: Span,
}

impl Arithmetic {
    #[must_use]
    pub fn new(init: Factor, rest: Vec<(ArithmeticOperator, Factor)>) -> Self {
        Self {
            init,
            rest,
            span: Span::DUMMY,
        }
    }

    /// A bare variable as an expression: `Factor::Var(name)` with no operators.
    #[must_use]
    pub fn var(name: &str) -> Self {
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

    pub fn init_mut(&mut self) -> &mut Factor {
        &mut self.init
    }

    pub fn rest_mut(&mut self) -> &mut [(ArithmeticOperator, Factor)] {
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

    /// `true` if a single constant with no ops.
    #[must_use]
    pub fn is_const(&self) -> bool {
        self.rest.is_empty() && self.init.is_const()
    }

    /// `true` if a single variable with no ops.
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
    /// Parse `factor (operator factor)*`.
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        let span = span_of(&parsed_rule, file);
        let mut inner = parsed_rule.into_inner();

        let init_pair = inner
            .next()
            .ok_or_else(|| grammar_bug("arithmetic missing initial factor"))?;
        let init = Factor::from_parsed_rule(init_pair, file)?;

        let mut rest = Vec::new();
        while let Some(op_pair) = inner.next() {
            let factor_pair = inner
                .next()
                .ok_or_else(|| grammar_bug("arithmetic expected (operator, factor) pair"))?;
            let op = ArithmeticOperator::from_parsed_rule(op_pair, file)?;
            let factor = Factor::from_parsed_rule(factor_pair, file)?;
            rest.push((op, factor));
        }

        Ok(Self { init, rest, span })
    }
}

#[cfg(test)]
mod tests {
    use ArithmeticOperator::Plus;
    use Factor::Var;

    use super::*;

    /// `vars()` preserves order and duplicates; `vars_set()` dedups. The
    /// two accessors exist because downstream passes need both: variable
    /// binding passes count occurrences (repeat = join predicate), while
    /// scope analysis needs the unique set. Collapsing either one into
    /// the other would silently break one of those callers.
    #[test]
    fn vars_preserves_dups_vars_set_dedups() {
        // x + x + y  →  vars = [x, x, y], vars_set = {x, y}
        let a = Arithmetic::new(
            Var("x".into()),
            vec![(Plus, Var("x".into())), (Plus, Var("y".into()))],
        );
        let x = "x".to_string();
        let y = "y".to_string();
        assert_eq!(a.vars(), vec![&x, &x, &y]);
        assert_eq!(a.vars_set().len(), 2);
    }

    /// A parenthesised sub-expression parses into `Factor::Group`,
    /// preserves its inner variables (in order), and round-trips through
    /// `Display` with its parentheses intact — the grouping must survive
    /// because arithmetic folds left-to-right with no operator precedence.
    #[test]
    fn parse_paren_group() {
        use pest::Parser;

        use crate::FlowLogParser;
        use crate::Rule;

        let mut pairs = FlowLogParser::parse(Rule::arithmetic_expr, "a * (b + c)").unwrap();
        let arith =
            Arithmetic::from_parsed_rule(pairs.next().unwrap(), flowlog_common::FileId::new(0))
                .unwrap();

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
    /// collapse to the bare factor at parse time — `(x)`, `("c")`, and
    /// `(f(x))` must behave exactly like their unparenthesised forms in
    /// fact detection, subtype narrowing, and assignment recognition.
    /// Nested parens around a multi-term expression collapse to one `Group`.
    #[test]
    fn parse_single_factor_group_collapses() {
        use pest::Parser;

        use crate::FlowLogParser;
        use crate::Rule;

        let parse = |src: &str| -> Factor {
            let mut pairs = FlowLogParser::parse(Rule::arithmetic_expr, src).unwrap();
            Arithmetic::from_parsed_rule(pairs.next().unwrap(), flowlog_common::FileId::new(0))
                .unwrap()
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
}
