//! Arithmetic expressions for FlowLog Datalog Programs.
//!
//! - [`ArithmeticOperator`]: `+ | - | * | / | %`
//! - [`Factor`]: variables or constants
//! - [`Arithmetic`]: `factor (op, factor)*`

use std::collections::HashSet;
use std::fmt;

use pest::iterators::Pair;

use super::{BuiltinCall, FnCall};
use crate::common::{FileId, Ignored, Span};
use crate::parser::error::{ParseError, grammar_bug};
use crate::parser::primitive::ConstType;
use crate::parser::{Lexeme, Rule, span_of, type_ref_name};

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
pub(crate) enum Factor {
    Var(String),
    Const(ConstType),
    /// User `.extern fn` call.
    FnCall(FnCall),
    /// Engine built-in (Soufflé-style intrinsic).
    Builtin(BuiltinCall),
    /// `as(factor, T)`. Runtime no-op; the typechecker lowers it away
    /// after validating the cast.
    Cast(Box<Cast>),
}

/// `as(factor, target_type)`. `inner` is a single [`Factor`] (not a
/// full [`Arithmetic`]) so the typechecker can lower `Cast(inner)` to
/// `inner` after subtype validation — downstream never sees a cast.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Cast {
    inner: Box<Factor>,
    /// User-written target type name; resolved by the typechecker.
    target_type: String,
    span: Ignored<Span>,
}

impl Cast {
    #[must_use]
    pub(crate) fn new(inner: Factor, target_type: String, span: Span) -> Self {
        Self {
            inner: Box::new(inner),
            target_type,
            span: Ignored(span),
        }
    }

    #[must_use]
    #[inline]
    pub(crate) fn inner(&self) -> &Factor {
        &self.inner
    }

    #[inline]
    pub(crate) fn inner_mut(&mut self) -> &mut Factor {
        &mut self.inner
    }

    #[must_use]
    #[inline]
    pub(crate) fn target_type(&self) -> &str {
        &self.target_type
    }

    #[must_use]
    #[inline]
    pub(crate) fn span(&self) -> Span {
        self.span.0
    }
}

impl fmt::Display for Cast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "as({}, {})", self.inner, self.target_type)
    }
}

impl Factor {
    #[must_use]
    pub(crate) fn is_var(&self) -> bool {
        matches!(self, Self::Var(_))
    }

    #[must_use]
    pub(crate) fn is_const(&self) -> bool {
        matches!(self, Self::Const(_))
    }

    /// Variables appearing in this factor.
    #[must_use]
    pub(crate) fn vars(&self) -> Vec<&String> {
        match self {
            Self::Var(v) => vec![v],
            Self::Const(_) => vec![],
            Self::FnCall(fc) => fc.vars(),
            Self::Builtin(bc) => bc.vars(),
            Self::Cast(c) => c.inner().vars(),
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
            Rule::builtin_fn_call => Self::Builtin(BuiltinCall::from_parsed_rule(inner, file)?),
            Rule::fn_call_expr => Self::FnCall(FnCall::from_parsed_rule(inner, file)?),
            Rule::variable => Self::Var(inner.as_str().to_string()),
            Rule::constant => Self::Const(ConstType::from_parsed_rule(inner, file)?),
            other => return Err(grammar_bug(format!("invalid factor rule: {other:?}"))),
        })
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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Arithmetic {
    init: Factor,
    rest: Vec<(ArithmeticOperator, Factor)>,
    span: Ignored<Span>,
}

impl Arithmetic {
    #[must_use]
    pub(crate) fn new(init: Factor, rest: Vec<(ArithmeticOperator, Factor)>) -> Self {
        Self {
            init,
            rest,
            span: Ignored(Span::DUMMY),
        }
    }

    /// Source location this expression was parsed from (`Span::DUMMY` for
    /// nodes synthesized without a concrete source range).
    #[must_use]
    #[inline]
    pub(crate) fn span(&self) -> Span {
        self.span.0
    }

    /// First term.
    #[must_use]
    pub(crate) fn init(&self) -> &Factor {
        &self.init
    }

    /// Remaining `(op, factor)` pairs.
    #[must_use]
    pub(crate) fn rest(&self) -> &[(ArithmeticOperator, Factor)] {
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
    pub(crate) fn vars(&self) -> Vec<&String> {
        let mut out = self.init.vars();
        for (_, f) in &self.rest {
            out.extend(f.vars());
        }
        out
    }

    /// Unique variables (deduplicated).
    #[must_use]
    pub(crate) fn vars_set(&self) -> HashSet<&String> {
        self.vars().into_iter().collect()
    }

    /// `true` if a single constant with no ops.
    #[must_use]
    pub(crate) fn is_const(&self) -> bool {
        self.rest.is_empty() && self.init.is_const()
    }

    /// `true` if a single variable with no ops.
    #[must_use]
    pub(crate) fn is_var(&self) -> bool {
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
    /// Parse `factor (operator factor)*`, optionally wrapped in a single
    /// pair of boundary parentheses.
    ///
    /// The grammar exposes `arithmetic_expr = { paren_arith |
    /// unparen_arith }`. Parens at the boundary of an arithmetic
    /// expression are pure grouping with no semantic effect (the
    /// engine's arithmetic has no operator precedence), so we strip
    /// them here and recurse into the inner expression. This keeps
    /// the AST flat — downstream stages never see a `Paren` node.
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        let span = span_of(&parsed_rule, file);

        // The `arithmetic_expr` rule wraps exactly one of
        // `paren_arith` or `unparen_arith`; peel that wrapper first.
        let body = match parsed_rule.as_rule() {
            Rule::arithmetic_expr => parsed_rule
                .into_inner()
                .next()
                .ok_or_else(|| grammar_bug("arithmetic_expr missing inner alternative"))?,
            _ => parsed_rule,
        };

        match body.as_rule() {
            Rule::paren_arith => {
                let inner = body
                    .into_inner()
                    .next()
                    .ok_or_else(|| grammar_bug("paren_arith missing inner arithmetic"))?;
                let mut nested = Self::from_parsed_rule(inner, file)?;
                // Preserve the *outer* span (including the parens) so
                // diagnostics point at the user-visible source range.
                nested.span = Ignored(span);
                Ok(nested)
            }
            Rule::unparen_arith => Self::from_unparen(body, file, span),
            other => Err(grammar_bug(format!(
                "expected paren_arith or unparen_arith, got {other:?}"
            ))),
        }
    }
}

impl Arithmetic {
    /// Parse the parenless body `factor (op factor)*`.
    fn from_unparen(
        parsed_rule: Pair<Rule>,
        file: FileId,
        span: Span,
    ) -> Result<Self, ParseError> {
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

        Ok(Self {
            init,
            rest,
            span: Ignored(span),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ArithmeticOperator::Plus;
    use Factor::Var;

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
}
