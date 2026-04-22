//! Arithmetic expressions for FlowLog Datalog Programs.
//!
//! - [`ArithmeticOperator`]: `+ | - | * | / | %`
//! - [`Factor`]: variables or constants
//! - [`Arithmetic`]: `factor (op, factor)*`

use super::FnCall;
use crate::parser::error::{grammar_bug, ParseError};
use crate::parser::primitive::ConstType;
use crate::parser::{span_of, Lexeme, Rule};

use crate::common::{FileId, Ignored, Span};
use pest::iterators::Pair;
use std::collections::HashSet;
use std::fmt;

/// Arithmetic operator.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum ArithmeticOperator {
    Plus,     // +
    Minus,    // -
    Multiply, // *
    Divide,   // /
    Modulo,   // %
    Cat,      // string concatenation
}

impl fmt::Display for ArithmeticOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sym = match self {
            Self::Plus => "+",
            Self::Minus => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::Modulo => "%",
            Self::Cat => "cat",
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
            Rule::cat => Self::Cat,
            other => {
                return Err(grammar_bug(format!(
                    "unknown arithmetic operator: {other:?}"
                )))
            }
        })
    }
}

/// Atomic operand for arithmetic: variable, constant, or function call.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Factor {
    Var(String),
    Const(ConstType),
    /// User-defined function call (e.g., `transform(x, y + 1)`).
    FnCall(FnCall),
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
        }
    }
}

impl fmt::Display for Factor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Var(v) => write!(f, "{v}"),
            Self::Const(c) => write!(f, "{c}"),
            Self::FnCall(fc) => write!(f, "{fc}"),
        }
    }
}

impl Lexeme for Factor {
    /// Parse a factor (variable, constant, or function call).
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        let inner = parsed_rule
            .into_inner()
            .next()
            .ok_or_else(|| grammar_bug("factor missing inner token"))?;
        Ok(match inner.as_rule() {
            Rule::fn_call_expr => Self::FnCall(FnCall::from_parsed_rule(inner, file)?),
            Rule::variable => Self::Var(inner.as_str().to_string()),
            Rule::constant => Self::Const(ConstType::from_parsed_rule(inner, file)?),
            other => return Err(grammar_bug(format!("invalid factor rule: {other:?}"))),
        })
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
