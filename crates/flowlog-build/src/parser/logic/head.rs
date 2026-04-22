//! Rule heads for FlowLog Datalog programs.
//!
//! - [`HeadArg`]: `Var | Arith | Aggregation`
//! - [`Head`]: `rel(arg1, ..., argN)`

use super::{Aggregation, Arithmetic};
use crate::common::compute_fp;
use crate::common::{FileId, Ignored, Span};
use crate::parser::error::{grammar_bug, ParseError};
use crate::parser::{span_of, Lexeme, Rule};
use pest::iterators::Pair;
use std::fmt;

/// Argument in a rule head.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum HeadArg {
    /// Pass-through variable.
    Var(String),
    /// Arithmetic expression (includes UDF calls).
    Arith(Arithmetic),
    /// Aggregation (e.g., `count(X)`).
    Aggregation(Aggregation),
}

impl HeadArg {
    /// Variables referenced by this argument (order preserved, duplicates kept).
    #[must_use]
    pub(crate) fn vars(&self) -> Vec<&String> {
        match self {
            Self::Var(v) => vec![v],
            Self::Arith(a) => a.vars(),
            Self::Aggregation(agg) => agg.vars(),
        }
    }
}

impl fmt::Display for HeadArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Var(v) => write!(f, "{v}"),
            Self::Arith(a) => write!(f, "{a}"),
            Self::Aggregation(agg) => write!(f, "{agg}"),
        }
    }
}

impl Lexeme for HeadArg {
    /// Parse a head argument from the grammar.
    ///
    /// Optimization: if the arithmetic is a single variable (`is_var()`), emit `Var` instead of `Arith`.
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        let inner = parsed_rule
            .into_inner()
            .next()
            .ok_or_else(|| grammar_bug("head_arg missing inner token"))?;

        Ok(match inner.as_rule() {
            Rule::arithmetic_expr => {
                let arith = Arithmetic::from_parsed_rule(inner, file)?;
                if arith.is_var() {
                    let name = arith
                        .init()
                        .vars()
                        .into_iter()
                        .next()
                        .ok_or_else(|| grammar_bug("is_var() but no variable in init"))?
                        .clone();
                    Self::Var(name)
                } else {
                    Self::Arith(arith)
                }
            }
            Rule::aggregate_expr => Self::Aggregation(Aggregation::from_parsed_rule(inner, file)?),
            other => {
                return Err(grammar_bug(format!(
                    "unexpected rule for HeadArg: {other:?}"
                )))
            }
        })
    }
}

/// `rel(arg1, ..., argN)`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Head {
    name: String,
    head_fingerprint: u64,
    head_arguments: Vec<HeadArg>,
    span: Ignored<Span>,
}

impl Head {
    #[cfg(test)]
    pub(crate) fn new(name: String, head_arguments: Vec<HeadArg>) -> Self {
        let name = name.to_lowercase();
        let head_fingerprint = compute_fp(&name);
        Self {
            name,
            head_fingerprint,
            head_arguments,
            span: Ignored(Span::DUMMY),
        }
    }

    /// Source location this head was parsed from.
    #[must_use]
    #[inline]
    pub(crate) fn span(&self) -> Span {
        self.span.0
    }

    /// Relation name.
    #[must_use]
    #[inline]
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Head fingerprint.
    #[must_use]
    #[inline]
    pub(crate) fn head_fingerprint(&self) -> u64 {
        self.head_fingerprint
    }

    /// Arguments.
    #[must_use]
    #[inline]
    pub(crate) fn head_arguments(&self) -> &[HeadArg] {
        &self.head_arguments
    }

    #[inline]
    pub(crate) fn head_arguments_mut(&mut self) -> &mut [HeadArg] {
        &mut self.head_arguments
    }

    /// Arity (number of arguments).
    #[must_use]
    #[inline]
    pub(crate) fn arity(&self) -> usize {
        self.head_arguments.len()
    }
}

impl fmt::Display for Head {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name)?;
        let mut first = true;
        for arg in &self.head_arguments {
            if !first {
                write!(f, ", ")?;
            }
            first = false;
            write!(f, "{arg}")?;
        }
        write!(f, ")")
    }
}

impl Lexeme for Head {
    /// Parse `relation_name "(" (head_arg ("," head_arg)*)? ")"`.
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        let span = span_of(&parsed_rule, file);
        let mut inner = parsed_rule.into_inner();

        let name_pair = inner
            .next()
            .ok_or_else(|| grammar_bug("head missing relation name"))?;
        let name = name_pair.as_str().to_lowercase();
        let head_fingerprint = compute_fp(&name);

        let mut args = Vec::new();
        for pair in inner {
            if pair.as_rule() == Rule::head_arg {
                args.push(HeadArg::from_parsed_rule(pair, file)?);
            }
        }

        Ok(Self {
            name,
            head_fingerprint,
            head_arguments: args,
            span: Ignored(span),
        })
    }
}
