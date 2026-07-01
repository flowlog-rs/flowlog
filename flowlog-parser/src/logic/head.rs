//! Rule heads for FlowLog Datalog programs.
//!
//! - [`HeadArg`]: `Var | Arith | Aggregation`
//! - [`Head`]: `rel(arg1, ..., argN)`

use std::fmt;

use educe::Educe;
use flowlog_common::FileId;
use flowlog_common::Span;
use flowlog_common::compute_fp;
use pest::iterators::Pair;

use super::Aggregation;
use super::Arithmetic;
use crate::Lexeme;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;
use crate::span_of;

/// Argument in a rule head.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HeadArg {
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
    pub fn vars(&self) -> Vec<&String> {
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
                )));
            }
        })
    }
}

/// `rel(arg1, ..., argN)`
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct Head {
    name: String,
    #[educe(PartialEq(ignore), Hash(ignore))]
    raw_name: String,
    head_fingerprint: u64,
    head_arguments: Vec<HeadArg>,
    #[educe(PartialEq(ignore), Hash(ignore))]
    span: Span,
}

impl Head {
    #[cfg(test)]
    pub fn new(name: String, head_arguments: Vec<HeadArg>) -> Self {
        let raw_name = name.clone();
        let name = name.to_lowercase();
        let head_fingerprint = compute_fp(&name);
        Self {
            name,
            raw_name,
            head_fingerprint,
            head_arguments,
            span: Span::DUMMY,
        }
    }

    /// Rename in-place. Lowercases and refreshes the cached fingerprint.
    /// Leaves `raw_name` untouched.
    pub fn set_name(&mut self, name: String) {
        let lname = name.to_lowercase();
        self.head_fingerprint = compute_fp(&lname);
        self.name = lname;
    }

    /// Source location this head was parsed from.
    #[must_use]
    #[inline]
    pub fn span(&self) -> Span {
        self.span
    }

    /// Canonical relation name.
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Original surface spelling of the relation name as the user wrote it.
    #[must_use]
    #[inline]
    pub fn raw_name(&self) -> &str {
        &self.raw_name
    }

    /// Head fingerprint.
    #[must_use]
    #[inline]
    pub fn head_fingerprint(&self) -> u64 {
        self.head_fingerprint
    }

    /// Arguments.
    #[must_use]
    #[inline]
    pub fn head_arguments(&self) -> &[HeadArg] {
        &self.head_arguments
    }

    #[inline]
    pub fn head_arguments_mut(&mut self) -> &mut [HeadArg] {
        &mut self.head_arguments
    }

    /// Arity (number of arguments).
    #[must_use]
    #[inline]
    pub fn arity(&self) -> usize {
        self.head_arguments.len()
    }
}

impl fmt::Display for Head {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name)?;
        for (i, arg) in self.head_arguments.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
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
        let raw_name = name_pair.as_str().to_string();
        let name = raw_name.to_lowercase();
        let head_fingerprint = compute_fp(&name);

        let head_arguments: Vec<HeadArg> = inner
            .filter(|pair| pair.as_rule() == Rule::head_arg)
            .map(|pair| HeadArg::from_parsed_rule(pair, file))
            .collect::<Result<_, _>>()?;

        Ok(Self {
            name,
            raw_name,
            head_fingerprint,
            head_arguments,
            span,
        })
    }
}
