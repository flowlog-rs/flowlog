//! Function call expressions for FlowLog rule heads and body predicates.
//!
//! A [`FnCall`] represents a user-defined function applied to arguments
//! in a rule head or as a boolean predicate (e.g., `my_udf(x, y + 1)`).

use std::fmt;

use educe::Educe;
use pest::iterators::Pair;

use super::Arithmetic;
use crate::error::{ParseError, grammar_bug};
use crate::{Lexeme, Rule, span_of};
use flowlog_common::{FileId, Span};

/// A user-defined function call in a rule head or body predicates.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct FnCall {
    /// Function name.
    name: String,
    /// Arguments.
    args: Vec<Arithmetic>,
    /// Whether the result is negated (i.e. `!fn_name(...)`).
    is_negated: bool,
    #[educe(PartialEq(ignore), Hash(ignore))]
    span: Span,
}

impl FnCall {
    /// Create a new function call.
    #[must_use]
    pub fn new(name: String, args: Vec<Arithmetic>, is_negated: bool) -> Self {
        Self {
            name,
            args,
            is_negated,
            span: Span::DUMMY,
        }
    }

    /// Attach a source span to this call.
    #[must_use]
    pub fn with_span(mut self, span: Span) -> Self {
        self.span = span;
        self
    }

    /// Source location this call was parsed from.
    #[must_use]
    #[inline]
    pub fn span(&self) -> Span {
        self.span
    }

    /// Function name.
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Arguments.
    #[must_use]
    #[inline]
    pub fn args(&self) -> &[Arithmetic] {
        &self.args
    }

    #[inline]
    pub fn args_mut(&mut self) -> &mut [Arithmetic] {
        &mut self.args
    }

    /// Whether the UDF result is negated.
    #[must_use]
    #[inline]
    pub fn is_negated(&self) -> bool {
        self.is_negated
    }

    /// Variables referenced by this function call.
    #[must_use]
    pub fn vars(&self) -> Vec<&String> {
        self.args.iter().flat_map(|a| a.vars()).collect()
    }
}

impl fmt::Display for FnCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let args = self
            .args
            .iter()
            .map(|a| a.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        if self.is_negated {
            write!(f, "!{}({})", self.name, args)
        } else {
            write!(f, "{}({})", self.name, args)
        }
    }
}

impl Lexeme for FnCall {
    /// Parse a `fn_call_expr` grammar node into a [`FnCall`].
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        let span = span_of(&parsed_rule, file);
        let mut children = parsed_rule.into_inner();
        let fn_name = children
            .next()
            .ok_or_else(|| grammar_bug("fn_call_expr missing function name"))?
            .as_str()
            .to_string();
        let args = children
            .filter(|p| p.as_rule() == Rule::arithmetic_expr)
            .map(|p| Arithmetic::from_parsed_rule(p, file))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            name: fn_name,
            args,
            is_negated: false,
            span,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FlowLogParser, Lexeme, Rule};
    use flowlog_common::FileId;
    use pest::Parser;

    #[test]
    fn parse_fn_call_expr() {
        let input = "my_udf(x, y)";
        let mut pairs = FlowLogParser::parse(Rule::fn_call_expr, input).unwrap();
        let fc = FnCall::from_parsed_rule(pairs.next().unwrap(), FileId::new(0)).unwrap();
        assert_eq!(fc.name(), "my_udf");
        assert_eq!(fc.args().len(), 2);
        assert!(!fc.is_negated());
    }
}
