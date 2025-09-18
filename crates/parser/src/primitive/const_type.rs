//! Constant value types for Macaron Datalog programs.

use crate::{error::ParserError, Lexeme, Result, Rule};
use pest::iterators::Pair;
use std::fmt;

/// A literal constant in a Macaron Datalog program.
///
/// Constants may appear in atom arguments, arithmetic expressions,
/// and comparisons. Currently supported:
/// - [`ConstType::Integer`] for 32-bit signed integers
/// - [`ConstType::Text`] for UTF-8 strings
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ConstType {
    /// 32-bit signed integer constant.
    Integer(i32),

    /// UTF-8 string constant.
    /// TODO: current unsupported in system, need to discuss
    Text(String),
}

impl fmt::Display for ConstType {
    /// Prints constants in Datalog syntax:
    /// integers as-is, strings with quotes.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Integer(v) => write!(f, "{v}"),
            Self::Text(s) => write!(f, "\"{s}\""),
        }
    }
}

impl Lexeme for ConstType {
    /// Parses a constant from the grammar.
    ///
    /// Return errors if:
    /// - no inner rule exists
    /// - number literal fails to parse as `i32`
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Result<Self> {
        let mut inner_iter = parsed_rule.into_inner();
        let inner = inner_iter
            .next()
            .ok_or_else(|| ParserError::IncompleteConstantRule("inner token".to_string()))?;
        match inner.as_rule() {
            Rule::integer => Ok(Self::Integer(
                inner
                    .as_str()
                    .parse()
                    .map_err(|_| ParserError::FailedToParseNumberLiteral)?,
            )),
            Rule::string => Ok(Self::Text(inner.as_str().trim_matches('"').to_string())),
            other => Err(ParserError::InvalidConstantRule(format!("{:?}", other))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_integer_golden() {
        assert_eq!(ConstType::Integer(42).to_string(), "42");
        assert_eq!(ConstType::Integer(-17).to_string(), "-17");
    }

    #[test]
    fn display_text_edge_cases() {
        let cases = [
            ("", "\"\""),
            ("hello world", "\"hello world\""),
            ("café 🦀", "\"café 🦀\""),
            ("say \"hello\"", "\"say \"hello\"\""),
        ];
        for (raw, expect) in cases {
            assert_eq!(ConstType::Text(raw.into()).to_string(), expect);
        }
    }

    #[test]
    fn equality_cross_type() {
        assert_ne!(ConstType::Integer(42), ConstType::Text("42".into()));
    }

    #[test]
    fn failed_to_parse_number_literal_overflow() {
        use crate::error::ParserError;
        use pest::Parser;

        let big = "9223372036854775807"; // > i32::MAX
        let pair = crate::MacaronParser::parse(Rule::constant, big)
            .unwrap()
            .next()
            .unwrap();
        let err = ConstType::from_parsed_rule(pair).unwrap_err();
        assert!(matches!(err, ParserError::FailedToParseNumberLiteral));
    }
}
