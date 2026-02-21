//! Constant value types for FlowLog Datalog programs.

use crate::{Lexeme, Rule};
use pest::iterators::Pair;
use std::fmt;

/// A literal constant in a FlowLog Datalog program.
///
/// Constants may appear in atom arguments, arithmetic expressions,
/// and comparisons. Currently supported:
/// - [`ConstType::Int32`] for 32-bit signed integers
/// - [`ConstType::Int64`] for 64-bit signed integers
/// - [`ConstType::Text`] for UTF-8 strings
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ConstType {
    /// 32-bit signed integer constant.
    Int32(i32),

    /// 64-bit signed integer constant.
    Int64(i64),

    /// UTF-8 string constant.
    Text(String),
}

impl fmt::Display for ConstType {
    /// Prints constants in Datalog syntax:
    /// integers as-is, strings with quotes.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int32(v) => write!(f, "{v}"),
            Self::Int64(v) => write!(f, "{v}"),
            Self::Text(s) => write!(f, "\"{s}\""),
        }
    }
}

impl Lexeme for ConstType {
    /// Parses a constant from the grammar.
    ///
    /// Integer literals are first tried as `i32`; if they overflow,
    /// they are promoted to `i64`.
    ///
    /// Panics if:
    /// - no inner rule exists
    /// - number literal fails to parse as either `i32` or `i64`
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let inner = parsed_rule
            .into_inner()
            .next()
            .expect("Parser error: constant rule had no inner value");
        match inner.as_rule() {
            Rule::integer => {
                let s = inner.as_str();
                // Try i32 first; promote to i64 on overflow.
                if let Ok(v) = s.parse::<i32>() {
                    Self::Int32(v)
                } else {
                    Self::Int64(
                        s.parse::<i64>()
                            .expect("Parser error: failed to parse number literal as i64"),
                    )
                }
            }
            Rule::string => Self::Text(inner.as_str().trim_matches('"').to_string()),
            _ => unreachable!(
                "Parser error: unexpected constant rule variant {:?}",
                inner.as_rule()
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_integer_golden() {
        assert_eq!(ConstType::Int32(42).to_string(), "42");
        assert_eq!(
            ConstType::Int64(10995116277782).to_string(),
            "10995116277782"
        );
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
        assert_ne!(ConstType::Int32(42), ConstType::Text("42".into()));
        assert_ne!(ConstType::Int32(42), ConstType::Int64(42));
    }
}
