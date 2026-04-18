//! Constant value types for FlowLog Datalog programs.

use crate::error::{grammar_bug, ParseError};
use crate::{Lexeme, Rule};
use common::source::FileId;
use ordered_float::OrderedFloat;
use pest::iterators::Pair;
use std::fmt;

/// A literal constant in a FlowLog Datalog program.
///
/// Constants may appear in atom arguments, arithmetic expressions,
/// and comparisons. Currently supported:
/// - [`ConstType::Int`] for integer constants (stored as i64)
/// - [`ConstType::Text`] for UTF-8 strings
/// - [`ConstType::Bool`] for boolean values
///
/// Integer literals are parsed into a single `i64` representation.
/// The actual column width (i8, i16, i32, i64) is determined by relation
/// declarations. Codegen emits unsuffixed literals so Rust infers the
/// correct type from context.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ConstType {
    /// Integer constant (stored as i64).
    Int(i64),

    /// Floating-point constant (stored as OrderedFloat<f64>).
    Float(OrderedFloat<f64>),

    /// UTF-8 string constant.
    Text(String),

    /// Boolean constant.
    Bool(bool),
}

impl fmt::Display for ConstType {
    /// Prints constants in Datalog syntax:
    /// integers as-is, strings with quotes.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int(v) => write!(f, "{v}"),
            Self::Float(v) => write!(f, "{v}"),
            Self::Text(s) => write!(f, "\"{s}\""),
            Self::Bool(b) => write!(f, "{}", if *b { "True" } else { "False" }),
        }
    }
}

impl Lexeme for ConstType {
    /// Parses a constant from the grammar.
    ///
    /// Integer literals are parsed directly as `i64`.
    fn from_parsed_rule(parsed_rule: Pair<Rule>, _file: FileId) -> Result<Self, ParseError> {
        let inner = parsed_rule
            .into_inner()
            .next()
            .ok_or_else(|| grammar_bug("constant rule had no inner value"))?;
        Ok(match inner.as_rule() {
            Rule::float => {
                let s = inner.as_str();
                let v = s
                    .parse::<f64>()
                    .map_err(|e| grammar_bug(format!("failed to parse float literal: {e}")))?;
                Self::Float(OrderedFloat(v))
            }
            Rule::integer => {
                let s = inner.as_str();
                let v = s
                    .parse::<i64>()
                    .map_err(|e| grammar_bug(format!("failed to parse integer literal: {e}")))?;
                Self::Int(v)
            }
            Rule::string => Self::Text(inner.as_str().trim_matches('"').to_string()),
            Rule::boolean => match inner.as_str() {
                "True" => Self::Bool(true),
                "False" => Self::Bool(false),
                other => {
                    return Err(grammar_bug(format!("invalid boolean constant: {other}")));
                }
            },
            other => {
                return Err(grammar_bug(format!(
                    "unexpected constant rule variant: {other:?}"
                )));
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_integer_golden() {
        assert_eq!(ConstType::Int(42).to_string(), "42");
        assert_eq!(ConstType::Int(10995116277782).to_string(), "10995116277782");
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
        assert_ne!(ConstType::Int(42), ConstType::Text("42".into()));
    }
}
