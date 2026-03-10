//! Constant value types for FlowLog Datalog programs.

use crate::{Lexeme, Rule};
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
    ///
    /// Panics if:
    /// - no inner rule exists
    /// - number literal fails to parse as `i64`
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let inner = parsed_rule
            .into_inner()
            .next()
            .expect("Parser error: constant rule had no inner value");
        match inner.as_rule() {
            Rule::float => {
                let s = inner.as_str();
                Self::Float(OrderedFloat(
                    s.parse::<f64>()
                        .expect("Parser error: failed to parse float literal as f64"),
                ))
            }
            Rule::integer => {
                let s = inner.as_str();
                Self::Int(
                    s.parse::<i64>()
                        .expect("Parser error: failed to parse number literal as i64"),
                )
            }
            Rule::string => Self::Text(inner.as_str().trim_matches('"').to_string()),
            Rule::boolean => match inner.as_str() {
                "True" => Self::Bool(true),
                "False" => Self::Bool(false),
                other => unreachable!("Parser error: invalid boolean constant: {other}"),
            },
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
