//! Primitive types for the FlowLog parser.
//!
//! This module contains the fundamental data types used throughout the parser,
//! including data type specifications and constant values.

use pest::iterators::Pair;
use std::fmt;
use std::str::FromStr;

use super::{Lexeme, Rule};

/// Data types supported in FlowLog.
///
/// FlowLog supports two primitive data types for relation attributes:
/// - `Integer`: Numeric values (mapped from "number" in grammar)
/// - `String`: Text values (mapped from "string" in grammar)
///
/// # Examples
///
/// ```
/// use std::str::FromStr;
/// use parser::DataType;
///
/// let int_type = DataType::from_str("number").unwrap();
/// let str_type = DataType::from_str("string").unwrap();
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    /// Numeric data type for integers
    Integer,
    /// Text data type for strings  
    String,
}

impl FromStr for DataType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "number" => Ok(Self::Integer),
            "string" => Ok(Self::String),
            _ => Err(format!("Invalid data type: '{s}'")),
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let type_str = match self {
            Self::Integer => "number",
            Self::String => "string",
        };
        write!(f, "{type_str}")
    }
}

/// A constant value used in expressions and atoms.
///
/// Constants represent literal values that can appear in FlowLog programs.
/// They are used as arguments to atoms and in arithmetic expressions.
///
/// # Examples
/// ```
/// use parser::Const;
/// let int_const = Const::Integer(42);
/// let text_const = Const::Text("hello".to_string());
///
/// println!("{}", int_const);  // Prints: 42
/// println!("{}", text_const); // Prints: "hello"
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Const {
    /// An integer constant
    Integer(i32),
    /// A string constant
    Text(String),
}

impl fmt::Display for Const {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Integer(value) => write!(f, "{value}"),
            Self::Text(text) => write!(f, "\"{text}\""),
        }
    }
}

impl Lexeme for Const {
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let inner = parsed_rule
            .into_inner()
            .next()
            .expect("Expected inner rule for constant");

        match inner.as_rule() {
            Rule::integer => {
                let value = inner
                    .as_str()
                    .parse::<i32>()
                    .expect("Failed to parse integer constant");
                Self::Integer(value)
            }
            Rule::string => {
                let text = inner.as_str().trim_matches('"').to_string();
                Self::Text(text)
            }
            _ => unreachable!("Invalid constant type: {:?}", inner.as_rule()),
        }
    }
}
