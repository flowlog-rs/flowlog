//! Data type definitions for FlowLog Datalog programs.

use std::fmt;
use std::str::FromStr;

/// Data types supported in FlowLog Datalog programs.
///
/// These types correspond to the primitive types in the FlowLog grammar:
/// - `"number"` → [`DataType::Integer`]
/// - `"string"` → [`DataType::String`]
///
/// They are used as attribute types in relations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    /// 32-bit integer type.
    Integer,
    /// UTF-8 string type.
    String,
}

impl FromStr for DataType {
    type Err = String;

    /// Parse a [`DataType`] from its grammar string representation.
    ///
    /// Returns `Err` if the string is not `"number"` or `"string"`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "number" => Ok(Self::Integer),
            "string" => Ok(Self::String),
            _ => Err(format!(
                "Parser error: '{s}'. Invalid data type, expected 'number' or 'string'"
            )),
        }
    }
}

impl fmt::Display for DataType {
    /// Returns the grammar string representation of this type.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let type_str = match self {
            Self::Integer => "number",
            Self::String => "string",
        };
        write!(f, "{type_str}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn display_roundtrip() {
        for t in [DataType::Integer, DataType::String] {
            let s = t.to_string();
            let parsed = DataType::from_str(&s).unwrap();
            assert_eq!(t, parsed);
        }
    }

    #[test]
    fn from_str_invalid_returns_err() {
        let err = DataType::from_str("invalid").unwrap_err();
        assert!(err.contains("Invalid data type"));
    }
}
