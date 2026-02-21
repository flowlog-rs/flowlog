//! Data type definitions for FlowLog Datalog programs.

use std::fmt;
use std::str::FromStr;

/// Data types supported in FlowLog Datalog programs.
///
/// These types correspond to the primitive types in the FlowLog grammar:
/// - `"int32"` → [`DataType::Int32`]
/// - `"int64"` → [`DataType::Int64`]
/// - `"string"` → [`DataType::String`]
///
/// They are used as attribute types in relations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    /// 32-bit signed integer type.
    Int32,
    /// 64-bit signed integer type.
    Int64,
    /// UTF-8 string type.
    String,
}

impl FromStr for DataType {
    type Err = String;

    /// Parse a [`DataType`] from its grammar string representation.
    ///
    /// Returns `Err` if the string is not `"int32"`, `"int64"`, or `"string"`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "int32" => Ok(Self::Int32),
            "int64" => Ok(Self::Int64),
            "string" => Ok(Self::String),
            _ => Err(format!(
                "Parser error: '{s}'. Invalid data type, expected 'int32', 'int64', or 'string'"
            )),
        }
    }
}

impl fmt::Display for DataType {
    /// Returns the grammar string representation of this type.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let type_str = match self {
            Self::Int32 => "int32",
            Self::Int64 => "int64",
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
        for t in [DataType::Int32, DataType::Int64, DataType::String] {
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
