//! Data type definitions for FlowLog Datalog programs.

use std::fmt;
use std::str::FromStr;

/// Data types supported in FlowLog Datalog programs.
///
/// These types correspond to the primitive types in the FlowLog grammar:
/// - `"int8"` → [`DataType::Int8`]
/// - `"int16"` → [`DataType::Int16`]
/// - `"int32"` / `"number"` → [`DataType::Int32`]
/// - `"int64"` → [`DataType::Int64`]
/// - `"uint8"` → [`DataType::UInt8`]
/// - `"uint16"` → [`DataType::UInt16`]
/// - `"uint32"` / `"unsigned"` → [`DataType::UInt32`]
/// - `"uint64"` → [`DataType::UInt64`]
/// - `"f32"` / `"float"` → [`DataType::Float32`]
/// - `"f64"` → [`DataType::Float64`]
/// - `"string"` / `"symbol"` → [`DataType::String`]
/// - `"bool"` → [`DataType::Bool`]
///
/// They are used as attribute types in relations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    /// 8-bit signed integer type.
    Int8,
    /// 16-bit signed integer type.
    Int16,
    /// 32-bit signed integer type.
    Int32,
    /// 64-bit signed integer type.
    Int64,
    /// 8-bit unsigned integer type.
    UInt8,
    /// 16-bit unsigned integer type.
    UInt16,
    /// 32-bit unsigned integer type.
    UInt32,
    /// 64-bit unsigned integer type.
    UInt64,
    /// 32-bit floating point type.
    Float32,
    /// 64-bit floating point type.
    Float64,
    /// UTF-8 string type.
    String,
    /// Boolean type.
    Bool,
}

impl FromStr for DataType {
    type Err = String;

    /// Parse a [`DataType`] from its grammar string representation.
    ///
    /// Accepted values: `"int8"`, `"int16"`, `"int32"` (alias `"number"`), `"int64"`,
    /// `"uint8"`, `"uint16"`, `"uint32"` (alias `"unsigned"`), `"uint64"`,
    /// `"f32"` (alias `"float"`), `"f64"`,
    /// `"string"` (alias `"symbol"`), `"bool"`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "int8" => Ok(Self::Int8),
            "int16" => Ok(Self::Int16),
            "int32" | "number" => Ok(Self::Int32),
            "int64" => Ok(Self::Int64),
            "uint8" => Ok(Self::UInt8),
            "uint16" => Ok(Self::UInt16),
            "uint32" | "unsigned" => Ok(Self::UInt32),
            "uint64" => Ok(Self::UInt64),
            "f32" | "float" => Ok(Self::Float32),
            "f64" => Ok(Self::Float64),
            "string" | "symbol" => Ok(Self::String),
            "bool" => Ok(Self::Bool),
            _ => Err(format!("Parser error: '{s}'. Invalid data type")),
        }
    }
}

impl fmt::Display for DataType {
    /// Returns the grammar string representation of this type.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let type_str = match self {
            Self::Int8 => "int8",
            Self::Int16 => "int16",
            Self::Int32 => "int32",
            Self::Int64 => "int64",
            Self::UInt8 => "uint8",
            Self::UInt16 => "uint16",
            Self::UInt32 => "uint32",
            Self::UInt64 => "uint64",
            Self::Float32 => "f32",
            Self::Float64 => "f64",
            Self::String => "string",
            Self::Bool => "bool",
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
        for t in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::String,
            DataType::Bool,
        ] {
            let s = t.to_string();
            let parsed = DataType::from_str(&s).unwrap();
            assert_eq!(t, parsed);
        }
    }

    #[test]
    fn number_alias_parses_as_int32() {
        let parsed = DataType::from_str("number").unwrap();
        assert_eq!(parsed, DataType::Int32);
    }

    #[test]
    fn symbol_alias_parses_as_string() {
        let parsed = DataType::from_str("symbol").unwrap();
        assert_eq!(parsed, DataType::String);
    }

    #[test]
    fn unsigned_alias_parses_as_uint32() {
        let parsed = DataType::from_str("unsigned").unwrap();
        assert_eq!(parsed, DataType::UInt32);
    }

    #[test]
    fn float_alias_parses_as_float32() {
        let parsed = DataType::from_str("float").unwrap();
        assert_eq!(parsed, DataType::Float32);
    }

    #[test]
    fn from_str_invalid_returns_err() {
        let err = DataType::from_str("invalid").unwrap_err();
        assert!(err.contains("Invalid data type"));
    }
}
