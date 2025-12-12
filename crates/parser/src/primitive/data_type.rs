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
    /// Boolean type.
    Boolean,
}

impl DataType {
    /// Infer a concrete type from an expected type (`self`) and an optional observed type (`other`)
    /// under the “wildcard boolean” rule.
    ///
    /// Rules:
    /// - If `is_nullary` is `true`:
    ///   - if `other` is `Some(t)`, require `self.matches(t)`; otherwise return `None`
    ///   - return `Some(Boolean)` (nullary relations are treated as boolean-valued)
    /// - Otherwise:
    ///   - if `other` is `None`, return `Some(self)`
    ///   - if `other` is `Some(t)` and not matched, return `None`
    ///   - if matched, prefer the non-`Boolean` type; if both `Boolean`, return `Boolean`
    #[inline]
    pub fn inference(self, other: Option<DataType>, is_nullary: bool) -> Option<DataType> {
        if is_nullary {
            if let Some(t) = other {
                if !self.matches(t) {
                    return None;
                }
            }
            return Some(DataType::Boolean);
        }

        match other {
            None => Some(self),
            Some(t) => {
                if !self.matches(t) {
                    return None;
                }
                Some(match (self, t) {
                    (DataType::Boolean, x) => x,
                    (x, DataType::Boolean) => x,
                    (x, _) => x, // both non-boolean (and must be equal due to matches)
                })
            }
        }
    }

    /// Returns `true` if these two types are compatible under FlowLog’s “wildcard boolean” rule.
    ///
    /// In this matching relation, `Boolean` acts like a wildcard type:
    /// - if either side is `Boolean`, the types are considered compatible
    /// - otherwise, the types must be exactly equal (`Integer` with `Integer`, `String` with `String`)
    fn matches(self, other: Self) -> bool {
        self == DataType::Boolean || other == DataType::Boolean || self == other
    }
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
            Self::Boolean => "boolean",
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
