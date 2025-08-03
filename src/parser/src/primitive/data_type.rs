//! Data type definitions for FlowLog programs.

use std::fmt;
use std::str::FromStr;

/// Data types supported in FlowLog.
///
/// FlowLog supports two primitive data types for relation attributes, which
/// correspond to the basic types supported by the FlowLog language grammar.
/// These types define what kind of values can be stored in relation columns.
///
/// # Type Mapping
///
/// The FlowLog grammar uses different names for types compared to the internal
/// representation:
/// - `"number"` in grammar → [`DataType::Integer`] internally
/// - `"string"` in grammar → [`DataType::String`] internally
///
/// # Examples
///
/// ```rust
/// use std::str::FromStr;
/// use parser::primitive::DataType;
///
/// // Parse from FlowLog grammar strings
/// let int_type = DataType::from_str("number").unwrap();
/// let str_type = DataType::from_str("string").unwrap();
///
/// assert_eq!(int_type, DataType::Integer);
/// assert_eq!(str_type, DataType::String);
///
/// // Display back as grammar strings
/// assert_eq!(int_type.to_string(), "number");
/// assert_eq!(str_type.to_string(), "string");
/// ```
///
/// # Error Handling
///
/// Parsing invalid type strings will return an error:
///
/// ```rust
/// use std::str::FromStr;
/// use parser::primitive::DataType;
///
/// let result = DataType::from_str("invalid");
/// assert!(result.is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    /// Numeric data type for integers.
    ///
    /// Corresponds to the `"number"` type in FlowLog grammar.
    /// Used for storing integer values in relations.
    Integer,

    /// Text data type for strings.
    ///
    /// Corresponds to the `"string"` type in FlowLog grammar.
    /// Used for storing text values in relations.
    String,
}

impl FromStr for DataType {
    type Err = String;

    /// Parse a data type from its FlowLog grammar representation.
    ///
    /// # Arguments
    ///
    /// * `s` - The string representation from FlowLog grammar
    ///
    /// # Returns
    ///
    /// * `Ok(DataType)` - Successfully parsed data type
    /// * `Err(String)` - Error message for invalid type string
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::str::FromStr;
    /// use parser::primitive::DataType;
    ///
    /// assert_eq!(DataType::from_str("number").unwrap(), DataType::Integer);
    /// assert_eq!(DataType::from_str("string").unwrap(), DataType::String);
    /// assert!(DataType::from_str("invalid").is_err());
    /// ```
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "number" => Ok(Self::Integer),
            "string" => Ok(Self::String),
            _ => Err(format!(
                "Invalid data type: '{s}'. Expected 'number' or 'string'"
            )),
        }
    }
}

impl fmt::Display for DataType {
    /// Format the data type as its FlowLog grammar representation.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::primitive::DataType;
    ///
    /// assert_eq!(DataType::Integer.to_string(), "number");
    /// assert_eq!(DataType::String.to_string(), "string");
    /// ```
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

    #[test]
    fn test_from_str_valid() {
        assert_eq!(DataType::from_str("number").unwrap(), DataType::Integer);
        assert_eq!(DataType::from_str("string").unwrap(), DataType::String);
    }

    #[test]
    fn test_from_str_invalid() {
        let result = DataType::from_str("invalid");
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(error.contains("Invalid data type: 'invalid'"));
        assert!(error.contains("Expected 'number' or 'string'"));
    }

    #[test]
    fn test_display() {
        assert_eq!(DataType::Integer.to_string(), "number");
        assert_eq!(DataType::String.to_string(), "string");
    }

    #[test]
    fn test_round_trip() {
        let types = [DataType::Integer, DataType::String];

        for data_type in types {
            let string_repr = data_type.to_string();
            let parsed = DataType::from_str(&string_repr).unwrap();
            assert_eq!(data_type, parsed);
        }
    }

    #[test]
    fn test_copy_clone() {
        let original = DataType::Integer;
        let copied = original;
        let cloned = original.clone();

        assert_eq!(original, copied);
        assert_eq!(original, cloned);
    }

    #[test]
    fn test_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(DataType::Integer);
        set.insert(DataType::String);
        set.insert(DataType::Integer); // Duplicate

        assert_eq!(set.len(), 2);
    }
}
