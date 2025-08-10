//! Data type definitions for Datalog programs (Macaron engine).

use std::fmt;
use std::str::FromStr;

/// Data types supported in Datalog programs.
///
/// Macaron supports two primitive data types for relation attributes, which
/// correspond to the basic types supported by the Datalog grammar.
/// These types define what kind of values can be stored in relation columns.
///
/// # Type Mapping
///
/// The Macaron grammar uses different names for types compared to the internal
/// representation:
/// - `"integer"` in grammar → [`DataType::Integer`] internally
/// - `"string"` in grammar → [`DataType::String`] internally
///
/// # Examples
///
/// ```rust
/// use std::str::FromStr;
/// use parser::primitive::DataType;
///
/// // Parse from Macaron grammar strings
/// let int_type = DataType::from_str("integer").unwrap();
/// let str_type = DataType::from_str("string").unwrap();
///
/// assert_eq!(int_type, DataType::Integer);
/// assert_eq!(str_type, DataType::String);
///
/// // Display back as grammar strings
/// assert_eq!(int_type.to_string(), "integer");
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
    /// Corresponds to the `"integer"` type in Macaron grammar.
    /// Used for storing integer values in relations.
    Integer,

    /// Text data type for strings.
    ///
    /// Corresponds to the `"string"` type in Macaron grammar.
    /// Used for storing text values in relations.
    String,
}

impl FromStr for DataType {
    type Err = String;

    /// Parse a data type from its Macaron grammar representation.
    ///
    /// # Arguments
    ///
    /// * `s` - The string representation from Macaron grammar
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
    /// assert_eq!(DataType::from_str("integer").unwrap(), DataType::Integer);
    /// assert_eq!(DataType::from_str("string").unwrap(), DataType::String);
    /// assert!(DataType::from_str("invalid").is_err());
    /// ```
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "integer" => Ok(Self::Integer),
            "string" => Ok(Self::String),
            _ => Err(format!(
                "Invalid data type: '{s}'. Expected 'integer' or 'string'"
            )),
        }
    }
}

impl fmt::Display for DataType {
    /// Format the data type as its Macaron grammar representation.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::primitive::DataType;
    ///
    /// assert_eq!(DataType::Integer.to_string(), "integer");
    /// assert_eq!(DataType::String.to_string(), "string");
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let type_str = match self {
            Self::Integer => "integer",
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
    assert_eq!(DataType::from_str("integer").unwrap(), DataType::Integer);
        assert_eq!(DataType::from_str("string").unwrap(), DataType::String);
    }

    #[test]
    fn test_from_str_invalid() {
        let result = DataType::from_str("invalid");
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(error.contains("Invalid data type: 'invalid'"));
    assert!(error.contains("Expected 'integer' or 'string'"));
    }

    #[test]
    fn test_display() {
    assert_eq!(DataType::Integer.to_string(), "integer");
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
