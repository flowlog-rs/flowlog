//! Constant value types for FlowLog programs.

use crate::{Lexeme, Rule};
use pest::iterators::Pair;
use std::fmt;

/// A constant value used in FlowLog.
///
/// Constants represent literal values that can appear in FlowLog programs.
/// They are used as arguments to atoms, in arithmetic expressions, and
/// anywhere a literal value is needed in the language.
///
/// # Supported Types
///
/// Currently, FlowLog supports two types of constants:
/// - [`ConstType::Integer`]: 32-bit signed integer values
/// - [`ConstType::Text`]: UTF-8 string values
///
/// # Examples
///
/// ```rust
/// use parser::primitive::ConstType;
///
/// // Creating constants
/// let int_const = ConstType::Integer(42);
/// let text_const = ConstType::Text("hello world".to_string());
///
/// // Display formatting
/// println!("{}", int_const);  // Prints: 42
/// println!("{}", text_const); // Prints: "hello world"
///
/// // Equality and hashing
/// assert_eq!(ConstType::Integer(5), ConstType::Integer(5));
/// assert_ne!(ConstType::Integer(5), ConstType::Integer(10));
/// ```
///
/// # Usage in FlowLog
///
/// Constants are typically used in:
/// - Atom arguments: `myRelation(42, "test")`
/// - Arithmetic expressions: `X + 10`
/// - Comparison operations: `Y = "value"`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ConstType {
    /// An integer constant.
    ///
    /// Represents a 32-bit signed integer value that can be used
    /// in arithmetic expressions and as atom arguments.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::primitive::ConstType;
    ///
    /// let positive = ConstType::Integer(42);
    /// let negative = ConstType::Integer(-17);
    /// let zero = ConstType::Integer(0);
    /// ```
    Integer(i32),

    /// A string constant.
    ///
    /// Represents a UTF-8 text value that can be used as atom arguments
    /// and in string comparisons. The string is stored without quotes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::primitive::ConstType;
    ///
    /// let text = ConstType::Text("hello".to_string());
    /// let empty = ConstType::Text(String::new());
    /// let unicode = ConstType::Text("café 🦀".to_string());
    /// ```
    Text(String),
}

impl fmt::Display for ConstType {
    /// Format the constant value for display.
    ///
    /// Integer constants are displayed as-is, while text constants
    /// are displayed with surrounding double quotes to distinguish
    /// them from variables or other identifiers.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::primitive::ConstType;
    ///
    /// let int_const = ConstType::Integer(42);
    /// let text_const = ConstType::Text("hello".to_string());
    ///
    /// assert_eq!(int_const.to_string(), "42");
    /// assert_eq!(text_const.to_string(), "\"hello\"");
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Integer(value) => write!(f, "{value}"),
            Self::Text(text) => write!(f, "\"{text}\""),
        }
    }
}

impl Lexeme for ConstType {
    /// Parse a constant value from a Pest parse rule.
    ///
    /// This method converts a parsed constant rule from the FlowLog grammar
    /// into a [`ConstType`] instance. It handles both integer and string
    /// constants, properly parsing and validating the values.
    ///
    /// # Arguments
    ///
    /// * `parsed_rule` - A Pest parse result containing a constant rule
    ///
    /// # Returns
    ///
    /// A [`ConstType`] instance representing the parsed constant value.
    ///
    /// # Panics
    ///
    /// This method will panic if:
    /// - The parse rule doesn't contain an expected inner rule
    /// - An integer constant cannot be parsed as a valid `i32`
    /// - The rule type is not recognized as a constant type
    ///
    /// # Examples
    ///
    /// This method is typically used internally by the parser when
    /// processing FlowLog source code, not called directly by users.
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
                // Remove surrounding quotes from the string literal
                let text = inner.as_str().trim_matches('"').to_string();
                Self::Text(text)
            }
            _ => unreachable!("Invalid constant type: {:?}", inner.as_rule()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_display() {
        let positive = ConstType::Integer(42);
        let negative = ConstType::Integer(-17);
        let zero = ConstType::Integer(0);

        assert_eq!(positive.to_string(), "42");
        assert_eq!(negative.to_string(), "-17");
        assert_eq!(zero.to_string(), "0");
    }

    #[test]
    fn test_text_display() {
        let simple = ConstType::Text("hello".to_string());
        let empty = ConstType::Text(String::new());
        let with_spaces = ConstType::Text("hello world".to_string());
        let with_quotes = ConstType::Text("say \"hello\"".to_string());

        assert_eq!(simple.to_string(), "\"hello\"");
        assert_eq!(empty.to_string(), "\"\"");
        assert_eq!(with_spaces.to_string(), "\"hello world\"");
        assert_eq!(with_quotes.to_string(), "\"say \"hello\"\"");
    }

    #[test]
    fn test_equality() {
        let int1 = ConstType::Integer(42);
        let int2 = ConstType::Integer(42);
        let int3 = ConstType::Integer(24);

        assert_eq!(int1, int2);
        assert_ne!(int1, int3);

        let text1 = ConstType::Text("hello".to_string());
        let text2 = ConstType::Text("hello".to_string());
        let text3 = ConstType::Text("world".to_string());

        assert_eq!(text1, text2);
        assert_ne!(text1, text3);

        // Different types should not be equal
        assert_ne!(ConstType::Integer(42), ConstType::Text("42".to_string()));
    }

    #[test]
    fn test_clone() {
        let original_int = ConstType::Integer(42);
        let cloned_int = original_int.clone();
        assert_eq!(original_int, cloned_int);

        let original_text = ConstType::Text("hello".to_string());
        let cloned_text = original_text.clone();
        assert_eq!(original_text, cloned_text);
    }

    #[test]
    fn test_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(ConstType::Integer(42));
        set.insert(ConstType::Text("hello".to_string()));
        set.insert(ConstType::Integer(42)); // Duplicate
        set.insert(ConstType::Text("hello".to_string())); // Duplicate

        assert_eq!(set.len(), 2);
    }
}
