//! Attribute declaration types for FlowLog relations.

use crate::primitive::DataType;

use std::fmt;

/// Represents an attribute declaration in a relation.
///
/// An attribute consists of a name and a data type, defining the schema
/// for a column in a relation. Attributes are used to specify the structure
/// of both EDB (Extensional Database) and IDB (Intensional Database) relations.
///
/// # Examples
///
/// ```rust
/// use parser::declaration::Attribute;
/// use parser::primitive::DataType;
///
/// // Create an attribute for a person's name
/// let name_attr = Attribute::new("name".to_string(), DataType::String);
/// assert_eq!(name_attr.name(), "name");
/// assert_eq!(*name_attr.data_type(), DataType::String);
///
/// // Create an attribute for a person's age
/// let age_attr = Attribute::new("age".to_string(), DataType::Integer);
/// assert_eq!(age_attr.to_string(), "age: number");
/// ```
///
/// # Usage in Relations
///
/// Attributes are typically used as part of relation declarations:
///
/// ```rust
/// use parser::declaration::{Attribute, Relation};
/// use parser::primitive::DataType;
///
/// let attributes = vec![
///     Attribute::new("id".to_string(), DataType::Integer),
///     Attribute::new("name".to_string(), DataType::String),
/// ];
///
/// let relation = Relation::new("users", attributes, None, None);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Attribute {
    name: String,
    data_type: DataType,
}

impl Attribute {
    /// Creates a new attribute from a name and data type.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the attribute (column name)
    /// * `data_type` - The data type of the attribute
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::declaration::Attribute;
    /// use parser::primitive::DataType;
    ///
    /// let attr = Attribute::new("user_id".to_string(), DataType::Integer);
    /// assert_eq!(attr.name(), "user_id");
    /// ```
    #[must_use]
    pub fn new(name: String, data_type: DataType) -> Self {
        Self { name, data_type }
    }

    /// Returns the name of this attribute.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::declaration::Attribute;
    /// use parser::primitive::DataType;
    ///
    /// let attr = Attribute::new("email".to_string(), DataType::String);
    /// assert_eq!(attr.name(), "email");
    /// ```
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the data type of this attribute.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::declaration::Attribute;
    /// use parser::primitive::DataType;
    ///
    /// let attr = Attribute::new("count".to_string(), DataType::Integer);
    /// assert_eq!(*attr.data_type(), DataType::Integer);
    /// ```
    #[must_use]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

impl fmt::Display for Attribute {
    /// Format the attribute as "name: type" using FlowLog grammar syntax.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::declaration::Attribute;
    /// use parser::primitive::DataType;
    ///
    /// let attr = Attribute::new("id".to_string(), DataType::Integer);
    /// assert_eq!(attr.to_string(), "id: number");
    ///
    /// let attr2 = Attribute::new("name".to_string(), DataType::String);
    /// assert_eq!(attr2.to_string(), "name: string");
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.name, self.data_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_attribute() {
        let attr = Attribute::new("test_name".to_string(), DataType::String);
        assert_eq!(attr.name(), "test_name");
        assert_eq!(*attr.data_type(), DataType::String);
    }

    #[test]
    fn test_attribute_display() {
        let int_attr = Attribute::new("id".to_string(), DataType::Integer);
        assert_eq!(int_attr.to_string(), "id: number");

        let str_attr = Attribute::new("name".to_string(), DataType::String);
        assert_eq!(str_attr.to_string(), "name: string");
    }

    #[test]
    fn test_attribute_equality() {
        let attr1 = Attribute::new("name".to_string(), DataType::String);
        let attr2 = Attribute::new("name".to_string(), DataType::String);
        let attr3 = Attribute::new("age".to_string(), DataType::Integer);

        assert_eq!(attr1, attr2);
        assert_ne!(attr1, attr3);
    }

    #[test]
    fn test_attribute_clone() {
        let original = Attribute::new("test".to_string(), DataType::Integer);
        let cloned = original.clone();

        assert_eq!(original, cloned);
        assert_eq!(original.name(), cloned.name());
        assert_eq!(original.data_type(), cloned.data_type());
    }

    #[test]
    fn test_attribute_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(Attribute::new("name".to_string(), DataType::String));
        set.insert(Attribute::new("age".to_string(), DataType::Integer));
        set.insert(Attribute::new("name".to_string(), DataType::String)); // Duplicate

        assert_eq!(set.len(), 2);
    }
}
