//! Attribute declaration types for Macaron Datalog programs.

use crate::primitive::DataType;
use std::fmt;

/// A single column in a relation schema: `name: DataType`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Attribute {
    name: String,
    data_type: DataType,
}

impl Attribute {
    /// Create a new attribute.
    #[must_use]
    #[inline]
    pub fn new(name: String, data_type: DataType) -> Self {
        Self { name, data_type }
    }

    /// Attribute (column) name.
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Attribute data type.
    #[must_use]
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

impl fmt::Display for Attribute {
    /// Formats as `name: type` using Macaron grammar strings.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.name, self.data_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitive::DataType::*;

    #[test]
    fn new_and_accessors() {
        let a = Attribute::new("name".into(), String);
        assert_eq!(a.name(), "name");
        assert_eq!(*a.data_type(), String);
    }

    #[test]
    fn display_golden() {
        let id = Attribute::new("id".into(), Integer);
        let nm = Attribute::new("name".into(), String);
        assert_eq!(id.to_string(), "id: integer");
        assert_eq!(nm.to_string(), "name: string");
    }

    #[test]
    fn equality_semantics() {
        let a = Attribute::new("age".into(), Integer);
        let b = Attribute::new("age".into(), Integer);
        let c = Attribute::new("age".into(), String);
        let d = Attribute::new("name".into(), Integer);
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_ne!(a, d);
    }
}
