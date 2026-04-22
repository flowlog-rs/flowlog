//! Attribute declaration types for FlowLog Datalog programs.

use crate::parser::primitive::DataType;
use std::fmt;

/// A single column in a relation schema: `name: DataType`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Attribute {
    name: String,
    data_type: DataType,
}

impl Attribute {
    /// Create a new attribute.
    #[must_use]
    #[inline]
    pub(crate) fn new(name: String, data_type: DataType) -> Self {
        Self {
            name: name.to_lowercase(),
            data_type,
        }
    }

    /// Attribute (column) name.
    #[must_use]
    #[inline]
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Attribute data type.
    #[must_use]
    #[inline]
    pub(crate) fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

impl fmt::Display for Attribute {
    /// Formats as `name: type` using FlowLog grammar strings.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.name, self.data_type)
    }
}
