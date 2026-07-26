//! Relation column schema.
//!
//! An `Attribute` carries two type facts in parallel:
//!
//! - `primitive_type` ([`DataType`]): the storage type. Read by every
//!   stage below the typechecker.
//! - `declared_id` ([`TypeId`]): the user-written type name. Read
//!   only by the typechecker to enforce subtype identity; dead weight
//!   downstream.

use std::fmt;

use crate::types::DataType;
use crate::types::TypeId;

/// One `name: type` column of a relation's schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Attribute {
    name: String,
    primitive_type: DataType,
    declared_id: TypeId,
}

impl Attribute {
    #[must_use]
    #[inline]
    pub(crate) fn with_type(name: String, primitive_type: DataType, declared_id: TypeId) -> Self {
        Self {
            name: name.to_lowercase(),
            primitive_type,
            declared_id,
        }
    }

    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.primitive_type
    }

    #[must_use]
    #[inline]
    pub(crate) fn declared_id(&self) -> TypeId {
        self.declared_id
    }
}

impl fmt::Display for Attribute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.name, self.primitive_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TypeRegistry;

    fn int32_id() -> TypeId {
        TypeRegistry::new()
            .primitive_id(DataType::Int32)
            .expect("int32 is a seeded primitive")
    }

    /// `with_type` lowercases the column name (names are case-insensitive)
    /// and stores the two types verbatim; the accessors read them back.
    #[test]
    fn with_type_lowercases_name_and_keeps_types() {
        let id = int32_id();
        let attr = Attribute::with_type("Age".into(), DataType::Int32, id);
        assert_eq!(attr.name(), "age");
        assert_eq!(attr.data_type(), &DataType::Int32);
        assert_eq!(attr.declared_id(), id);
    }

    /// `Display` renders `name: type` with the canonical (lowercased) name;
    /// an empty (default) rendering is caught.
    #[test]
    fn display_renders_name_and_type() {
        let attr = Attribute::with_type("Age".into(), DataType::Int32, int32_id());
        assert_eq!(attr.to_string(), "age: int32");
    }
}
