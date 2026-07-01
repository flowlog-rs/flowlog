//! Relation column schema.
//!
//! An `Attribute` carries two type facts in parallel:
//!
//! - `primitive_type` ([`DataType`]) — the storage type. Read by every
//!   stage below the typechecker.
//! - `declared_id` ([`TypeId`]) — the user-written type name. Read
//!   only by the typechecker to enforce subtype identity; dead weight
//!   downstream.

use std::fmt;

use crate::primitive::DataType;
use crate::primitive::TypeId;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Attribute {
    name: String,
    primitive_type: DataType,
    declared_id: TypeId,
}

impl Attribute {
    #[must_use]
    #[inline]
    pub fn with_type(name: String, primitive_type: DataType, declared_id: TypeId) -> Self {
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
    pub fn declared_id(&self) -> TypeId {
        self.declared_id
    }
}

impl fmt::Display for Attribute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.name, self.primitive_type)
    }
}
