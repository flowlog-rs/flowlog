//! The type system: what types exist and how they relate.
//!
//! - [`DataType`]: the type vocabulary (concrete runtime types plus the
//!   checking-time literal families) and its compatibility algebra.
//! - [`TypeRegistry`] / [`TypeId`]: the per-program table of named types
//!   and the subtype order over it.
//!
//! Both kinds of polymorphism resolve during type checking and are
//! erased before anything downstream runs: literals are pinned to a
//! concrete width, subtypes erase to their root primitives.

mod data_type;
mod registry;

pub use data_type::DataType;
pub(crate) use registry::TypeId;
pub(crate) use registry::TypeRegistry;
