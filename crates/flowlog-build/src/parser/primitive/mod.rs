//! Primitive types and the per-program type registry.
//!
//! - [`DataType`]: the closed set of 12 built-in runtime primitives
//!   (`int8`..`uint64`, `f32`/`f64`, `string`, `bool`).
//! - [`ConstType`]: literal constants (numbers, text).
//! - [`TypeRegistry`] / [`TypeId`]: per-program registry of every named
//!   type — primitives plus user-declared `.type` aliases and subtypes.
//!   The typechecker enforces compatibility through [`TypeId`];
//!   downstream stages read only the root [`DataType`].
//!
//! These types form the building blocks of FlowLog Datalog programs and
//! appear in atoms, expressions, and relation schemas.

mod const_type;
mod data_type;

pub use const_type::ConstType;
pub(crate) use data_type::TypeId;
pub use data_type::{DataType, TypeRegistry};
