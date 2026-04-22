//! Primitive types for the FlowLog Datalog programs.
//!
//! This module defines the core types used in the parser layer:
//! - [`DataType`]: relation attribute types (`int32`, `int64`, `string`)
//! - [`ConstType`]: literal constants (numbers, text)
//!
//! These types form the building blocks of FlowLog Datalog programs and appear
//! in atoms, expressions, and relation schemas.
//!
mod const_type;
mod data_type;

pub use const_type::ConstType;
pub use data_type::DataType;
