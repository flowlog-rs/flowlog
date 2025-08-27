//! Primitive types for the Macaron Datalog programs.
//!
//! This module defines the core types used in the parser layer:
//! - [`DataType`]: relation attribute types (`integer`, `string`)
//! - [`ConstType`]: literal constants (integers, text)
//!
//! These types form the building blocks of Macaron Datalog programs and appear
//! in atoms, expressions, and relation schemas.
//!
//! # Example
//! ```rust
//! use parser::primitive::{DataType, ConstType};
//! use std::str::FromStr;
//!
//! let ty = DataType::from_str("integer").unwrap();
//! let c = ConstType::Integer(42);
//! assert_eq!(ty.to_string(), "integer");
//! assert_eq!(c.to_string(), "42");
//! ```

pub mod const_type;
pub mod data_type;

pub use const_type::ConstType;
pub use data_type::DataType;
