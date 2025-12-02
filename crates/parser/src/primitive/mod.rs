//! Primitive types for the FlowLog Datalog programs.
//!
//! This module defines the core types used in the parser layer:
//! - [`DataType`]: relation attribute types (`number`, `string`)
//! - [`ConstType`]: literal constants (numbers, text)
//!
//! These types form the building blocks of FlowLog Datalog programs and appear
//! in atoms, expressions, and relation schemas.
//!
//! # Example
//! ```rust
//! use parser::primitive::{DataType, ConstType};
//! use std::str::FromStr;
//!
//! let ty = DataType::from_str("number").unwrap();
//! let c = ConstType::Integer(42);
//! assert_eq!(ty.to_string(), "number");
//! assert_eq!(c.to_string(), "42");
//! ```

pub mod const_type;
pub mod data_type;

pub use const_type::ConstType;
pub use data_type::DataType;
