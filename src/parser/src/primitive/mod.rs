//! Primitive types for the FlowLog parser.
//!
//! This module contains the fundamental data types used throughout the FlowLog parser,
//! including data type specifications and constant values that form the building blocks
//! of FlowLog programs.
//!
//! # Overview
//!
//! FlowLog is a declarative language for expressing network policies and protocols.
//! This module provides the primitive types that represent the basic elements:
//!
//! - [`DataType`]: Represents the types that can be used for relation attributes
//! - [`ConstType`]: Represents constant literal values used in expressions
//!
//! # Examples
//!
//! ```rust
//! use parser::primitive::{DataType, ConstType};
//! use std::str::FromStr;
//!
//! // Working with data types
//! let number_type = DataType::from_str("number").unwrap();
//! let string_type = DataType::from_str("string").unwrap();
//!
//! // Working with constants
//! let int_const = ConstType::Integer(42);
//! let text_const = ConstType::Text("hello world".to_string());
//!
//! println!("Type: {}, Constant: {}", number_type, int_const);
//! ```

pub mod const_type;
pub mod data_type;

pub use const_type::ConstType;
pub use data_type::DataType;
