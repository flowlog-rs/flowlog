//! Declaration types for Macaron Datalog programs.
//!
//! This module defines the schema-layer declarations parsed from source:
//! - [`Attribute`]: a single column (name + [`DataType`](crate::primitive::DataType))
//! - [`Relation`]: a full relation schema with attributes
//! - [`InputDirective`]: input directive specifying how to read EDB data
//! - [`OutputDirective`]: output directive specifying which relation to output
//! - [`PrintSizeDirective`]: print size directive for size reporting
//!
//! # Example
//! ```rust
//! use parser::declaration::{Attribute, Relation};
//! use parser::primitive::DataType;
//!
//! let name = Attribute::new("name".into(), DataType::String);
//! let age  = Attribute::new("age".into(), DataType::Integer);
//!
//! let person = Relation::new("person", vec![name, age]);
//!
//! // Minimal sanity checks (run as doctest):
//! assert_eq!(person.name(), "person");
//! assert_eq!(person.attributes().len(), 2);
//! ```
pub mod attribute;
pub mod directive;
pub mod relation;

pub use attribute::Attribute;
pub use directive::{InputDirective, OutputDirective, PrintSizeDirective};
pub use relation::Relation;
