//! Declaration types for FlowLog programs.
//!
//! This module contains types for representing relation and attribute declarations
//! in FlowLog programs. Relations define the schema and metadata for data tables
//! that can be used throughout a FlowLog program.
//!
//! # Components
//!
//! - [`Attribute`]: Represents a single column in a relation with name and type
//! - [`Relation`]: Represents a complete relation declaration with attributes and paths
//!
//! # Examples
//!
//! ```rust
//! use parser::declaration::{Attribute, Relation};
//! use parser::primitive::DataType;
//!
//! // Create attributes for a person relation
//! let name_attr = Attribute::new("name".to_string(), DataType::String);
//! let age_attr = Attribute::new("age".to_string(), DataType::Integer);
//!
//! // Create a relation declaration
//! let person_rel = Relation::new(
//!     "person",
//!     vec![name_attr, age_attr],
//!     Some("input/people.csv"),
//!     None,
//! );
//!
//! println!("Relation: {}", person_rel);
//! ```

pub mod attribute;
pub mod relation;

pub use attribute::Attribute;
pub use relation::Relation;
