//! Declaration types for FlowLog Datalog programs.
//!
//! This module defines the schema-layer declarations parsed from source:
//! - [`Attribute`]: a single column (name + [`DataType`](crate::primitive::DataType))
//! - [`Relation`]: a full relation schema with attributes
//! - [`InputDirective`]: input directive specifying how to read EDB data
//! - [`OutputDirective`]: output directive specifying which relation to output
//! - [`PrintSizeDirective`]: print size directive for size reporting
mod attribute;
mod comp;
mod directive;
mod extern_fn;
mod relation;

pub use attribute::Attribute;
pub(crate) use comp::CompDecl;
pub(crate) use comp::InitDecl;
pub(crate) use comp::RawItem;
pub(crate) use comp::RawRelation;
pub(crate) use comp::RawTypeOp;
pub(crate) use comp::SuperRef;
pub(crate) use comp::split_type_alias;
pub(crate) use directive::InputDirective;
pub(crate) use directive::OutputDirective;
pub(crate) use directive::PrintSizeDirective;
pub use extern_fn::ExternFn;
pub use relation::Relation;
