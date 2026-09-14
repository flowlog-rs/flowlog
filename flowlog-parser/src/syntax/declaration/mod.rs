//! Schema-layer declarations parsed from `.dl` source.
//!
//! - [`Attribute`]: one `name: type` relation column.
//! - [`Relation`]: a relation `.decl` with its attributes and I/O config.
//! - [`ExternFn`]: an `.extern fn` signature.
//! - [`InputDirective`] / [`OutputDirective`] / [`PrintSizeDirective`]:
//!   `.input` / `.output` / `.printsize`, each folded into its `Relation`
//!   as an [`InputSource`] / [`OutputSink`].
//! - `comp`: raw `.comp` / `.init` AST, inlined and discarded before
//!   typechecking.
//! - `type_decl`: raw `.type` declaration parsing.

mod attribute;
mod comp;
mod directive;
mod extern_fn;
mod relation;
mod type_decl;

pub use attribute::Attribute;
pub(crate) use comp::CompDecl;
pub(crate) use comp::InitDecl;
pub(crate) use comp::RawItem;
pub(crate) use comp::RawRelation;
pub(crate) use comp::SuperRef;
pub(crate) use directive::InputDirective;
pub use directive::InputSource;
pub use directive::OrderKey;
pub(crate) use directive::OutputDirective;
pub use directive::OutputSink;
pub(crate) use directive::PrintSizeDirective;
pub use extern_fn::ExternFn;
pub use relation::Relation;
pub(crate) use type_decl::RawTypeOp;
pub(crate) use type_decl::split_type_alias;
