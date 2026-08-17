//! Generating a program's input side: the per-relation handlers and the
//! container that holds them.
//!
//! - [`handler`]: one `RelationSpec`, `relation!` expansion, and `Ingest`
//!   impl per relation.
//! - [`container`]: the `Inputs` struct those handlers live in.
//!
//! The three naming conventions are here rather than in either file,
//! because the compiler and both library engines all spell them and had
//! drifted into re-deriving them by hand.

pub mod container;
pub mod handler;

use flowlog_parser::Relation;
use proc_macro2::Ident;
use quote::format_ident;

/// Field ident inside [`gen_inputs_container`](container::gen_inputs_container)'s struct.
///
/// Prefixed, so no relation name can produce a Rust keyword. The container
/// is crate-internal, so the prefix never surfaces in the user API.
pub fn inputs_field_ident(rel: &Relation) -> Ident {
    format_ident!("in_{}", rel.name())
}

/// Ident for a relation's input handler (e.g. `Reledge`), the type the
/// runtime's `relation!` macro defines and [`gen_inputs_container`](container::gen_inputs_container) holds
/// one of per relation.
pub fn input_struct_ident(rel: &Relation) -> Ident {
    format_ident!("Rel{}", rel.name())
}

/// Ident for the raw differential handle a relation's dataflow yields,
/// before it is wrapped in its [`input_struct_ident`] type.
pub fn handle_ident(rel: &Relation) -> Ident {
    format_ident!("h{}", rel.name())
}
