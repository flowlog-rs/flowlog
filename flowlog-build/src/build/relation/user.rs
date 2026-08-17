//! User-facing type aliases inside `pub mod rel { … }`.
//!
//! Each non-nullary relation (EDB or output IDB) gets a `pub type Edge =
//! (i32, i32);` tuple alias. Users construct values as plain tuples; the
//! engine converts user tuples to the internal `Tuple` representation
//! (interning strings, wrapping floats) inline at the insert / drain sites.

use flowlog_parser::Program;
use flowlog_parser::Relation;
use proc_macro2::TokenStream;
use quote::quote;

use super::user_struct_ident;
use crate::codegen::user_tuple_tokens;

/// Emit `pub mod rel { pub type Edge = (i32, i32); … }`.
pub(crate) fn gen_public_rel_module(program: &Program) -> TokenStream {
    let aliases: Vec<TokenStream> = collect_user_rels(program)
        .into_iter()
        .map(gen_type_alias)
        .collect();

    quote! {
        pub mod rel {
            #(#aliases)*
        }
    }
}

/// Unique non-nullary relations across `.input` EDBs and `.output` IDBs —
/// a hybrid relation (both `.input` and `.output`) shows up once.
pub(crate) fn collect_user_rels(program: &Program) -> Vec<&Relation> {
    let mut seen: Vec<&Relation> = Vec::new();
    for rel in program.edbs().into_iter().chain(program.output_idbs()) {
        if rel.arity() == 0 {
            continue;
        }
        if !seen.iter().any(|r| r.name() == rel.name()) {
            seen.push(rel);
        }
    }
    seen
}

fn gen_type_alias(rel: &Relation) -> TokenStream {
    let ident = user_struct_ident(rel);
    let tuple_ty = user_tuple_tokens(&rel.data_type());
    quote! { pub type #ident = #tuple_ty; }
}
