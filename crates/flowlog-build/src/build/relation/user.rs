//! User-facing type aliases inside `pub mod rel { … }`.
//!
//! Each non-nullary relation (EDB or output IDB) gets a `pub type Edge =
//! (i32, i32);` tuple alias. Users construct values as plain tuples; the
//! engine converts user tuples to the internal `Tuple` representation
//! (interning strings, wrapping floats) inline at the insert / drain sites.

use proc_macro2::TokenStream;
use quote::quote;

use crate::parser::{DataType, Program, Relation};

use crate::codegen::user_tuple_tokens;

use super::user_struct_ident;

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

/// Expression converting position `i` of a user-tuple binding `src` to its
/// internal tuple slot. `f32 → OrderedFloat(f32)`, `String → Spur` under
/// interning. Positions with no transform return `src.i` unchanged.
pub(crate) fn user_to_tuple_expr(
    dt: &DataType,
    string_intern: bool,
    src: TokenStream,
) -> TokenStream {
    match *dt {
        DataType::Float32 | DataType::Float64 => quote! { OrderedFloat(#src) },
        DataType::String if string_intern => quote! { intern(&#src) },
        _ => src,
    }
}

/// Inverse of `user_to_tuple_expr`: internal slot → user-facing expression,
/// used at drain time. Interned strings resolve through the flat snapshot
/// path (`resolve_out`) since drain runs after the dataflow's fixpoint.
pub(crate) fn tuple_to_user_expr(
    dt: &DataType,
    string_intern: bool,
    src: TokenStream,
) -> TokenStream {
    match *dt {
        DataType::Float32 | DataType::Float64 => quote! { (#src).into_inner() },
        DataType::String if string_intern => quote! { resolve_out(#src).to_string() },
        _ => src,
    }
}
