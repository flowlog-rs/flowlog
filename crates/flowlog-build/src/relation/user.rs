//! User-facing type aliases inside `pub mod rel { … }`.
//!
//! Each non-nullary relation (EDB or output IDB) gets a `pub type Edge =
//! (i32, i32);` tuple alias. Users construct values as plain tuples; the
//! engine converts user tuples to the internal `Tuple` representation
//! (interning strings, wrapping floats) inline at the insert / drain sites.

use proc_macro2::TokenStream;
use quote::quote;

use parser::{DataType, Program, Relation};

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
fn collect_user_rels(program: &Program) -> Vec<&Relation> {
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
    let tuple_ty = user_tuple_type(&rel.data_type());
    quote! { pub type #ident = #tuple_ty; }
}

/// User-facing tuple type for a relation (e.g. `(i32, String, f32)`).
/// Uses `String` / `f32` regardless of internal interning / float wrapping —
/// the engine handles the conversion.
pub(crate) fn user_tuple_type(dts: &[DataType]) -> TokenStream {
    let elems: Vec<TokenStream> = dts.iter().map(user_facing_type).collect();
    // A 1-element tuple needs the trailing comma to parse as a tuple.
    if let [only] = elems.as_slice() {
        quote! { ( #only, ) }
    } else {
        quote! { ( #(#elems),* ) }
    }
}

/// User-facing Rust type for a column. Keeps `f32` / `String` for ergonomics;
/// `OrderedFloat` / `Spur` only appear inside the internal `Tuple`.
pub(crate) fn user_facing_type(dt: &DataType) -> TokenStream {
    match *dt {
        DataType::Int8 => quote! { i8 },
        DataType::Int16 => quote! { i16 },
        DataType::Int32 => quote! { i32 },
        DataType::Int64 => quote! { i64 },
        DataType::UInt8 => quote! { u8 },
        DataType::UInt16 => quote! { u16 },
        DataType::UInt32 => quote! { u32 },
        DataType::UInt64 => quote! { u64 },
        DataType::Float32 => quote! { f32 },
        DataType::Float64 => quote! { f64 },
        DataType::String => quote! { String },
        DataType::Bool => quote! { bool },
    }
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
/// used at drain time.
pub(crate) fn tuple_to_user_expr(
    dt: &DataType,
    string_intern: bool,
    src: TokenStream,
) -> TokenStream {
    match *dt {
        DataType::Float32 | DataType::Float64 => quote! { (#src).into_inner() },
        DataType::String if string_intern => quote! { resolve(#src).to_string() },
        _ => src,
    }
}
