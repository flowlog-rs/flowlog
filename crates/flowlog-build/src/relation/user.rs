//! Generate the user-facing relation structs inside `pub mod rel { … }`.
//!
//! For each `.input` relation, emit `pub struct Edge { x: i32, y: i32 }`
//! plus `impl Relation for Edge` so users can `engine.insert_edge(Edge{..})`.
//! For each `.output` relation, emit the same `pub struct` plus a private
//! `from_tuple` helper the engine calls during drain.

use proc_macro2::{Literal, TokenStream};
use quote::{format_ident, quote};

use generator::data_type_tokens;
use parser::{DataType, Program, Relation};

use super::{pascal_case, user_struct_ident};

/// Emit `pub use ::flowlog_runtime::Relation; pub mod rel { … }`.
pub(crate) fn gen_public_rel_module(
    program: &Program,
    string_intern: bool,
    type_attrs: &[(String, String)],
    field_attrs: &[(String, String)],
) -> TokenStream {
    let struct_defs: Vec<TokenStream> = collect_user_rels(program)
        .into_iter()
        .map(|spec| gen_user_struct(spec, string_intern, type_attrs, field_attrs))
        .collect();

    let supers = gen_rel_supers(program, string_intern);

    quote! {
        pub use ::flowlog_runtime::Relation;

        pub mod rel {
            #supers
            #(#struct_defs)*
        }
    }
}

/// One entry per unique user-facing relation. A "hybrid" relation that
/// is both `.input` and `.output` shows up once with both flags set, so we
/// emit a single `pub struct X { … }` carrying both `to_tuple` and
/// `from_tuple` impls instead of two conflicting struct definitions.
struct UserRelSpec<'a> {
    rel: &'a Relation,
    needs_to_tuple: bool,
    needs_from_tuple: bool,
}

fn collect_user_rels(program: &Program) -> Vec<UserRelSpec<'_>> {
    let mut specs: Vec<UserRelSpec<'_>> = Vec::new();
    for (rel, to_tuple, from_tuple) in program
        .edbs()
        .into_iter()
        .map(|r| (r, true, false))
        .chain(program.output_idbs().into_iter().map(|r| (r, false, true)))
    {
        if rel.arity() == 0 {
            continue;
        }
        if let Some(existing) = specs.iter_mut().find(|s| s.rel.name() == rel.name()) {
            existing.needs_to_tuple |= to_tuple;
            existing.needs_from_tuple |= from_tuple;
        } else {
            specs.push(UserRelSpec {
                rel,
                needs_to_tuple: to_tuple,
                needs_from_tuple: from_tuple,
            });
        }
    }
    specs
}

// =========================================================================
// Struct generation
// =========================================================================

/// `use super::X;` imports brought into `mod rel` for its contents to reach
/// types declared at the enclosing scope (`Relation`, `OrderedFloat`, etc.).
fn gen_rel_supers(program: &Program, string_intern: bool) -> TokenStream {
    // Single pass over every relation's columns — float and (when interning)
    // string presence are decided together to avoid two chained traversals.
    let mut has_float = false;
    let mut has_string = false;
    for rel in program.edbs().iter().chain(program.idbs().iter()) {
        for dt in rel.data_type() {
            match dt {
                DataType::Float32 | DataType::Float64 => has_float = true,
                DataType::String => has_string = true,
                _ => {}
            }
            if has_float && (!string_intern || has_string) {
                break;
            }
        }
    }

    let mut uses = vec![quote! { use super::Relation; }];
    if has_float {
        uses.push(quote! { use super::OrderedFloat; });
    }
    if string_intern && has_string {
        uses.push(quote! { use super::{intern, resolve, Spur}; });
    }
    quote! { #(#uses)* }
}

fn gen_user_struct(
    spec: UserRelSpec<'_>,
    string_intern: bool,
    type_attrs: &[(String, String)],
    field_attrs: &[(String, String)],
) -> TokenStream {
    let UserRelSpec {
        rel,
        needs_to_tuple,
        needs_from_tuple,
    } = spec;
    let struct_ident = user_struct_ident(rel);
    let struct_name = pascal_case(rel.name());
    let fields = user_facing_fields(rel, &struct_name, field_attrs);
    let extra_type_attrs = matched_type_attrs(type_attrs, &struct_name);

    let to_tuple_impl = if needs_to_tuple {
        gen_to_tuple_impl(rel, string_intern, &struct_ident)
    } else {
        quote! {}
    };
    let from_tuple_impl = if needs_from_tuple {
        gen_from_tuple_impl(rel, string_intern, &struct_ident)
    } else {
        quote! {}
    };

    quote! {
        #[derive(Clone, Debug)]
        #(#extra_type_attrs)*
        pub struct #struct_ident {
            #(#fields),*
        }

        #to_tuple_impl
        #from_tuple_impl
    }
}

fn gen_to_tuple_impl(
    rel: &Relation,
    string_intern: bool,
    struct_ident: &proc_macro2::Ident,
) -> TokenStream {
    let rel_name = rel.name();
    let dts = rel.data_type();
    let tuple_ty = data_type_tokens(&dts, string_intern);

    let field_exprs: Vec<TokenStream> = rel
        .attributes()
        .iter()
        .zip(dts.iter())
        .map(|(attr, dt)| {
            let name = format_ident!("{}", attr.name());
            user_to_tuple_expr(dt, string_intern, quote! { self.#name })
        })
        .collect();
    let tuple_expr = if field_exprs.len() == 1 {
        let e0 = &field_exprs[0];
        quote! { ( #e0, ) }
    } else {
        quote! { ( #(#field_exprs),* ) }
    };

    quote! {
        impl Relation for #struct_ident {
            type Tuple = #tuple_ty;
            #[inline]
            fn relation_name() -> &'static str { #rel_name }
            #[inline]
            fn to_tuple(self) -> Self::Tuple { #tuple_expr }
        }
    }
}

fn gen_from_tuple_impl(
    rel: &Relation,
    string_intern: bool,
    struct_ident: &proc_macro2::Ident,
) -> TokenStream {
    let dts = rel.data_type();
    let tuple_ty = data_type_tokens(&dts, string_intern);

    let from_fields: Vec<TokenStream> = rel
        .attributes()
        .iter()
        .zip(dts.iter())
        .enumerate()
        .map(|(i, (attr, dt))| {
            let name = format_ident!("{}", attr.name());
            let idx = Literal::usize_unsuffixed(i);
            let expr = tuple_to_user_expr(dt, string_intern, quote! { tuple.#idx });
            quote! { #name: #expr }
        })
        .collect();

    quote! {
        impl #struct_ident {
            #[doc(hidden)]
            #[inline]
            pub fn from_tuple(tuple: #tuple_ty) -> Self {
                Self { #(#from_fields),* }
            }
        }
    }
}

fn user_facing_fields(
    rel: &Relation,
    struct_name: &str,
    field_attrs: &[(String, String)],
) -> Vec<TokenStream> {
    rel.attributes()
        .iter()
        .zip(rel.data_type().iter())
        .map(|(attr, dt)| {
            let name = format_ident!("{}", attr.name());
            let ty = user_facing_type(dt);
            let attrs = matched_field_attrs(field_attrs, struct_name, attr.name());
            quote! { #(#attrs)* pub #name: #ty }
        })
        .collect()
}

// =========================================================================
// Type + expression conversions
// =========================================================================

/// User-facing Rust type for a column. Keeps `f32`/`String` for ergonomics;
/// the interned/wrapped forms live inside the `Tuple` representation only.
fn user_facing_type(dt: &DataType) -> TokenStream {
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

/// Convert a user-facing expression into its tuple-slot representation
/// (`f32` → `OrderedFloat<f32>`, `&str` → `Spur` under string interning).
fn user_to_tuple_expr(dt: &DataType, string_intern: bool, user: TokenStream) -> TokenStream {
    match *dt {
        DataType::Float32 | DataType::Float64 => quote! { OrderedFloat(#user) },
        DataType::String if string_intern => quote! { intern(&#user) },
        _ => user,
    }
}

/// Inverse of `user_to_tuple_expr`: convert a tuple slot back to the
/// user-facing shape during output drain.
fn tuple_to_user_expr(dt: &DataType, string_intern: bool, tuple: TokenStream) -> TokenStream {
    match *dt {
        DataType::Float32 | DataType::Float64 => quote! { (#tuple).into_inner() },
        DataType::String if string_intern => quote! { resolve(#tuple).to_string() },
        _ => tuple,
    }
}

// =========================================================================
// User-supplied attribute matching
// =========================================================================

/// Resolve `Builder::type_attribute(matcher, attr)` entries whose matcher
/// targets `struct_name`. Parses each stored attribute string into tokens;
/// invalid syntax panics with a clear error.
fn matched_type_attrs(attrs: &[(String, String)], struct_name: &str) -> Vec<TokenStream> {
    attrs
        .iter()
        .filter(|(m, _)| type_matches(m, struct_name))
        .map(|(m, a)| {
            a.parse::<TokenStream>()
                .unwrap_or_else(|e| panic!("type_attribute({m:?}, {a:?}): {e}"))
        })
        .collect()
}

fn matched_field_attrs(
    attrs: &[(String, String)],
    struct_name: &str,
    field: &str,
) -> Vec<TokenStream> {
    attrs
        .iter()
        .filter(|(m, _)| field_matches(m, struct_name, field))
        .map(|(m, a)| {
            a.parse::<TokenStream>()
                .unwrap_or_else(|e| panic!("field_attribute({m:?}, {a:?}): {e}"))
        })
        .collect()
}

fn type_matches(matcher: &str, struct_name: &str) -> bool {
    matcher == "*" || matcher == struct_name
}

fn field_matches(matcher: &str, struct_name: &str, field_name: &str) -> bool {
    let Some((m_type, m_field)) = matcher.split_once('.') else {
        return false;
    };
    (m_type == "*" || m_type == struct_name) && (m_field == "*" || m_field == field_name)
}
