//! Library-mode relation codegen.
//!
//! Emits:
//!
//! - Per-EDB `{Name}Input` structs with inherent methods (no `RelOps` trait,
//!   no dynamic dispatch) — see [`handler`].
//! - A concrete `Inputs` container the library engine holds directly.
//! - User-facing `rel::Foo` tuple aliases — see [`user`].
//!
//! Binary mode has its own relation codegen in `flowlog-compiler`.

mod handler;
pub(crate) mod user;

use proc_macro2::{Ident, Span, TokenStream};
use quote::{format_ident, quote};

use crate::codegen::{CodegenError, Features};
use crate::parser::{Program, Relation};

/// Emit the body of the library-mode `relops` module — EDB input handlers
/// + the `Inputs` container — plus the `use` lines they depend on.
pub(crate) fn gen_input_module(
    program: &Program,
    features: &Features,
) -> Result<TokenStream, CodegenError> {
    let edbs = program.edbs();
    let string_intern = features.string_intern();

    let preamble = gen_preamble(program, features);
    let input_structs = edbs
        .iter()
        .map(|rel| handler::gen_input_struct(rel, program.facts().get(rel.name()), string_intern))
        .collect::<Result<Vec<_>, _>>()?;
    let inputs_container = gen_inputs_container(&edbs);

    Ok(quote! {
        #preamble
        #(#input_structs)*
        #inputs_container
    })
}

// ------------------------------------------------------------
// Naming helpers — shared across relation codegen consumers
// ------------------------------------------------------------

/// Convert a snake_case / lowercase relation name to `PascalCase`.
pub(crate) fn pascal_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut capitalize = true;
    for c in name.chars() {
        if c == '_' || c == '-' {
            capitalize = true;
            continue;
        }
        if capitalize {
            out.extend(c.to_uppercase());
            capitalize = false;
        } else {
            out.push(c);
        }
    }
    out
}

/// Turn a user-supplied name (attribute or relation field) into a Rust
/// `Ident`, using raw-identifier syntax (`r#ref`, `r#type`) when the name
/// collides with a Rust keyword. All call sites that embed a user name as a
/// Rust ident must route through this.
pub(crate) fn rust_ident(name: &str) -> Ident {
    let is_keyword = matches!(
        name,
        "as" | "break"
            | "const"
            | "continue"
            | "crate"
            | "else"
            | "enum"
            | "extern"
            | "false"
            | "fn"
            | "for"
            | "if"
            | "impl"
            | "in"
            | "let"
            | "loop"
            | "match"
            | "mod"
            | "move"
            | "mut"
            | "pub"
            | "ref"
            | "return"
            | "self"
            | "Self"
            | "static"
            | "struct"
            | "super"
            | "trait"
            | "true"
            | "type"
            | "unsafe"
            | "use"
            | "where"
            | "while"
            | "async"
            | "await"
            | "dyn"
            | "abstract"
            | "become"
            | "box"
            | "do"
            | "final"
            | "macro"
            | "override"
            | "priv"
            | "typeof"
            | "unsized"
            | "virtual"
            | "yield"
            | "try"
    );
    if matches!(name, "crate" | "self" | "Self" | "super") {
        // These four keywords cannot be raw identifiers (`r#self` etc. is
        // rejected by the compiler), so escape them with a trailing underscore.
        format_ident!("{}_", name)
    } else if is_keyword {
        Ident::new_raw(name, Span::call_site())
    } else {
        Ident::new(name, Span::call_site())
    }
}

/// Ident for the user-facing struct generated from a relation (e.g. `Edge`).
pub(crate) fn user_struct_ident(rel: &Relation) -> Ident {
    format_ident!("{}", pascal_case(rel.name()))
}

/// Ident for the engine-internal input-handler struct (e.g. `EdgeInput`).
pub(crate) fn input_struct_ident(rel: &Relation) -> Ident {
    format_ident!("{}Input", pascal_case(rel.name()))
}

// ------------------------------------------------------------
// Preamble
// ------------------------------------------------------------

/// `use` lines the emitted input-handler code needs — only pulls in
/// interning / `OrderedFloat` / `SEMIRING_ONE` when at least one EDB
/// actually needs them.
fn gen_preamble(program: &Program, features: &Features) -> TokenStream {
    let facts = program.facts();
    let edbs = program.edbs();
    let has_any_inline = edbs.iter().any(|rel| facts.contains_key(rel.name()));
    let needs_ordered_float = edbs
        .iter()
        .any(|rel| rel.data_type().iter().any(|dt| dt.is_float()));

    let intern_import = if features.string_intern() {
        quote! {
            use super::intern;
            use lasso::Spur;
        }
    } else {
        quote! {}
    };
    let ordered_float_import = if needs_ordered_float {
        quote! { use ordered_float::OrderedFloat; }
    } else {
        quote! {}
    };
    let semiring_one_import = if has_any_inline {
        quote! { use super::SEMIRING_ONE; }
    } else {
        quote! {}
    };

    quote! {
        use differential_dataflow::input::InputSession;

        use super::{Diff, Ts};
        #semiring_one_import
        #intern_import
        #ordered_float_import
    }
}

// ------------------------------------------------------------
// `Inputs` container — one field per EDB, bulk-apply helpers
// ------------------------------------------------------------

fn gen_inputs_container(edbs: &[&Relation]) -> TokenStream {
    if edbs.is_empty() {
        return quote! {};
    }

    let fields: Vec<TokenStream> = edbs
        .iter()
        .map(|rel| {
            let f = rust_ident(rel.name());
            let ty = input_struct_ident(rel);
            quote! { pub #f: #ty }
        })
        .collect();

    // `Inputs::new` takes each already-constructed `{Name}Input` by value
    // so the signature stays free of `InputSession` type parameters (which
    // would make it unwieldy to call from the engine).
    let fn_params: Vec<TokenStream> = edbs
        .iter()
        .map(|rel| {
            let p = format_ident!("h_{}", rel.name());
            let ty = input_struct_ident(rel);
            quote! { #p: #ty }
        })
        .collect();

    let inits: Vec<TokenStream> = edbs
        .iter()
        .map(|rel| {
            let f = rust_ident(rel.name());
            let p = format_ident!("h_{}", rel.name());
            quote! { #f: #p }
        })
        .collect();

    let per_field = |method: TokenStream| -> Vec<TokenStream> {
        edbs.iter()
            .map(|rel| {
                let f = rust_ident(rel.name());
                quote! { self.#f.#method; }
            })
            .collect()
    };
    let apply_inline = per_field(quote! { apply_inline(index) });
    let close = per_field(quote! { close() });
    let advance = per_field(quote! { advance_to(t) });
    let flush = per_field(quote! { flush() });

    quote! {
        /// Concrete container holding one input handler per EDB. The library
        /// engine owns this and calls typed methods directly on each field
        /// — no dynamic dispatch, no downcast.
        pub(crate) struct Inputs {
            #(#fields,)*
        }

        impl Inputs {
            pub fn new(#(#fn_params),*) -> Self {
                Self { #(#inits,)* }
            }

            pub fn apply_inline_all(&mut self, index: usize) {
                #(#apply_inline)*
            }

            pub fn close_all(&mut self) {
                #(#close)*
            }

            pub fn advance_to_all(&mut self, t: Ts) {
                #(#advance)*
            }

            pub fn flush_all(&mut self) {
                #(#flush)*
            }
        }
    }
}

#[cfg(test)]
mod ident_tests {
    use super::rust_ident;
    use quote::quote;

    /// All Rust strict + reserved keywords. A relation/field name may coincide
    /// with any of these, so `rust_ident` must escape every one into a valid,
    /// usable identifier.
    const RUST_KEYWORDS: &[&str] = &[
        "as", "break", "const", "continue", "crate", "dyn", "else", "enum", "extern", "false",
        "fn", "for", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref",
        "return", "self", "Self", "static", "struct", "super", "trait", "true", "type", "unsafe",
        "use", "where", "while", "async", "await", "abstract", "become", "box", "do", "final",
        "macro", "override", "priv", "typeof", "unsized", "virtual", "yield", "try",
    ];

    /// Non-keyword names pass through unchanged (the common case).
    #[test]
    fn non_keyword_names_pass_through() {
        assert_eq!(rust_ident("VarPointsTo").to_string(), "VarPointsTo");
        assert_eq!(rust_ident("method_lookup").to_string(), "method_lookup");
    }

    /// Raw-able keywords become raw identifiers; the four that cannot be raw
    /// (`crate`/`self`/`Self`/`super`) get a trailing underscore.
    #[test]
    fn keywords_are_escaped() {
        assert_eq!(rust_ident("type").to_string(), "r#type");
        assert_eq!(rust_ident("match").to_string(), "r#match");
        assert_eq!(rust_ident("in").to_string(), "r#in");
        assert_eq!(rust_ident("true").to_string(), "r#true");
        assert_eq!(rust_ident("super").to_string(), "super_");
        assert_eq!(rust_ident("self").to_string(), "self_");
        assert_eq!(rust_ident("Self").to_string(), "Self_");
        assert_eq!(rust_ident("crate").to_string(), "crate_");
    }

    /// Every keyword must yield an identifier usable both as a binding and as
    /// an expression — `let <id> = 1; let _ = <id>;` must parse as valid Rust.
    /// This is exactly how codegen uses these idents, and it guards against any
    /// keyword that is neither bare-usable nor raw-able (previously `crate`,
    /// `self`, `Self`, `super` panicked here via `Ident::new_raw`).
    #[test]
    fn every_keyword_yields_usable_binding() {
        for kw in RUST_KEYWORDS {
            let id = rust_ident(kw);
            let ts = quote! { { let #id = 1; let _ = #id; } };
            syn::parse2::<syn::Block>(ts).unwrap_or_else(|e| {
                panic!("keyword {kw:?} -> `{id}` is not a usable binding: {e}")
            });
        }
    }

    /// Distinct keyword names never collapse to the same identifier.
    #[test]
    fn keyword_escapes_are_distinct() {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        for kw in RUST_KEYWORDS {
            assert!(
                seen.insert(rust_ident(kw).to_string()),
                "duplicate escaped ident for keyword {kw:?}"
            );
        }
    }
}
