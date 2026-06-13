//! Library-mode result-struct codegen.
//!
//! Two structs, two semantics:
//!
//! - `BatchResults` (returned by `DatalogBatchEngine::run()`) is a
//!   snapshot. One field per output-surfaced relation:
//!     - `.output Foo` (arity > 0) → `pub foo: Vec<rel::Foo>`
//!     - `.output Foo` (nullary)   → `pub foo: bool`
//!     - `.printsize Foo`          → `pub foo_size: usize`
//!
//! - `IncrementalResults` (returned by `Transaction::commit()`) is a
//!   delta. The engine forwards the per-epoch differential output
//!   without folding it into a running state — callers that want a
//!   snapshot maintain it themselves. Per relation:
//!     - `.output Foo` (arity > 0) → `pub foo: Vec<(rel::Foo, i32)>`
//!     - `.output Foo` (nullary)   → `pub foo: i32`  (net diff)
//!     - `.printsize Foo`          → `pub foo_size: i32` (size delta)
//!
//! A relation carrying both directives gets both fields.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::parser::Program;

use crate::build::relation::{printsize_field_ident, results_field_ident, user_struct_ident};

/// `BatchResults` — returned by `DatalogBatchEngine::run()`.
pub(crate) fn gen_batch_results(program: &Program) -> TokenStream {
    let struct_ident = format_ident!("BatchResults");
    let mut fields: Vec<TokenStream> = Vec::new();

    for rel in program.output_idbs() {
        let field = results_field_ident(rel);
        if rel.arity() == 0 {
            fields.push(quote! { pub #field: bool });
        } else {
            let tuple_struct = user_struct_ident(rel);
            fields.push(quote! { pub #field: Vec<rel::#tuple_struct> });
        }
    }

    for rel in program.printsize_idbs() {
        let field = printsize_field_ident(rel);
        fields.push(quote! { pub #field: usize });
    }

    quote! {
        #[derive(Clone, Debug, Default)]
        pub struct #struct_ident {
            #(#fields),*
        }
    }
}

/// `IncrementalResults` — returned by `Transaction::commit()`. Carries
/// the deltas produced by the just-closed epoch, not a snapshot.
pub(crate) fn gen_incremental_results(program: &Program) -> TokenStream {
    let struct_ident = format_ident!("IncrementalResults");
    let mut fields: Vec<TokenStream> = Vec::new();

    for rel in program.output_idbs() {
        let field = results_field_ident(rel);
        if rel.arity() == 0 {
            fields.push(quote! { pub #field: i32 });
        } else {
            let tuple_struct = user_struct_ident(rel);
            fields.push(quote! { pub #field: Vec<(rel::#tuple_struct, i32)> });
        }
    }

    for rel in program.printsize_idbs() {
        let field = printsize_field_ident(rel);
        fields.push(quote! { pub #field: i32 });
    }

    quote! {
        #[derive(Clone, Debug, Default)]
        pub struct #struct_ident {
            #(#fields),*
        }
    }
}
