//! Library-mode result-struct codegen.
//!
//! Emits one public struct per execution mode with the same shape: one
//! field per output-surfaced relation.
//!
//! - `.output Foo` → `pub foo: Vec<rel::Foo>` (or `pub foo: bool` if nullary)
//! - `.printsize Foo` → `pub foo_size: usize`
//!
//! A relation carrying both directives gets both fields. The only thing
//! that differs between modes is the name of the wrapping struct, so the
//! body lives here and the mode-specific entry points just pass a name.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::parser::Program;

use crate::build::relation::{rust_ident, user_struct_ident};

/// `BatchResults` — returned by `DatalogBatchEngine::run()`.
pub(crate) fn gen_batch_results(program: &Program) -> TokenStream {
    gen_results(program, "BatchResults")
}

/// `IncrementalResults` — returned by `Transaction::commit()`.
pub(crate) fn gen_incremental_results(program: &Program) -> TokenStream {
    gen_results(program, "IncrementalResults")
}

fn gen_results(program: &Program, struct_name: &str) -> TokenStream {
    let struct_ident = format_ident!("{struct_name}");
    let mut fields: Vec<TokenStream> = Vec::new();

    for rel in program.output_idbs() {
        let field = rust_ident(rel.name());
        if rel.arity() == 0 {
            fields.push(quote! { pub #field: bool });
        } else {
            let tuple_struct = user_struct_ident(rel);
            fields.push(quote! { pub #field: Vec<rel::#tuple_struct> });
        }
    }

    for rel in program.printsize_idbs() {
        let field = format_ident!("{}_size", rel.name());
        fields.push(quote! { pub #field: usize });
    }

    quote! {
        #[derive(Clone, Debug, Default)]
        pub struct #struct_ident {
            #(#fields),*
        }
    }
}
