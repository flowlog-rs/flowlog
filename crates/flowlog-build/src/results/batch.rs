//! Generate the `BatchResults` struct returned by `engine.run()`.
//!
//! One field per output-surfaced relation:
//!
//! - `.output Foo` → `pub foo: Vec<rel::Foo>` (or `pub foo: bool` if nullary)
//! - `.printsize Foo` → `pub foo_size: usize`
//!
//! A relation carrying both directives gets both fields.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use parser::Program;

use crate::relation::{rust_ident, user_struct_ident};

pub(crate) fn gen_batch_results(program: &Program) -> TokenStream {
    let mut fields: Vec<TokenStream> = Vec::new();

    for rel in program.output_idbs() {
        let field = rust_ident(rel.name());
        if rel.arity() == 0 {
            fields.push(quote! { pub #field: bool });
        } else {
            let struct_ident = user_struct_ident(rel);
            fields.push(quote! { pub #field: Vec<rel::#struct_ident> });
        }
    }

    for rel in program.printsize_idbs() {
        let field = format_ident!("{}_size", rel.name());
        fields.push(quote! { pub #field: usize });
    }

    quote! {
        #[derive(Clone, Debug, Default)]
        pub struct BatchResults {
            #(#fields),*
        }
    }
}
