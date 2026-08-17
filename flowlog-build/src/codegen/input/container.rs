//! The container holding one input handler per EDB.
//!
//! Both modes generate this, and both reach a relation as a named field:
//! the handlers have different types, so a `Vec` or map of them would mean
//! boxing behind an erased trait, and every call would be a vtable jump for
//! a relation the program already knows statically.
//!
//! Built inside the dataflow closure, where the sessions it wraps are born.

use flowlog_parser::Relation;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;

use crate::codegen::input::input_struct_ident;
use crate::codegen::input::inputs_field_ident;

/// Emit `struct Inputs` and its bulk-apply helpers.
///
/// There is no constructor: the fields are public and the caller builds it
/// with a struct literal where the sessions are created, which keeps the
/// number of relations out of any signature.
pub fn gen_inputs_container(edbs: &[&Relation]) -> TokenStream {
    if edbs.is_empty() {
        return quote! {};
    }

    let fields: Vec<TokenStream> = edbs
        .iter()
        .map(|rel| {
            let f = inputs_field_ident(rel);
            let ty = input_struct_ident(rel);
            quote! { pub #f: #ty }
        })
        .collect();

    let per_field = |call: &dyn Fn(&Ident) -> TokenStream| -> Vec<TokenStream> {
        edbs.iter()
            .map(|rel| call(&inputs_field_ident(rel)))
            .collect()
    };
    let inline_facts = per_field(&|f| {
        quote! { ::flowlog_runtime::io::Ingest::inline_facts(&mut self.#f, index); }
    });
    let advance = per_field(&|f| {
        quote! { ::flowlog_runtime::io::Ingest::advance_to(&mut self.#f, t); }
    });
    let flush = per_field(&|f| quote! { ::flowlog_runtime::io::Ingest::flush(&mut self.#f); });
    let close = per_field(&|f| quote! { ::flowlog_runtime::io::Ingest::close(&mut self.#f); });

    quote! {
        /// One input handler per EDB, reached as a named field: no dynamic
        /// dispatch, no downcast, no allocation.
        pub(crate) struct Inputs {
            #(#fields,)*
        }

        // Which bulk helpers a mode calls depends on the mode: a batch
        // driver closes its inputs and never advances them, an incremental
        // one does both every epoch. Sharing one container means some are
        // unused in any single program.
        #[allow(dead_code)]
        impl Inputs {
            /// Apply every relation's `.fact` rows, on worker `index`.
            pub fn apply_inline_all(&mut self, index: usize) {
                #(#inline_facts)*
            }

            /// Move every relation's session to epoch `t`.
            pub fn advance_to_all(&mut self, t: Ts) {
                #(#advance)*
            }

            /// Push every relation's buffered updates into the dataflow.
            pub fn flush_all(&mut self) {
                #(#flush)*
            }

            /// Close every relation's session, so the dataflow can drain.
            pub fn close_all(&mut self) {
                #(#close)*
            }
        }
    }
}
