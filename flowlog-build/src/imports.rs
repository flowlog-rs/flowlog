//! `use` statements emitted into the library-mode generated file.
//!
//! Generated code spells every external crate path through
//! `::flowlog_runtime::`, which re-exports them, so the consumer needs only
//! `flowlog-runtime` in `[dependencies]` and this module imports nothing
//! external by name.

use proc_macro2::TokenStream;
use quote::quote;

/// Emit every import the generated library-mode module needs.
pub(crate) fn gen_lib_imports(relops_body: &TokenStream) -> TokenStream {
    quote! {
        mod relops {
            #relops_body
        }
        use relops::*;
    }
}
