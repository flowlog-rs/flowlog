//! Terms: a rule's arguments (variables, constants, arithmetic, built-in and
//! UDF calls) as Rust expressions, shared by the projections, comparisons,
//! and constraints above.

use proc_macro2::TokenStream;
use quote::quote;

pub(crate) mod arithmetic;
pub(crate) mod builtin;
pub(crate) mod constant;
pub(crate) mod udf;

/// Returns a string operand as a `&str`, resolving an interned key.
pub(super) fn as_str(operand: &TokenStream, string_intern: bool) -> TokenStream {
    if string_intern {
        quote! { ::flowlog_runtime::intern::resolve(#operand) }
    } else {
        quote! { (#operand).as_str() }
    }
}
