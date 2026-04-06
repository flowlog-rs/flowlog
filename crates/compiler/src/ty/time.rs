//! Time type codegen for the `(Data, Diff, Time)` triple.
//!
//! Differential dataflow timestamps form a lattice. Non-recursive programs use
//! a single outer timestamp; recursive programs nest an inner iteration counter
//! via `Product<Outer, Inner>`.

use proc_macro2::TokenStream;
use quote::quote;

use crate::Compiler;

impl Compiler {
    /// Outer dataflow timestamp used as the `dataflow::<T, _, _>` parameter.
    ///
    /// - Incremental mode: `u32` (monotonically advancing epoch).
    /// - Batch mode: `()` (single-shot execution).
    pub(crate) fn outer_time_type(&self) -> TokenStream {
        if self.config.is_incremental() {
            quote! { u32 }
        } else {
            quote! { () }
        }
    }

    /// Inner iteration timestamp for recursive strata (`type Iter = u16`).
    ///
    /// Only emitted when the program contains at least one recursive stratum.
    /// The generated type alias is referenced by `Product<Outer, Iter>` inside
    /// recursive `iterate` scopes.
    pub(crate) fn inner_time_type(&self) -> TokenStream {
        if self.imports.is_recursive() {
            quote! { type Iter = u16; }
        } else {
            quote! {}
        }
    }
}
