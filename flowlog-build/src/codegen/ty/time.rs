//! Time type codegen for the `(Data, Diff, Time)` triple.
//!
//! Differential dataflow timestamps form a lattice. Non-recursive programs use
//! a single outer timestamp; recursive programs nest an inner iteration counter
//! via `Product<Outer, Inner>`.

use flowlog_common::ExecutionMode;
use proc_macro2::TokenStream;
use quote::quote;

use crate::codegen::CodeGen;

impl CodeGen {
    /// Emit `type Ts = ...;` alias for the outer dataflow timestamp.
    ///
    /// - Incremental: `u32` (monotonically advancing epoch).
    /// - Batch: `()` (single-shot execution).
    pub(crate) fn timestamp_alias(&self) -> TokenStream {
        match self.config.mode() {
            ExecutionMode::Inc => quote! { type Ts = u32; },
            ExecutionMode::Batch => quote! { type Ts = (); },
        }
    }

    /// Inner iteration timestamp for recursive strata (`type Iter = u16`).
    ///
    /// Only emitted when the program contains at least one recursive stratum.
    /// The generated type alias is referenced by `Product<Outer, Iter>` inside
    /// recursive `iterate` scopes.
    pub(crate) fn inner_time_type(&self) -> TokenStream {
        if self.features.recursive() {
            quote! { type Iter = u16; }
        } else {
            quote! {}
        }
    }
}
