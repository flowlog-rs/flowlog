//! Diff type codegen for the `(Data, Diff, Time)` triple.
//!
//! The diff (or "difference") type controls how multiplicities are represented
//! in the differential dataflow collections:
//!
//! - **`DatalogBatch`**: uses `Present` — a Boolean semiring where
//!   `Present + Present = Present`, naturally enforcing set semantics.
//! - **Other modes**: uses `i32` — an integer ring that requires explicit
//!   `threshold` operators to normalise multiplicities.

use common::ExecutionMode;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Compiler;

impl Compiler {
    /// Emit the `type Diff = ...` alias for the current execution mode.
    pub(crate) fn diff_type(&self) -> TokenStream {
        match self.config.mode() {
            ExecutionMode::DatalogBatch => {
                quote! { type Diff = differential_dataflow::difference::Present; }
            }
            _ => quote! { type Diff = i32; },
        }
    }

    /// Emit `const SEMIRING_ONE: Diff = ...` when `threshold_semigroup` is used.
    ///
    /// Only emitted when at least one dedup operator requires a semiring unit
    /// value (e.g. recursive dedup in `DatalogBatch` mode).
    pub(crate) fn semiring_one_value(&self) -> TokenStream {
        if !self.imports.needs_semiring_one() {
            return quote! {};
        }

        match self.config.mode() {
            ExecutionMode::DatalogBatch => {
                quote! { const SEMIRING_ONE: Diff = differential_dataflow::difference::Present; }
            }
            _ => quote! { const SEMIRING_ONE: Diff = 1; },
        }
    }
}
