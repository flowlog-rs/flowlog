//! Diff type codegen for the `(Data, Diff, Time)` triple.
//!
//! The diff (or "difference") type controls how multiplicities are represented
//! in the differential dataflow collections:
//!
//! - **`DatalogBatch`**: uses `Present` — a Boolean semiring where
//!   `Present + Present = Present`, naturally enforcing set semantics.
//! - **Other modes**: uses `i32` — an integer ring that requires explicit
//!   `threshold` operators to normalise multiplicities.

use proc_macro2::TokenStream;
use quote::quote;

use common::ExecutionMode;

use crate::codegen::CodeGen;

impl CodeGen {
    /// Emit the `type Diff = ...` alias for the current execution mode.
    pub(crate) fn diff_type(&self) -> TokenStream {
        match self.config.mode() {
            ExecutionMode::DatalogBatch => {
                quote! { type Diff = differential_dataflow::difference::Present; }
            }
            _ => quote! { type Diff = i32; },
        }
    }

    /// Emit `const SEMIRING_ONE: Diff = ...`.
    pub(crate) fn semiring_one_value(&self) -> TokenStream {
        match self.config.mode() {
            ExecutionMode::DatalogBatch => {
                quote! { const SEMIRING_ONE: Diff = differential_dataflow::difference::Present; }
            }
            _ => quote! { const SEMIRING_ONE: Diff = 1; },
        }
    }
}
