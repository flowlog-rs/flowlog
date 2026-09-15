//! Diff type codegen for the `(Data, Diff, Time)` triple.
//!
//! Batch mode uses idempotent `StaticPresent` weights. Incremental mode
//! uses signed `i32` counts, with thresholds maintaining set membership.

use flowlog_common::ExecutionMode;
use proc_macro2::TokenStream;
use quote::quote;

use crate::codegen::CodeGen;

impl CodeGen {
    /// Emit the `type Diff = ...` alias for the current execution mode.
    pub(crate) fn diff_type(&self) -> TokenStream {
        match self.config.mode() {
            ExecutionMode::Batch => {
                quote! { type Diff = ::flowlog_runtime::diff::StaticPresent; }
            }
            ExecutionMode::Inc => quote! { type Diff = i32; },
        }
    }

    /// Emit `const SEMIRING_ONE: Diff = ...`.
    ///
    /// The constant carries `allow(dead_code)`: only generated programs
    /// that preload files or apply inline facts reference it, and which
    /// of those paths exist varies per program and mode.
    pub(crate) fn semiring_one_value(&self) -> TokenStream {
        match self.config.mode() {
            ExecutionMode::Batch => {
                quote! {
                    #[allow(dead_code)]
                    const SEMIRING_ONE: Diff = ::flowlog_runtime::diff::StaticPresent;
                }
            }
            ExecutionMode::Inc => quote! {
                #[allow(dead_code)]
                const SEMIRING_ONE: Diff = 1;
            },
        }
    }
}
