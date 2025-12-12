use crate::Compiler;
use common::ExecutionMode;
use proc_macro2::TokenStream;
use quote::quote;

/// Deduplication strategy for different execution modes.
impl Compiler {
    /// Deduplication for differential dataflow collections.
    pub(crate) fn dedup_collection(&self) -> TokenStream {
        match self.config.mode() {
            ExecutionMode::Incremental => {
                quote! { .threshold(|_, w| if *w > 0 { 1 } else { 0 }) }
            }
            ExecutionMode::Batch => {
                quote! {
                    .threshold_semigroup(move |_, _, old| old.is_none().then_some(SEMIRING_ONE))
                }
            }
        }
    }
}
