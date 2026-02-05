use crate::Compiler;
use common::ExecutionMode;
use proc_macro2::TokenStream;
use quote::quote;

/// Deduplication strategy for different execution modes.
impl Compiler {
    /// Deduplication for differential dataflow collections.
    pub(crate) fn dedup_collection(&mut self) -> TokenStream {
        match self.config.mode() {
            ExecutionMode::Incremental => {
                self.imports.mark_threshold();
                quote! { .threshold(|_, w| if *w > 0 { 1i32 } else { 0 }) }
            }
            ExecutionMode::Batch => {
                self.imports.mark_semiring_one();
                self.imports.mark_threshold_total();
                self.imports.mark_semiring_one();
                quote! {
                    .threshold_semigroup(move |_, _, old| old.is_none().then_some(SEMIRING_ONE))
                }
            }
        }
    }
}
