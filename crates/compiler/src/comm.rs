use crate::Compiler;
use common::ExecutionMode;
use proc_macro2::TokenStream;
use quote::quote;

/// Deduplication strategy for different execution modes.
impl Compiler {
    /// Deduplication for differential dataflow collections.
    pub(crate) fn dedup_collection(&mut self) -> TokenStream {
        self.imports.mark_timely_map();
        match self.config.mode() {
            ExecutionMode::Incremental => {
                self.imports.mark_threshold();
                quote! { .threshold(|_, w| if *w > 0 { 1 } else { 0 }) }
            }
            ExecutionMode::Batch => {
                self.imports.mark_threshold_total();
                quote! {
                    .threshold_semigroup(move |_, _, old| old.is_none().then_some(SEMIRING_ONE))
                }
            }
        }
    }
}
