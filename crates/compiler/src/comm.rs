use crate::Compiler;
use common::ExecutionMode;
use proc_macro2::TokenStream;
use quote::quote;

impl Compiler {
    pub(crate) fn threshold_chain(&self) -> TokenStream {
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
