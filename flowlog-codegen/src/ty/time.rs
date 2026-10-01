//! Time type codegen for the `(Data, Diff, Time)` triple, at the two levels
//! tabled in `flowlog_runtime::time`: the engine's outer timestamp, which is
//! program-wide, and each loop's inner one, which follows its mutability.

use flowlog_parser::Mutability;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;

impl Codegen {
    /// Returns `type Ts = ...;`, the engine's outer timestamp: `time::Epoch`
    /// when some input is mutable, else `time::Once`.
    pub(crate) fn outer_time_tokens(&self) -> TokenStream {
        if self.program.is_incremental() {
            quote! { type Ts = ::flowlog_runtime::time::Epoch; }
        } else {
            quote! { type Ts = ::flowlog_runtime::time::Once; }
        }
    }

    /// Returns the inner time of a loop whose heads have `mutability`, and
    /// the summary of its feedback edge. An engine that runs once loops at
    /// `OnceLoop`. An engine whose epochs advance loops a mutable loop at
    /// `EpochLoop` and a static one at `LexLoop`, whose total order the
    /// presence operators need.
    ///
    /// Returns an internal error for a mutable loop in an engine that runs
    /// once: no input there can change, so the planner cannot produce one.
    pub(crate) fn inner_time_tokens(
        &self,
        mutability: Mutability,
    ) -> Result<(TokenStream, TokenStream), CodegenError> {
        let product_step = quote! { timely::order::Product::new(Default::default(), 1) };
        match (self.program.is_incremental(), mutability) {
            (false, Mutability::Static) => {
                Ok((quote! { ::flowlog_runtime::time::OnceLoop }, product_step))
            }
            (false, Mutability::Mutable) => Err(CodegenError::internal(
                "a mutable loop in an engine without a mutable input",
            )),
            (true, Mutability::Static) => Ok((
                quote! { ::flowlog_runtime::time::LexLoop },
                quote! { ::flowlog_runtime::time::LexLoop::NEXT_ITERATION },
            )),
            (true, Mutability::Mutable) => {
                Ok((quote! { ::flowlog_runtime::time::EpochLoop }, product_step))
            }
        }
    }
}
