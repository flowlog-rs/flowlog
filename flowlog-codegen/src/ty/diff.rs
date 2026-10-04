//! Diff type codegen for the `(Data, Diff, Time)` triple: each collection's
//! weight, and the mutability recorded for it by fingerprint.
//!
//! Each collection carries the weight of its own mutability: idempotent
//! presence, `diff::Static` or `diff::Append`, for a collection that never
//! loses a row, and signed `diff::Mutable` counts, with thresholds
//! maintaining set membership, for a mutable one. Operators dispatch on
//! the weight, so only the inputs name theirs; every derived collection's
//! weight follows from its operator's inputs.

use flowlog_parser::Mutability;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;

/// Returns the runtime weight type of a collection with `mutability`.
pub fn weight_tokens(mutability: Mutability) -> TokenStream {
    match mutability {
        Mutability::Static => quote! { ::flowlog_runtime::diff::Static },
        Mutability::Append => quote! { ::flowlog_runtime::diff::Append },
        Mutability::Mutable => quote! { ::flowlog_runtime::diff::Mutable },
    }
}

impl Codegen {
    /// Returns the mutability recorded for the collection or relation `fp`,
    /// or an internal error when none is: every collection's mutability is
    /// recorded before codegen reads it.
    pub(crate) fn mutability(&self, fp: u64) -> Result<Mutability, CodegenError> {
        self.global_fp_to_mutability
            .get(&fp)
            .copied()
            .ok_or_else(|| {
                CodegenError::internal(format!(
                    "collection `{}` (fingerprint 0x{fp:016x}) has no recorded mutability",
                    self.display_name(fp),
                ))
            })
    }
}
