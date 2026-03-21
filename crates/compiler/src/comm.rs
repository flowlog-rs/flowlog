use crate::Compiler;
use proc_macro2::TokenStream;
use quote::quote;

/// Deduplication strategy for different execution modes.
impl Compiler {
    /// Pure set-dedup: ensure each distinct record appears exactly once.
    ///
    /// Suitable for **non-recursive** contexts (EDB input, non-recursive
    /// post-flow union) where no persistent trace is needed.
    ///
    /// - `DatalogBatch` (`Present` diff): `consolidate` merges
    ///   `Present + Present = Present` without a persistent trace.
    /// - Other modes (`i32` diff): `threshold` normalises multiplicities
    ///   to 0/1 so set semantics are maintained.
    pub(crate) fn dedup_collection(&mut self) -> TokenStream {
        if self.config.is_datalog_batch() {
            quote! { .consolidate() }
        } else {
            quote! { .threshold(|_, w| if *w > 0 { 1i32 } else { 0 }) }
        }
    }

    /// Set-dedup inside **recursive / iterative scopes**.
    ///
    /// Inside a recursive scope the operator must maintain a persistent trace
    /// across iterations so that tuples emitted in earlier iterations are not
    /// re-emitted. `consolidate` drops its trace immediately and therefore
    /// causes duplicate explosion across iterations.
    ///
    /// - `DatalogBatch` (`Present` diff): `threshold_semigroup` keeps its
    ///   trace and correctly deduplicates across iterations.
    /// - Other modes (`i32` diff): `threshold` (which also keeps a trace)
    ///   normalises multiplicities to 0/1.
    pub(crate) fn dedup_recursive(&mut self) -> TokenStream {
        if self.config.is_datalog_batch() {
            self.imports.mark_semiring_one();
            self.imports.mark_threshold_total();
            quote! {
                .threshold_semigroup(move |_, _, old| old.is_none().then_some(SEMIRING_ONE))
            }
        } else {
            quote! { .threshold(|_, w| if *w > 0 { 1i32 } else { 0 }) }
        }
    }
}
