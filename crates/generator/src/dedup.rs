use crate::Generator;
use proc_macro2::TokenStream;
use quote::quote;

/// Deduplication strategies for different execution modes.
///
/// Each method emits the appropriate differential-dataflow operator to enforce
/// set semantics (at most one copy of each distinct record).
///
/// The strategy depends on the diff type used by the execution mode:
///
/// - **`DatalogBatch`** uses `Present` diffs where `Present + Present = Present`,
///   so `consolidate` alone is sufficient outside recursive scopes.
/// - **Other modes** use `i32` diffs, requiring `threshold` to normalise
///   multiplicities to 0 or 1.
///
/// Inside recursive scopes an operator with a *persistent trace* is required so
/// that tuples emitted in earlier iterations are not re-emitted. `consolidate`
/// drops its trace after each batch, making it unsuitable for recursion.

impl Generator {
    /// Non-recursive dedup.
    ///
    /// Suitable for EDB input, non-recursive post-flow unions, and any context
    /// where no persistent trace across iterations is needed.
    pub(crate) fn dedup_nonrecursive(&mut self) -> TokenStream {
        if self.config.is_datalog_batch() {
            quote! { .consolidate() }
        } else {
            quote! { .threshold(|_, w| if *w > 0 { SEMIRING_ONE } else { 0 }) }
        }
    }

    /// Recursive dedup — trace-retaining for cross-iteration stability.
    ///
    /// Uses a trace-retaining operator so deduplication is maintained across
    /// iterations within a recursive / iterative scope.
    pub(crate) fn dedup_recursive(&mut self) -> TokenStream {
        if self.config.is_datalog_batch() {
            self.features.mark_threshold_total();
            quote! {
                .threshold_semigroup(move |_, _, old| old.is_none().then_some(SEMIRING_ONE))
            }
        } else {
            quote! { .threshold(|_, w| if *w > 0 { SEMIRING_ONE } else { 0 }) }
        }
    }

    /// Antijoin intermediate dedup — no-op for DatalogBatch (Present is idempotent).
    ///
    /// Normalises multiplicities from upstream joins before the pos/neg weight
    /// encoding used in antijoin sub-expressions. For `DatalogBatch` this is a
    /// no-op since `Present` diffs are already idempotent.
    pub(crate) fn dedup_antijoin(&mut self) -> TokenStream {
        if self.config.is_datalog_batch() {
            quote! {}
        } else {
            quote! { .threshold(|_, w| if *w > 0 { SEMIRING_ONE } else { 0 }) }
        }
    }
}
