//! Deduplication operator emission for differential-dataflow.
//!
//! Each method emits the operator that enforces set semantics for a given
//! context. Two axes matter:
//!
//! - **Diff type.** `DatalogBatch` uses `Present` diffs (`P + P = P`), so
//!   `consolidate` alone is set-correct outside recursion. Other modes use
//!   `i32` diffs and need `threshold` to clamp multiplicities to 0/1.
//! - **Persistent trace.** Inside recursive scopes, tuples from earlier
//!   iterations must stay deduplicated; `consolidate` drops its trace per
//!   batch, so recursion needs `threshold_semigroup` (or plain `threshold`
//!   for i32-diff modes).

use proc_macro2::TokenStream;
use quote::quote;

use crate::codegen::CodeGen;

impl CodeGen {
    /// Dedup for EDBs and non-recursive flows — no persistent trace needed.
    pub(crate) fn dedup_nonrecursive(&mut self) -> TokenStream {
        if self.config.is_datalog_batch() {
            quote! { .consolidate() }
        } else {
            threshold_nonzero()
        }
    }

    /// Dedup for recursive / iterative scopes — trace-retaining so tuples
    /// from earlier iterations stay deduplicated.
    pub(crate) fn dedup_recursive(&mut self) -> TokenStream {
        if self.config.is_datalog_batch() {
            self.features.mark_threshold_total();
            quote! {
                .threshold_semigroup(move |_, _, old| old.is_none().then_some(SEMIRING_ONE))
            }
        } else {
            threshold_nonzero()
        }
    }

    /// Dedup before the pos/neg weight encoding inside antijoin — a no-op
    /// under `DatalogBatch` since `Present` diffs are already idempotent.
    pub(crate) fn dedup_antijoin(&mut self) -> TokenStream {
        if self.config.is_datalog_batch() {
            quote! {}
        } else {
            threshold_nonzero()
        }
    }
}

/// `threshold` clamped to 0/1 — the non-batch diff-mode dedup.
fn threshold_nonzero() -> TokenStream {
    quote! { .threshold(|_, w| if *w > 0 { SEMIRING_ONE } else { 0 }) }
}
