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

#[cfg(test)]
mod tests {
    use super::*;
    use flowlog_parser::Program;
    use flowlog_common::{Config, ExecutionMode};

    fn codegen_with_mode(mode: ExecutionMode) -> CodeGen {
        let config = Config {
            mode,
            ..Config::default()
        };
        CodeGen::new(config, Program::default())
    }

    /// `DatalogBatch` uses `Present` diffs, so `consolidate()` alone is
    /// set-correct for non-recursive flows, `threshold_semigroup(...)`
    /// retains the trace across recursion, and antijoin dedup is a
    /// no-op. The `threshold_semigroup` path also *must* mark
    /// `features.threshold_total` so `build::imports` pulls in the
    /// required trait — dropping that mark breaks recursive runs at
    /// compile time of the generated crate.
    #[test]
    fn datalog_batch_emits_expected_variants_and_marks_threshold_total() {
        let mut cg = codegen_with_mode(ExecutionMode::DatalogBatch);

        let non_rec = cg.dedup_nonrecursive().to_string();
        assert!(
            non_rec.contains("consolidate"),
            "batch non-recursive must emit consolidate(), got: {non_rec}"
        );

        assert!(
            !cg.features().threshold_total(),
            "threshold_total must start unset"
        );
        let rec = cg.dedup_recursive().to_string();
        assert!(
            rec.contains("threshold_semigroup"),
            "batch recursive must emit threshold_semigroup(...), got: {rec}"
        );
        assert!(
            cg.features().threshold_total(),
            "dedup_recursive under batch must mark threshold_total"
        );

        let anti = cg.dedup_antijoin().to_string();
        assert!(
            anti.trim().is_empty(),
            "batch antijoin dedup is a no-op, got: `{anti}`"
        );
    }

    /// In incremental mode (`i32` diffs), every dedup site uses the
    /// clamping `threshold(...)`. This guards against a new
    /// `ExecutionMode` silently falling into the batch branch — every
    /// mode except `DatalogBatch` must go through `threshold_nonzero()`.
    #[test]
    fn datalog_inc_emits_threshold_uniformly() {
        let mut cg = codegen_with_mode(ExecutionMode::DatalogInc);

        for (name, tokens) in [
            ("dedup_nonrecursive", cg.dedup_nonrecursive().to_string()),
            ("dedup_recursive", cg.dedup_recursive().to_string()),
            ("dedup_antijoin", cg.dedup_antijoin().to_string()),
        ] {
            assert!(
                tokens.contains("threshold")
                    && !tokens.contains("threshold_semigroup")
                    && !tokens.contains("consolidate"),
                "{name} under incremental mode must emit plain threshold(...), got: {tokens}"
            );
        }
    }
}
