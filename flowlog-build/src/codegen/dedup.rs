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
    ///
    /// In incremental modes the diff type is `i32`, so multiplicities must be
    /// clamped to 0/1. The outer timestamp is a total order (`u32`), so we use
    /// the total-order-specialised `threshold_total` rather than the general
    /// `threshold` (which routes through the partial-order `reduce_abelian`
    /// machinery). Same result, lighter operator.
    pub(crate) fn dedup_nonrecursive(&mut self) -> TokenStream {
        if self.config.is_datalog_batch() {
            quote! { .consolidate() }
        } else {
            self.features.mark_threshold_total();
            threshold_total_nonzero()
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
    ///
    /// Antijoin runs in the outer (total-order) scope, so incremental modes
    /// use the total-order `threshold_total`, matching `dedup_nonrecursive`.
    pub(crate) fn dedup_antijoin(&mut self) -> TokenStream {
        if self.config.is_datalog_batch() {
            quote! {}
        } else {
            self.features.mark_threshold_total();
            threshold_total_nonzero()
        }
    }
}

/// `threshold` clamped to 0/1 — the non-batch diff-mode dedup for scopes whose
/// timestamp is only a lattice (e.g. inside recursion, `Product<_, _>`).
fn threshold_nonzero() -> TokenStream {
    quote! { .threshold(|_, w| if *w > 0 { SEMIRING_ONE } else { 0 }) }
}

/// `threshold_total` clamped to 0/1 — the total-order-specialised dedup for
/// non-recursive / antijoin scopes in incremental modes (outer `u32` time).
/// Semantically identical to [`threshold_nonzero`] on a total order, but avoids
/// the general `reduce_abelian` path, so it is cheaper to build and maintain.
fn threshold_total_nonzero() -> TokenStream {
    quote! { .threshold_total(|_, w| if *w > 0 { SEMIRING_ONE } else { 0 }) }
}

#[cfg(test)]
mod tests {
    use flowlog_common::Config;
    use flowlog_common::ExecutionMode;
    use flowlog_parser::Program;

    use super::*;

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

    /// In incremental mode (`i32` diffs) the diff type still needs clamping to
    /// 0/1, but the *scope's timestamp* decides which operator is cheapest:
    /// non-recursive / antijoin flows run in the outer total order (`u32`) and
    /// use the specialised `threshold_total`, while recursive flows run under a
    /// `Product<_, _>` lattice (only a partial order) and must fall back to the
    /// general `threshold`. This guards against a new `ExecutionMode` silently
    /// changing that mapping.
    #[test]
    fn datalog_inc_uses_threshold_total_outside_recursion() {
        let mut cg = codegen_with_mode(ExecutionMode::DatalogInc);

        let non_rec = cg.dedup_nonrecursive().to_string();
        assert!(
            non_rec.contains("threshold_total") && !non_rec.contains("consolidate"),
            "inc non-recursive must emit threshold_total(...), got: {non_rec}"
        );

        let anti = cg.dedup_antijoin().to_string();
        assert!(
            anti.contains("threshold_total"),
            "inc antijoin must emit threshold_total(...), got: {anti}"
        );

        let rec = cg.dedup_recursive().to_string();
        assert!(
            rec.contains("threshold")
                && !rec.contains("threshold_total")
                && !rec.contains("threshold_semigroup"),
            "inc recursive must emit the general threshold(...) (Product time is only \
             a partial order), got: {rec}"
        );
    }
}
