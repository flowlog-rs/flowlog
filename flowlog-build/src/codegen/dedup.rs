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
//! - **Timestamp order (i32-diff modes only).** The outer scope runs under a
//!   total-order `u32` time, where the specialised `threshold_total` is
//!   cheapest. Inside an `iterate` scope the time is `Product<_, _>` — only a
//!   partial order — where `threshold_total` (`TotalOrder`-bound) does not
//!   compile, so the general `threshold` is required. [`CodeGen::threshold_scoped`]
//!   centralises this choice so every scope-sensitive site stays correct by
//!   construction.

use proc_macro2::TokenStream;
use quote::quote;

use crate::codegen::CodeGen;

impl CodeGen {
    /// Set-dedup for flows that carry **no fixpoint trace**: EDB
    /// normalization, non-recursive rule outputs, and the SIP
    /// projection-to-key step. Under `DatalogBatch` (`Present` diffs)
    /// `consolidate()` is set-correct on its own. In the `i32`-diff modes the
    /// operator is **scope-aware**: most call sites sit in the outer scope
    /// (`recursive = false`), but SIP also projects *recursive* relations,
    /// emitting this dedup inside the `iterate` scope where the timestamp is
    /// only a partial order — see [`CodeGen::threshold_scoped`].
    pub(crate) fn dedup_setwise(&mut self, recursive: bool) -> TokenStream {
        if self.config.is_datalog_batch() {
            quote! { .consolidate() }
        } else {
            self.threshold_scoped(recursive)
        }
    }

    /// Dedup for recursive / iterative scopes — trace-retaining so tuples
    /// from earlier iterations stay deduplicated. Always emitted inside an
    /// `iterate` scope, so the diff-mode path is unconditionally the
    /// partial-order `threshold`.
    pub(crate) fn dedup_recursive(&mut self) -> TokenStream {
        if self.config.is_datalog_batch() {
            self.features.mark_threshold_total();
            quote! {
                .threshold_semigroup(move |_, _, old| old.is_none().then_some(SEMIRING_ONE))
            }
        } else {
            self.threshold_scoped(true)
        }
    }

    /// Dedup before the pos/neg weight encoding inside antijoin — a no-op
    /// under `DatalogBatch` since `Present` diffs are already idempotent.
    ///
    /// The right operator is **scope-dependent** in incremental modes.
    /// Antijoins in the outer (non-recursive) scope run under the total-order
    /// `u32` timestamp; stratified negation is also legal *inside* a recursive
    /// stratum (negating a lower stratum), where the antijoin — and this dedup
    /// — is emitted under the `Product<_, _>` iteration timestamp. `recursive`
    /// is threaded from the call site, the only place that knows whether the
    /// transformation is emitted inside an `iterate` scope; the operator choice
    /// itself lives in [`CodeGen::threshold_scoped`].
    pub(crate) fn dedup_antijoin(&mut self, recursive: bool) -> TokenStream {
        if self.config.is_datalog_batch() {
            quote! {}
        } else {
            self.threshold_scoped(recursive)
        }
    }

    /// Single source of truth for the diff-mode dedup operator. The choice is
    /// purely *"is the current scope's timestamp a total order?"*: the outer
    /// scope runs under the total-order `u32` inc timestamp, so it takes the
    /// specialised `threshold_total` (which routes around the general
    /// partial-order `reduce_abelian` machinery — same result, lighter
    /// operator); inside an `iterate` scope the timestamp is `Product<_, _>`,
    /// only a partial order, where `threshold_total` requires `TotalOrder` and
    /// would fail to compile, so it falls back to the general `threshold`.
    /// Every scope-sensitive dedup site routes through here, so a new site
    /// cannot silently pick `threshold_total` and break under `Product` time.
    fn threshold_scoped(&mut self, recursive: bool) -> TokenStream {
        if recursive {
            threshold_nonzero()
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
    /// set-correct for trace-free flows, `threshold_semigroup(...)`
    /// retains the trace across recursion, and antijoin dedup is a
    /// no-op. The `threshold_semigroup` path also *must* mark
    /// `features.threshold_total` so `build::imports` pulls in the
    /// required trait — dropping that mark breaks recursive runs at
    /// compile time of the generated crate. `dedup_setwise` is
    /// scope-insensitive under batch (`consolidate()` either way).
    #[test]
    fn datalog_batch_emits_expected_variants_and_marks_threshold_total() {
        let mut cg = codegen_with_mode(ExecutionMode::DatalogBatch);

        for recursive in [false, true] {
            let set = cg.dedup_setwise(recursive).to_string();
            assert!(
                set.contains("consolidate"),
                "batch dedup_setwise({recursive}) must emit consolidate(), got: {set}"
            );
        }

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

        let anti = cg.dedup_antijoin(false).to_string();
        assert!(
            anti.trim().is_empty(),
            "batch antijoin dedup is a no-op, got: `{anti}`"
        );
        let anti_rec = cg.dedup_antijoin(true).to_string();
        assert!(
            anti_rec.trim().is_empty(),
            "batch antijoin dedup is a no-op in recursive scope too, got: `{anti_rec}`"
        );
    }

    /// In incremental mode (`i32` diffs) the diff type still needs clamping to
    /// 0/1, but the *scope's timestamp* decides which operator is cheapest:
    /// outer-scope flows (trace-free dedup, outer-scope antijoin) run in the
    /// outer total order (`u32`) and use the specialised `threshold_total`,
    /// while flows emitted inside an `iterate` scope — recursion, stratified
    /// negation *inside* recursion, and SIP projecting a *recursive* relation —
    /// run under a `Product<_, _>` lattice (only a partial order) and must fall
    /// back to the general `threshold`. This guards against a new
    /// `ExecutionMode` silently changing that mapping, and against any dedup
    /// site regressing to an unconditional `threshold_total` (which fails to
    /// compile under `Product` time).
    #[test]
    fn datalog_inc_uses_threshold_total_outside_recursion() {
        let mut cg = codegen_with_mode(ExecutionMode::DatalogInc);

        let set_outer = cg.dedup_setwise(false).to_string();
        assert!(
            set_outer.contains("threshold_total") && !set_outer.contains("consolidate"),
            "inc outer-scope dedup_setwise must emit threshold_total(...), got: {set_outer}"
        );

        // SIP projects recursive relations too, emitting this dedup inside the
        // `iterate` scope (`Product` time) — it must fall back to `threshold`.
        let set_rec = cg.dedup_setwise(true).to_string();
        assert!(
            set_rec.contains("threshold")
                && !set_rec.contains("threshold_total")
                && !set_rec.contains("threshold_semigroup"),
            "inc recursive-scope dedup_setwise must fall back to the general threshold(...) \
             (Product time is only a partial order), got: {set_rec}"
        );

        let anti_outer = cg.dedup_antijoin(false).to_string();
        assert!(
            anti_outer.contains("threshold_total"),
            "inc outer-scope antijoin must emit threshold_total(...), got: {anti_outer}"
        );

        let anti_rec = cg.dedup_antijoin(true).to_string();
        assert!(
            anti_rec.contains("threshold")
                && !anti_rec.contains("threshold_total")
                && !anti_rec.contains("threshold_semigroup"),
            "inc recursive-scope antijoin must fall back to the general threshold(...) \
             (Product time is only a partial order), got: {anti_rec}"
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
