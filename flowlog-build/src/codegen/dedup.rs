//! Dedup operator emission. Every choice in this file reduces to three
//! questions:
//!
//! 1. **Diff type.** `DatalogBatch` runs on the `Present` semiring —
//!    idempotent (`P + P = P`), so collections are sets by construction.
//!    All other modes carry `i32` diffs, where duplicates accumulate real
//!    multiplicities that must be clamped back to 0/1.
//! 2. **Trace retention** (batch only). Inside `iterate`, a tuple re-derived
//!    in a later iteration must stay suppressed, which needs an operator
//!    that remembers history: `consolidate()` compacts one batch and forgets,
//!    so recursion upgrades to the arrangement-backed `threshold_semigroup`.
//!    The `i32` clamps below build arrangements anyway, so this question
//!    changes nothing for them.
//! 3. **Scope** (`i32` modes only). `threshold_total` needs a totally-ordered
//!    time; the outer scope has one (`u32` incrementally, `()` under
//!    extend-batch) so it takes the cheap operator, while recursion nests
//!    `Product<_, u16>` — partial under `u32` — and falls back to the general
//!    `threshold`. The fallback keys off the `recursive` flag, not the exact
//!    order, so it stays valid even where the product is technically total.
//!
//! The two common buckets — [`CodeGen::dedup_nonrecursive`] (always outer) and
//! [`CodeGen::dedup_recursive`] (always in-loop) — need no flag. Only the two
//! constructs whose scope varies per rule take one: SIP projection
//! ([`CodeGen::dedup_projection`]) and antijoin ([`CodeGen::dedup_antijoin`]).

use proc_macro2::TokenStream;
use quote::quote;

use crate::codegen::CodeGen;

impl CodeGen {
    /// Outer-scope set-dedup (EDBs, rule outputs): trace-free and always a
    /// total order, so batch `consolidate`s and `i32` takes `threshold_total`.
    pub(crate) fn dedup_nonrecursive(&mut self) -> TokenStream {
        if self.config.is_datalog_batch() {
            quote! { .consolidate() }
        } else {
            self.features.mark_threshold_total();
            quote! { .threshold_total(|_, w| if *w > 0 { SEMIRING_ONE } else { 0 }) }
        }
    }

    /// In-loop dedup (unions / feedback inside `iterate`), where earlier
    /// iterations must stay deduplicated: batch needs the trace-retaining
    /// `threshold_semigroup` (`consolidate` forgets); the `i32` `threshold`
    /// retains a trace anyway.
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

    /// SIP projection-to-key. Trace-free, but SIP also runs on recursive rules,
    /// so its scope varies: batch `consolidate`s either way, while `i32` reuses
    /// the outer or in-loop clamp (never the trace-retaining semigroup — the
    /// batch guard runs first).
    pub(crate) fn dedup_projection(&mut self, recursive: bool) -> TokenStream {
        if self.config.is_datalog_batch() {
            quote! { .consolidate() }
        } else if recursive {
            self.dedup_recursive()
        } else {
            self.dedup_nonrecursive()
        }
    }

    /// Antijoin `(inter, final)` dedups: `inter` set-ifies the arms so the ±1
    /// weight encoding is sound (batch: no-op, already sets); `final` re-clamps
    /// after the weight concat — batch via the trace-retaining
    /// `threshold_semigroup`, since antijoins run inside recursion too.
    pub(crate) fn dedup_antijoin(&mut self, recursive: bool) -> (TokenStream, TokenStream) {
        if self.config.is_datalog_batch() {
            (quote! {}, self.dedup_recursive())
        } else {
            let clamp = self.dedup_projection(recursive);
            (clamp.clone(), clamp)
        }
    }
}

#[cfg(test)]
mod tests {
    use flowlog_common::Config;
    use flowlog_common::ExecutionMode;
    use flowlog_parser::Program;
    use rstest::rstest;

    use super::*;

    /// An empty program — the only supported way to build one is to parse.
    fn empty_program() -> Program {
        use flowlog_common::SourceMap;
        use tempfile::NamedTempFile;
        let tmp = NamedTempFile::new().expect("temp file");
        flowlog_parser::parse(
            &tmp.path().to_string_lossy(),
            &[],
            &mut SourceMap::new(),
            &mut Config::default(),
        )
        .expect("empty program parses")
    }

    fn codegen_with_mode(mode: ExecutionMode) -> CodeGen {
        let config = Config {
            mode,
            ..Config::default()
        };
        CodeGen::new(config, empty_program())
    }

    #[derive(Debug)]
    enum Emits {
        Consolidate,
        ThresholdTotal,
        GeneralThreshold,
        ThresholdSemigroup,
        Nothing,
    }

    fn assert_emits(tokens: TokenStream, expected: Emits) {
        let t = tokens.to_string();
        let ok = match expected {
            Emits::Consolidate => t.contains("consolidate"),
            Emits::ThresholdTotal => t.contains("threshold_total") && !t.contains("consolidate"),
            Emits::GeneralThreshold => {
                t.contains("threshold")
                    && !t.contains("threshold_total")
                    && !t.contains("threshold_semigroup")
            }
            Emits::ThresholdSemigroup => t.contains("threshold_semigroup"),
            Emits::Nothing => t.trim().is_empty(),
        };
        assert!(ok, "expected {expected:?}, got: `{t}`");
    }

    /// Batch is scope-insensitive: `consolidate` for the trace-free sites,
    /// `threshold_semigroup` for the trace-retaining ones, regardless of scope.
    #[rstest]
    #[case::nonrecursive(|cg: &mut CodeGen| cg.dedup_nonrecursive(), Emits::Consolidate)]
    #[case::projection_outer(|cg: &mut CodeGen| cg.dedup_projection(false), Emits::Consolidate)]
    #[case::projection_recursive(|cg: &mut CodeGen| cg.dedup_projection(true), Emits::Consolidate)]
    #[case::antijoin_inter_outer(|cg: &mut CodeGen| cg.dedup_antijoin(false).0, Emits::Nothing)]
    #[case::antijoin_inter_recursive(|cg: &mut CodeGen| cg.dedup_antijoin(true).0, Emits::Nothing)]
    #[case::antijoin_final_outer(|cg: &mut CodeGen| cg.dedup_antijoin(false).1, Emits::ThresholdSemigroup)]
    #[case::antijoin_final_recursive(|cg: &mut CodeGen| cg.dedup_antijoin(true).1, Emits::ThresholdSemigroup)]
    #[case::recursive(|cg: &mut CodeGen| cg.dedup_recursive(), Emits::ThresholdSemigroup)]
    fn datalog_batch_dedup_is_scope_insensitive(
        #[case] emit: fn(&mut CodeGen) -> TokenStream,
        #[case] expected: Emits,
    ) {
        let mut cg = codegen_with_mode(ExecutionMode::DatalogBatch);
        assert_emits(emit(&mut cg), expected);
    }

    /// Incremental: outer scope → `threshold_total`; inside `iterate`
    /// (`Product` time, partial order) → general `threshold`.
    #[rstest]
    #[case::nonrecursive(|cg: &mut CodeGen| cg.dedup_nonrecursive(), Emits::ThresholdTotal)]
    #[case::projection_outer(|cg: &mut CodeGen| cg.dedup_projection(false), Emits::ThresholdTotal)]
    #[case::antijoin_inter_outer(|cg: &mut CodeGen| cg.dedup_antijoin(false).0, Emits::ThresholdTotal)]
    #[case::antijoin_final_outer(|cg: &mut CodeGen| cg.dedup_antijoin(false).1, Emits::ThresholdTotal)]
    #[case::projection_recursive(|cg: &mut CodeGen| cg.dedup_projection(true), Emits::GeneralThreshold)]
    #[case::antijoin_inter_recursive(|cg: &mut CodeGen| cg.dedup_antijoin(true).0, Emits::GeneralThreshold)]
    #[case::antijoin_final_recursive(|cg: &mut CodeGen| cg.dedup_antijoin(true).1, Emits::GeneralThreshold)]
    #[case::recursive(|cg: &mut CodeGen| cg.dedup_recursive(), Emits::GeneralThreshold)]
    fn datalog_inc_dedup_operator_by_scope(
        #[case] emit: fn(&mut CodeGen) -> TokenStream,
        #[case] expected: Emits,
    ) {
        let mut cg = codegen_with_mode(ExecutionMode::DatalogInc);
        assert_emits(emit(&mut cg), expected);
    }

    /// Emissions using the `ThresholdTotal` trait must mark the feature that
    /// gates its import in the generated crate — dropping the mark breaks the
    /// generated build.
    #[rstest]
    #[case::batch_semigroup(ExecutionMode::DatalogBatch, |cg: &mut CodeGen| cg.dedup_recursive())]
    #[case::inc_threshold_total(ExecutionMode::DatalogInc, |cg: &mut CodeGen| cg.dedup_nonrecursive())]
    fn dedup_marks_threshold_total_feature(
        #[case] mode: ExecutionMode,
        #[case] emit: fn(&mut CodeGen) -> TokenStream,
    ) {
        let mut cg = codegen_with_mode(mode);
        assert!(
            !cg.features().threshold_total(),
            "threshold_total must start unset"
        );
        emit(&mut cg);
        assert!(
            cg.features().threshold_total(),
            "emission must mark the threshold_total feature"
        );
    }
}
