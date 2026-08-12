//! Per-EDB `(handle, collection)` declarations for the dataflow scope.

use flowlog_parser::DataType;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::codegen::CodeGen;
use crate::codegen::ty::data::data_type_tokens;

impl CodeGen {
    /// Generate per-EDB declarations as `(handle, collection)` pairs:
    ///
    /// ```ignore
    /// let (h_<rel>, <rel>) = scope.new_collection::<_, Diff>();
    /// ```
    ///
    /// The collection is rebound through the outer-scope set clamp unless
    /// [`flowlog_common::Config::skips_edb_normalization`] waives it for
    /// this relation.
    pub(crate) fn gen_edb_decls(&mut self, plan_graph: &mut Option<PlanGraph>) -> Vec<TokenStream> {
        // A trusted caller already delivers set-correct deltas, so the clamp
        // is pure cost for them: one arrangement per EDB on the hottest
        // path. Every downstream dedup (rule outputs, recursion) stays in
        // place either way.
        let trusted = self.config.skips_edb_normalization();

        // `.fact` rows are the compiler's own startup inserts, outside the
        // caller's promise: the source may list a row twice, so a relation
        // seeded that way keeps its clamp. Decided per relation here,
        // before the `&mut self` call below.
        let clamped: Vec<bool> = self
            .program
            .edbs()
            .iter()
            .map(|rel| !trusted || self.program.has_inline_facts(rel.name()))
            .collect();

        let normalize = if trusted && !clamped.iter().any(|keep| *keep) {
            quote! {}
        } else {
            self.dedup_nonrecursive()
        };

        let edbs = self.program.edbs();
        if edbs.is_empty() {
            return Vec::new();
        }

        self.features.mark_dd_input();

        if self.config.str_intern_enabled()
            && edbs
                .iter()
                .any(|rel| rel.data_type().contains(&DataType::String))
        {
            self.features.mark_string_intern();
        }

        if edbs.iter().any(|rel| {
            let dt = rel.data_type();
            dt.contains(&DataType::Float32) || dt.contains(&DataType::Float64)
        }) {
            self.features.mark_ordered_float();
        }

        // Record the enter-inputs block when profiling is on
        with_plan_graph(plan_graph, |plan_graph| {
            plan_graph.update_input_block();
        });

        let str_intern = self.config.str_intern_enabled();
        let empty = TokenStream::new();
        edbs.iter()
            .zip(clamped)
            .map(|(rel, normalize_this)| {
                let handle = format_ident!("h{}", rel.name());
                // The collection binding comes from the global ident map —
                // never re-derived from the name — so it always matches the
                // ident every downstream flow resolves via fingerprint.
                let coll = self.find_global_ident(rel.fingerprint());

                // Record the source-file input and dedup operators when profiling is on
                with_plan_graph(plan_graph, |plan_graph| {
                    plan_graph.input_edb_operator(rel.raw_name().to_string(), coll.to_string());
                    if normalize_this {
                        plan_graph.input_dedup_operator(
                            rel.raw_name().to_string(),
                            coll.to_string(),
                            coll.to_string(),
                        );
                    }
                });

                // The matching `InputSession` handle is always typed from the
                // declared column types (see `gen_input_struct`), so annotating
                // the collection's element type identically is provably
                // consistent and frees the generated crate from fragile
                // inference (e.g. fact-only or orphan relations whose element
                // type is otherwise un-inferable).
                let ty = data_type_tokens(&rel.data_type(), str_intern);
                let clamp = if normalize_this { &normalize } else { &empty };

                quote! {
                    let (#handle, #coll) = scope.new_collection::<#ty, Diff>();
                    let #coll = #coll #clamp;
                }
            })
            .collect()
    }

    /// Generate a *single* mutable handle binding pattern for one or more handles.
    ///
    /// We intentionally shadow the original handles returned from `new_collection(...)`
    /// so downstream code can uniformly work with mutable handles:
    /// - 0 inputs: emits a harmless binding and returns `()`.
    /// - 1 input:  `let mut hR = worker.dataflow(...);` and returns `hR`.
    /// - N inputs: `let (mut hA, mut hB, ...) = worker.dataflow(...);` and returns `(hA, hB, ...)`.
    ///
    /// In **incremental** mode, we additionally bind/return a `probe` handle as the last element.
    pub(crate) fn gen_handle_binding(&self) -> (TokenStream, TokenStream) {
        let edb_names = self.program.edb_names();
        let hs: Vec<_> = edb_names.iter().map(|n| format_ident!("h{}", n)).collect();

        // Incremental mode additionally binds a probe handle as the last element.
        if self.config.is_incremental() {
            let probe = format_ident!("probe");
            match hs.len() {
                0 => (quote! { ( #probe, ) }, quote! { #probe }),
                1 => {
                    let h = &hs[0];
                    (quote! { ( #h, #probe ) }, quote! { ( #h, #probe ) })
                }
                _ => (
                    quote! { ( #(#hs),*, #probe ) },
                    quote! { ( #(#hs),*, #probe ) },
                ),
            }
        } else {
            match hs.len() {
                0 => (quote! { _handles }, quote! { () }),
                1 => {
                    let h = &hs[0];
                    (quote! { #h }, quote! { #h })
                }
                _ => (quote! { ( #(#hs),* ) }, quote! { ( #(#hs),* ) }),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flowlog_common::Config;
    use flowlog_common::ExecutionMode;
    use flowlog_common::SourceMap;
    use rstest::rstest;
    use tempfile::NamedTempFile;

    use super::*;

    const PROGRAM: &str = "\
        .decl Edge(src: int32, dst: int32)\n\
        .decl Reach(src: int32, dst: int32)\n\
        .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
        .output Reach\n\
        Reach(s, d) :- Edge(s, d).\n";

    /// `Seed` is fed by the compiler's own `.fact` rows and `Edge` only by
    /// the caller: the two halves of the trusted-inputs decision in one
    /// program.
    const PROGRAM_WITH_FACTS: &str = "\
        .decl Edge(src: int32, dst: int32)\n\
        .decl Seed(src: int32, dst: int32)\n\
        .decl Reach(src: int32, dst: int32)\n\
        .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
        .output Reach\n\
        Seed(1, 2).\n\
        Seed(1, 2).\n\
        Reach(s, d) :- Edge(s, d).\n\
        Reach(s, d) :- Seed(s, d).\n";

    /// The rendered `let (h<rel>, <rel>) = ...` block under one flag
    /// combination. Goes through [`CodeGen`] rather than a hand-built map
    /// because `gen_edb_decls` resolves collection idents through the
    /// global fingerprint map.
    fn edb_decls_of(source: &str, mode: ExecutionMode, trusted_set_inputs: bool) -> Vec<String> {
        let mut tmp = NamedTempFile::new().expect("temp file");
        tmp.write_all(source.as_bytes()).expect("write program");
        let mut config = Config {
            program: tmp.path().to_string_lossy().into_owned(),
            mode,
            trusted_set_inputs,
            ..Config::default()
        };
        let path = config.program.clone();
        let program = flowlog_parser::parse(&path, &[], &mut SourceMap::new(), &mut config)
            .expect("program parses");

        let mut cg = CodeGen::new(config, program);
        cg.make_global_ident_map();
        cg.gen_edb_decls(&mut None)
            .iter()
            .map(TokenStream::to_string)
            .collect()
    }

    fn edb_decls(mode: ExecutionMode, trusted_set_inputs: bool) -> String {
        edb_decls_of(PROGRAM, mode, trusted_set_inputs).join("\n")
    }

    /// The one declaration whose handle ident derives from `name`. Handle
    /// idents are the lowercased relation name, as `Relation::name` returns.
    fn decl_for(decls: &[String], name: &str) -> String {
        let handle = format!("h{} ,", name.to_lowercase());
        decls
            .iter()
            .find(|decl| decl.contains(&handle))
            .unwrap_or_else(|| panic!("no declaration for {name} in {decls:?}"))
            .clone()
    }

    #[rstest]
    #[case::datalog_batch(ExecutionMode::DatalogBatch, "consolidate")]
    #[case::datalog_inc(ExecutionMode::DatalogInc, "threshold_total")]
    #[case::extend_batch(ExecutionMode::ExtendBatch, "threshold_total")]
    #[case::extend_inc(ExecutionMode::ExtendInc, "threshold_total")]
    fn edb_inputs_are_clamped_by_default(
        #[case] mode: ExecutionMode,
        #[case] expected_clamp: &str,
    ) {
        let decls = edb_decls(mode, false);
        assert!(
            decls.contains(expected_clamp),
            "{mode:?} must clamp EDB inputs by default, got: `{decls}`"
        );
    }

    #[test]
    fn trusted_set_inputs_drops_the_incremental_edb_clamp() {
        let decls = edb_decls(ExecutionMode::DatalogInc, true);
        assert!(
            !decls.contains("threshold"),
            "trusted inputs must emit no input clamp, got: `{decls}`"
        );
        assert!(
            decls.contains("new_collection"),
            "the input collection itself must still be declared, got: `{decls}`"
        );
    }

    /// Only `DatalogInc` inputs are the caller's deltas; the other modes
    /// feed themselves and must ignore the promise.
    #[rstest]
    #[case::datalog_batch(ExecutionMode::DatalogBatch, "consolidate")]
    #[case::extend_batch(ExecutionMode::ExtendBatch, "threshold_total")]
    #[case::extend_inc(ExecutionMode::ExtendInc, "threshold_total")]
    fn trusted_set_inputs_leaves_every_other_mode_clamped(
        #[case] mode: ExecutionMode,
        #[case] expected_clamp: &str,
    ) {
        let decls = edb_decls(mode, true);
        assert!(
            decls.contains(expected_clamp),
            "{mode:?} must keep its EDB clamp under trusted inputs, got: `{decls}`"
        );
    }

    /// `.fact` rows are inserted by the generated engine itself and can
    /// repeat, so the caller's promise cannot cover the relation they seed.
    #[test]
    fn a_relation_with_inline_facts_keeps_its_clamp_under_trusted_inputs() {
        let decls = edb_decls_of(PROGRAM_WITH_FACTS, ExecutionMode::DatalogInc, true);
        let seed = decl_for(&decls, "Seed");
        let edge = decl_for(&decls, "Edge");
        assert!(
            seed.contains("threshold_total"),
            "a `.fact`-seeded relation must stay clamped, got: `{seed}`"
        );
        assert!(
            !edge.contains("threshold_total"),
            "a caller-only relation must drop its clamp, got: `{edge}`"
        );
    }

    /// Without the promise the per-relation split must not appear at all.
    #[test]
    fn inline_facts_change_nothing_by_default() {
        for decl in edb_decls_of(PROGRAM_WITH_FACTS, ExecutionMode::DatalogInc, false) {
            assert!(
                decl.contains("threshold_total"),
                "every EDB is clamped by default, got: `{decl}`"
            );
        }
    }
}
