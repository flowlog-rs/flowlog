//! Per-EDB `(handle, collection)` declarations for the dataflow scope.

use flowlog_common::ExecutionMode;
use flowlog_parser::DataType;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::codegen::CodeGen;
use crate::codegen::ty::data::data_type_tokens;

impl CodeGen {
    /// Generate per-EDB declarations as `(handle, collection)` pairs,
    /// set-normalizing the input so duplicate facts cannot inflate
    /// multiplicities:
    ///
    /// ```ignore
    /// let (h_<rel>, <rel>) = scope.new_collection::<_, Diff>();
    /// let <rel> = flowlog_dedup(<rel>);
    /// ```
    pub(crate) fn gen_edb_decls(&mut self, plan_graph: &mut Option<PlanGraph>) -> Vec<TokenStream> {
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
        edbs.iter()
            .map(|rel| {
                let handle = format_ident!("h{}", rel.name());
                // The collection binding comes from the global ident map —
                // never re-derived from the name — so it always matches the
                // ident every downstream flow resolves via fingerprint.
                let coll = self.find_global_ident(rel.fingerprint());

                // Record the source-file input and dedup operators when profiling is on
                with_plan_graph(plan_graph, |plan_graph| {
                    plan_graph.input_edb_operator(rel.raw_name().to_string(), coll.to_string());
                    plan_graph.input_dedup_operator(
                        rel.raw_name().to_string(),
                        coll.to_string(),
                        coll.to_string(),
                    );
                });

                // The matching `InputSession` handle is always typed from the
                // declared column types (see `gen_input_struct`), so annotating
                // the collection's element type identically is provably
                // consistent and frees the generated crate from fragile
                // inference (e.g. fact-only or orphan relations whose element
                // type is otherwise un-inferable).
                let ty = data_type_tokens(&rel.data_type(), str_intern);

                quote! {
                    let (#handle, #coll) = scope.new_collection::<#ty, Diff>();
                    let #coll = ::flowlog_runtime::operators::flowlog_dedup(#coll);
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
        match self.config.mode() {
            ExecutionMode::Inc => {
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
            }
            ExecutionMode::Batch => match hs.len() {
                0 => (quote! { _handles }, quote! { () }),
                1 => {
                    let h = &hs[0];
                    (quote! { #h }, quote! { #h })
                }
                _ => (quote! { ( #(#hs),* ) }, quote! { ( #(#hs),* ) }),
            },
        }
    }
}
