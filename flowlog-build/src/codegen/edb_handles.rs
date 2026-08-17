//! Per-EDB `(handle, collection)` declarations for the dataflow scope.

use flowlog_parser::DataType;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::TokenStream;
use quote::quote;

use crate::codegen::CodeGen;
use crate::codegen::input::handle_ident;
use crate::codegen::input::input_struct_ident;
use crate::codegen::input::inputs_field_ident;
use crate::codegen::ty::data::data_type_tokens;

impl CodeGen {
    /// Generate per-EDB declarations as `(handle, collection)` pairs:
    ///
    /// ```ignore
    /// let (h_<rel>, <rel>) = scope.new_collection::<_, Diff>();
    /// ```
    pub(crate) fn gen_edb_decls(&mut self, plan_graph: &mut Option<PlanGraph>) -> Vec<TokenStream> {
        let normalize = self.dedup_nonrecursive();

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
                let handle = handle_ident(rel);
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
                    let #coll = #coll #normalize;
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
        let edbs = self.program.edbs();

        // The dataflow yields the container itself, not a tuple of handles:
        // the sessions are born in this scope, so this is where they are
        // wrapped and bundled. A program with many relations then costs a
        // two-element return rather than an N-element one.
        let inits: Vec<TokenStream> = edbs
            .iter()
            .map(|rel| {
                let field = inputs_field_ident(rel);
                let ty = input_struct_ident(rel);
                let handle = handle_ident(rel);
                quote! { #field: #ty::new(#handle) }
            })
            .collect();

        if self.config.is_incremental() {
            if edbs.is_empty() {
                return (quote! { probe }, quote! { probe });
            }
            return (
                quote! { (mut inputs, probe) },
                quote! { (Inputs { #(#inits),* }, probe) },
            );
        }

        if edbs.is_empty() {
            return (quote! { _handles }, quote! { () });
        }
        (quote! { mut inputs }, quote! { Inputs { #(#inits),* } })
    }
}
