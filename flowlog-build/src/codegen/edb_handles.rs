//! Per-EDB `(handle, collection)` declarations for the dataflow scope.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::codegen::CodeGen;
use crate::codegen::ty::data::data_type_tokens;
use flowlog_parser::DataType;
use flowlog_profiler::{Profiler, with_profiler};

impl CodeGen {
    /// Generate per-EDB declarations as `(handle, collection)` pairs:
    ///
    /// ```ignore
    /// let (h_<rel>, <rel>) = scope.new_collection::<_, Diff>();
    /// ```
    pub(crate) fn gen_edb_decls(&mut self, profiler: &mut Option<Profiler>) -> Vec<TokenStream> {
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

        // Record enter inputs block if profiler is enabled
        with_profiler(profiler, |profiler| {
            profiler.update_input_block();
        });

        let str_intern = self.config.str_intern_enabled();
        edbs.iter()
            .map(|rel| {
                let handle = format_ident!("h{}", rel.name());
                // The collection binding comes from the global ident map —
                // never re-derived from the name — so it always matches the
                // ident every downstream flow resolves via fingerprint.
                let coll = self.find_global_ident(rel.fingerprint());

                // Record source file input operator and dedup operator in profiler if enabled
                with_profiler(profiler, |profiler| {
                    profiler.input_edb_operator(rel.raw_name().to_string(), coll.to_string());
                    profiler.input_dedup_operator(
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
