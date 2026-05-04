//! Per-EDB `(handle, collection)` declarations for the dataflow scope.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::codegen::CodeGen;
use crate::parser::DataType;
use crate::profiler::{with_profiler, Profiler};

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

        edbs.iter()
            .map(|rel| {
                let handle = format_ident!("h{}", rel.name());
                let coll = format_ident!("{}", rel.name());

                // Record source file input operator and dedup operator in profiler if enabled
                with_profiler(profiler, |profiler| {
                    profiler.input_edb_operator(rel.name().to_string(), coll.to_string());
                    profiler.input_dedup_operator(
                        rel.name().to_string(),
                        coll.to_string(),
                        coll.to_string(),
                    );
                });

                quote! {
                    let (#handle, #coll) = scope.new_collection::<_, Diff>();
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
