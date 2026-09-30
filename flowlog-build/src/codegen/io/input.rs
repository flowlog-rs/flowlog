//! Input collections: each EDB's `(handle, collection)` pair inside the
//! dataflow scope, and the handles the dataflow returns to its driver.

use flowlog_parser::DataType;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::codegen::CodeGen;
use crate::codegen::input_handle_ident;
use crate::codegen::tuple_tokens;
use crate::codegen::ty::data::data_type_tokens;
use crate::codegen::ty::diff::weight_tokens;

impl CodeGen {
    /// Returns each EDB's input declaration: a `(handle, collection)` pair
    /// at the weight of the relation's declared mutability, deduplicated so
    /// a repeated fact cannot raise a multiplicity. Also marks the features
    /// the inputs need: DD inputs, and the interner and ordered floats when
    /// an input column needs them.
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

        with_plan_graph(plan_graph, |plan_graph| {
            plan_graph.update_input_block();
        });

        let str_intern = self.config.str_intern_enabled();
        edbs.iter()
            .map(|rel| {
                let handle = input_handle_ident(rel.name());
                // The collection binding comes from the global ident map,
                // never re-derived from the name, so it always matches the
                // ident every downstream flow resolves via fingerprint.
                let coll = self.find_global_ident(rel.fingerprint());

                with_plan_graph(plan_graph, |plan_graph| {
                    plan_graph.input_edb_operator(rel.raw_name().to_string(), coll.to_string());
                    plan_graph.input_dedup_operator(
                        rel.raw_name().to_string(),
                        coll.to_string(),
                        coll.to_string(),
                        rel.input_mutability(),
                    );
                });

                let ty = data_type_tokens(&rel.data_type(), str_intern);
                let weight = weight_tokens(rel.input_mutability());

                quote! {
                    let (#handle, #coll) = scope.new_collection::<#ty, #weight>();
                    let #coll = ::flowlog_runtime::operators::flowlog_dedup(#coll);
                }
            })
            .collect()
    }

    /// Returns the tuple of handles the dataflow closure returns and its
    /// caller binds, both spelled the same: every EDB's handle in name
    /// order, then `probe` for an incremental engine.
    pub(crate) fn gen_handles(&self) -> TokenStream {
        let handles = self
            .program
            .edb_names()
            .into_iter()
            .map(|name| input_handle_ident(&name))
            .chain(
                self.program
                    .is_incremental()
                    .then(|| format_ident!("probe")),
            );
        tuple_tokens(handles.map(|h| quote! { #h }))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flowlog_common::Config;
    use flowlog_common::SourceMap;
    use rstest::rstest;

    use super::*;

    // Cases: program, handles.
    #[rstest]
    #[case::no_input("", quote! { () })]
    #[case::one_input(".decl A(x: int32)\n.input A", quote! { (ha,) })]
    #[case::two_inputs(".decl B(x: int32)\n.input B\n.decl A(x: int32)\n.input A", quote! { (ha, hb) })]
    #[case::incremental(".decl A(x: int32) mutable\n.input A", quote! { (ha, probe) })]
    fn the_handles_are_every_input_by_name_then_the_probe(
        #[case] source: &str,
        #[case] expected: TokenStream,
    ) {
        let mut file = tempfile::NamedTempFile::new().expect("tempfile");
        writeln!(file, "{source}").expect("write");
        let mut config = Config::default();
        let program = flowlog_parser::parse(
            &file.path().to_string_lossy(),
            &[],
            &mut SourceMap::default(),
            &mut config,
        )
        .expect("program parses");
        let handles = CodeGen::new(config, program).gen_handles();
        assert_eq!(handles.to_string(), expected.to_string());
    }
}
