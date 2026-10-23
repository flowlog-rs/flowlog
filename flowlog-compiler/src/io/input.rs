//! Binary input construction and preload scheduling.

use flowlog_codegen::Skeleton;
use flowlog_codegen::input_field_ident;
use flowlog_codegen::input_handle_ident;
use flowlog_parser::InputSource;
use flowlog_parser::Relation;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Compiler;

#[derive(Debug)]
pub(crate) struct Input {
    pub initialize_inputs: TokenStream,
    /// Reads every startup input into `loaded`, a `Result` that holds the
    /// first failure; the loads after it are skipped.
    pub load_inputs: TokenStream,
    /// Applies the inline facts, closes the static inputs and, when
    /// anything was preloaded, drains the first epoch.
    pub preload_inputs: TokenStream,
    /// Publishes and emits the preload epoch; empty when nothing was
    /// preloaded. The driver puts its check of `loaded` between this and
    /// `preload_inputs`, so a failed load publishes nothing.
    pub publish_preload: TokenStream,
}

impl Compiler {
    /// Builds loaders and resolves preload filenames against the runtime
    /// fact directory.
    pub(crate) fn gen_input(&self, skeleton: &Skeleton, emit_output: &TokenStream) -> Input {
        let edbs = self.program.edbs();
        let handles = edbs
            .iter()
            .map(|relation| input_handle_ident(relation.name()));
        let uses_ord = self.config.serialize_load();
        let initialize_inputs = quote! {
            let mut inputs = Inputs::new(#(#handles,)* worker.peers(), index, #uses_ord)
                .expect("valid worker coordinates");
        };

        let load_files: Vec<TokenStream> = self
            .program
            .file_inputs()
            .map(|(relation, filename)| {
                let field = input_field_ident(relation.name());
                let load = gen_load(
                    relation,
                    quote! { inputs.#field },
                    quote! { &path },
                    quote! { ::flowlog_runtime::diff::Unit::one() },
                );
                quote! {
                    {
                        let path = fact_dir.join(#filename);
                        #load?;
                    }
                }
            })
            .collect();
        let load_inputs = quote! {
            let loaded: Result<(), ::flowlog_runtime::RuntimeError> = (|| {
                #(#load_files)*
                Ok(())
            })();
        };

        // Static inputs close after the initial load, before the first
        // advance: an open static input would hold every static operator at
        // time 0, and no later command may change it.
        let publish = &skeleton.publish;
        let has_preload = !load_files.is_empty() || !self.program.facts().is_empty();
        let drain_preload = if has_preload {
            quote! {
                time_stamp += 1;
                inputs.advance_dynamic_to(time_stamp);
                inputs.flush_dynamic();
                while probe.less_than(&time_stamp) {
                    worker.step();
                }
            }
        } else {
            quote! {}
        };
        let preload_inputs = quote! {
            inputs.apply_inline_all();
            inputs.close_static();
            #drain_preload
        };
        let publish_preload = if has_preload {
            quote! {
                #publish
                barrier.wait();
                if index == 0 {
                    #emit_output
                }
                barrier.wait();
            }
        } else {
            quote! {}
        };

        Input {
            initialize_inputs,
            load_inputs,
            preload_inputs,
            publish_preload,
        }
    }
}

/// Routes disk reads according to the relation's declared source. Command
/// sources and declarations without an input directive retain text loading.
pub(crate) fn gen_load(
    relation: &Relation,
    loader: TokenStream,
    path: TokenStream,
    diff: TokenStream,
) -> TokenStream {
    match relation.input() {
        Some(InputSource::Sqlite { .. }) => {
            let columns = relation
                .attributes()
                .iter()
                .map(|attribute| attribute.name());
            quote! { #loader.load_sqlite(#path, &[#(#columns),*], #diff) }
        }
        Some(InputSource::File { .. } | InputSource::Command { .. }) | None => {
            quote! { #loader.load_file(#path, #diff) }
        }
    }
}
