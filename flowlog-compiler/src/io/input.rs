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
    pub load_files: Vec<TokenStream>,
    pub preload_inputs: TokenStream,
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
                let name = relation.raw_name();
                let load = gen_load(
                    relation,
                    quote! { inputs.#field },
                    quote! { &path },
                    quote! { ::flowlog_runtime::diff::Unit::one() },
                );
                let exit_on_error = matches!(relation.input(), Some(InputSource::Sqlite { .. }))
                    .then(|| quote! { std::process::exit(1); });
                quote! {
                    let path = fact_dir.join(#filename);
                    if let Err(error) = #load {
                        eprintln!("[relation][{}] {} in {}", #name, error, path.display());
                        #exit_on_error
                    }
                }
            })
            .collect();

        // Static inputs close after the initial load, before the first
        // advance: an open static input would hold every static operator at
        // time 0, and no later command may change it.
        let publish = &skeleton.publish;
        let preload_inputs = if !load_files.is_empty() || !self.program.facts().is_empty() {
            quote! {
                #(#load_files)*
                inputs.apply_inline_all();
                inputs.close_static();
                time_stamp += 1;
                inputs.advance_dynamic_to(time_stamp);
                inputs.flush_dynamic();
                while probe.less_than(&time_stamp) {
                    worker.step();
                }
                #publish
                barrier.wait();
                if index == 0 {
                    #emit_output
                }
                barrier.wait();
            }
        } else {
            quote! {
                inputs.apply_inline_all();
                inputs.close_static();
            }
        };

        Input {
            initialize_inputs,
            load_files,
            preload_inputs,
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
