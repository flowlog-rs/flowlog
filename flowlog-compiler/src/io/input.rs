//! Binary input construction and preload scheduling.

use flowlog_build::CodeParts;
use flowlog_parser::InputSource;
use flowlog_parser::Relation;
use proc_macro2::TokenStream;
use quote::format_ident;
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
    pub(crate) fn gen_input(&self, parts: &CodeParts, emit_output: &TokenStream) -> Input {
        let edbs = self.program.edbs();
        let handles = edbs
            .iter()
            .map(|relation| format_ident!("h{}", relation.name()));
        let uses_ord = self.config.serialize_load();
        let initialize_inputs = quote! {
            let mut inputs = Inputs::new(#(#handles,)* worker.peers(), index, #uses_ord)
                .expect("valid worker coordinates");
        };

        let load_files: Vec<TokenStream> = edbs
            .iter()
            .filter_map(|relation| {
                relation
                    .input()
                    .filter(|source| source.is_file_backed())
                    .and_then(InputSource::filename)
                    .map(|filename| (relation, filename))
            })
            .map(|(relation, filename)| {
                let field = format_ident!("in_{}", relation.name());
                let name = relation.raw_name();
                let load = gen_load(
                    relation,
                    quote! { inputs.#field },
                    quote! { &path },
                    quote! { SEMIRING_ONE },
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

        let flush = &parts.flush;
        let preload_inputs = if !load_files.is_empty() || !self.program.facts().is_empty() {
            quote! {
                #(#load_files)*
                inputs.apply_inline_all();
                time_stamp += 1;
                inputs.advance_to_all(time_stamp);
                inputs.flush_all();
                while probe.less_than(&time_stamp) {
                    worker.step();
                }
                #(#flush)*
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
