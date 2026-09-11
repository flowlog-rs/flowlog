//! Binary input construction and preload scheduling.

use flowlog_build::CodeParts;
use flowlog_parser::InputSource;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::Compiler;

pub(crate) struct Input {
    pub initialize: TokenStream,
    pub file_ingests: Vec<TokenStream>,
    pub preload: TokenStream,
}

impl Compiler {
    /// Builds loaders and resolves preload filenames against the runtime
    /// fact directory.
    pub(crate) fn gen_input(&self, parts: &CodeParts, merge_section: &TokenStream) -> Input {
        let edbs = self.program.edbs();
        let handles = edbs.iter().map(|rel| format_ident!("h{}", rel.name()));
        let uses_ord = self.config.serialize_load();
        let initialize = quote! {
            let mut inputs = Inputs::new(#(#handles,)* worker.peers(), index, #uses_ord)
                .expect("valid worker coordinates");
        };

        let file_ingests: Vec<TokenStream> = edbs
            .iter()
            .filter_map(|rel| {
                rel.input()
                    .filter(|source| source.is_file_backed())
                    .and_then(InputSource::filename)
                    .map(|filename| (rel, filename))
            })
            .map(|(rel, filename)| {
                let field = format_ident!("in_{}", rel.name());
                let name = rel.raw_name();
                let delimiter = rel.input().and_then(InputSource::delim).unwrap_or(b'\t');
                let has_header = rel.input().is_some_and(InputSource::has_header);
                quote! {
                    let path = fact_dir.join(#filename);
                    if let Err(error) = inputs.#field.load_file(
                        &path, #delimiter, #has_header, SEMIRING_ONE,
                    ) {
                        eprintln!("[relation][{}] {} in {}", #name, error, path.display());
                    }
                }
            })
            .collect();

        let flush = &parts.flush;
        let preload = if !file_ingests.is_empty() || !self.program.facts().is_empty() {
            quote! {
                #(#file_ingests)*
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
                    #merge_section
                }
                barrier.wait();
            }
        } else {
            quote! {}
        };

        Input {
            initialize,
            file_ingests,
            preload,
        }
    }
}
