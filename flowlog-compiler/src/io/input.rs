//! Binary input construction and preload scheduling.

use flowlog_codegen::Skeleton;
use flowlog_codegen::input_field_ident;
use flowlog_codegen::input_handle_ident;
use flowlog_parser::InputSource;
use flowlog_parser::OutputSink;
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
    /// Before the workers start, in a program that interns strings: loads
    /// the symbol table of the SQLite input or output database that has
    /// one, so that a symbol's ordinal is its id in every table read or
    /// written afterwards. Empty otherwise.
    pub load_dictionary: TokenStream,
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
        // `gen_inputs_container` gives `Inputs::new` these parameters exactly
        // when `has_inputs` holds, so the call asks the same question.
        let coordinates = self
            .program
            .has_inputs()
            .then(|| quote! { worker.peers(), index, #uses_ord });
        let initialize_inputs = quote! {
            let mut inputs = Inputs::new(#(#handles,)* #coordinates)
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

        // The dictionary must be the interner's first strings, so it loads
        // before any worker interns a literal or a fact. An output database
        // is a candidate too: a run that extends one must number its new
        // strings after the ones the database holds.
        let mut sqlite_inputs: Vec<&str> = self
            .program
            .file_inputs()
            .filter(|(relation, _)| matches!(relation.input(), Some(InputSource::Sqlite { .. })))
            .map(|(_, filename)| filename)
            .collect();
        sqlite_inputs.sort_unstable();
        sqlite_inputs.dedup();
        let mut sqlite_outputs: Vec<&str> = self
            .program
            .idbs()
            .into_iter()
            .filter_map(|relation| match relation.output_sink() {
                Some(OutputSink::Sqlite { filename, .. }) => Some(filename.as_str()),
                Some(OutputSink::File { .. }) | None => None,
            })
            .collect();
        sqlite_outputs.sort_unstable();
        sqlite_outputs.dedup();
        let interns = self.config.str_intern_enabled();
        let load_dictionary = if interns && !(sqlite_inputs.is_empty() && sqlite_outputs.is_empty())
        {
            // `mut` only where an output extends the list: a warning in the
            // generated crate is a generator bug.
            let mutable = (!sqlite_outputs.is_empty()).then(|| quote! { mut });
            let outputs = (!sqlite_outputs.is_empty()).then(|| {
                quote! {
                    if let Some(output_dir) = &output_dir {
                        databases.extend([#(output_dir.join(#sqlite_outputs)),*]);
                    }
                }
            });
            quote! {
                let #mutable databases: Vec<std::path::PathBuf> = vec![#(fact_dir.join(#sqlite_inputs)),*];
                #outputs
                if let Err(error) = ::flowlog_runtime::intern::dictionary::load(&databases) {
                    eprintln!("{error}");
                    std::process::exit(1);
                }
            }
        } else {
            quote! {}
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
            load_dictionary,
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
