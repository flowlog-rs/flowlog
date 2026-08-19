//! Binary-mode-only relation registry + preload codegen.
//!
//! Encodes binary-mode's `HashMap<String, Box<dyn Relation>>` dispatch
//! model. Building it here keeps the generator free of mode-specific
//! assumptions.

use std::path::Path;

use flowlog_build::CodeParts;
use flowlog_parser::InputSource;
use flowlog_parser::Relation;
use proc_macro2::Ident;
use proc_macro2::Span;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Compiler;

pub(crate) struct Input {
    pub registry_inserts: Vec<TokenStream>,
    pub file_ingests: Vec<TokenStream>,
    pub maybe_peers: TokenStream,
    pub preload: TokenStream,
}

impl Compiler {
    /// Build the binary-mode EDB registry + preload fragments from the
    /// program's input relations and the compiler's fact directory.
    pub(crate) fn gen_input(&self, parts: &CodeParts, merge_section: &TokenStream) -> Input {
        let edbs = self.program.edbs();

        let registry_inserts: Vec<TokenStream> = edbs
            .iter()
            .map(|rel| {
                let rel_name = rel.name();
                let handle_ident = Ident::new(&format!("h{rel_name}"), Span::call_site());
                let ops_ty_ident = Ident::new(&format!("Rel{rel_name}"), Span::call_site());
                quote! {
                    rels.insert(
                        #rel_name.to_string(),
                        Box::new(#ops_ty_ident::new(#handle_ident)),
                    );
                }
            })
            .collect();

        let has_file_backed_edbs = edbs.iter().any(|rel| preload_file(rel).is_some());
        let has_inline_facts = !self.program.facts().is_empty();
        let needs_preload = has_file_backed_edbs || has_inline_facts;

        // We want a deterministic load whenever the program uses `ord`,
        // so its value depends on interning order, deterministic across
        // different runs; evaluation still runs on every worker.
        let deterministic_load = self.config.serialize_load();

        let maybe_peers = if has_file_backed_edbs && !deterministic_load {
            quote! { let peers = worker.peers(); }
        } else {
            quote! {}
        };

        let file_ingests: Vec<TokenStream> = edbs
            .iter()
            .filter_map(|rel| preload_file(rel).map(|name| (rel, name)))
            .map(|(rel, file_name)| {
                let rel_name = rel.name();
                let file_name = file_name.to_string();
                let path = self
                    .options
                    .fact_dir()
                    .map(|dir| {
                        Path::new(dir)
                            .join(&file_name)
                            .to_string_lossy()
                            .into_owned()
                    })
                    .unwrap_or_else(|| file_name);
                if deterministic_load {
                    // Worker 0 alone reads the whole file (`peers = 1, index = 0`);
                    // other workers skip loading. Interning order thus matches `-w 1`.
                    quote! {
                        if index == 0 {
                            rels.get_mut(#rel_name).unwrap()
                                .apply_file(std::path::Path::new(#path), SEMIRING_ONE, 1, 0);
                        }
                    }
                } else {
                    // Each worker ingests its own ~1/N byte-range slice in parallel.
                    quote! {
                        rels.get_mut(#rel_name).unwrap()
                            .apply_file(std::path::Path::new(#path), SEMIRING_ONE, peers, index);
                    }
                }
            })
            .collect();

        let flush = &parts.flush;
        let preload = if needs_preload {
            quote! {
                #(#file_ingests)*
                for (_, r) in rels.iter_mut() {
                    r.apply_inline(index);
                }
                time_stamp += 1;
                for (_, r) in rels.iter_mut() {
                    r.advance_to(time_stamp);
                    r.flush();
                }
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
            registry_inserts,
            file_ingests,
            maybe_peers,
            preload,
        }
    }
}

/// The file this relation's facts are read from before the first round,
/// `None` when it has none.
///
/// A nullary relation's file holds nothing a reader could take a column
/// from, and a `put`-fed source waits for tuples instead of reading at all.
fn preload_file(rel: &Relation) -> Option<&str> {
    if rel.arity() == 0 {
        return None;
    }
    rel.input()
        .filter(|source| source.is_file_backed())
        .and_then(InputSource::filename)
}
