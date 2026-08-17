//! Binary-mode preload and transaction dispatch.
//!
//! The `Inputs` container and its handlers are the shared generator's; what
//! is here is only what this mode adds: reading the files a `.input`
//! directive names, and resolving a transaction's relation name to a field.

use std::path::Path;

use flowlog_parser::InputSource;
use flowlog_parser::Relation;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Compiler;

pub(crate) struct Input {
    pub file_ingests: Vec<TokenStream>,
    pub maybe_peers: TokenStream,
    /// Feed every `.input` file and inline fact into the sessions. Uses
    /// `inputs` directly, so it must run before the event callback exists.
    pub preload_ingest: TokenStream,
    /// Run the preload epoch through the event callback `on`, which it
    /// rebinds mutably; nothing when there is no preload.
    pub preload_epoch: TokenStream,
    /// Resolve a txn's relation name to a field, for `put` and for `file`.
    /// Incremental mode only: no other mode learns a name at run time.
    pub put_dispatch: TokenStream,
    pub file_dispatch: TokenStream,
    /// The relation names the txn prompt completes against.
    pub rel_names: TokenStream,
    /// The epoch preload leaves off at: one when it ran, zero otherwise.
    pub start_time: u32,
}

impl Compiler {
    /// The path a relation's input is loaded from: `file_name` under the
    /// fact directory when one is set.
    fn ingest_path(&self, file_name: &str) -> String {
        self.options
            .fact_dir()
            .map(|dir| {
                Path::new(dir)
                    .join(file_name)
                    .to_string_lossy()
                    .into_owned()
            })
            .unwrap_or_else(|| file_name.to_owned())
    }

    /// Build the binary-mode EDB registry + preload fragments from the
    /// program's input relations and the compiler's fact directory.
    pub(crate) fn gen_input(&self) -> Input {
        let edbs = self.program.edbs();

        // One definition, so the three questions asked of it below cannot
        // drift apart, and the program's own notion of file-backed rather
        // than a second one here. Arity is not a filter: a nullary
        // relation's file holds empty rows and reads like any other. Each
        // relation carries the file it reads, so no later step re-derives
        // one; every file-backed source names one, so the filter drops
        // nothing.
        let preload_files: Vec<(&Relation, &str)> = self
            .program
            .file_backed_relations()
            .into_iter()
            .filter_map(|rel| {
                rel.input()
                    .and_then(InputSource::filename)
                    .map(|n| (rel, n))
            })
            .collect();
        let has_inline_facts = !self.program.facts().is_empty();
        let needs_preload = !preload_files.is_empty() || has_inline_facts;

        // Which loads must serialize under `ord` is the runtime's call,
        // made per relation inside the runtime reader (a text scan interns
        // while reading and collapses to worker 0; a dictionary-encoded
        // scan does not). Every worker just calls `load` with its own
        // coordinates.
        let maybe_peers = if preload_files.is_empty() {
            quote! {}
        } else {
            quote! { let peers = worker.peers(); }
        };

        let file_ingests: Vec<TokenStream> = preload_files
            .iter()
            .map(|(rel, file_name)| {
                let rel_name = rel.name();
                let path = self.ingest_path(file_name);
                // Every worker calls with its own coordinates; whether it
                // gets a share is the source's decision. A fatal load error
                // would otherwise leave the relation silently short, so it
                // stops the run, attributed to what failed where.
                let field = ::flowlog_build::inputs_field_ident(rel);
                quote! {
                    if let Err(e) = ::flowlog_runtime::io::Ingest::load_file(
                        &mut inputs.#field,
                        std::path::Path::new(#path),
                        SEMIRING_ONE,
                        peers,
                        index,
                    ) {
                        eprintln!("[relation][{}] fatal: {} reading {}", #rel_name, e, #path);
                        std::process::exit(1);
                    }
                }
            })
            .collect();

        // A relation is named as a lowercased string in a txn, so each arm
        // compares without allocating: `to_ascii_lowercase` would allocate
        // once per command just to hash it.
        let dispatch = |body: &dyn Fn(&Ident) -> TokenStream| -> TokenStream {
            let arms: Vec<TokenStream> = edbs
                .iter()
                .map(|rel| {
                    let lower = rel.name().to_ascii_lowercase();
                    let field = ::flowlog_build::inputs_field_ident(rel);
                    let call = body(&field);
                    quote! { _ if rel.eq_ignore_ascii_case(#lower) => { #call } }
                })
                .collect();
            quote! {
                match () {
                    #(#arms)*
                    _ => panic!("unknown relation: '{rel}'"),
                }
            }
        };
        let put_dispatch = dispatch(&|field| {
            quote! {
                ::flowlog_runtime::io::Ingest::load_line(
                    &mut inputs.#field, tuple, *diff, peers, index,
                );
            }
        });
        // A failed load ends the process rather than the command: this
        // worker's share may already be in its session, and the commit
        // would otherwise land a partially loaded relation with no abort
        // path.
        let file_dispatch = dispatch(&|field| {
            quote! {
                if let Err(e) = ::flowlog_runtime::io::Ingest::load_file(
                    &mut inputs.#field, path.as_path(), *diff, peers, index,
                ) {
                    eprintln!("[relation][{rel}] fatal: {e} reading {}", path.display());
                    std::process::exit(1);
                }
            }
        });
        let name_literals: Vec<String> = edbs
            .iter()
            .map(|rel| rel.name().to_ascii_lowercase())
            .collect();
        let rel_names = quote! {
            static REL_NAMES: &[&str] = &[#(#name_literals),*];
        };

        let preload_ingest = if needs_preload {
            quote! {
                #(#file_ingests)*
                inputs.apply_inline_all(index);
            }
        } else {
            quote! {}
        };

        // The epoch itself (advance, merge, rendezvous) is the shell's
        // `preload_epoch`, answered by the same callback every later epoch
        // uses, so preload work is metered and drained like a commit's.
        // The rebinding makes the callback mutable only here: a program
        // with no preload keeps it immutable, and `-Dwarnings` clean.
        let preload_epoch = if needs_preload {
            quote! {
                let mut on = on;
                ::flowlog_txn::driver::preload_epoch(&shared, index == 0, 0, &mut on);
            }
        } else {
            quote! {}
        };

        Input {
            file_ingests,
            maybe_peers,
            preload_ingest,
            preload_epoch,
            put_dispatch,
            file_dispatch,
            rel_names,
            start_time: u32::from(needs_preload),
        }
    }
}
