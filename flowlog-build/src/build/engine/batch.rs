//! `BatchEngine` struct + `run()` body assembly for library mode.
//!
//! The generated engine is a host-side buffer with a terminal `run()`
//! method. Users stage typed tuples via `insert_<rel>(Vec<rel::Foo>)` (or
//! `set_<rel>()` for nullary presence facts); `run()` spins up timely
//! workers, builds the dataflow, steps to fixpoint, and drains the shared
//! output buffers into `BatchResults`.
//!
//! Runtime loaders partition and convert the staged rows on each worker.
//! Library mode has no file I/O; users load their own data. See the
//! top-level crate docs for the typical `build.rs` + `include!()` pattern.

use flowlog_parser::Program;
use flowlog_parser::Relation;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::CodeParts;
use crate::build::bindings::inputs_field_ident;
use crate::build::bindings::printsize_field_ident;
use crate::build::bindings::results_field_ident;
use crate::build::bindings::user_tuple_ident;
use crate::codegen::user_tuple_tokens;

pub(crate) fn gen_lib_engine(program: &Program, uses_ord: bool, parts: &CodeParts) -> TokenStream {
    let edbs = program.edbs();

    let struct_def = gen_engine_struct(&edbs);
    let new_body = gen_new_body(&edbs);
    let method_blocks = edbs.iter().map(|rel| gen_one_rel_methods(rel));
    let run_body = gen_run_body(program, &edbs, parts, uses_ord);

    quote! {
        #struct_def

        impl BatchEngine {
            /// Stages inputs for a run with `workers` timely workers.
            /// A zero worker count is treated as one.
            pub fn new(workers: usize) -> Self {
                let workers = workers.max(1);
                #new_body
            }

            #(#method_blocks)*

            pub fn run(self) -> BatchResults {
                #run_body
            }
        }
    }
}

// =============================================================================
// Staged inputs
// =============================================================================

fn gen_engine_struct(edbs: &[&Relation]) -> TokenStream {
    let fields: Vec<TokenStream> = edbs
        .iter()
        .map(|rel| {
            let field = data_field_ident(rel);
            let tuple_ty = user_tuple_tokens(&rel.data_type());
            quote! { #field: Vec<#tuple_ty> }
        })
        .collect();

    quote! {
        pub struct BatchEngine {
            #(#fields,)*
            workers: usize,
        }
    }
}

fn gen_new_body(edbs: &[&Relation]) -> TokenStream {
    let inits: Vec<TokenStream> = edbs
        .iter()
        .map(|rel| {
            let f = data_field_ident(rel);
            quote! { #f: Vec::new() }
        })
        .collect();
    quote! {
        Self {
            #(#inits,)*
            workers,
        }
    }
}

// =============================================================================
// Per-relation user API
// =============================================================================

fn gen_one_rel_methods(rel: &Relation) -> TokenStream {
    let name = rel.name();
    let data = data_field_ident(rel);

    if rel.arity() == 0 {
        let set = format_ident!("set_{}", name);
        return quote! {
            /// Assert the nullary fact. Omit the call if it should not hold.
            pub fn #set(&mut self) {
                self.#data.push(());
            }
        };
    }

    let struct_ident = user_tuple_ident(rel);
    let insert = format_ident!("insert_{}", name);
    quote! {
        /// Appends typed rows to this relation's staged input.
        /// Rows are loaded when `run()` starts the workers.
        pub fn #insert(&mut self, items: Vec<rel::#struct_ident>) {
            self.#data.extend(items);
        }
    }
}

// =============================================================================
// Run assembly
// =============================================================================

fn gen_run_body(
    program: &Program,
    edbs: &[&Relation],
    parts: &CodeParts,
    uses_ord: bool,
) -> TokenStream {
    let edb_decls = &parts.edb_decls;
    let handle_binding = &parts.handle_binding;
    let dataflow_return = &parts.dataflow_return;
    let flows = &parts.flows;
    let output_bufs = &parts.output_bufs;
    let output_buf_clones = &parts.output_buf_clones;
    let local_bufs = &parts.local_bufs;
    let inspectors = &parts.inspectors;
    let flush = &parts.flush;
    let size_cell_decls = &parts.size_cell_decls;
    let size_cell_clones = &parts.size_cell_clones;
    let profile_init = &parts.profile_init;
    let metrics_write = &parts.metrics_write;
    let step_loop = &parts.step_loop;

    let staged_inputs = gen_staged_inputs(edbs);
    let inputs_new_args = edbs.iter().map(|rel| format_ident!("h{}", rel.name()));
    let typed_ingest = gen_typed_ingest(edbs);
    let drain_locals = gen_drain_blocks(program);
    let result_fields = gen_result_fields(program);

    quote! {
        let workers = self.workers;
        #(#staged_inputs)*

        #(#output_bufs)*
        #(#size_cell_decls)*

        timely::execute(timely::Config::process(workers), {
            #(#output_buf_clones)*
            #(#size_cell_clones)*

            move |worker| {
                let index = worker.index();
                #profile_init
                #(#local_bufs)*

                let #handle_binding =
                    worker.dataflow::<Ts, _, _>(|scope| {
                        #(#edb_decls)*
                        #(#flows)*
                        #(#inspectors)*
                        #dataflow_return
                    });

                let mut inputs = Inputs::new(
                    #(#inputs_new_args,)* worker.peers(), index, #uses_ord,
                ).expect("invalid input worker coordinates");
                #(#typed_ingest)*
                inputs.apply_inline_all();
                inputs.close_all();

                #step_loop

                #(#flush)*

                #metrics_write
            }
        })
        .expect("timely::execute failed");

        #(#drain_locals)*
        BatchResults { #(#result_fields),* }
    }
}

/// Shares each relation's staged rows without assigning worker partitions.
fn gen_staged_inputs(edbs: &[&Relation]) -> Vec<TokenStream> {
    // Loaders borrow the same source and select their own partitions. Sharing
    // it avoids generated worker buckets, but keeps the source alive through
    // execution and copies owned string fields when they are not interned.
    edbs.iter()
        .map(|rel| {
            let data = data_field_ident(rel);
            quote! { let #data = std::sync::Arc::new(self.#data); }
        })
        .collect()
}

/// Gives every loader the same source so it can select its worker's share.
fn gen_typed_ingest(edbs: &[&Relation]) -> Vec<TokenStream> {
    edbs.iter()
        .map(|rel| {
            let field = inputs_field_ident(rel);
            let data = data_field_ident(rel);
            quote! {
                inputs.#field.load_rows(#data.as_slice(), SEMIRING_ONE)
                    .expect("failed to load staged rows");
            }
        })
        .collect()
}

// =========================================================================
// Result assembly: post-`timely::execute`, drain shared buffers into typed
// locals on the host thread, then fold them into `BatchResults`.
// =========================================================================

fn gen_result_fields(program: &Program) -> Vec<TokenStream> {
    let mut fields = Vec::new();
    for rel in program.output_idbs() {
        let ident = results_field_ident(rel);
        fields.push(quote! { #ident });
    }
    for rel in program.printsize_idbs() {
        let ident = printsize_field_ident(rel);
        fields.push(quote! { #ident });
    }
    fields
}

/// Binds runtime snapshots and independent counts to public result fields.
fn gen_drain_blocks(program: &Program) -> Vec<TokenStream> {
    let mut blocks = Vec::new();
    for rel in program.output_idbs() {
        let field = results_field_ident(rel);
        let buf = format_ident!("buf_{}", rel.name());
        blocks.push(quote! { let #field = #buf.emit_host::<false, _>(); });
    }
    for rel in program.printsize_idbs() {
        let field = printsize_field_ident(rel);
        let buf = format_ident!("buf_{}", rel.name());
        blocks.push(quote! { let #field: usize = #buf.batch_size(); });
    }
    blocks
}

// =========================================================================
// Ident helpers.
// =========================================================================

fn data_field_ident(rel: &Relation) -> Ident {
    format_ident!("{}_data", rel.name())
}
