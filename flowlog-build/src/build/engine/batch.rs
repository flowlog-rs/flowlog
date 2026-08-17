//! `DatalogBatchEngine` struct + `run()` body assembly for library mode.
//!
//! The generated engine is a host-side buffer with a terminal `run()`
//! method. Users stage typed tuples via `insert_<rel>(Vec<rel::Foo>)` (or
//! `set_<rel>()` for nullary presence facts); `run()` spins up timely
//! workers, builds the dataflow, steps to fixpoint, and drains the shared
//! output buffers into `BatchResults`.
//!
//! Library mode has no file I/O — users load their own data. See the
//! top-level crate docs for the typical `build.rs` + `include!()` pattern.

use flowlog_parser::Program;
use flowlog_parser::Relation;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::CodeParts;
use crate::build::relation::printsize_field_ident;
use crate::build::relation::results_field_ident;
use crate::build::relation::user_struct_ident;
use crate::codegen::inputs_field_ident;
use crate::gen_drain_block;

pub(crate) fn gen_lib_engine(
    program: &Program,
    string_intern: bool,
    parts: &CodeParts,
) -> TokenStream {
    let edbs = program.edbs();

    let struct_def = gen_engine_struct(&edbs);
    let new_body = gen_new_body(&edbs);
    let method_blocks = gen_per_rel_methods(&edbs);
    let run_body = gen_run_body(program, &edbs, parts, string_intern);

    quote! {
        #struct_def

        impl DatalogBatchEngine {
            /// Create an engine configured for `workers` timely workers.
            /// Worker count is fixed at construction because inputs are
            /// pre-bucketed per worker during `insert_*` — and because
            /// timely itself cannot change worker count after `execute`
            /// spawns the cluster.
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

// =========================================================================
// Engine struct: one flat `Vec` of user tuples per EDB, shared read-only
// with the workers at `run()`; each worker's `VecReader` opens its index
// range and decodes its share in `accept`, exactly like a file share.
// Nullary EDBs are a presence flag worker 0 applies.
// =========================================================================

fn gen_engine_struct(edbs: &[&Relation]) -> TokenStream {
    let fields: Vec<TokenStream> = edbs
        .iter()
        .map(|rel| {
            let field = data_field_ident(rel);
            if rel.arity() == 0 {
                quote! { #field: bool }
            } else {
                let user_ty = user_struct_ident(rel);
                quote! { #field: Vec<rel::#user_ty> }
            }
        })
        .collect();

    quote! {
        pub struct DatalogBatchEngine {
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
            if rel.arity() == 0 {
                quote! { #f: false }
            } else {
                quote! { #f: Vec::new() }
            }
        })
        .collect();
    quote! {
        Self {
            #(#inits,)*
            workers,
        }
    }
}

// =========================================================================
// Per-relation user API: `insert_<rel>(iterator)`, `set_<rel>()` (nullary).
// =========================================================================

fn gen_per_rel_methods(edbs: &[&Relation]) -> Vec<TokenStream> {
    edbs.iter().copied().map(gen_one_rel_methods).collect()
}

fn gen_one_rel_methods(rel: &Relation) -> TokenStream {
    let name = rel.name();
    let data = data_field_ident(rel);

    if rel.arity() == 0 {
        let set = format_ident!("set_{}", name);
        return quote! {
            /// Assert the nullary fact. Omit the call if it should not hold.
            pub fn #set(&mut self) {
                self.#data = true;
            }
        };
    }

    let struct_ident = user_struct_ident(rel);
    let insert = format_ident!("insert_{}", name);
    quote! {
        /// Stage a batch of tuples. Callable multiple times; each call
        /// just appends. Distribution and slot conversion happen on the
        /// workers at `run()`.
        pub fn #insert(&mut self, mut items: Vec<rel::#struct_ident>) {
            self.#data.append(&mut items);
        }
    }
}

// =========================================================================
// `run()` body: partition → dataflow → ingest → drain.
// =========================================================================

fn gen_run_body(
    program: &Program,
    edbs: &[&Relation],
    parts: &CodeParts,
    string_intern: bool,
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

    let (host_partitions, worker_partition_clones) = gen_host_partitions(edbs);
    let typed_ingest = gen_typed_ingest(edbs);
    let drain_locals = gen_drain_blocks(program, string_intern);
    let result_fields = gen_result_fields(program);

    quote! {
        let workers = self.workers;
        #(#host_partitions)*

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(workers));
        #(#output_bufs)*
        #(#size_cell_decls)*

        timely::execute(timely::Config::process(workers), {
            let barrier = barrier.clone();
            #(#output_buf_clones)*
            #(#size_cell_clones)*
            #(#worker_partition_clones)*

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

                #(#typed_ingest)*
                inputs.apply_inline_all(index);
                inputs.close_all();

                #step_loop

                #(#flush)*
                barrier.wait();

                #metrics_write
            }
        })
        .expect("timely::execute failed");

        #(#drain_locals)*
        BatchResults { #(#result_fields),* }
    }
}

// =========================================================================
// Host share — each relation's rows move into an `Arc` the workers read;
// decode runs on the workers, so nothing is cloned or moved per tuple.
// =========================================================================

/// Emit, per EDB, the host-side `Arc` over its staged rows (`host`) plus
/// a matching `let x = x.clone();` for the worker closure (`clones`).
fn gen_host_partitions(edbs: &[&Relation]) -> (Vec<TokenStream>, Vec<TokenStream>) {
    let mut host = Vec::with_capacity(edbs.len());
    let mut clones = Vec::with_capacity(edbs.len());
    for rel in edbs {
        let d = data_field_ident(rel);
        let slots = partition_slots_ident(rel);
        if rel.arity() == 0 {
            host.push(quote! { let #slots = self.#d; });
            clones.push(quote! { let #slots = #slots; });
        } else {
            host.push(quote! { let #slots = std::sync::Arc::new(self.#d); });
            clones.push(quote! { let #slots = #slots.clone(); });
        }
    }
    (host, clones)
}

/// Per-worker ingest: open this worker's index range of the shared rows
/// and drive it into the relation's sink, decoding in `accept` like any
/// other source. A nullary relation is a presence flag worker 0 applies
/// through the put path.
fn gen_typed_ingest(edbs: &[&Relation]) -> Vec<TokenStream> {
    edbs.iter()
        .map(|rel| {
            let field = inputs_field_ident(rel);
            let slots = partition_slots_ident(rel);
            if rel.arity() == 0 {
                return quote! {
                    if #slots {
                        // One row of the empty tuple asserts the fact, with
                        // no text to render and re-parse.
                        ::flowlog_runtime::io::Ingest::load_vec(
                            &mut inputs.#field, &[()], SEMIRING_ONE, workers, index,
                        );
                    }
                };
            }
            quote! {
                ::flowlog_runtime::io::Ingest::load_vec(
                    &mut inputs.#field, &#slots[..], SEMIRING_ONE, workers, index,
                );
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

/// Per-output block that produces the typed local (`reach`, `tc_size`, …)
/// `BatchResults` then names in its struct literal.
fn gen_drain_blocks(program: &Program, string_intern: bool) -> Vec<TokenStream> {
    let mut blocks = Vec::new();

    for rel in program.output_idbs() {
        let field = results_field_ident(rel);
        let buf = format_ident!("buf_{}", rel.name());

        if rel.arity() == 0 {
            blocks.push(quote! {
                let #field: bool = {
                    let guard = #buf.lock().expect("output buffer poisoned");
                    guard.iter().any(|worker_buf| !worker_buf.is_empty())
                };
            });
        } else {
            let struct_ident = user_struct_ident(rel);
            // A host Vec is a writer like any other, and the only one whose
            // close cannot fail.
            let preamble = quote! {
                let mut sink =
                    ::flowlog_runtime::io::VecWriter::<rel::#struct_ident>::new();
            };
            let write_row = quote! {
                let _ = (&time, &diff);
                ::flowlog_runtime::io::Writer::push(&mut sink, tuple, None);
            };
            // The drain block evaluates to its postamble, so the writer
            // stays scoped inside and the field is bound from the result.
            // Declaring the field first would let a relation named `Sink`
            // shadow the writer local, which is legal Datalog.
            let postamble = quote! { sink.into_rows() };
            let drain = gen_drain_block(&buf, rel, preamble, write_row, postamble, string_intern);
            blocks.push(quote! {
                let #field: Vec<rel::#struct_ident> = #drain;
            });
        }
    }

    for rel in program.printsize_idbs() {
        let field = printsize_field_ident(rel);
        let cell = format_ident!("size_{}", rel.name());
        // The size cell stores `(Ts, i32)`; clamp negatives to 0 — they
        // shouldn't happen in batch mode but surfacing `usize` to the user
        // requires a non-negative value regardless.
        blocks.push(quote! {
            let #field: usize = {
                let (_, raw) = *#cell.lock().expect("size cell poisoned");
                if raw < 0 { 0 } else { raw as usize }
            };
        });
    }

    blocks
}

// =========================================================================
// Ident helpers.
// =========================================================================

fn data_field_ident(rel: &Relation) -> Ident {
    format_ident!("{}_data", rel.name())
}

fn partition_slots_ident(rel: &Relation) -> Ident {
    format_ident!("{}_parts", rel.name())
}
