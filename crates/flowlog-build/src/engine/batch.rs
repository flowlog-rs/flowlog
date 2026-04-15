//! `DatalogBatchEngine` struct + `run()` body assembly for library mode.
//!
//! The generated engine is a host-side buffer with a terminal `run()`
//! method. Users stage typed tuples via `insert_<rel>` / `insert_batch_<rel>`
//! (or `set_<rel>` for nullary booleans); `run()` spins up timely workers,
//! builds the dataflow, partitions the staged data across workers, steps
//! to fixpoint, and drains the shared output buffers into `BatchResults`.
//!
//! Library mode has no file I/O — users load their own data. See the
//! top-level crate docs for the typical `build.rs` + `include!()` pattern.

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};

use generator::{data_type_tokens, gen_drain_block, AssemblyParts};
use parser::{Program, Relation};

use crate::relation::{input_struct_ident, user_struct_ident};

pub(crate) fn gen_lib_engine(
    program: &Program,
    string_intern: bool,
    parts: &AssemblyParts,
) -> TokenStream {
    let edbs = program.edbs();

    let struct_def = gen_engine_struct(&edbs, string_intern);
    let new_body = gen_new_body(&edbs);
    let method_blocks = gen_per_rel_methods(&edbs);
    let run_body = gen_run_body(program, &edbs, parts, string_intern);

    quote! {
        #struct_def

        impl DatalogBatchEngine {
            pub fn new() -> Self {
                #new_body
            }

            pub fn workers(mut self, n: usize) -> Self {
                self.workers = n.max(1);
                self
            }

            #(#method_blocks)*

            pub fn run(self) -> BatchResults {
                #run_body
            }
        }

        impl Default for DatalogBatchEngine {
            fn default() -> Self { Self::new() }
        }
    }
}

// =========================================================================
// Engine struct: one `<rel>_data: Vec<Tuple>` per EDB + a workers count.
// =========================================================================

fn gen_engine_struct(edbs: &[&Relation], string_intern: bool) -> TokenStream {
    let fields: Vec<TokenStream> = edbs
        .iter()
        .map(|rel| {
            let field = data_field_ident(rel);
            let tuple_ty = data_type_tokens(&rel.data_type(), string_intern);
            quote! { #field: Vec<#tuple_ty> }
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
            quote! { #f: Vec::new() }
        })
        .collect();
    quote! {
        Self {
            #(#inits,)*
            workers: 1,
        }
    }
}

// =========================================================================
// Per-relation user API: `insert_*`, `insert_batch_*`, `set_*` (nullary).
// =========================================================================

fn gen_per_rel_methods(edbs: &[&Relation]) -> Vec<TokenStream> {
    edbs.iter().copied().map(gen_one_rel_methods).collect()
}

fn gen_one_rel_methods(rel: &Relation) -> TokenStream {
    let name = rel.name();
    let data = data_field_ident(rel);

    if rel.arity() == 0 {
        // Nullary: a boolean presence. `set_foo(true)` marks it as a single
        // fact; `set_foo(false)` clears it.
        let set = format_ident!("set_{}", name);
        return quote! {
            pub fn #set(&mut self, value: bool) {
                if value { self.#data.push(()); } else { self.#data.clear(); }
            }
        };
    }

    let struct_ident = user_struct_ident(rel);
    let insert = format_ident!("insert_{}", name);
    let insert_batch = format_ident!("insert_batch_{}", name);
    quote! {
        pub fn #insert(&mut self, item: rel::#struct_ident) {
            self.#data.push(<rel::#struct_ident as Relation>::to_tuple(item));
        }
        pub fn #insert_batch(&mut self, items: Vec<rel::#struct_ident>) {
            self.#data.extend(
                items.into_iter().map(<rel::#struct_ident as Relation>::to_tuple),
            );
        }
    }
}

// =========================================================================
// `run()` body: partition → dataflow → ingest → drain.
// =========================================================================

fn gen_run_body(
    program: &Program,
    edbs: &[&Relation],
    parts: &AssemblyParts,
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

    let (host_partitions, worker_partition_clones) = gen_host_partitions(edbs);
    let inputs_new_args = gen_inputs_new_args(edbs);
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
                #(#local_bufs)*

                let #handle_binding =
                    worker.dataflow::<Ts, _, _>(|scope| {
                        #(#edb_decls)*
                        #(#flows)*
                        #(#inspectors)*
                        #dataflow_return
                    });

                let mut inputs = Inputs::new(#(#inputs_new_args),*);
                #(#typed_ingest)*
                inputs.apply_inline_all(index);
                inputs.close_all();

                while worker.step() {}

                #(#flush)*
                barrier.wait();
            }
        })
        .expect("timely::execute failed");

        #(#drain_locals)*
        BatchResults { #(#result_fields),* }
    }
}

// =========================================================================
// Host-side partitioning — pre-split each `Vec<Tuple>` into one slot per
// worker so each worker can *move* its slice into `InputSession` without
// cloning individual tuples.
// =========================================================================

/// Emit, per EDB, a slot array `Arc<Vec<Mutex<Vec<Tuple>>>>` on the host
/// (`host`) plus a matching `let x = x.clone();` for the worker closure
/// (`clones`). Each slot is touched by exactly one worker, which calls
/// `mem::take` under the (uncontended) lock to move its partition out.
fn gen_host_partitions(edbs: &[&Relation]) -> (Vec<TokenStream>, Vec<TokenStream>) {
    let mut host = Vec::with_capacity(edbs.len());
    let mut clones = Vec::with_capacity(edbs.len());
    for rel in edbs {
        let d = data_field_ident(rel);
        let slots = partition_slots_ident(rel);
        host.push(quote! {
            let #slots = std::sync::Arc::new(
                ::flowlog_runtime::io::partition(self.#d, workers)
                    .into_iter()
                    .map(std::sync::Mutex::new)
                    .collect::<Vec<_>>(),
            );
        });
        clones.push(quote! { let #slots = #slots.clone(); });
    }
    (host, clones)
}

/// `Inputs::new(EdgeInput::new(hedge), NodeInput::new(hnode), …)` — handles
/// (`h<name>`) are bound by the earlier `#handle_binding`.
fn gen_inputs_new_args(edbs: &[&Relation]) -> Vec<TokenStream> {
    edbs.iter()
        .map(|rel| {
            let input_struct = input_struct_ident(rel);
            let handle = format_ident!("h{}", rel.name());
            quote! { #input_struct::new(#handle) }
        })
        .collect()
}

/// Per-worker ingest: take our pre-partitioned slot by value and move each
/// tuple into the matching `InputSession`. No per-tuple clone — DD
/// re-shuffles by key downstream.
fn gen_typed_ingest(edbs: &[&Relation]) -> Vec<TokenStream> {
    edbs.iter()
        .map(|rel| {
            let field = format_ident!("{}", rel.name());
            let slots = partition_slots_ident(rel);
            quote! {
                {
                    let my_part = std::mem::take(
                        &mut *#slots[index].lock().expect("partition slot poisoned"),
                    );
                    for tuple in my_part {
                        inputs.#field.update_tuple(tuple, SEMIRING_ONE);
                    }
                }
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
        let ident = format_ident!("{}", rel.name());
        fields.push(quote! { #ident });
    }
    for rel in program.printsize_idbs() {
        let ident = format_ident!("{}_size", rel.name());
        fields.push(quote! { #ident });
    }
    fields
}

fn gen_drain_blocks(program: &Program, string_intern: bool) -> Vec<TokenStream> {
    let mut blocks = Vec::new();

    for rel in program.output_idbs() {
        let field = format_ident!("{}", rel.name());
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
            let write_row = quote! {
                #field.push(rel::#struct_ident::from_tuple(row.0.clone()));
            };
            let drain = gen_drain_block(&buf, rel, quote! {}, write_row, string_intern);
            blocks.push(quote! {
                let mut #field: Vec<rel::#struct_ident> = Vec::new();
                #drain
            });
        }
    }

    for rel in program.printsize_idbs() {
        let field = format_ident!("{}_size", rel.name());
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
