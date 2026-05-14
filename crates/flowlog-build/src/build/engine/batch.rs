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

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};

use crate::parser::{Program, Relation};

use super::{needs_conversion, per_position_tuple, user_to_tuple_convert};
use crate::build::relation::user::tuple_to_user_expr;
use crate::build::relation::{input_struct_ident, rust_ident, user_struct_ident};
use crate::{CodeParts, data_type_tokens, gen_drain_block};

pub(crate) fn gen_lib_engine(
    program: &Program,
    string_intern: bool,
    parts: &CodeParts,
) -> TokenStream {
    let edbs = program.edbs();

    let struct_def = gen_engine_struct(&edbs, string_intern);
    let new_body = gen_new_body(&edbs);
    let method_blocks = gen_per_rel_methods(&edbs, string_intern);
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
// Engine struct: per-EDB pre-bucketed input.
//
// Each non-nullary EDB carries `<rel>_data: Vec<Vec<Tuple>>` of length
// `workers` — one bucket per worker. `insert_<rel>` chunk-distributes the
// input across buckets in a single pass (with a 1-item spread of any
// remainder), so `run()` has no host-side partition.
//
// Nullary EDBs (presence booleans) use `Vec<Vec<()>>` for layout symmetry
// — only bucket[0] is ever non-empty, so worker 0 ingests the singleton
// fact and others get nothing.
// =========================================================================

fn gen_engine_struct(edbs: &[&Relation], string_intern: bool) -> TokenStream {
    let fields: Vec<TokenStream> = edbs
        .iter()
        .map(|rel| {
            let field = data_field_ident(rel);
            let tuple_ty = data_type_tokens(&rel.data_type(), string_intern);
            quote! { #field: Vec<Vec<#tuple_ty>> }
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
            quote! { #f: vec![Vec::new(); workers] }
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

fn gen_per_rel_methods(edbs: &[&Relation], string_intern: bool) -> Vec<TokenStream> {
    edbs.iter()
        .copied()
        .map(|rel| gen_one_rel_methods(rel, string_intern))
        .collect()
}

fn gen_one_rel_methods(rel: &Relation, string_intern: bool) -> TokenStream {
    let name = rel.name();
    let data = data_field_ident(rel);

    if rel.arity() == 0 {
        // Nullary relations encode presence: call `set_foo()` to assert
        // the fact, or don't call it at all. Bucket 0 → worker 0 ingests
        // the singleton.
        let set = format_ident!("set_{}", name);
        return quote! {
            /// Assert the nullary fact. Omit the call if it should not hold.
            pub fn #set(&mut self) {
                self.#data[0].push(());
            }
        };
    }

    let struct_ident = user_struct_ident(rel);
    let insert = format_ident!("insert_{}", name);
    // `extend` chooses its bucket-side body based on whether per-position
    // conversion is needed — identity case is a direct `Vec::extend` from
    // a sized sub-iterator (`Take<vec::IntoIter>` implements `TrustedLen`,
    // so the realloc check collapses and it lowers close to a memcpy).
    let extend = if needs_conversion(rel, string_intern) {
        let map_expr = user_to_tuple_convert(rel, string_intern);
        quote! { bucket.extend(iter.by_ref().take(take).map(|item| #map_expr)) }
    } else {
        quote! { bucket.extend(iter.by_ref().take(take)) }
    };

    quote! {
        /// Stage a batch of tuples, evenly distributed across worker
        /// buckets. Callable multiple times; each call just appends.
        pub fn #insert(&mut self, items: Vec<rel::#struct_ident>) {
            let total = items.len();
            if total == 0 { return; }
            let workers = self.workers;
            let chunk = total / workers;
            let remainder = total % workers;
            let mut iter = items.into_iter();
            for i in 0..workers {
                let take = chunk + if i < remainder { 1 } else { 0 };
                if take == 0 { continue; }
                let bucket = &mut self.#data[i];
                bucket.reserve(take);
                #extend;
            }
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
    let step_loop = &parts.step_loop_batch;
    let time_profile_write = &parts.time_profile_write_batch;
    let memory_profile_write = &parts.memory_profile_write_batch;

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
                #profile_init
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

                #step_loop

                #(#flush)*
                barrier.wait();

                #time_profile_write
                #memory_profile_write
            }
        })
        .expect("timely::execute failed");

        #(#drain_locals)*
        BatchResults { #(#result_fields),* }
    }
}

// =========================================================================
// Per-worker slot wrap — buckets are already populated per worker by
// `insert_<rel>`, so this just wraps each bucket in a `Mutex` and shares
// the resulting slot array via `Arc`. Each slot is touched by exactly one
// worker; `mem::take` under the uncontended lock moves the bucket out
// without per-tuple work.
// =========================================================================

/// Emit, per EDB, a slot array `Arc<Vec<Mutex<Vec<Tuple>>>>` on the host
/// (`host`) plus a matching `let x = x.clone();` for the worker closure
/// (`clones`). No partition pass — buckets ship as-is.
fn gen_host_partitions(edbs: &[&Relation]) -> (Vec<TokenStream>, Vec<TokenStream>) {
    let mut host = Vec::with_capacity(edbs.len());
    let mut clones = Vec::with_capacity(edbs.len());
    for rel in edbs {
        let d = data_field_ident(rel);
        let slots = partition_slots_ident(rel);
        host.push(quote! {
            let #slots = std::sync::Arc::new(
                self.#d
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
            let field = rust_ident(rel.name());
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
        let ident = rust_ident(rel.name());
        fields.push(quote! { #ident });
    }
    for rel in program.printsize_idbs() {
        let ident = format_ident!("{}_size", rel.name());
        fields.push(quote! { #ident });
    }
    fields
}

/// Per-output block that produces the typed local (`reach`, `tc_size`, …)
/// `BatchResults` then names in its struct literal.
fn gen_drain_blocks(program: &Program, string_intern: bool) -> Vec<TokenStream> {
    let mut blocks = Vec::new();

    for rel in program.output_idbs() {
        let field = rust_ident(rel.name());
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
            let user_tuple = tuple_to_user_convert(rel, string_intern);
            let write_row = quote! {
                #field.push(#user_tuple);
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

/// Internal `Tuple` `row.0` → user-tuple. Used at drain time (batch-only
/// binding: the shared buffer row is `(Tuple, Ts, i32)`).
fn tuple_to_user_convert(rel: &Relation, string_intern: bool) -> TokenStream {
    per_position_tuple(
        rel,
        string_intern,
        quote! { row.0.clone() },
        |i| {
            let idx = proc_macro2::Literal::usize_unsuffixed(i);
            quote! { row.0.#idx.clone() }
        },
        |dt, src| tuple_to_user_expr(dt, string_intern, src),
    )
}
