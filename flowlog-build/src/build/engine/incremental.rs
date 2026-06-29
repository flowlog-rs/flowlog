//! `DatalogIncrementalEngine` codegen for library mode.
//!
//! Mirrors [`super::batch::gen_lib_engine`] but drives a stateful,
//! epoch-based incremental engine. The surface matches binary-mode's
//! REPL semantics:
//!
//! - `begin()` — mark "in txn" + clear any leftover staged updates.
//! - `insert_*` / `remove_*` / `set_*` / `unset_*` — auto-begin if
//!   idle, then append to the per-worker staging buckets.
//! - `abort()` — mark "not in txn" + clear staged.
//! - `commit()` — **panics** if called without an active txn; else
//!   flushes the staged batch as one epoch and returns the
//!   [`IncrementalResults`] *delta* produced by that epoch.
//!
//! Threading model:
//!
//! - `new()` spawns a host-owned thread that calls `timely::execute`
//!   with `workers` workers.
//! - Workers wait on an `Arc<Barrier>` of `workers + 1` parties; the
//!   host (user) thread is the extra party and drives the protocol.
//! - Transaction state is broadcast through an
//!   `Arc<RwLock<TxnState>>` from `flowlog_runtime::txn`.
//! - Commit protocol: host moves per-worker staged buckets into shared
//!   `Mutex<Vec<_>>` slots, publishes a `TxnAction::Commit` snapshot,
//!   then barriers twice (start + end) before draining output buffers.
//! - `Drop` publishes `TxnAction::Quit`, barriers twice to release the
//!   workers, then joins the timely thread.

use flowlog_parser::Program;
use flowlog_parser::Relation;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use super::per_position_tuple;
use super::user_to_tuple_convert;
use crate::CodeParts;
use crate::build::relation::input_struct_ident;
use crate::build::relation::inputs_field_ident;
use crate::build::relation::printsize_field_ident;
use crate::build::relation::results_field_ident;
use crate::build::relation::user::tuple_to_user_expr;
use crate::build::relation::user_struct_ident;
use crate::data_type_tokens;

pub(crate) fn gen_lib_incremental_engine(
    program: &Program,
    string_intern: bool,
    parts: &CodeParts,
) -> TokenStream {
    let edbs = program.edbs();
    let non_nullary_edbs: Vec<&Relation> = edbs.iter().copied().filter(|r| r.arity() > 0).collect();
    let nullary_edbs: Vec<&Relation> = edbs.iter().copied().filter(|r| r.arity() == 0).collect();

    let inc_imports = gen_imports();
    let engine_struct = gen_engine_struct(program, &non_nullary_edbs, &nullary_edbs, string_intern);
    let new_body = gen_new_body(program, &non_nullary_edbs, &nullary_edbs, parts);
    let clear_staged_body = gen_clear_staged_body(&non_nullary_edbs, &nullary_edbs);
    let commit_body = gen_commit_body(program, &non_nullary_edbs, &nullary_edbs, string_intern);
    let drop_body = gen_drop_body();
    let staging_methods = gen_staging_methods(&non_nullary_edbs, &nullary_edbs, string_intern);

    quote! {
        #inc_imports

        #engine_struct

        impl DatalogIncrementalEngine {
            /// Spawn a pool of `workers` timely workers on a dedicated
            /// thread and return the engine handle. The dataflow stays
            /// alive for the engine's lifetime; `Drop` joins it.
            pub fn new(workers: usize) -> Self {
                let workers = workers.max(1);
                #new_body
            }

            /// Open a transaction. Sets the in-txn flag and clears any
            /// leftover staged updates. Called implicitly by the first
            /// `insert_*` / `remove_*` / `set_*` / `unset_*` after idle.
            pub fn begin(&mut self) {
                self.in_txn = true;
                self.clear_staged();
            }

            /// Abort the current transaction: discard every staged
            /// update and return to the idle state. No-op if not in a
            /// transaction.
            pub fn abort(&mut self) {
                self.in_txn = false;
                self.clear_staged();
            }

            /// Apply all staged updates as one epoch. Panics if no
            /// transaction is active — call `begin()` or any
            /// `insert_*` / `remove_*` method first. Returns the
            /// per-output deltas produced by this epoch.
            pub fn commit(&mut self) -> IncrementalResults {
                assert!(
                    self.in_txn,
                    "DatalogIncrementalEngine::commit called with no active transaction; \
                     call begin() or stage at least one update first",
                );
                let results = { #commit_body };
                self.in_txn = false;
                results
            }

            #staging_methods

            fn ensure_txn(&mut self) {
                if !self.in_txn {
                    self.begin();
                }
            }

            fn clear_staged(&mut self) {
                #clear_staged_body
            }
        }

        impl Drop for DatalogIncrementalEngine {
            fn drop(&mut self) {
                #drop_body
            }
        }
    }
}

// =========================================================================
// Imports specific to the incremental engine module body.
// =========================================================================

fn gen_imports() -> TokenStream {
    quote! {
        use ::flowlog_runtime::timely::dataflow::operators::probe::Handle as ProbeHandle;
        use ::flowlog_runtime::txn::{TxnAction, TxnState};
    }
}

// =========================================================================
// Engine struct — carries the shared runtime handles (slots, bufs, size
// cells) and the host-side staging buffers (one Vec per worker for
// non-nullary EDBs, one Option<i32> for nullary).
// =========================================================================

fn gen_engine_struct(
    program: &Program,
    non_nullary_edbs: &[&Relation],
    nullary_edbs: &[&Relation],
    string_intern: bool,
) -> TokenStream {
    let staged_fields: Vec<TokenStream> = non_nullary_edbs
        .iter()
        .map(|rel| {
            let ident = staged_ident(rel);
            let tuple_ty = data_type_tokens(&rel.data_type(), string_intern);
            quote! { #ident: Vec<Vec<(#tuple_ty, i32)>> }
        })
        .collect();

    let nullary_staged_fields: Vec<TokenStream> = nullary_edbs
        .iter()
        .map(|rel| {
            let ident = staged_ident(rel);
            quote! { #ident: Option<i32> }
        })
        .collect();

    let slot_fields: Vec<TokenStream> = non_nullary_edbs
        .iter()
        .map(|rel| {
            let ident = slots_ident(rel);
            let tuple_ty = data_type_tokens(&rel.data_type(), string_intern);
            quote! {
                #ident: Arc<Vec<::std::sync::Mutex<Vec<(#tuple_ty, i32)>>>>
            }
        })
        .collect();

    let nullary_slot_fields: Vec<TokenStream> = nullary_edbs
        .iter()
        .map(|rel| {
            let ident = slots_ident(rel);
            quote! {
                #ident: Arc<::std::sync::Mutex<Option<i32>>>
            }
        })
        .collect();

    let output_buf_fields: Vec<TokenStream> = program
        .output_idbs()
        .iter()
        .map(|rel| {
            let ident = buf_ident(rel);
            let tuple_ty = data_type_tokens(&rel.data_type(), string_intern);
            quote! {
                #ident: Arc<Mutex<Vec<Vec<(#tuple_ty, Ts, i32)>>>>
            }
        })
        .collect();

    let size_cell_fields: Vec<TokenStream> = program
        .printsize_idbs()
        .iter()
        .map(|rel| {
            let ident = size_cell_ident(rel);
            quote! {
                #ident: Arc<Mutex<(Ts, i32)>>
            }
        })
        .collect();

    quote! {
        pub struct DatalogIncrementalEngine {
            workers: usize,
            epoch: u32,
            in_txn: bool,

            #(#staged_fields,)*
            #(#nullary_staged_fields,)*

            #(#slot_fields,)*
            #(#nullary_slot_fields,)*

            shared_txn: Arc<::std::sync::RwLock<TxnState>>,
            barrier: Arc<::std::sync::Barrier>,

            #(#output_buf_fields,)*
            #(#size_cell_fields,)*

            worker_thread: Option<::std::thread::JoinHandle<()>>,
        }
    }
}

// =========================================================================
// `new()` body.
// =========================================================================

fn gen_new_body(
    program: &Program,
    non_nullary_edbs: &[&Relation],
    nullary_edbs: &[&Relation],
    parts: &CodeParts,
) -> TokenStream {
    let slot_inits: Vec<TokenStream> = non_nullary_edbs
        .iter()
        .map(|rel| {
            let ident = slots_ident(rel);
            quote! {
                let #ident = Arc::new(
                    (0..workers)
                        .map(|_| ::std::sync::Mutex::new(Vec::new()))
                        .collect::<Vec<_>>(),
                );
            }
        })
        .collect();

    let nullary_slot_inits: Vec<TokenStream> = nullary_edbs
        .iter()
        .map(|rel| {
            let ident = slots_ident(rel);
            quote! {
                let #ident = Arc::new(::std::sync::Mutex::new(None));
            }
        })
        .collect();

    let slot_clones_for_thread: Vec<TokenStream> = non_nullary_edbs
        .iter()
        .chain(nullary_edbs.iter())
        .map(|rel| {
            let ident = slots_ident(rel);
            quote! { let #ident = #ident.clone(); }
        })
        .collect();

    let slot_struct_inits: Vec<TokenStream> = non_nullary_edbs
        .iter()
        .chain(nullary_edbs.iter())
        .map(|rel| {
            let ident = slots_ident(rel);
            quote! { #ident }
        })
        .collect();

    let staged_self_inits: Vec<TokenStream> = non_nullary_edbs
        .iter()
        .map(|rel| {
            let ident = staged_ident(rel);
            quote! { #ident: vec![Vec::new(); workers] }
        })
        .collect();

    let nullary_staged_self_inits: Vec<TokenStream> = nullary_edbs
        .iter()
        .map(|rel| {
            let ident = staged_ident(rel);
            quote! { #ident: None }
        })
        .collect();

    let output_bufs = &parts.output_bufs;
    let output_buf_clones = &parts.output_buf_clones;
    let output_buf_self_inits: Vec<TokenStream> = program
        .output_idbs()
        .iter()
        .map(|rel| {
            let ident = buf_ident(rel);
            quote! { #ident }
        })
        .collect();

    let size_cell_decls = &parts.size_cell_decls;
    let size_cell_clones = &parts.size_cell_clones;
    let size_cell_self_inits: Vec<TokenStream> = program
        .printsize_idbs()
        .iter()
        .map(|rel| {
            let ident = size_cell_ident(rel);
            quote! { #ident }
        })
        .collect();

    let worker_closure = gen_worker_closure(program, non_nullary_edbs, nullary_edbs, parts);

    quote! {
        let barrier = Arc::new(::std::sync::Barrier::new(workers + 1));
        let shared_txn = Arc::new(::std::sync::RwLock::new(TxnState::default()));

        #(#slot_inits)*
        #(#nullary_slot_inits)*

        #(#output_bufs)*
        #(#size_cell_decls)*

        let worker_thread = ::std::thread::spawn({
            let barrier = barrier.clone();
            let shared_txn = shared_txn.clone();
            #(#slot_clones_for_thread)*
            #(#output_buf_clones)*
            #(#size_cell_clones)*

            move || {
                ::flowlog_runtime::timely::execute(
                    ::flowlog_runtime::timely::Config::process(workers),
                    #worker_closure,
                )
                .expect("timely::execute failed");
            }
        });

        Self {
            workers,
            epoch: 0,
            in_txn: false,
            #(#staged_self_inits,)*
            #(#nullary_staged_self_inits,)*
            #(#slot_struct_inits,)*
            shared_txn,
            barrier,
            #(#output_buf_self_inits,)*
            #(#size_cell_self_inits,)*
            worker_thread: Some(worker_thread),
        }
    }
}

// =========================================================================
// Worker closure (runs inside `timely::execute`).
// =========================================================================

fn gen_worker_closure(
    program: &Program,
    non_nullary_edbs: &[&Relation],
    nullary_edbs: &[&Relation],
    parts: &CodeParts,
) -> TokenStream {
    let edb_decls = &parts.edb_decls;
    let handle_binding = &parts.handle_binding;
    let dataflow_return = &parts.dataflow_return;
    let flows = &parts.flows;
    let local_bufs = &parts.local_bufs;
    let inspectors = &parts.inspectors;
    let flush = &parts.flush;
    let profile_init = &parts.profile_init;
    let metrics_write = &parts.metrics_write_incremental;

    let inputs_new_args = gen_inputs_new_args(program);

    let edge_apply_blocks: Vec<TokenStream> = non_nullary_edbs
        .iter()
        .map(|rel| {
            let slots = slots_ident(rel);
            let field = inputs_field_ident(rel);
            quote! {
                {
                    let my_chunk = ::std::mem::take(
                        &mut *#slots[index].lock().expect("slot poisoned"),
                    );
                    for (tuple, diff) in my_chunk {
                        inputs.#field.update_tuple(tuple, diff);
                    }
                }
            }
        })
        .collect();

    let nullary_apply_blocks: Vec<TokenStream> = nullary_edbs
        .iter()
        .map(|rel| {
            let slots = slots_ident(rel);
            let field = inputs_field_ident(rel);
            quote! {
                if index == 0 {
                    if let Some(diff) = #slots.lock().expect("slot poisoned").take() {
                        inputs.#field.update_tuple((), diff);
                    }
                }
            }
        })
        .collect();

    quote! {
        move |worker| {
            let index = worker.index();
            #profile_init
            #(#local_bufs)*

            let #handle_binding =
                worker.dataflow::<Ts, _, _>(|scope| {
                    #(#edb_decls)*
                    #(#flows)*

                    let mut probe = ProbeHandle::new();

                    #(#inspectors)*

                    #dataflow_return
                });

            let mut inputs = Inputs::new(#(#inputs_new_args),*);
            inputs.apply_inline_all(index);

            let mut time_stamp: Ts = 0;
            let mut last_epoch: u32 = 0;

            loop {
                barrier.wait();

                let snap = shared_txn.read().expect("shared_txn poisoned").clone();
                debug_assert!(
                    snap.epoch > last_epoch,
                    "stale epoch observed in incremental worker"
                );
                last_epoch = snap.epoch;

                match snap.action {
                    TxnAction::Commit => {
                        // Apply deltas at the current `time_stamp`. On the
                        // first commit this is 0, the same time the inline
                        // facts were staged at — they get summed together
                        // and processed in a single batch.
                        #(#edge_apply_blocks)*
                        #(#nullary_apply_blocks)*

                        // Close out this time and advance so DD will
                        // emit outputs for it. Stepping until the probe
                        // catches up finalizes the just-ended time.
                        time_stamp += 1;
                        inputs.advance_to_all(time_stamp);
                        inputs.flush_all();
                        while probe.less_than(&time_stamp) {
                            worker.step();
                        }

                        #metrics_write

                        #(#flush)*

                        barrier.wait();
                    }
                    TxnAction::Quit => {
                        inputs.close_all();
                        while probe.less_than(&time_stamp) {
                            worker.step();
                        }
                        barrier.wait();
                        break;
                    }
                    TxnAction::None => {
                        unreachable!("host never publishes TxnAction::None");
                    }
                }
            }
        }
    }
}

fn gen_inputs_new_args(program: &Program) -> Vec<TokenStream> {
    program
        .edbs()
        .iter()
        .map(|rel| {
            let input_struct = input_struct_ident(rel);
            let handle = format_ident!("h{}", rel.name());
            quote! { #input_struct::new(#handle) }
        })
        .collect()
}

// =========================================================================
// `clear_staged()` body — zeroes host-side staged buffers without
// freeing the per-worker Vec allocations. Called by `begin()`/`abort()`.
// =========================================================================

fn gen_clear_staged_body(
    non_nullary_edbs: &[&Relation],
    nullary_edbs: &[&Relation],
) -> TokenStream {
    let clears: Vec<TokenStream> = non_nullary_edbs
        .iter()
        .map(|rel| {
            let staged = staged_ident(rel);
            quote! {
                for bucket in self.#staged.iter_mut() {
                    bucket.clear();
                }
            }
        })
        .collect();

    let nullary_clears: Vec<TokenStream> = nullary_edbs
        .iter()
        .map(|rel| {
            let staged = staged_ident(rel);
            quote! { self.#staged = None; }
        })
        .collect();

    quote! {
        #(#clears)*
        #(#nullary_clears)*
    }
}

// =========================================================================
// `commit()` body.
// =========================================================================

fn gen_commit_body(
    program: &Program,
    non_nullary_edbs: &[&Relation],
    nullary_edbs: &[&Relation],
    string_intern: bool,
) -> TokenStream {
    // `mem::take` moves each staged bucket into the shared slot, leaving
    // the staging Vec empty for the next commit cycle without freeing
    // the outer allocation.
    let stage_moves: Vec<TokenStream> = non_nullary_edbs
        .iter()
        .map(|rel| {
            let staged = staged_ident(rel);
            let slots = slots_ident(rel);
            quote! {
                for (i, bucket) in self.#staged.iter_mut().enumerate() {
                    *self.#slots[i].lock().expect("slot poisoned") =
                        ::std::mem::take(bucket);
                }
            }
        })
        .collect();

    let nullary_stage_moves: Vec<TokenStream> = nullary_edbs
        .iter()
        .map(|rel| {
            let staged = staged_ident(rel);
            let slots = slots_ident(rel);
            quote! {
                *self.#slots.lock().expect("slot poisoned") = self.#staged.take();
            }
        })
        .collect();

    let drain_blocks = gen_drain_blocks(program, string_intern);
    let result_field_names = gen_result_field_names(program);

    quote! {
        #(#stage_moves)*
        #(#nullary_stage_moves)*

        self.epoch += 1;
        *self.shared_txn.write().expect("shared_txn poisoned") = TxnState {
            epoch: self.epoch,
            action: TxnAction::Commit,
            pending: Vec::new(),
        };

        self.barrier.wait();
        self.barrier.wait();

        #(#drain_blocks)*

        IncrementalResults {
            #(#result_field_names),*
        }
    }
}

/// Per-output drain block: pulls this commit's output rows from the
/// shared buffer and binds a typed local — `Vec<(rel::Foo, i32)>` for
/// non-nullary outputs, `i32` net diff for nullary, and the raw size
/// delta for `.printsize` cells. The engine no longer folds across
/// commits; callers maintain a snapshot if they need one.
fn gen_drain_blocks(program: &Program, string_intern: bool) -> Vec<TokenStream> {
    let mut blocks = Vec::new();

    for rel in program.output_idbs() {
        let field = results_field_ident(rel);
        let buf = buf_ident(rel);
        if rel.arity() == 0 {
            blocks.push(quote! {
                let #field: i32 = {
                    let drained: Vec<Vec<_>> = ::std::mem::take(
                        &mut *self.#buf.lock().expect("output buffer poisoned"),
                    );
                    let mut net: i32 = 0;
                    for worker_buf in drained {
                        for (_tuple, _time, diff) in worker_buf {
                            net += diff;
                        }
                    }
                    net
                };
            });
        } else {
            let struct_ident = user_struct_ident(rel);
            let user_tuple = tuple_to_user_from_row(rel, string_intern);
            blocks.push(quote! {
                let #field: Vec<(rel::#struct_ident, i32)> = {
                    let drained: Vec<Vec<_>> = ::std::mem::take(
                        &mut *self.#buf.lock().expect("output buffer poisoned"),
                    );
                    let cap: usize = drained.iter().map(|w| w.len()).sum();
                    let mut out: Vec<(rel::#struct_ident, i32)> = Vec::with_capacity(cap);
                    for worker_buf in drained {
                        for row in worker_buf {
                            out.push((#user_tuple, row.2));
                        }
                    }
                    out
                };
            });
        }
    }

    for rel in program.printsize_idbs() {
        let field = printsize_field_ident(rel);
        let cell = size_cell_ident(rel);
        blocks.push(quote! {
            let #field: i32 = {
                let (_, raw) = *self.#cell.lock().expect("size cell poisoned");
                raw
            };
        });
    }

    blocks
}

fn gen_result_field_names(program: &Program) -> Vec<TokenStream> {
    let mut names = Vec::new();
    for rel in program.output_idbs() {
        let field = results_field_ident(rel);
        names.push(quote! { #field });
    }
    for rel in program.printsize_idbs() {
        let field = printsize_field_ident(rel);
        names.push(quote! { #field });
    }
    names
}

// =========================================================================
// `Drop` body.
// =========================================================================

fn gen_drop_body() -> TokenStream {
    quote! {
        if let Some(handle) = self.worker_thread.take() {
            self.epoch += 1;
            *self.shared_txn.write().expect("shared_txn poisoned") =
                TxnState::as_quit_snapshot(self.epoch);
            self.barrier.wait();
            self.barrier.wait();
            let _ = handle.join();
        }
    }
}

// =========================================================================
// Per-EDB staging methods: `insert_<rel>` / `remove_<rel>` for typed
// relations, `set_<rel>` / `unset_<rel>` for nullary.
// =========================================================================

fn gen_staging_methods(
    non_nullary_edbs: &[&Relation],
    nullary_edbs: &[&Relation],
    string_intern: bool,
) -> TokenStream {
    let per_rel: Vec<TokenStream> = non_nullary_edbs
        .iter()
        .map(|rel| gen_one_rel_staging(rel, string_intern))
        .collect();
    let nullary: Vec<TokenStream> = nullary_edbs
        .iter()
        .map(|rel| gen_nullary_staging(rel))
        .collect();

    quote! {
        #(#per_rel)*
        #(#nullary)*
    }
}

fn gen_one_rel_staging(rel: &Relation, string_intern: bool) -> TokenStream {
    let name = rel.name();
    let struct_ident = user_struct_ident(rel);
    let staged = staged_ident(rel);
    let insert = format_ident!("insert_{}", name);
    let remove = format_ident!("remove_{}", name);

    // `user_to_tuple_convert` already short-circuits to `quote! { item }` when
    // no per-position conversion is needed, so no local guard is required.
    let map_expr = user_to_tuple_convert(rel, string_intern);

    let distribute = |diff_tok: TokenStream| -> TokenStream {
        quote! {
            if items.is_empty() { return; }
            self.ensure_txn();
            let total = items.len();
            let workers = self.workers;
            let chunk = total / workers;
            let remainder = total % workers;
            let mut iter = items.into_iter();
            for i in 0..workers {
                let take = chunk + if i < remainder { 1 } else { 0 };
                if take == 0 { continue; }
                let bucket = &mut self.#staged[i];
                bucket.reserve(take);
                for item in iter.by_ref().take(take) {
                    bucket.push((#map_expr, #diff_tok));
                }
            }
        }
    };

    let insert_body = distribute(quote! { 1_i32 });
    let remove_body = distribute(quote! { -1_i32 });

    quote! {
        /// Stage tuples to insert at the next `commit()`. Auto-begins
        /// a transaction if none is active; an empty `items` slice is
        /// a no-op and does not auto-begin.
        pub fn #insert(&mut self, items: Vec<rel::#struct_ident>) {
            #insert_body
        }

        /// Stage tuples to retract at the next `commit()`. Same
        /// auto-begin + empty-slice semantics as `insert`.
        pub fn #remove(&mut self, items: Vec<rel::#struct_ident>) {
            #remove_body
        }
    }
}

fn gen_nullary_staging(rel: &Relation) -> TokenStream {
    let name = rel.name();
    let staged = staged_ident(rel);
    let set = format_ident!("set_{}", name);
    let unset = format_ident!("unset_{}", name);
    quote! {
        /// Assert the nullary fact at the next `commit()`. Auto-begins
        /// a transaction if none is active.
        pub fn #set(&mut self) {
            self.ensure_txn();
            self.#staged = Some(1);
        }

        /// Retract the nullary fact at the next `commit()`. Auto-begins
        /// a transaction if none is active.
        pub fn #unset(&mut self) {
            self.ensure_txn();
            self.#staged = Some(-1);
        }
    }
}

/// Output-row tuple → user-tuple. The drain consumes each row by value
/// (`for row in worker_buf`), so the tuple at `row.0` and its fields
/// can be moved out without cloning.
fn tuple_to_user_from_row(rel: &Relation, string_intern: bool) -> TokenStream {
    per_position_tuple(
        rel,
        string_intern,
        quote! { row.0 },
        |i| {
            let idx = proc_macro2::Literal::usize_unsuffixed(i);
            quote! { row.0.#idx }
        },
        |dt, src| tuple_to_user_expr(dt, string_intern, src),
    )
}

// =========================================================================
// Ident helpers.
// =========================================================================

fn slots_ident(rel: &Relation) -> Ident {
    format_ident!("{}_slots", rel.name())
}

fn staged_ident(rel: &Relation) -> Ident {
    format_ident!("{}_staged", rel.name())
}

fn buf_ident(rel: &Relation) -> Ident {
    format_ident!("buf_{}", rel.name())
}

fn size_cell_ident(rel: &Relation) -> Ident {
    format_ident!("size_{}", rel.name())
}
