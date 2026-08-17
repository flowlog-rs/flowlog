//! `DatalogIncrementalEngine` codegen for library mode.
//!
//! Mirrors [`super::batch::gen_lib_engine`] but drives a stateful,
//! epoch-based incremental engine. The surface matches binary-mode's
//! REPL semantics:
//!
//! - `begin()` — mark "in txn" + clear any leftover staged updates.
//! - `insert_*` / `remove_*` / `set_*` / `unset_*` — auto-begin if
//!   idle, then append to the host-side staging buckets.
//! - `abort()` — mark "not in txn" + clear staged.
//! - `commit()` — **panics** if called without an active txn; else
//!   flushes the staged batch as one epoch and returns the
//!   [`IncrementalResults`] *delta* produced by that epoch.
//!
//! One of two mutually exclusive engine shapes is emitted, picked at
//! build time by [`flowlog_common::Config::inlines_single_worker`]. Both
//! share the surface above; nothing branches on the shape at run time.
//!
//! Threaded shape (the default):
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
//!
//! Inline shape (opt-in, `DatalogInc` only):
//!
//! - `new()` accepts one worker and asserts on any other count; the
//!   engine owns that worker and steps it on the calling thread.
//! - `commit()` applies the staged updates straight to the input
//!   sessions, steps until the probe passes the epoch, and reads the
//!   inspectors' local buffers.
//! - Nothing shared exists to synchronize: no thread, barrier, txn
//!   snapshot, per-worker slot, or shared output buffer is emitted, and
//!   the resulting engine is `!Send`.
//! - `Drop` closes the inputs and steps until no dataflow remains.

use flowlog_parser::Program;
use flowlog_parser::Relation;
use proc_macro2::Ident;
use proc_macro2::Literal;
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
use crate::codegen::local_buf_ident;
use crate::codegen::tuple_type;
use crate::data_type_tokens;

pub(crate) fn gen_lib_incremental_engine(
    program: &Program,
    string_intern: bool,
    inline: bool,
    parts: &CodeParts,
) -> TokenStream {
    let edbs = program.edbs();
    let non_nullary_edbs: Vec<&Relation> = edbs.iter().copied().filter(|r| r.arity() > 0).collect();
    let nullary_edbs: Vec<&Relation> = edbs.iter().copied().filter(|r| r.arity() == 0).collect();

    if inline {
        gen_inline_engine(
            program,
            &non_nullary_edbs,
            &nullary_edbs,
            string_intern,
            parts,
        )
    } else {
        gen_threaded_engine(
            program,
            &non_nullary_edbs,
            &nullary_edbs,
            string_intern,
            parts,
        )
    }
}

// =========================================================================
// Shared surface: every method whose body is the same in both shapes,
// wrapped around the fragments that differ.
// =========================================================================

/// The per-shape fragments [`gen_engine_api`] splices. One value per
/// shape, so a new method on the engine is written once and a new shape
/// has to answer every question the existing one answers.
struct EngineApi {
    new_doc: TokenStream,
    new_prelude: TokenStream,
    new_body: TokenStream,
    commit_body: TokenStream,
    staging_methods: TokenStream,
    clear_staged_body: TokenStream,
    drop_body: TokenStream,
}

fn gen_engine_api(api: &EngineApi) -> TokenStream {
    let EngineApi {
        new_doc,
        new_prelude,
        new_body,
        commit_body,
        staging_methods,
        clear_staged_body,
        drop_body,
    } = api;

    quote! {
        impl DatalogIncrementalEngine {
            #new_doc
            pub fn new(workers: usize) -> Self {
                #new_prelude
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
// Threaded shape.
// =========================================================================

fn gen_threaded_engine(
    program: &Program,
    non_nullary_edbs: &[&Relation],
    nullary_edbs: &[&Relation],
    string_intern: bool,
    parts: &CodeParts,
) -> TokenStream {
    let inc_imports = gen_threaded_imports();
    let engine_struct =
        gen_threaded_engine_struct(program, non_nullary_edbs, nullary_edbs, string_intern);
    let api = gen_engine_api(&EngineApi {
        new_doc: quote! {
            /// Spawn a pool of `workers` timely workers on a dedicated
            /// thread and return the engine handle. The dataflow stays
            /// alive for the engine's lifetime; `Drop` joins it.
        },
        new_prelude: quote! { let workers = workers.max(1); },
        new_body: gen_threaded_new_body(program, non_nullary_edbs, nullary_edbs, parts),
        commit_body: gen_threaded_commit_body(
            program,
            non_nullary_edbs,
            nullary_edbs,
            string_intern,
        ),
        staging_methods: gen_staging_methods(
            non_nullary_edbs,
            nullary_edbs,
            string_intern,
            Staging::PerWorker,
        ),
        clear_staged_body: gen_clear_staged_body(
            non_nullary_edbs,
            nullary_edbs,
            Staging::PerWorker,
        ),
        drop_body: gen_threaded_drop_body(),
    });

    quote! {
        #inc_imports

        #engine_struct

        #api
    }
}

// =========================================================================
// Inline shape: a one-worker engine the caller's thread drives directly.
// The staged updates, the timely worker, the input sessions, the probe and
// the inspectors' local buffers all live in the engine, so a commit is a
// straight-line apply / step / drain with nothing shared to synchronize.
// =========================================================================

fn gen_inline_engine(
    program: &Program,
    non_nullary_edbs: &[&Relation],
    nullary_edbs: &[&Relation],
    string_intern: bool,
    parts: &CodeParts,
) -> TokenStream {
    let engine_struct =
        gen_inline_engine_struct(program, non_nullary_edbs, nullary_edbs, string_intern);
    let api = gen_engine_api(&EngineApi {
        new_doc: quote! {
            /// Build the dataflow and return the engine handle. This
            /// engine owns its single timely worker and steps it on the
            /// calling thread. `workers` is kept so the same call site
            /// compiles against either generated shape, but `1` is the
            /// only accepted value; any other count panics. The dataflow
            /// stays alive for the engine's lifetime; `Drop` shuts it
            /// down.
        },
        new_prelude: quote! {
            assert_eq!(
                workers, 1,
                "DatalogIncrementalEngine was generated with \
                 Builder::inline_single_worker(true): it owns one timely worker \
                 on the calling thread, so new() accepts 1 worker only",
            );
        },
        new_body: gen_inline_new_body(program, non_nullary_edbs, nullary_edbs, parts),
        commit_body: gen_inline_commit_body(
            program,
            non_nullary_edbs,
            nullary_edbs,
            string_intern,
            parts,
        ),
        staging_methods: gen_staging_methods(
            non_nullary_edbs,
            nullary_edbs,
            string_intern,
            Staging::Single,
        ),
        clear_staged_body: gen_clear_staged_body(non_nullary_edbs, nullary_edbs, Staging::Single),
        drop_body: quote! {
            // Closing the inputs lets every operator drain and retire;
            // stepping until no dataflow remains is timely's own
            // single-threaded shutdown.
            self.inputs.close_all();
            while self.worker.has_dataflows() {
                self.worker.step_or_park(None);
            }
        },
    });

    quote! {
        use ::flowlog_runtime::timely::dataflow::operators::probe::Handle as ProbeHandle;

        #engine_struct

        #api
    }
}

fn gen_inline_engine_struct(
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
            quote! { #ident: Vec<(#tuple_ty, i32)> }
        })
        .collect();

    let nullary_staged_fields: Vec<TokenStream> = nullary_edbs
        .iter()
        .map(|rel| {
            let ident = staged_ident(rel);
            quote! { #ident: Option<i32> }
        })
        .collect();

    let local_buf_fields: Vec<TokenStream> = program
        .output_idbs()
        .iter()
        .map(|rel| {
            let ident = local_buf_ident(rel);
            let elem_ty = tuple_type(rel, string_intern);
            quote! { #ident: Rc<RefCell<Vec<#elem_ty>>> }
        })
        .collect();

    let size_cell_fields: Vec<TokenStream> = program
        .printsize_idbs()
        .iter()
        .map(|rel| {
            let ident = size_cell_ident(rel);
            quote! { #ident: Arc<Mutex<(Ts, i32)>> }
        })
        .collect();

    quote! {
        pub struct DatalogIncrementalEngine {
            in_txn: bool,

            #(#staged_fields,)*
            #(#nullary_staged_fields,)*

            #(#local_buf_fields,)*
            #(#size_cell_fields,)*

            inputs: Inputs,
            probe: ProbeHandle<Ts>,
            time_stamp: Ts,
            worker: ::flowlog_runtime::timely::worker::Worker,
        }
    }
}

/// Inline `new()` body: mirrors `timely::execute_directly` with a worker
/// on a thread allocator, built here rather than at the first commit so
/// dataflow construction stays out of a caller's measured region.
fn gen_inline_new_body(
    program: &Program,
    non_nullary_edbs: &[&Relation],
    nullary_edbs: &[&Relation],
    parts: &CodeParts,
) -> TokenStream {
    let local_bufs = &parts.local_bufs;
    let size_cell_decls = &parts.size_cell_decls;
    let dataflow_build = gen_dataflow_build(parts);
    let inputs_new_args = gen_inputs_new_args(program);

    let staged_self_inits: Vec<TokenStream> = non_nullary_edbs
        .iter()
        .map(|rel| {
            let ident = staged_ident(rel);
            quote! { #ident: Vec::new() }
        })
        .collect();

    let nullary_staged_self_inits: Vec<TokenStream> = nullary_edbs
        .iter()
        .map(|rel| {
            let ident = staged_ident(rel);
            quote! { #ident: None }
        })
        .collect();

    let local_buf_self_inits: Vec<TokenStream> = program
        .output_idbs()
        .iter()
        .map(|rel| {
            let ident = local_buf_ident(rel);
            quote! { #ident }
        })
        .collect();

    let size_cell_self_inits: Vec<TokenStream> = program
        .printsize_idbs()
        .iter()
        .map(|rel| {
            let ident = size_cell_ident(rel);
            quote! { #ident }
        })
        .collect();

    quote! {
        // The lone worker is worker 0, so every `.fact` row partitioned
        // by worker index lands here.
        let index = 0usize;
        #(#local_bufs)*
        #(#size_cell_decls)*

        let mut worker = ::flowlog_runtime::timely::worker::Worker::new(
            ::flowlog_runtime::timely::WorkerConfig::default(),
            ::flowlog_runtime::timely::communication::Allocator::Thread(
                ::std::default::Default::default(),
            ),
            Some(::std::time::Instant::now()),
        );

        #dataflow_build

        let mut inputs = Inputs::new(#(#inputs_new_args),*);
        inputs.apply_inline_all(index);

        Self {
            in_txn: false,
            #(#staged_self_inits,)*
            #(#nullary_staged_self_inits,)*
            #(#local_buf_self_inits,)*
            #(#size_cell_self_inits,)*
            inputs,
            probe,
            time_stamp: 0,
            worker,
        }
    }
}

/// Inline `commit()` body. Every field it touches is a distinct field of
/// `self`, so draining a staging bucket into an input session and stepping
/// the worker borrow disjointly.
fn gen_inline_commit_body(
    program: &Program,
    non_nullary_edbs: &[&Relation],
    nullary_edbs: &[&Relation],
    string_intern: bool,
    parts: &CodeParts,
) -> TokenStream {
    let step_loop = &parts.step_loop;

    let apply_blocks: Vec<TokenStream> = non_nullary_edbs
        .iter()
        .map(|rel| {
            let staged = staged_ident(rel);
            let field = inputs_field_ident(rel);
            quote! {
                for (tuple, diff) in self.#staged.drain(..) {
                    self.inputs.#field.update_tuple(tuple, diff);
                }
            }
        })
        .collect();

    let nullary_apply_blocks: Vec<TokenStream> = nullary_edbs
        .iter()
        .map(|rel| {
            let staged = staged_ident(rel);
            let field = inputs_field_ident(rel);
            quote! {
                if let Some(diff) = self.#staged.take() {
                    self.inputs.#field.update_tuple((), diff);
                }
            }
        })
        .collect();

    let drain_blocks = gen_inline_drain_blocks(program, string_intern);
    let result_field_names = gen_result_field_names(program);

    quote! {
        #(#apply_blocks)*
        #(#nullary_apply_blocks)*

        // Close out this time and advance so DD emits outputs for it;
        // stepping until the probe catches up finalizes the just-ended
        // time.
        self.time_stamp += 1;
        self.inputs.advance_to_all(self.time_stamp);
        self.inputs.flush_all();
        {
            let time_stamp = self.time_stamp;
            let probe = &self.probe;
            let worker = &mut self.worker;
            #step_loop
        }

        #(#drain_blocks)*

        IncrementalResults {
            #(#result_field_names),*
        }
    }
}

/// Inline counterpart of [`gen_threaded_drain_blocks`]: the rows come straight out
/// of the inspector's local buffer, so there is no per-worker nesting to
/// flatten and no shared buffer to lock.
fn gen_inline_drain_blocks(program: &Program, string_intern: bool) -> Vec<TokenStream> {
    let mut blocks = Vec::new();

    for rel in program.output_idbs() {
        let field = results_field_ident(rel);
        let local = local_buf_ident(rel);
        if rel.arity() == 0 {
            blocks.push(quote! {
                let #field: i32 = {
                    let drained = ::std::mem::take(&mut *self.#local.borrow_mut());
                    let mut net: i32 = 0;
                    for (_tuple, _time, diff) in drained {
                        net += diff;
                    }
                    net
                };
            });
        } else {
            let struct_ident = user_struct_ident(rel);
            let user_tuple = tuple_to_user_from_row(rel, string_intern);
            blocks.push(quote! {
                let #field: Vec<(rel::#struct_ident, i32)> = {
                    let drained = ::std::mem::take(&mut *self.#local.borrow_mut());
                    let mut out: Vec<(rel::#struct_ident, i32)> =
                        Vec::with_capacity(drained.len());
                    for row in drained {
                        out.push((#user_tuple, row.2));
                    }
                    out
                };
            });
        }
    }

    blocks.extend(gen_printsize_blocks(program));
    blocks
}

// =========================================================================
// Threaded shape: imports specific to its module body.
// =========================================================================

fn gen_threaded_imports() -> TokenStream {
    quote! {
        use ::flowlog_runtime::timely::dataflow::operators::probe::Handle as ProbeHandle;
        use ::flowlog_runtime::txn::{TxnAction, TxnState};
    }
}

// =========================================================================
// Threaded shape: engine struct. Carries the shared runtime handles
// (slots, bufs, size cells) and the host-side staging buffers: one Vec
// per worker for non-nullary EDBs, one Option<i32> for nullary.
// =========================================================================

fn gen_threaded_engine_struct(
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
// Threaded shape: `new()` body.
// =========================================================================

fn gen_threaded_new_body(
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
// Threaded shape: worker closure (runs inside `timely::execute`).
// =========================================================================

fn gen_worker_closure(
    program: &Program,
    non_nullary_edbs: &[&Relation],
    nullary_edbs: &[&Relation],
    parts: &CodeParts,
) -> TokenStream {
    let local_bufs = &parts.local_bufs;
    let flush = &parts.flush;
    let profile_init = &parts.profile_init;
    let metrics_write = &parts.metrics_write;
    let step_loop = &parts.step_loop;

    let dataflow_build = gen_dataflow_build(parts);
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

            #dataflow_build

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
                        #step_loop

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

/// The `worker.dataflow(..)` call that binds the EDB handles and the probe.
/// Identical in both shapes: the dataflow a worker builds does not depend
/// on who drives it, only the local output buffers it writes into have to
/// be in scope already.
fn gen_dataflow_build(parts: &CodeParts) -> TokenStream {
    let edb_decls = &parts.edb_decls;
    let handle_binding = &parts.handle_binding;
    let dataflow_return = &parts.dataflow_return;
    let flows = &parts.flows;
    let inspectors = &parts.inspectors;

    quote! {
        let #handle_binding =
            worker.dataflow::<Ts, _, _>(|scope| {
                #(#edb_decls)*
                #(#flows)*

                let mut probe = ProbeHandle::new();

                #(#inspectors)*

                #dataflow_return
            });
    }
}

// =========================================================================
// Shared helpers: fragments both shapes splice, differing only in how the
// staged updates are bucketed.
// =========================================================================

// `clear_staged()` body: zeroes host-side staged buffers without freeing
// their Vec allocations. Called by `begin()`/`abort()`.

fn gen_clear_staged_body(
    non_nullary_edbs: &[&Relation],
    nullary_edbs: &[&Relation],
    staging: Staging,
) -> TokenStream {
    let clears: Vec<TokenStream> = non_nullary_edbs
        .iter()
        .map(|rel| {
            let staged = staged_ident(rel);
            match staging {
                Staging::PerWorker => quote! {
                    for bucket in self.#staged.iter_mut() {
                        bucket.clear();
                    }
                },
                Staging::Single => quote! { self.#staged.clear(); },
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
// Threaded shape: `commit()` body.
// =========================================================================

fn gen_threaded_commit_body(
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

    let drain_blocks = gen_threaded_drain_blocks(program, string_intern);
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

/// Threaded per-output drain block: pulls this commit's output rows from
/// the shared buffer and binds a typed local, `Vec<(rel::Foo, i32)>` for
/// non-nullary outputs and an `i32` net diff for nullary. `.printsize`
/// cells are appended by [`gen_printsize_blocks`]. The engine no longer
/// folds across commits; callers maintain a snapshot if they need one.
fn gen_threaded_drain_blocks(program: &Program, string_intern: bool) -> Vec<TokenStream> {
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

    blocks.extend(gen_printsize_blocks(program));
    blocks
}

/// Per-`.printsize` block binding the raw size delta this commit left in
/// the size cell. Shared by both shapes: the size cell is an
/// `Arc<Mutex<..>>` field on either engine, written by an inspector that
/// lives in the dataflow rather than in either driver.
fn gen_printsize_blocks(program: &Program) -> Vec<TokenStream> {
    program
        .printsize_idbs()
        .iter()
        .map(|rel| {
            let field = printsize_field_ident(rel);
            let cell = size_cell_ident(rel);
            quote! {
                let #field: i32 = {
                    let (_, raw) = *self.#cell.lock().expect("size cell poisoned");
                    raw
                };
            }
        })
        .collect()
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
// Threaded shape: `Drop` body.
// =========================================================================

fn gen_threaded_drop_body() -> TokenStream {
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

/// How host-side updates are bucketed before a commit hands them over.
#[derive(Clone, Copy)]
enum Staging {
    /// One bucket per worker; `insert_*` spreads the batch across them so
    /// each worker applies its own chunk.
    PerWorker,
    /// A single bucket: the inline shape has exactly one worker, and the
    /// commit that drains it runs on the same thread.
    Single,
}

fn gen_staging_methods(
    non_nullary_edbs: &[&Relation],
    nullary_edbs: &[&Relation],
    string_intern: bool,
    staging: Staging,
) -> TokenStream {
    let per_rel: Vec<TokenStream> = non_nullary_edbs
        .iter()
        .map(|rel| gen_one_rel_staging(rel, string_intern, staging))
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

fn gen_one_rel_staging(rel: &Relation, string_intern: bool, staging: Staging) -> TokenStream {
    let name = rel.name();
    let struct_ident = user_struct_ident(rel);
    let staged = staged_ident(rel);
    let insert = format_ident!("insert_{}", name);
    let remove = format_ident!("remove_{}", name);

    // `user_to_tuple_convert` already short-circuits to `quote! { item }` when
    // no per-position conversion is needed, so no local guard is required.
    let map_expr = user_to_tuple_convert(rel, string_intern);

    let distribute = |diff_tok: TokenStream| -> TokenStream {
        match staging {
            Staging::PerWorker => quote! {
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
            },
            Staging::Single => quote! {
                if items.is_empty() { return; }
                self.ensure_txn();
                self.#staged.reserve(items.len());
                for item in items {
                    self.#staged.push((#map_expr, #diff_tok));
                }
            },
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

/// Converts an output-row tuple to a user-tuple. Both shapes' drains
/// consume each row by value, so the tuple at `row.0` and its fields can
/// be moved out without cloning.
fn tuple_to_user_from_row(rel: &Relation, string_intern: bool) -> TokenStream {
    per_position_tuple(
        rel,
        string_intern,
        quote! { row.0 },
        |i| {
            let idx = Literal::usize_unsuffixed(i);
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

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flowlog_common::Config;
    use flowlog_common::ExecutionMode;
    use flowlog_common::SourceMap;
    use flowlog_planner::planner::ProgramPlanner;
    use rstest::rstest;
    use tempfile::NamedTempFile;

    use super::*;
    use crate::CodeGen;

    const PROGRAM: &str = "\
        .decl Edge(src: int32, dst: int32)\n\
        .decl Reach(src: int32, dst: int32)\n\
        .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
        .output Reach\n\
        Reach(s, d) :- Edge(s, d).\n\
        Reach(s, d) :- Reach(s, m), Edge(m, d).\n";

    /// Same rules, but `Reach` is reported as a size rather than as rows,
    /// which is the only way the `.printsize` drain path is reached.
    const PROGRAM_WITH_PRINTSIZE: &str = "\
        .decl Edge(src: int32, dst: int32)\n\
        .decl Reach(src: int32, dst: int32)\n\
        .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
        .printsize Reach\n\
        Reach(s, d) :- Edge(s, d).\n\
        Reach(s, d) :- Reach(s, m), Edge(m, d).\n";

    /// The engine module rendered for `PROGRAM`, as a token string. Runs
    /// the real pipeline because the engine is assembled from the same
    /// [`CodeParts`] fragments the dataflow passes emit.
    fn engine(inline_single_worker: bool) -> String {
        engine_tokens(PROGRAM, inline_single_worker).to_string()
    }

    /// The engine module rendered for `source`, as a token stream.
    fn engine_tokens(source: &str, inline_single_worker: bool) -> TokenStream {
        let mut tmp = NamedTempFile::new().expect("temp file");
        tmp.write_all(source.as_bytes()).expect("write program");
        let mut config = Config {
            program: tmp.path().to_string_lossy().into_owned(),
            mode: ExecutionMode::DatalogInc,
            inline_single_worker,
            ..Config::default()
        };
        let path = config.program.clone();
        let program = flowlog_parser::parse(&path, &[], &mut SourceMap::new(), &mut config)
            .expect("program parses");
        let planner =
            ProgramPlanner::from_program(&config, &program, &mut None).expect("program plans");
        let mut cg = CodeGen::new(config.clone(), program.clone());
        let parts = cg.generate(&planner, &mut None).expect("codegen succeeds");

        gen_lib_incremental_engine(&program, false, config.inlines_single_worker(), &parts)
    }

    #[test]
    fn the_inline_engine_owns_and_steps_its_worker() {
        let src = engine(true);
        assert!(
            src.contains("timely :: worker :: Worker :: new"),
            "the inline engine must build its own timely worker"
        );
        assert!(
            src.contains("assert_eq ! (workers , 1"),
            "the inline engine must reject any worker count but one"
        );
        assert!(
            src.contains("self . inputs . in_edge . update_tuple"),
            "an inline commit must apply staged updates straight to the input session"
        );
        assert!(
            src.contains("self . local_reach . borrow_mut"),
            "an inline commit must drain the inspector's local buffer"
        );
    }

    /// The two shapes are exclusive: picking the inline one must leave no
    /// trace of the threaded protocol to pay for or reason about.
    #[test]
    fn the_inline_engine_emits_no_threaded_machinery() {
        let src = engine(true);
        for absent in [
            "timely :: execute",
            "worker_thread",
            "barrier",
            "shared_txn",
            "TxnAction",
            "_slots",
            "buf_reach",
        ] {
            assert!(
                !src.contains(absent),
                "the inline engine must not emit `{absent}`"
            );
        }
    }

    /// The inline shape is opt-in, so an unconfigured build must emit
    /// exactly the engine it emitted before the option existed.
    #[test]
    fn the_default_engine_is_the_threaded_one() {
        let src = engine(false);
        assert!(
            src.contains("timely :: execute"),
            "the default build runs the timely cluster"
        );
        assert!(
            src.contains("barrier . wait"),
            "the default build keeps the barrier commit protocol"
        );
        assert!(
            src.contains("TxnAction :: Commit"),
            "the default build keeps publishing transaction snapshots"
        );
        for absent in ["Worker :: new", "assert_eq ! (workers , 1"] {
            assert!(
                !src.contains(absent),
                "the default build must not emit `{absent}`"
            );
        }
    }

    /// `.printsize` outputs are reported through a size cell instead of a
    /// row buffer. Both shapes share the block that reads the cell, so
    /// both must carry the field it reads and must still parse as Rust.
    #[rstest]
    #[case::threaded(false)]
    #[case::inline(true)]
    fn both_shapes_report_printsize_from_the_size_cell(#[case] inline: bool) {
        let tokens = engine_tokens(PROGRAM_WITH_PRINTSIZE, inline);
        let src = tokens.to_string();
        assert!(
            src.contains("size_reach : Arc < Mutex <"),
            "inline={inline} must own the `.printsize` size cell"
        );
        assert!(
            src.contains("let reach_size : i32 = { let (_ , raw) = * self . size_reach . lock ()"),
            "inline={inline} must read the size cell when the commit drains"
        );
        assert!(
            !src.contains("local_reach"),
            "inline={inline} must not also buffer rows for a `.printsize` output"
        );
        syn::parse2::<syn::File>(tokens)
            .unwrap_or_else(|e| panic!("inline={inline} must emit parsable Rust: {e}"));
    }

    /// Both shapes build the same dataflow; only the driver around it
    /// differs, so neither may drift into a second copy of it.
    #[test]
    fn both_shapes_build_the_same_dataflow() {
        let dataflow = "worker . dataflow :: < Ts , _ , _ >";
        for inline in [false, true] {
            assert_eq!(
                engine(inline).matches(dataflow).count(),
                1,
                "inline={inline} must build the dataflow exactly once"
            );
        }
    }
}
