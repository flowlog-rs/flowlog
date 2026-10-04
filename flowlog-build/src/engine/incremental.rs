//! Incremental library execution over staged host batches.
//!
//! Each batch keeps the weight of its insert or remove call. A commit
//! publishes the batches, then workers load their shares through the
//! runtime in per-relation call order before advancing the epoch. A static
//! relation offers only inserts, staged before the first commit, which
//! loads them and closes the relation's input.

use flowlog_codegen::Skeleton;
use flowlog_codegen::input_field_ident;
use flowlog_codegen::input_handle_ident;
use flowlog_codegen::output_emitter_ident;
use flowlog_codegen::relation_marker_ident;
use flowlog_codegen::user_tuple_tokens;
use flowlog_codegen::weight_tokens;
use flowlog_parser::Mutability;
use flowlog_parser::Program;
use flowlog_parser::Relation;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::bindings::printsize_field_ident;
use crate::bindings::results_field_ident;
use crate::bindings::user_tuple_ident;

pub(crate) fn gen_lib_incremental_engine(
    program: &Program,
    uses_ord: bool,
    skeleton: &Skeleton,
) -> TokenStream {
    let edbs = program.edbs();
    let non_nullary_edbs: Vec<&Relation> = edbs.iter().copied().filter(|r| r.arity() > 0).collect();
    let nullary_edbs: Vec<&Relation> = edbs.iter().copied().filter(|r| r.arity() == 0).collect();

    let inc_imports = gen_imports();
    let slot_aliases = (!non_nullary_edbs.is_empty()).then(|| {
        quote! {
            /// A relation's staged rows, one batch per `insert_*` or
            /// `remove_*` call, each with its weight.
            type Batches<T, W> = Vec<(Vec<T>, W)>;
            /// The shared slot a relation's batches pass through from the
            /// host to the workers at a commit.
            type Slot<T, W> = Arc<::std::sync::Mutex<Arc<Batches<T, W>>>>;
        }
    });
    let engine_struct = gen_engine_struct(program, &non_nullary_edbs, &nullary_edbs);
    let new_body = gen_new_body(
        program,
        &non_nullary_edbs,
        &nullary_edbs,
        skeleton,
        uses_ord,
    );
    let clear_staged_body = gen_clear_staged_body(&non_nullary_edbs, &nullary_edbs);
    let commit_body = gen_commit_body(program, &non_nullary_edbs, &nullary_edbs);
    let drop_body = gen_drop_body();
    let staging_methods = gen_staging_methods(&non_nullary_edbs, &nullary_edbs);

    quote! {
        #inc_imports
        #slot_aliases

        #engine_struct

        impl IncrementalEngine {
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

            /// Applies staged updates as one epoch and returns output deltas.
            ///
            /// # Panics
            ///
            /// Panics if no transaction is active. Call `begin()` or stage
            /// an update before committing.
            pub fn commit(&mut self) -> IncrementalResults {
                assert!(
                    self.in_txn,
                    "IncrementalEngine::commit called with no active transaction; \
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

        impl Drop for IncrementalEngine {
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
        use std::sync::Arc;
        use ::flowlog_runtime::timely::dataflow::operators::probe::Handle as ProbeHandle;
        use ::flowlog_runtime::txn::{TxnAction, TxnState};
    }
}

// =============================================================================
// Engine state
// =============================================================================

fn gen_engine_struct(
    program: &Program,
    non_nullary_edbs: &[&Relation],
    nullary_edbs: &[&Relation],
) -> TokenStream {
    let staged_fields: Vec<TokenStream> = non_nullary_edbs
        .iter()
        .map(|rel| {
            let ident = staged_ident(rel);
            let (tuple, weight) = slot_params(rel);
            quote! { #ident: Batches<#tuple, #weight> }
        })
        .collect();

    let nullary_staged_fields: Vec<TokenStream> = nullary_edbs
        .iter()
        .map(|rel| {
            let ident = staged_ident(rel);
            let weight = weight_tokens(rel.input_mutability());
            quote! { #ident: Option<#weight> }
        })
        .collect();

    let slot_fields: Vec<TokenStream> = non_nullary_edbs
        .iter()
        .map(|rel| {
            let ident = slots_ident(rel);
            let (tuple, weight) = slot_params(rel);
            quote! { #ident: Slot<#tuple, #weight> }
        })
        .collect();

    let nullary_slot_fields: Vec<TokenStream> = nullary_edbs
        .iter()
        .map(|rel| {
            let ident = slots_ident(rel);
            let weight = weight_tokens(rel.input_mutability());
            quote! {
                #ident: Arc<::std::sync::Mutex<Option<#weight>>>
            }
        })
        .collect();

    let emitter_fields: Vec<TokenStream> = program
        .idbs()
        .iter()
        .map(|rel| {
            let ident = output_emitter_ident(rel.name());
            let marker = relation_marker_ident(rel.name());
            quote! { #ident: ::flowlog_runtime::io::output::Emitter<#marker, Ts> }
        })
        .collect();

    quote! {
        pub struct IncrementalEngine {
            epoch: u32,
            in_txn: bool,

            #(#staged_fields,)*
            #(#nullary_staged_fields,)*

            #(#slot_fields,)*
            #(#nullary_slot_fields,)*

            shared_txn: Arc<::std::sync::RwLock<TxnState>>,
            barrier: Arc<::std::sync::Barrier>,

            #(#emitter_fields,)*

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
    skeleton: &Skeleton,
    uses_ord: bool,
) -> TokenStream {
    let slot_inits: Vec<TokenStream> = non_nullary_edbs
        .iter()
        .map(|rel| {
            let ident = slots_ident(rel);
            let (tuple, weight) = slot_params(rel);
            quote! { let #ident = Slot::<#tuple, #weight>::default(); }
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

    let emitters = &skeleton.emitters;
    let emitter_captures = &skeleton.emitter_captures;
    let emitter_self_inits: Vec<TokenStream> = program
        .idbs()
        .iter()
        .map(|rel| {
            let ident = output_emitter_ident(rel.name());
            quote! { #ident }
        })
        .collect();

    let worker_closure =
        gen_worker_closure(program, non_nullary_edbs, nullary_edbs, skeleton, uses_ord);

    quote! {
        let barrier = Arc::new(::std::sync::Barrier::new(workers + 1));
        let shared_txn = Arc::new(::std::sync::RwLock::new(TxnState::default()));

        #(#slot_inits)*
        #(#nullary_slot_inits)*

        #emitters

        let worker_thread = ::std::thread::spawn({
            let barrier = barrier.clone();
            let shared_txn = shared_txn.clone();
            #(#slot_clones_for_thread)*
            #emitter_captures

            move || {
                ::flowlog_runtime::timely::execute(
                    ::flowlog_runtime::timely::Config::process(workers),
                    #worker_closure,
                )
                .expect("timely::execute failed");
            }
        });

        Self {
            epoch: 0,
            in_txn: false,
            #(#staged_self_inits,)*
            #(#nullary_staged_self_inits,)*
            #(#slot_struct_inits,)*
            shared_txn,
            barrier,
            #(#emitter_self_inits,)*
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
    skeleton: &Skeleton,
    uses_ord: bool,
) -> TokenStream {
    let Skeleton {
        worker_init,
        dataflow,
        step_loop,
        metrics_write,
        publish,
        ..
    } = skeleton;

    let inputs_new_args = program
        .edbs()
        .into_iter()
        .map(|rel| input_handle_ident(rel.name()));

    let apply_block = |rel: &&Relation| {
        let slots = slots_ident(rel);
        let field = input_field_ident(rel.name());
        match rel.arity() {
            0 => quote! {
                if let Some(diff) = *#slots.lock().expect("slot poisoned") {
                    inputs.#field.load_rows(&[()], diff).expect("nullary update");
                }
            },
            _ => quote! {
                {
                    let batches = Arc::clone(&#slots.lock().expect("slot poisoned"));
                    for (rows, diff) in batches.iter() {
                        inputs.#field.load_rows(rows.as_slice(), *diff).expect("typed input loading");
                    }
                }
            },
        }
    };
    let (static_edbs, dynamic_edbs): (Vec<&Relation>, Vec<&Relation>) = non_nullary_edbs
        .iter()
        .chain(nullary_edbs)
        .copied()
        .partition(|rel| match rel.input_mutability() {
            Mutability::Static => true,
            Mutability::Append | Mutability::Mutable => false,
        });
    let static_apply_blocks: Vec<TokenStream> = static_edbs.iter().map(apply_block).collect();
    let dynamic_apply_blocks: Vec<TokenStream> = dynamic_edbs.iter().map(apply_block).collect();

    quote! {
        move |worker| {
            let index = worker.index();
            #worker_init

            #dataflow

            let mut inputs = Inputs::new(#(#inputs_new_args,)* worker.peers(), index, #uses_ord)
                .expect("valid worker coordinates");
            inputs.apply_inline_all();

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
                        // facts were staged at; they get summed together
                        // and processed in a single batch.
                        #(#dynamic_apply_blocks)*

                        // Static relations load only here, and close before
                        // the epoch advances: an open static input would
                        // hold every static operator at time 0.
                        if time_stamp == 0 {
                            #(#static_apply_blocks)*
                            inputs.close_static();
                        }

                        // Close out this time and advance so DD will
                        // emit outputs for it. Stepping until the probe
                        // catches up finalizes the just-ended time.
                        time_stamp += 1;
                        inputs.advance_dynamic_to(time_stamp);
                        inputs.flush_dynamic();
                        #step_loop

                        #metrics_write

                        #publish

                        barrier.wait();
                    }
                    TxnAction::Quit => {
                        inputs.close_dynamic();
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

// =============================================================================
// Staging lifecycle
// =============================================================================

fn gen_clear_staged_body(
    non_nullary_edbs: &[&Relation],
    nullary_edbs: &[&Relation],
) -> TokenStream {
    let clears: Vec<TokenStream> = non_nullary_edbs
        .iter()
        .map(|rel| {
            let staged = staged_ident(rel);
            quote! {
                self.#staged.clear();
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
) -> TokenStream {
    let stage_moves: Vec<TokenStream> = non_nullary_edbs
        .iter()
        .map(|rel| {
            let staged = staged_ident(rel);
            let slots = slots_ident(rel);
            quote! {
                *self.#slots.lock().expect("slot poisoned") =
                    Arc::new(::std::mem::take(&mut self.#staged));
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

    let release_rows = non_nullary_edbs.iter().map(|rel| {
        let slots = slots_ident(rel);
        quote! { *self.#slots.lock().expect("slot poisoned") = Arc::new(Vec::new()); }
    });

    let drain_blocks = gen_drain_blocks(program);
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

        // Workers have finished reading, so committed host rows need not
        // stay allocated while the engine waits for the next transaction.
        #(#release_rows)*

        #(#drain_blocks)*

        IncrementalResults {
            #(#result_field_names),*
        }
    }
}

/// Binds this commit's weighted deltas and independent count deltas.
fn gen_drain_blocks(program: &Program) -> Vec<TokenStream> {
    let mut blocks = Vec::new();
    for rel in program.output_idbs() {
        let field = results_field_ident(rel);
        let emitter = output_emitter_ident(rel.name());
        blocks.push(quote! { let #field = self.#emitter.emit_host::<true, _>(); });
    }
    for rel in program.printsize_idbs() {
        let field = printsize_field_ident(rel);
        let emitter = output_emitter_ident(rel.name());
        blocks.push(quote! { let #field: i32 = self.#emitter.delta_size(); });
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

fn gen_staging_methods(non_nullary_edbs: &[&Relation], nullary_edbs: &[&Relation]) -> TokenStream {
    let per_rel: Vec<TokenStream> = non_nullary_edbs
        .iter()
        .map(|rel| gen_one_rel_staging(rel))
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

fn gen_one_rel_staging(rel: &Relation) -> TokenStream {
    let name = rel.name();
    let struct_ident = user_tuple_ident(rel);
    let staged = staged_ident(rel);
    let insert = format_ident!("insert_{}", name);
    let remove = format_ident!("remove_{}", name);

    match rel.input_mutability() {
        Mutability::Static => {
            let static_check = static_check(rel);
            quote! {
                /// Stages a batch to insert at the first `commit()`, which
                /// loads this static relation once.
                ///
                /// Begins a transaction if none is active. An empty batch has
                /// no effect and does not begin a transaction.
                ///
                /// # Panics
                ///
                /// Panics once a commit has run: a static relation cannot
                /// change after its initial load.
                pub fn #insert(&mut self, items: Vec<rel::#struct_ident>) {
                    #static_check
                    if items.is_empty() { return; }
                    self.ensure_txn();
                    self.#staged.push((items, ::flowlog_runtime::diff::Static));
                }
            }
        }
        Mutability::Append => quote! {
            /// Stages a batch to insert at the next `commit()`. An append
            /// relation offers no removal.
            ///
            /// Begins a transaction if none is active. An empty batch has
            /// no effect and does not begin a transaction.
            pub fn #insert(&mut self, items: Vec<rel::#struct_ident>) {
                if items.is_empty() { return; }
                self.ensure_txn();
                self.#staged.push((items, ::flowlog_runtime::diff::Append));
            }
        },
        Mutability::Mutable => {
            let stage = |diff: TokenStream| -> TokenStream {
                quote! {
                    if items.is_empty() { return; }
                    self.ensure_txn();
                    self.#staged.push((items, #diff));
                }
            };
            let insert_body = stage(quote! { 1_i32 });
            let remove_body = stage(quote! { -1_i32 });
            quote! {
                /// Stages a batch to insert at the next `commit()`.
                ///
                /// Begins a transaction if none is active. An empty batch has
                /// no effect and does not begin a transaction.
                pub fn #insert(&mut self, items: Vec<rel::#struct_ident>) {
                    #insert_body
                }

                /// Stages a batch to retract at the next `commit()`.
                ///
                /// Begins a transaction if none is active. An empty batch has
                /// no effect and does not begin a transaction.
                pub fn #remove(&mut self, items: Vec<rel::#struct_ident>) {
                    #remove_body
                }
            }
        }
    }
}

fn gen_nullary_staging(rel: &Relation) -> TokenStream {
    let name = rel.name();
    let staged = staged_ident(rel);
    let set = format_ident!("set_{}", name);
    let unset = format_ident!("unset_{}", name);

    match rel.input_mutability() {
        Mutability::Static => {
            let static_check = static_check(rel);
            quote! {
                /// Assert the nullary fact at the first `commit()`, which
                /// loads this static relation once. Auto-begins a transaction
                /// if none is active.
                ///
                /// # Panics
                ///
                /// Panics once a commit has run: a static relation cannot
                /// change after its initial load.
                pub fn #set(&mut self) {
                    #static_check
                    self.ensure_txn();
                    self.#staged = Some(::flowlog_runtime::diff::Static);
                }
            }
        }
        Mutability::Append => quote! {
            /// Assert the nullary fact at the next `commit()`. An append
            /// relation offers no retraction. Auto-begins a transaction if
            /// none is active.
            pub fn #set(&mut self) {
                self.ensure_txn();
                self.#staged = Some(::flowlog_runtime::diff::Append);
            }
        },
        Mutability::Mutable => quote! {
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
        },
    }
}

/// Emits the guard that rejects an update to static `rel` after the first
/// commit has loaded it.
fn static_check(rel: &Relation) -> TokenStream {
    let relation = rel.raw_name();
    quote! {
        assert!(
            self.epoch == 0,
            "{}",
            ::flowlog_runtime::RuntimeError::StaticRelation { relation: #relation },
        );
    }
}

/// Returns `rel`'s row tuple and load weight, the type arguments of its
/// `Batches` and `Slot`.
fn slot_params(rel: &Relation) -> (TokenStream, TokenStream) {
    let tuple = user_tuple_tokens(&rel.data_type());
    let weight = weight_tokens(rel.input_mutability());
    (tuple, weight)
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
