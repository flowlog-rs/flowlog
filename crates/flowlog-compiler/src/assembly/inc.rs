//! Incremental mode (`DatalogInc` / `ExtendInc`) main function generation.
//!
//! Generates a `fn main()` that:
//! 1. Constructs the timely dataflow graph with a probe handle.
//! 2. Builds the relation registry and runs the preload epoch (if any).
//! 3. Enters an interactive command loop where worker 0 drives
//!    transactions (`begin` / `put` / `file` / `commit` / `quit`)
//!    and non-zero workers follow via shared state + barriers.

use proc_macro2::TokenStream;
use quote::quote;

use generator::AssemblyParts;

/// Emit the complete incremental-mode `fn main() { ... }` token stream.
pub(crate) fn gen_incremental_main(p: &AssemblyParts) -> TokenStream {
    let AssemblyParts {
        edb_inputs,
        handle_binding,
        dataflow_return,
        flows,
        output_bufs,
        output_buf_clones,
        local_bufs,
        inspectors,
        flush,
        merge,
        registry_inserts,
        preload,
        profile_init,
        time_profile_write_incremental: time_profile_write,
        memory_profile_write_incremental: memory_profile_write,
        ..
    } = p;

    quote! {
        fn main() {
            let args: Vec<String> = std::env::args().collect();

            let shared_txn: Arc<RwLock<TxnState>> =
                Arc::new(RwLock::new(TxnState::default()));
            let barrier = worker_barrier_from_args(&args);

            #(#output_bufs)*

            timely::execute_from_args(args.into_iter(), {
                let shared_txn = shared_txn.clone();
                let barrier = barrier.clone();
                #(#output_buf_clones)*

                move |worker| {
                    let timer = Instant::now();
                    let peers = worker.peers();
                    let index = worker.index();

                    #profile_init

                    #(#local_bufs)*

                    let #handle_binding =
                        worker.dataflow::<Ts, _, _>(|scope| {
                            #(#edb_inputs)*
                            #(#flows)*

                            let mut probe = ProbeHandle::new();

                            #(#inspectors)*

                            #dataflow_return
                        });

                    let mut rels: HashMap<String, Box<dyn RelOps>> = HashMap::new();
                    #(#registry_inserts)*

                    let mut time_stamp: u32 = 0;

                    #preload

                    // Helper: apply a list of txn ops to this worker's input handles.
                    fn apply_ops(
                        rels: &mut HashMap<String, Box<dyn RelOps>>,
                        ops: &[TxnOp],
                        peers: usize,
                        index: usize,
                    ) {
                        for op in ops {
                            match op {
                                TxnOp::Put { rel, tuple, diff } => {
                                    let r = rels
                                        .get_mut(&rel.to_ascii_lowercase())
                                        .unwrap_or_else(|| {
                                            panic!("unknown relation: '{rel}'")
                                        });
                                    r.apply_tuple(tuple, *diff, peers, index);
                                }
                                TxnOp::File { rel, path, diff } => {
                                    let r = rels
                                        .get_mut(&rel.to_ascii_lowercase())
                                        .unwrap_or_else(|| {
                                            panic!("unknown relation: '{rel}'")
                                        });
                                    r.apply_file(path.as_path(), *diff, peers, index);
                                }
                            }
                        }
                    }

                    if index == 0 {
                        println!("{:?}:\tDataflow assembled", timer.elapsed());
                        println!(
                            "FlowLog Incremental Interactive Shell, type 'help' for commands."
                        );
                    }

                    let mut last_epoch_seen: u32 = 0;

                    // -------------------------------
                    // Worker != 0: listen & apply published txn snapshots
                    // -------------------------------
                    if index != 0 {
                        loop {
                            barrier.wait();

                            let snap = shared_txn.read().unwrap().clone();
                            assert!(
                                snap.epoch > last_epoch_seen,
                                "stale epoch observed"
                            );
                            last_epoch_seen = snap.epoch;

                            match snap.action {
                                TxnAction::Commit => {
                                    apply_ops(&mut rels, snap.pending.as_slice(), peers, index);

                                    time_stamp += 1;
                                    for r in rels.values_mut() {
                                        r.advance_to(time_stamp);
                                        r.flush();
                                    }
                                    while probe.less_than(&time_stamp) {
                                        worker.step();
                                    }

                                    #time_profile_write
                                    #memory_profile_write

                                    // Flush thread-local buffers into shared buffers.
                                    #(#flush)*

                                    barrier.wait();
                                }

                                TxnAction::Quit => {
                                    for r in rels.values_mut() {
                                        r.close();
                                    }
                                    while probe.less_than(&time_stamp) {
                                        worker.step();
                                    }

                                    barrier.wait();
                                    break;
                                }

                                TxnAction::None => {
                                    unreachable!("worker 0 only publishes Commit or Quit");
                                }
                            }

                            barrier.wait();
                        }
                        return;
                    }

                    // -------------------------------
                    // Worker 0: interactive driver
                    // -------------------------------
                    let rel_words = rels.keys().cloned().collect::<Vec<_>>();
                    let mut prompt = Prompt::new(rel_words);

                    let mut local_txn: TxnState = TxnState::default();

                    loop {
                        let Some(c) = prompt.next_cmd(time_stamp) else { continue };

                        match c {
                            Cmd::Help => println!("{}", cmd::help_text()),

                            Cmd::Begin => {
                                local_txn.begin();
                                println!("(txn begin)");
                            }

                            Cmd::Abort => {
                                local_txn.abort();
                                println!("(txn aborted)");
                            }

                            Cmd::Put { rel, tuple, diff } => {
                                if !local_txn.in_txn {
                                    local_txn.begin();
                                }
                                local_txn.enqueue(TxnOp::Put { rel, tuple, diff });
                                println!("(queued put)");
                            }

                            Cmd::File { rel, path, diff } => {
                                if !local_txn.in_txn {
                                    local_txn.begin();
                                }
                                local_txn.enqueue(TxnOp::File { rel, path, diff });
                                println!("(queued file)");
                            }

                            Cmd::Commit => {
                                if !local_txn.in_txn {
                                    println!("(no active txn)");
                                    continue;
                                }

                                let round_timer = Instant::now();

                                let next_epoch = shared_txn.read().unwrap().epoch + 1;
                                {
                                    let mut w = shared_txn.write().unwrap();
                                    *w = local_txn.as_commit_snapshot(next_epoch);
                                }

                                barrier.wait();

                                // Apply exactly what got published (keeps behavior consistent).
                                let snap = shared_txn.read().unwrap().clone();
                                apply_ops(&mut rels, snap.pending.as_slice(), peers, index);

                                time_stamp += 1;
                                for r in rels.values_mut() {
                                    r.advance_to(time_stamp);
                                    r.flush();
                                }
                                while probe.less_than(&time_stamp) {
                                    worker.step();
                                }

                                #time_profile_write
                                #memory_profile_write

                                // Flush thread-local buffers into shared buffers.
                                #(#flush)*

                                barrier.wait();

                                if index == 0 {
                                    // === Merge output buffers (sort, limit, write) ===
                                    #(#merge)*

                                    println!("{:?}:\tCommitted & executed", round_timer.elapsed());
                                }

                                local_txn.abort();

                                barrier.wait();

                                {
                                    let mut w = shared_txn.write().unwrap();
                                    w.action = TxnAction::None;
                                    w.pending.clear();
                                    w.in_txn = false;
                                }
                            }

                            Cmd::Quit => {
                                let next_epoch = shared_txn.read().unwrap().epoch + 1;
                                {
                                    let mut w = shared_txn.write().unwrap();
                                    *w = TxnState::as_quit_snapshot(next_epoch);
                                }

                                barrier.wait();

                                for r in rels.values_mut() {
                                    r.close();
                                }
                                while probe.less_than(&time_stamp) {
                                    worker.step();
                                }

                                barrier.wait();
                                break;
                            }
                        }
                    }
                }
            })
            .unwrap();
        }
    }
}
