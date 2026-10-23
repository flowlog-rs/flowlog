//! Incremental assembly. Preload and interactive transactions share live
//! workers, with barriers separating output emission from the next epoch.

use flowlog_codegen::Skeleton;
use proc_macro2::TokenStream;
use quote::quote;

use crate::io::input::Input;
use crate::io::output::Output;

/// Returns the incremental `main`: the runtime arguments and output setup,
/// preload, and an interactive loop over persistent workers. The emit
/// fragment runs on worker 0 after every worker has published its results.
pub(super) fn gen_incremental_main(
    skeleton: &Skeleton,
    input: &Input,
    runtime_args: &TokenStream,
    output: &Output,
) -> TokenStream {
    let Skeleton {
        emitters,
        emitter_captures,
        worker_init,
        dataflow,
        step_loop,
        metrics_write,
        publish,
        ..
    } = skeleton;
    let Input {
        initialize_inputs,
        load_inputs,
        preload_inputs,
        publish_preload,
    } = input;
    let Output {
        initialize: initialize_output,
        emit: emit_output,
    } = output;

    let exit_on_worker_error = super::exit_on_worker_error();

    quote! {
        fn main() {
            #runtime_args
            #initialize_output

            let shared_txn: Arc<RwLock<TxnState>> =
                Arc::new(RwLock::new(TxnState::default()));
            let workers = match &timely_config.communication {
                timely::CommunicationConfig::Thread => 1,
                timely::CommunicationConfig::Process(workers)
                | timely::CommunicationConfig::ProcessBinary(workers) => *workers,
                timely::CommunicationConfig::Cluster { threads, .. } => *threads,
            };
            let barrier = Arc::new(std::sync::Barrier::new(workers));
            let load_failed = Arc::new(std::sync::atomic::AtomicBool::new(false));

            #emitters

            let timer = Instant::now();
            let guards = timely::execute(timely_config, {
                let shared_txn = shared_txn.clone();
                let barrier = barrier.clone();
                let load_failed = load_failed.clone();
                #emitter_captures

                move |worker| -> Result<(), ::flowlog_runtime::RuntimeError> {
                    let index = worker.index();

                    #worker_init

                    #dataflow

                    #initialize_inputs

                    let mut time_stamp: u32 = 0;

                    #load_inputs
                    #preload_inputs

                    // A failed startup load stops every worker before
                    // anything is published or the shell starts: the
                    // failing worker raises the shared flag, all workers
                    // meet at the barrier, and each returns, the failing
                    // one with its error. The preload epoch is drained
                    // first so every worker's progress completes.
                    if loaded.is_err() {
                        load_failed.store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                    barrier.wait();
                    if load_failed.load(std::sync::atomic::Ordering::SeqCst) {
                        return loaded;
                    }

                    #publish_preload

                    fn apply_ops(inputs: &mut Inputs, ops: &[TxnOp]) {
                        for (ordinal, op) in ops.iter().enumerate() {
                            match inputs.apply(op, ordinal) {
                                Some(Ok(())) => {}
                                Some(Err(error)) => eprintln!("{error}"),
                                None => eprintln!("unknown relation: '{}'", op.rel()),
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

                    // --- Workers other than 0 apply each published snapshot ---
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
                                    apply_ops(&mut inputs, snap.pending.as_slice());

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
                                    unreachable!("worker 0 only publishes Commit or Quit");
                                }
                            }

                            barrier.wait();
                        }
                        return Ok(());
                    }

                    // --- Worker 0 drives the interactive shell ---
                    let rel_words = Inputs::names()
                        .iter()
                        .map(|name| (*name).to_owned())
                        .collect::<Vec<_>>();
                    let mut prompt = Prompt::new(rel_words);

                    let mut local_txn: TxnState = TxnState::default();
                    let mut in_txn: bool = false;

                    loop {
                        let Some(c) = prompt.next_cmd(time_stamp) else { continue };

                        match c {
                            Cmd::Help => println!("{}", cmd::help_text()),

                            Cmd::Begin => {
                                in_txn = true;
                                local_txn.clear_pending();
                                println!("(txn begin)");
                            }

                            Cmd::Abort => {
                                in_txn = false;
                                local_txn.clear_pending();
                                println!("(txn aborted)");
                            }

                            Cmd::Op(op) => {
                                if !in_txn {
                                    in_txn = true;
                                    local_txn.clear_pending();
                                }
                                local_txn.enqueue(op);
                                println!("(queued)");
                            }

                            Cmd::Commit => {
                                if !in_txn {
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

                                // Apply the published snapshot, not `local_txn`, so worker
                                // 0 applies exactly what every other worker does.
                                let snap = shared_txn.read().unwrap().clone();
                                apply_ops(&mut inputs, snap.pending.as_slice());

                                time_stamp += 1;
                                inputs.advance_dynamic_to(time_stamp);
                                inputs.flush_dynamic();
                                #step_loop

                                #metrics_write

                                #publish

                                barrier.wait();

                                #emit_output

                                println!("{:?}:\tCommitted & executed", round_timer.elapsed());

                                in_txn = false;
                                local_txn.clear_pending();

                                barrier.wait();

                                {
                                    let mut w = shared_txn.write().unwrap();
                                    w.action = TxnAction::None;
                                    w.pending.clear();
                                }
                            }

                            Cmd::Quit => {
                                let next_epoch = shared_txn.read().unwrap().epoch + 1;
                                {
                                    let mut w = shared_txn.write().unwrap();
                                    *w = TxnState::as_quit_snapshot(next_epoch);
                                }

                                barrier.wait();

                                inputs.close_dynamic();
                                while probe.less_than(&time_stamp) {
                                    worker.step();
                                }

                                barrier.wait();
                                break;
                            }
                        }
                    }
                    Ok(())
                }
            })
            .unwrap_or_else(|error| {
                eprintln!("{error}");
                std::process::exit(1);
            });

            #exit_on_worker_error
        }
    }
}
