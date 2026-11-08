//! Incremental assembly. Preload and interactive transactions share live
//! workers, which run every transaction through one epoch loop: worker 0
//! reads it from the shell and publishes it, and each worker applies it.

use flowlog_codegen::Skeleton;
use proc_macro2::TokenStream;
use quote::quote;

use crate::io::input::Input;
use crate::io::output::Output;

/// Returns the incremental `main`: the runtime arguments and output setup,
/// preload, and the epoch loop every persistent worker runs. The emit
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
        load_dictionary,
    } = input;
    let Output {
        initialize: initialize_output,
        emit: emit_output,
    } = output;

    let exit_on_worker_error = super::exit_on_worker_error();

    quote! {
        fn main() {
            #runtime_args
            ::flowlog_runtime::initialize();
            #load_dictionary
            #initialize_output

            let shared_txn: Arc<RwLock<TxnState>> =
                Arc::new(RwLock::new(TxnState::default()));
            let workers = match &timely_config.communication {
                CommunicationConfig::Thread => 1,
                CommunicationConfig::Process(workers)
                | CommunicationConfig::ProcessBinary(workers) => *workers,
                CommunicationConfig::Cluster { threads, .. } => *threads,
            };
            let barrier = Arc::new(std::sync::Barrier::new(workers));
            let failure = ::flowlog_runtime::Failure::new();

            #emitters

            let timer = Instant::now();
            let guards = ::flowlog_runtime::timely::execute(timely_config, {
                let shared_txn = shared_txn.clone();
                let barrier = barrier.clone();
                let failure = failure.clone();
                #emitter_captures

                move |worker| -> Result<(), ::flowlog_runtime::RuntimeError> {
                    let index = worker.index();

                    #worker_init

                    #dataflow

                    #initialize_inputs

                    let mut time_stamp: u32 = 0;

                    #load_inputs
                    if let Err(error) = &loaded {
                        failure.record(error.to_string());
                    }
                    #preload_inputs

                    // A failed startup load stops every worker before
                    // anything is published or the shell starts. Draining
                    // the preload epoch needs every worker's inputs
                    // advanced, which each does after recording, so the
                    // drain is also the point past which every failure is
                    // recorded. Without a preload there is nothing to
                    // load, and nothing recorded.
                    if failure.any() {
                        return loaded;
                    }

                    #publish_preload

                    // Applies a transaction's updates. The first that
                    // fails to load is this worker's error; a failure on
                    // any worker, seen past the barrier, stops every
                    // worker before the epoch is published, as a failed
                    // startup load does. The shell staged only updates
                    // `Inputs::check` accepted, so every op names a
                    // relation.
                    fn apply_ops(
                        inputs: &mut Inputs,
                        ops: &[TxnOp],
                        failure: &::flowlog_runtime::Failure,
                        barrier: &std::sync::Barrier,
                    ) -> std::ops::ControlFlow<Result<(), ::flowlog_runtime::RuntimeError>> {
                        let mut applied = Ok(());
                        for (ordinal, op) in ops.iter().enumerate() {
                            match inputs.apply(op, ordinal) {
                                Some(Ok(())) => {}
                                Some(Err(error)) => {
                                    applied = Err(error);
                                    break;
                                }
                                None => unreachable!("the shell checks each update"),
                            }
                        }
                        if let Err(error) = &applied {
                            failure.record(error.to_string());
                        }
                        barrier.wait();
                        if failure.any() {
                            std::ops::ControlFlow::Break(applied)
                        } else {
                            std::ops::ControlFlow::Continue(())
                        }
                    }

                    if index == 0 {
                        println!("{:?}:\tDataflow assembled", timer.elapsed());
                        println!(
                            "FlowLog Incremental Interactive Shell, type 'help' for commands."
                        );
                    }

                    // Worker 0 alone reads the shell, and publishes each
                    // request as the next epoch's snapshot.
                    let mut shell = (index == 0).then(|| Shell::new(Inputs::names(), Inputs::check));
                    let mut last_epoch: u32 = 0;

                    loop {
                        if let Some(shell) = &mut shell {
                            let (action, pending) = match shell.next_request(time_stamp) {
                                Request::Commit(updates) => (TxnAction::Commit, updates),
                                Request::Quit => (TxnAction::Quit, Vec::new()),
                            };
                            let mut txn = shared_txn.write().expect("shared_txn poisoned");
                            *txn = TxnState {
                                epoch: txn.epoch + 1,
                                action,
                                pending,
                            };
                        }
                        // Started once the shell returns, so it leaves out
                        // the time spent typing.
                        let round_timer = Instant::now();

                        // Every worker reads the snapshot past this barrier,
                        // which worker 0 reaches only after writing it.
                        barrier.wait();
                        let snap = shared_txn.read().expect("shared_txn poisoned").clone();
                        assert!(snap.epoch > last_epoch, "stale epoch observed");
                        last_epoch = snap.epoch;

                        match snap.action {
                            TxnAction::Commit => {
                                if let std::ops::ControlFlow::Break(result) =
                                    apply_ops(&mut inputs, snap.pending.as_slice(), &failure, &barrier)
                                {
                                    return result;
                                }

                                time_stamp += 1;
                                inputs.advance_dynamic_to(time_stamp);
                                inputs.flush_dynamic();
                                #step_loop

                                #metrics_write

                                #publish

                                // Worker 0 emits once every worker has
                                // published. The others wait for the next
                                // snapshot at the barrier above, which worker
                                // 0 reaches only after emitting.
                                barrier.wait();
                                if index == 0 {
                                    #emit_output
                                    println!("{:?}:\tCommitted & executed", round_timer.elapsed());

                                    // Every worker has applied this transaction,
                                    // so release the shared copy's row strings
                                    // and paths before waiting for another command.
                                    shared_txn
                                        .write()
                                        .expect("shared_txn poisoned")
                                        .pending
                                        .clear();
                                }
                            }

                            // Timely steps the dataflow to completion after
                            // the closure returns, so no worker waits here
                            // for the others.
                            TxnAction::Quit => {
                                inputs.close_dynamic();
                                worker.step_while(|| probe.less_than(&time_stamp));
                                return Ok(());
                            }

                            TxnAction::None => {
                                unreachable!("worker 0 only publishes Commit or Quit");
                            }
                        }
                    }
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
