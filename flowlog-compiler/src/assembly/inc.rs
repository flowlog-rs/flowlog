//! Incremental assembly. Preload and interactive transactions share live
//! workers: worker 0 drives a session from the shell, and the runtime's
//! `Broadcast` runs each transaction on every worker.

use flowlog_codegen::Skeleton;
use proc_macro2::TokenStream;
use quote::quote;

use crate::io::input::Input;
use crate::io::output::Output;

/// Returns the incremental `main`: the runtime arguments and output setup,
/// preload, and each persistent worker's part of the session. The emit
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
        emit_reads_epoch,
    } = output;

    let exit_on_worker_error = super::exit_on_worker_error();
    // Only draining a preload advances time before the session starts, and
    // `publish_preload` is empty exactly when nothing was preloaded; a
    // needless `mut` would warn in the generated crate.
    let mutable = (!publish_preload.is_empty()).then(|| quote! { mut });
    // Likewise, the epoch just committed is named only if the emit
    // fragment reads it.
    let committed_epoch = if *emit_reads_epoch {
        quote! { time_stamp }
    } else {
        quote! { _time_stamp }
    };

    quote! {
        fn main() {
            #runtime_args
            ::flowlog_runtime::initialize();
            #load_dictionary
            #initialize_output

            let workers = match &timely_config.communication {
                CommunicationConfig::Thread => 1,
                CommunicationConfig::Process(workers)
                | CommunicationConfig::ProcessBinary(workers) => *workers,
                CommunicationConfig::Cluster { threads, .. } => *threads,
            };
            let broadcast = Broadcast::new(workers);
            let failure = ::flowlog_runtime::Failure::new();

            #emitters

            let timer = Instant::now();
            let guards = ::flowlog_runtime::timely::execute(timely_config, {
                let failure = failure.clone();
                #emitter_captures

                move |worker| -> Result<(), ::flowlog_runtime::RuntimeError> {
                    let index = worker.index();
                    let mut broadcast = broadcast.clone();

                    #worker_init

                    #dataflow

                    #initialize_inputs

                    let #mutable time_stamp: u32 = 0;

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

                    // How this worker runs each phase of the session. The
                    // shell staged only updates `Inputs::check` accepted, so
                    // every op names a relation. The first update that fails
                    // to load ends the session on every worker, as a failed
                    // startup load does.
                    let run_phase = |phase: Phase<'_>| -> Result<(), ::flowlog_runtime::RuntimeError> {
                        match phase {
                            Phase::Load { updates, .. } => {
                                for (ordinal, op) in updates.iter().enumerate() {
                                    inputs.apply(op, ordinal).expect("the shell checks each update")?;
                                }
                            }
                            Phase::Run(time_stamp) => {
                                inputs.advance_dynamic_to(time_stamp);
                                inputs.flush_dynamic();
                                #step_loop

                                #metrics_write

                                #publish
                            }
                            // Timely steps the dataflow to completion after
                            // the closure returns, so no worker waits here
                            // for the others.
                            Phase::Close(time_stamp) => {
                                inputs.close_dynamic();
                                worker.step_while(|| probe.less_than(&time_stamp));
                            }
                        }
                        Ok(())
                    };

                    if index == 0 {
                        println!("{:?}:\tDataflow assembled", timer.elapsed());
                        println!(
                            "FlowLog Incremental Interactive Shell, type 'help' for commands."
                        );
                        // Worker 0 alone reads the shell. It emits once every
                        // worker has published, and times each commit from
                        // when the shell returned, leaving out the time spent
                        // typing.
                        let mut shell = Shell::new(Inputs::names(), Inputs::check);
                        broadcast.drive(
                            time_stamp,
                            |time_stamp| shell.next_request(time_stamp),
                            |#committed_epoch, made| {
                                #emit_output
                                println!("{:?}:\tCommitted & executed", made.elapsed());
                            },
                            run_phase,
                        )
                    } else {
                        broadcast.serve(time_stamp, run_phase)
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
