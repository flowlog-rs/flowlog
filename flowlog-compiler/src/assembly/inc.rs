//! Incremental assembly. Preload and interactive transactions share live
//! workers: worker 0 drives a session from the shell, and the runtime's
//! `Broadcast` runs the preload, then each transaction, on every worker.

use flowlog_codegen::Skeleton;
use proc_macro2::TokenStream;
use quote::quote;

use crate::io::input::Input;
use crate::io::output::Output;

/// Returns the incremental `main`: the runtime arguments and output setup,
/// the startup load, and each persistent worker's part of the session,
/// which runs what was loaded as its first epoch. The emit fragment runs on
/// worker 0 after every worker has published an epoch's results.
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
        has_preload,
        load_dictionary,
    } = input;
    let Output {
        initialize: initialize_output,
        emit: emit_output,
        emit_reads_epoch,
    } = output;

    let exit_on_worker_error = super::exit_on_worker_error();
    // The session starts with a preload only if anything was loaded for
    // one. Without it nothing would read `loaded`, which would warn in the
    // generated crate, so nothing binds it either.
    let (load_inputs, preloaded) = if *has_preload {
        (quote! { #load_inputs }, quote! { Some(loaded) })
    } else {
        (quote! {}, quote! { None })
    };
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
                #emitter_captures

                move |worker| -> Result<(), ::flowlog_runtime::RuntimeError> {
                    let index = worker.index();
                    let mut broadcast = broadcast.clone();

                    #worker_init

                    #dataflow

                    #initialize_inputs

                    // Static inputs close after the startup load, before
                    // the session: no later command may change one, and
                    // while one is open no static operator completes, nor
                    // would the step loop, which waits for the static
                    // outputs.
                    #load_inputs
                    inputs.apply_inline_all();
                    inputs.close_static();

                    // How this worker runs each phase of the session. The
                    // shell staged only updates `Inputs::check` accepted, so
                    // every op names a relation. The first update that fails
                    // to load ends the session on every worker, as a failed
                    // startup load does before the preload runs.
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
                        // Worker 0 alone reads the shell, which greets before
                        // its first command. It emits once every worker has
                        // published, and times each commit from when the
                        // shell returned, leaving out the time spent typing,
                        // and the preload from when the session started.
                        let mut shell = Shell::new(Inputs::names(), Inputs::check);
                        broadcast.drive(
                            #preloaded,
                            |time_stamp| shell.next_request(time_stamp),
                            |#committed_epoch, made| {
                                #emit_output
                                println!("{:?}:\tCommitted & executed", made.elapsed());
                            },
                            run_phase,
                        )
                    } else {
                        broadcast.serve(#preloaded, run_phase)
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
