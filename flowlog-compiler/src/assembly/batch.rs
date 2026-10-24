//! Batch assembly. Workers publish their results to the runtime's output
//! emitters; dropping their guards joins them before the main thread emits
//! the outputs and sizes.

use flowlog_codegen::Skeleton;
use proc_macro2::TokenStream;
use quote::quote;

use crate::io::input::Input;
use crate::io::output::Output;

/// Returns the batch `main`: the runtime arguments and output setup, a
/// single dataflow run, and the outputs after the workers join. The emit
/// fragment may reference only state declared outside the workers.
pub(super) fn gen_batch_main(
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
        ..
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
            let failure = ::flowlog_runtime::Failure::new();

            #emitters

            let timer = Instant::now();
            let guards = timely::execute(timely_config, {
                let failure = failure.clone();
                #emitter_captures

                move |worker| -> Result<(), ::flowlog_runtime::RuntimeError> {
                    let index = worker.index();

                    #worker_init

                    #dataflow

                    if index == 0 {
                        println!("{:?}:\tDataflow assembled", timer.elapsed());
                    }

                    // Closing the inputs, all static in a batch engine, is
                    // what lets the dataflow drain to fixpoint. A worker
                    // whose load failed records it before closing them and
                    // drains like the others, so the drain is also the
                    // point past which every worker's failure is recorded;
                    // every worker then returns instead of publishing.
                    #initialize_inputs
                    #load_inputs
                    if let Err(error) = &loaded {
                        failure.record(error.to_string());
                    }
                    inputs.apply_inline_all();
                    inputs.close_static();

                    #step_loop

                    if failure.any() {
                        return loaded;
                    }

                    #publish

                    #metrics_write
                    Ok(())
                }
            })
            .unwrap_or_else(|error| {
                eprintln!("{error}");
                std::process::exit(1);
            });

            #exit_on_worker_error

            println!("{:?}:\tDataflow executed", timer.elapsed());
            #emit_output
        }
    }
}
