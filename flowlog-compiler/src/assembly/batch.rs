//! Batch assembly. Workers publish their results to the runtime's output
//! emitters; dropping their guards joins them before the main thread emits
//! the outputs and sizes.

use flowlog_codegen::Skeleton;
use proc_macro2::TokenStream;
use quote::quote;

use crate::io::input::Input;

/// Returns the batch `main`: startup, a single dataflow run, and the outputs
/// after the workers join. `emit_output` may reference only state declared
/// outside the workers.
pub(super) fn gen_batch_main(
    skeleton: &Skeleton,
    input: &Input,
    startup: &TokenStream,
    emit_output: &TokenStream,
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
        load_files,
        ..
    } = input;

    quote! {
        fn main() {
            #startup

            #emitters

            let timer = Instant::now();
            timely::execute(timely_config, {
                #emitter_captures

                move |worker| {
                    let index = worker.index();

                    #worker_init

                    #dataflow

                    if index == 0 {
                        println!("{:?}:\tDataflow assembled", timer.elapsed());
                    }

                    // Closing the inputs, all static in a batch engine, is
                    // what lets the dataflow drain to fixpoint.
                    #initialize_inputs
                    #(#load_files)*
                    inputs.apply_inline_all();
                    inputs.close_static();

                    #step_loop

                    #publish

                    #metrics_write
                }
            })
            .unwrap();

            println!("{:?}:\tDataflow executed", timer.elapsed());
            #emit_output
        }
    }
}
