//! Batch mode (`DatalogBatch` / `ExtendBatch`) main function generation.
//!
//! Generates a `fn main()` that runs the dataflow once, to fixpoint, and
//! then writes outputs. Each worker:
//!
//! 1. builds the timely dataflow graph from generator fragments,
//! 2. ingests its share of the input (files and inline facts),
//! 3. closes its input handles and steps to fixpoint,
//! 4. flushes its output buffers into the shared ones, and exits.
//!
//! The main thread then drains the shared buffers (sort / limit / write).

use flowlog_build::CodeParts;
use proc_macro2::TokenStream;
use quote::quote;

use crate::io::input::Input;

/// Emit the complete batch-mode `fn main() { ... }` token stream.
pub(crate) fn gen_batch_main(
    parts: &CodeParts,
    input: &Input,
    merge_section: &TokenStream,
) -> TokenStream {
    let CodeParts {
        edb_decls,
        handle_binding,
        dataflow_return,
        flows,
        output_bufs,
        output_buf_clones,
        local_bufs,
        inspectors,
        flush,
        size_cell_decls,
        size_cell_clones,
        profile_init,
        metrics_write,
        step_loop,
        ..
    } = parts;
    let Input {
        file_ingests,
        maybe_peers,
        ..
    } = input;

    quote! {
        fn main() {
            let args: Vec<String> = std::env::args().collect();
            let timer = Instant::now();

            #(#output_bufs)*
            #(#size_cell_decls)*

            timely::execute_from_args(args.into_iter(), {
                #(#output_buf_clones)*
                #(#size_cell_clones)*

                move |worker| {
                    let index = worker.index();
                    #maybe_peers

                    #profile_init
                    #(#local_bufs)*

                    let #handle_binding =
                        worker.dataflow::<Ts, _, _>(|scope| {
                            #(#edb_decls)*
                            #(#flows)*
                            #(#inspectors)*
                            #dataflow_return
                        });

                    if index == 0 {
                        println!("{:?}:\tDataflow assembled", timer.elapsed());
                    }

                    #(#file_ingests)*
                    inputs.apply_inline_all(index);
                    inputs.close_all();

                    if index == 0 {
                        println!("{:?}:\tInputs ingested", timer.elapsed());
                    }

                    #step_loop

                    #(#flush)*

                    #metrics_write
                }
            })
            .unwrap();

            println!("{:?}:\tDataflow executed", timer.elapsed());
            #merge_section
            println!("{:?}:\tOutputs written", timer.elapsed());
        }
    }
}
