//! Batch assembly. Workers fill shared buffers; dropping their guards joins
//! them before the main thread drains output and reports sizes.

use flowlog_build::CodeParts;
use proc_macro2::TokenStream;
use quote::quote;

use crate::io::input::Input;

/// Emits startup, a single dataflow run, and output after workers join.
/// `merge_section` may reference only state declared outside the workers.
pub(super) fn gen_batch_main(
    parts: &CodeParts,
    input: &Input,
    startup: &TokenStream,
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
        initialize,
        file_ingests,
        ..
    } = input;

    quote! {
        fn main() {
            #startup

            #(#output_bufs)*
            #(#size_cell_decls)*

            let timer = Instant::now();
            timely::execute(timely_config, {
                #(#output_buf_clones)*
                #(#size_cell_clones)*

                move |worker| {
                    let index = worker.index();

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

                    // Closing the inputs is what lets the dataflow drain to
                    // fixpoint.
                    #initialize
                    #(#file_ingests)*
                    inputs.apply_inline_all();
                    inputs.close_all();

                    #step_loop

                    #(#flush)*

                    #metrics_write
                }
            })
            .unwrap();

            println!("{:?}:\tDataflow executed", timer.elapsed());
            #merge_section
        }
    }
}
