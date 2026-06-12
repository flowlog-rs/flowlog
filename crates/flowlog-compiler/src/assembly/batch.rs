//! Batch-mode `fn main()` generator.
//!
//! Runs the dataflow once, to fixpoint, then writes outputs:
//!
//! 1. Construct the timely dataflow graph from generator fragments.
//! 2. Build the EDB registry and ingest all data (files + inline facts).
//! 3. Close input handles and step the worker until idle.
//! 4. Flush worker-local output buffers into the shared buffers.
//! 5. Worker 0 drains the shared buffers (sort / limit / write).

use proc_macro2::TokenStream;
use quote::quote;

use flowlog_build::CodeParts;

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
        time_profile_write_batch: time_profile_write,
        memory_profile_write_batch: memory_profile_write,
        ..
    } = parts;
    let Input {
        registry_inserts,
        file_ingests,
        maybe_peers,
        ..
    } = input;

    quote! {
        fn main() {
            let args: Vec<String> = std::env::args().collect();
            let barrier = worker_barrier_from_args(&args);

            // Shared output machinery constructed before workers spawn.
            #(#output_bufs)*
            #(#size_cell_decls)*

            timely::execute_from_args(args.into_iter(), {
                let barrier = barrier.clone();
                #(#output_buf_clones)*
                #(#size_cell_clones)*

                move |worker| {
                    let timer = Instant::now();
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

                    // Register input handlers, ingest data, then close inputs
                    // so the dataflow can drain to fixpoint.
                    let mut rels: HashMap<String, Box<dyn Relation>> = HashMap::new();
                    #(#registry_inserts)*
                    #(#file_ingests)*
                    for r in rels.values_mut() {
                        r.apply_inline(index);
                    }
                    for r in rels.values_mut() {
                        r.close();
                    }

                    // Inputs are closed, so the only remaining work is draining
                    // the dataflow to fixpoint. Park (rather than busy-spin on
                    // `worker.step()`) whenever a worker has no pending
                    // activations: all further progress arrives as buzzing
                    // channel messages, so parked workers are always re-awoken.
                    // This is timely's own batch-drain idiom (`execute.rs`) and
                    // cuts the per-round progress-barrier spin that otherwise
                    // dominates CPU on large recursive programs.
                    while worker.step_or_park(None) {}

                    // Flush per-worker output buffers into the shared ones,
                    // then worker 0 merges and writes results.
                    #(#flush)*
                    barrier.wait();

                    #time_profile_write
                    #memory_profile_write

                    if index == 0 {
                        println!("{:?}:\tDataflow executed", timer.elapsed());
                        #merge_section
                    }
                }
            })
            .unwrap();
        }
    }
}
