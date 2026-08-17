//! Incremental mode (`DatalogInc` / `ExtendInc`) main function generation.
//!
//! Generates a `fn main()` that builds the timely dataflow graph, runs the
//! preload epoch, and hands the transaction loop to the shell: worker 0
//! `drive`s at the prompt, every other worker `follow`s, and one generated
//! callback answers each `Event` with what only this program knows: op
//! dispatch, the advance sequence, the output merge, the shutdown drain.

use flowlog_build::CodeParts;
use proc_macro2::TokenStream;
use quote::quote;

use crate::io::input::Input;

/// Emit the complete incremental-mode `fn main() { ... }` token stream.
pub(crate) fn gen_incremental_main(
    p: &CodeParts,
    rp: &Input,
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
    } = p;
    let Input {
        preload_ingest,
        preload_epoch,
        put_dispatch,
        file_dispatch,
        rel_names,
        start_time,
        ..
    } = rp;

    quote! {
        fn main() {
            let args: Vec<String> = std::env::args().collect();
            let shared = SharedTxn::from_args(&args);

            #(#output_bufs)*
            #(#size_cell_decls)*

            timely::execute_from_args(args.into_iter(), {
                let shared = shared.clone();
                #(#output_buf_clones)*
                #(#size_cell_clones)*

                move |worker| {
                    let timer = Instant::now();
                    let peers = worker.peers();
                    let index = worker.index();

                    #profile_init

                    #(#local_bufs)*

                    let #handle_binding =
                        worker.dataflow::<Ts, _, _>(|scope| {
                            #(#edb_decls)*
                            #(#flows)*

                            let mut probe = ProbeHandle::new();

                            #(#inspectors)*

                            #dataflow_return
                        });

                    #rel_names

                    let mut time_stamp: u32 = 0;

                    #preload_ingest

                    let on = |event: Event| match event {
                        Event::Apply(ops) => {
                            for op in &ops {
                                match op {
                                    TxnOp::Put { rel, tuple, diff } => {
                                        #put_dispatch
                                    }
                                    TxnOp::File { rel, path, diff } => {
                                        #file_dispatch
                                    }
                                }
                            }
                        }
                        Event::Advance(t) => {
                            time_stamp = t;
                            inputs.advance_to_all(time_stamp);
                            inputs.flush_all();
                            #step_loop

                            #metrics_write

                            #(#flush)*
                        }
                        Event::Merge(t) => {
                            // The merge fragment names `time_stamp` in its
                            // epoch-stamped file paths.
                            let time_stamp = t;
                            #merge_section
                        }
                        Event::Close(t) => {
                            inputs.close_all();
                            while probe.less_than(&t) {
                                worker.step();
                            }
                        }
                    };

                    #preload_epoch

                    if index == 0 {
                        println!("{:?}:\tDataflow assembled", timer.elapsed());
                    }

                    if index == 0 {
                        let rel_words =
                            REL_NAMES.iter().map(|s| (*s).to_string()).collect::<Vec<_>>();
                        let mut prompt = Prompt::new(rel_words);
                        drive(&shared, #start_time, |t| prompt.next_cmd(t), on);
                    } else {
                        follow(&shared, #start_time, on);
                    }
                }
            })
            .unwrap();
        }
    }
}
