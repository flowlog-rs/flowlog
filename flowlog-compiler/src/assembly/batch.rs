//! Batch-mode `fn main()` generator.
//!
//! One dataflow, run once to fixpoint, then written out. Incremental mode
//! (`inc.rs`) keeps its workers alive across epochs and merges inside them;
//! batch has no epochs, so it lets them finish and die first.
//!
//! The generated `main` falls into three phases:
//!
//! **Before the workers spawn.** The shared output buffers and `.printsize`
//! size cells are declared, so they outlive the workers that fill them. Only
//! clones cross into the closure.
//!
//! **Inside each worker.** Everything from graph construction through the
//! flush, profiling metrics included: those stay worker-local, one table
//! pair each.
//!
//! **Once they have joined.** `timely::execute_from_args` returns only after
//! every worker has exited, which is why no barrier appears in this file: the
//! join already establishes that every flush has landed. The main thread
//! drains the shared buffers (sort, limit, write) and prints the `.printsize`
//! counts, by which point every arrangement has been dropped, so the output
//! is formatted against a freed dataflow rather than beside a live one.

use flowlog_build::CodeParts;
use proc_macro2::TokenStream;
use quote::quote;

use crate::io::input::Input;

/// Emit the complete batch-mode `fn main() { ... }` token stream.
///
/// `merge_section` is spliced in after the workers join, so it may not
/// reference anything worker-local: by then the only state left is what
/// was declared outside `timely::execute_from_args`.
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
        registry_inserts,
        file_ingests,
        maybe_peers,
        ..
    } = input;

    quote! {
        fn main() {
            let args: Vec<String> = std::env::args().collect();
            let input_failed = std::sync::Arc::new(
                std::sync::atomic::AtomicBool::new(false),
            );

            #(#output_bufs)*
            #(#size_cell_decls)*

            let timer = Instant::now();
            let guards = match timely::execute_from_args(args.into_iter(), {
                let input_failed = input_failed.clone();
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

                    // Closing the inputs is what lets the dataflow drain to
                    // fixpoint.
                    let mut rels: HashMap<String, Box<dyn Relation>> = HashMap::new();
                    #(#registry_inserts)*
                    #(#file_ingests)*
                    if !input_failed.load(std::sync::atomic::Ordering::Acquire) {
                        for r in rels.values_mut() {
                            r.apply_inline(index);
                        }
                    }
                    if input_failed.load(std::sync::atomic::Ordering::Acquire) {
                        for r in rels.values_mut() {
                            r.close();
                        }
                        return;
                    }
                    for r in rels.values_mut() {
                        r.close();
                    }

                    #step_loop

                    #(#flush)*

                    #metrics_write
                }
            }) {
                Ok(guards) => guards,
                Err(error) => {
                    eprintln!("[flowlog-runtime] cannot start workers: {error}");
                    std::process::exit(2);
                }
            };
            for result in guards.join() {
                if let Err(error) = result {
                    eprintln!("[flowlog-runtime] worker failed: {error}");
                    std::process::exit(2);
                }
            }
            if input_failed.load(std::sync::atomic::Ordering::Acquire) {
                std::process::exit(2);
            }
            println!("{:?}:\tDataflow executed", timer.elapsed());
            #merge_section
        }
    }
}
