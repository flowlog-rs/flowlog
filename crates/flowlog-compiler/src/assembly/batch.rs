//! Batch mode (`DatalogBatch` / `ExtendBatch`) main function generation.
//!
//! Generates a `fn main()` that:
//! 1. Constructs the timely dataflow graph.
//! 2. Builds the relation registry and ingests all data (files + inline facts).
//! 3. Closes input handles and runs the dataflow to fixpoint.
//! 4. Flushes output buffers and writes results.

use proc_macro2::TokenStream;
use quote::quote;

use generator::AssemblyParts;

/// Emit the complete batch-mode `fn main() { ... }` token stream.
pub(crate) fn gen_batch_main(p: &AssemblyParts) -> TokenStream {
    let AssemblyParts {
        edb_inputs,
        handle_binding,
        dataflow_return,
        flows,
        output_bufs,
        output_buf_clones,
        local_bufs,
        inspectors,
        flush,
        merge,
        registry_inserts,
        file_ingests,
        maybe_peers,
        profile_init,
        time_profile_write_batch: time_profile_write,
        memory_profile_write_batch: memory_profile_write,
        ..
    } = p;

    quote! {
        fn main() {
            let args: Vec<String> = std::env::args().collect();
            let barrier = worker_barrier_from_args(&args);

            #(#output_bufs)*

            timely::execute_from_args(args.into_iter(), {
                let barrier = barrier.clone();
                #(#output_buf_clones)*

                move |worker| {
                let timer = Instant::now();
                let index = worker.index();
                #maybe_peers

                #profile_init

                #(#local_bufs)*

                let #handle_binding =
                    worker.dataflow::<Ts, _, _>(|scope| {
                        #(#edb_inputs)*
                        #(#flows)*
                        #(#inspectors)*
                        #dataflow_return
                    });

                if index == 0 {
                    println!("{:?}:\tDataflow assembled", timer.elapsed());
                }

                let mut rels: HashMap<String, Box<dyn RelOps>> = HashMap::new();
                #(#registry_inserts)*

                #(#file_ingests)*
                for (_, r) in rels.iter_mut() {
                    r.apply_inline(index);
                }
                for (_, r) in rels.iter_mut() {
                    r.close();
                }

                while worker.step() {}

                #(#flush)*

                barrier.wait();

                #time_profile_write
                #memory_profile_write

                if index == 0 {
                    println!("{:?}:\tDataflow executed", timer.elapsed());
                    #(#merge)*
                }
            }})
            .unwrap();
        }
    }
}
