//! Shared startup and source assembly. The `batch` and `inc` modules emit
//! the main function for the batch and incremental engines.

mod batch;
mod inc;

use flowlog_build::Skeleton;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Compiler;

impl Compiler {
    /// Returns `main.rs`, with compiled directory defaults that runtime
    /// arguments can override before workers start.
    pub(crate) fn assemble(&self, skeleton: &Skeleton, imports: &TokenStream) -> String {
        let (initialize_output, emit_output) = self.gen_output();
        let input = self.gen_input(skeleton, &emit_output);
        let fact_dir = self.options.fact_dir().unwrap_or(".");
        let output_dir = if self.config.output_to_stdout() {
            "-"
        } else {
            self.options.output_dir().unwrap_or(".")
        };
        let create_output_dir = if self.program.idbs().iter().any(|idb| idb.has_output()) {
            quote! {
                if let Some(dir) = &output_dir {
                    if let Err(error) = std::fs::create_dir_all(dir) {
                        eprintln!(
                            "failed to create output directory '{}': {}",
                            dir.display(), error,
                        );
                        std::process::exit(1);
                    }
                }
            }
        } else {
            quote! {}
        };
        let startup = quote! {
            let ::flowlog_runtime::RuntimeArgs {
                config: timely_config, fact_dir, output_dir,
            } = ::flowlog_runtime::RuntimeArgs::from_env(#fact_dir, #output_dir);
            #create_output_dir
            #initialize_output
        };
        let main_fn = if self.program.is_incremental() {
            inc::gen_incremental_main(skeleton, &input, &startup, &emit_output)
        } else {
            batch::gen_batch_main(skeleton, &input, &startup, &emit_output)
        };

        let declarations = &skeleton.declarations;

        let file_ts = quote! {
            #imports
            #declarations
            #main_fn
        };

        flowlog_common::pretty_print(file_ts)
    }
}
