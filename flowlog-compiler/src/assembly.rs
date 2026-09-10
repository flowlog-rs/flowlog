//! Shared startup and source assembly. The `batch` and `inc` modules emit
//! the main function for each execution mode.

mod batch;
mod inc;

use flowlog_build::CodeParts;
use flowlog_common::ExecutionMode;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Compiler;
use crate::CompilerError;

impl Compiler {
    /// Renders `main.rs` with compiled directory defaults that runtime
    /// arguments can override before workers start.
    pub(crate) fn assemble(
        &self,
        parts: &CodeParts,
        imports: &TokenStream,
    ) -> Result<String, CompilerError> {
        let merge_section = self.gen_merge_section()?;
        let input = self.gen_input(parts, &merge_section);
        let fact_dir = self.options.fact_dir().unwrap_or(".");
        let output_dir = if self.config.output_to_stdout() {
            "-"
        } else {
            self.options.output_dir().unwrap_or(".")
        };
        let prepare_output = if self.program.idbs().iter().any(|idb| idb.has_output()) {
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
            #prepare_output
        };
        let main_fn = match self.config.mode() {
            ExecutionMode::Batch => batch::gen_batch_main(parts, &input, &startup, &merge_section),
            ExecutionMode::Inc => {
                inc::gen_incremental_main(parts, &input, &startup, &merge_section)
            }
        };

        let type_declarations = &parts.type_declarations;
        let profile_structs = &parts.profile_structs;
        let profile_ops = &parts.profile_ops;

        let file_ts = quote! {
            #imports
            #type_declarations
            #profile_structs
            #profile_ops
            #main_fn
        };

        Ok(flowlog_common::pretty_print(file_ts))
    }
}
