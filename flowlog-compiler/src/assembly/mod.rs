//! Final assembly of the generated Rust program.
//!
//! Takes the [`CodeParts`] bundle produced by [`flowlog_build::CodeGen`]
//! and assembles a complete `main.rs` source by delegating to a
//! mode-specific assembler:
//!
//! - [`batch`] — single-pass run-to-completion execution.
//! - [`inc`] — interactive incremental shell with epoch-based updates.

pub(crate) mod batch;
pub(crate) mod inc;

use flowlog_build::CodeParts;
use flowlog_common::ExecutionMode;
use quote::quote;

use crate::Compiler;
use crate::CompilerError;

impl Compiler {
    pub(crate) fn assemble_main(
        &self,
        parts: &CodeParts,
        imports: &proc_macro2::TokenStream,
        worker_helpers: &proc_macro2::TokenStream,
    ) -> Result<String, CompilerError> {
        let merge_section = self.gen_merge_section()?;
        let input = self.gen_input(parts, &merge_section);

        let main_fn = match self.config.mode() {
            ExecutionMode::DatalogBatch | ExecutionMode::ExtendBatch => {
                batch::gen_batch_main(parts, &input, &merge_section)
            }
            ExecutionMode::DatalogInc | ExecutionMode::ExtendInc => {
                inc::gen_incremental_main(parts, &input, &merge_section)
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
            #worker_helpers
            #main_fn
        };

        Ok(flowlog_common::pretty_print(file_ts))
    }
}
