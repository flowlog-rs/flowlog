//! Final assembly of the generated Rust program.
//!
//! Takes [`AssemblyParts`] produced by the `generator` crate and
//! assembles a complete `main.rs` source file by delegating to a
//! mode-specific generator:
//!
//! - [`batch`] — single-pass run-to-completion execution.
//! - [`inc`] — interactive incremental shell with epoch-based updates.

pub(crate) mod batch;
pub(crate) mod inc;

use quote::quote;

use common::ExecutionMode;
use generator::AssemblyParts;

use crate::Compiler;

impl Compiler {
    pub(crate) fn assemble_main(
        &self,
        parts: &AssemblyParts,
        imports: &proc_macro2::TokenStream,
        worker_helpers: &proc_macro2::TokenStream,
    ) -> String {
        let merge_blocks = self.gen_merge_blocks();
        let input = self.gen_input(parts, &merge_blocks);

        let main_fn = match self.config.mode() {
            ExecutionMode::DatalogBatch | ExecutionMode::ExtendBatch => {
                batch::gen_batch_main(parts, &input, &merge_blocks)
            }
            ExecutionMode::DatalogInc | ExecutionMode::ExtendInc => {
                inc::gen_incremental_main(parts, &input, &merge_blocks)
            }
        };

        let type_declarations = &parts.type_declarations;
        let profile_structs = &parts.profile_structs;

        let file_ts = quote! {
            #imports
            #type_declarations
            #profile_structs
            #worker_helpers
            #main_fn
        };

        common::pretty_print(file_ts)
    }
}
