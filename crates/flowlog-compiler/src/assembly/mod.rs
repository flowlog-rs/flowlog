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
    /// Assemble a complete `main.rs` source file from [`AssemblyParts`].
    ///
    /// 1. Delegates to [`batch::gen_batch_main`] or [`inc::gen_incremental_main`].
    /// 2. Prepends imports, type declarations, and helper functions.
    /// 3. Pretty-prints via [`common::pretty_print`].
    pub(crate) fn assemble_main(&self, parts: &AssemblyParts) -> String {
        let main_fn = match self.config.mode() {
            ExecutionMode::DatalogBatch | ExecutionMode::ExtendBatch => {
                batch::gen_batch_main(parts)
            }
            ExecutionMode::DatalogInc | ExecutionMode::ExtendInc => {
                inc::gen_incremental_main(parts)
            }
        };

        let AssemblyParts {
            imports,
            type_declarations,
            profile_structs,
            worker_helpers,
            ..
        } = parts;

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
