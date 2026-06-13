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

use quote::quote;

use flowlog_build::CodeParts;
use flowlog_build::common::ExecutionMode;

use crate::{Compiler, CompilerError};

/// Emit a Linux-only startup guard that warns when `vm.max_map_count` sits at
/// the conservative kernel default (65530/65536).
///
/// The generated binary's global allocator is mimalloc, which backs large
/// analyses with a great many memory mappings. Once a process's VMA count
/// reaches `vm.max_map_count`, the next `mmap` returns `ENOMEM`, the allocator
/// hands back null, and Rust aborts with `SIGABRT` and a bare
/// `memory allocation of <N> bytes failed` — even when the machine has
/// hundreds of GB of RAM free, which makes it read like a spurious OOM with no
/// output. Raising the cap is a system-config step (documented in the FlowLog
/// guide, linked below), not something the binary should do itself — so this
/// guard only *diagnoses* the condition, pointing the operator at the fix.
pub(crate) fn gen_max_map_count_guard() -> proc_macro2::TokenStream {
    quote! {
        #[cfg(target_os = "linux")]
        {
            if let Ok(raw) = std::fs::read_to_string("/proc/sys/vm/max_map_count") {
                if let Ok(limit) = raw.trim().parse::<u64>() {
                    if limit <= 65_536 {
                        eprintln!(
                            "flowlog: warning: vm.max_map_count = {} is at the kernel default; \
                             large analyses can exhaust per-process memory mappings and abort with \
                             \"memory allocation of <N> bytes failed\" even with ample free RAM. \
                             See https://www.flowlog-rs.com/tutorial/getting-started/system-config",
                            limit
                        );
                    }
                }
            }
        }
    }
}

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

        Ok(flowlog_build::common::pretty_print(file_ts))
    }
}
