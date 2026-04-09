//! Project scaffolding for the FlowLog compiler.
//!
//! This module materializes a standalone Rust crate produced by the compiler.
//! It is responsible for writing:
//! - `Cargo.toml`
//! - `.cargo/config.toml` (rustflags, etc.)
//! - `src/main.rs` (compiler-generated)
//! - `src/cmd.rs` (optional: interactive txn command parser for incremental mode)

use std::io;

use generator::AssemblyParts;
use profiler::{with_profiler_ref, Profiler};

use crate::fs_utils::{ensure_dir, write_file};
use crate::Compiler;

/// Embedded template for the incremental interactive command parser.
///
/// Notes:
/// - `include_str!` is evaluated at *compile time* of the compiler crate.
/// - The released binary does NOT need the template file at runtime.
const CMD_RS_TMPL: &str =
    include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/templates/cmd_rs.tpl"));
const PROMPT_RS_TMPL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/templates/prompt_rs.tpl"
));

impl Compiler {
    /// Create the project layout and write all files from [`AssemblyParts`].
    pub(crate) fn write_project(
        &self,
        parts: &AssemblyParts,
        main_rs: &str,
        profiler: &Option<Profiler>,
    ) -> io::Result<()> {
        let config = &self.config;
        let root = config.build_dir();
        let src_dir = root.join("src");
        ensure_dir(&src_dir)?;

        // Project metadata + dependencies
        write_file(&root.join("Cargo.toml"), parts.cargo_toml.trim_start())?;

        // Build flags (e.g., -Dwarnings)
        let cargo_dir = root.join(".cargo");
        ensure_dir(&cargo_dir)?;
        write_file(
            &cargo_dir.join("config.toml"),
            parts.cargo_config.trim_start(),
        )?;

        // Compiler-generated entrypoint
        write_file(&src_dir.join("main.rs"), main_rs)?;

        // Relation handlers (used by both batch and incremental for data ingestion)
        write_file(&src_dir.join("relops.rs"), parts.relops_rs.trim_start())?;

        // Incremental mode: interactive command parser + prompt
        if config.is_incremental() {
            write_file(&src_dir.join("cmd.rs"), CMD_RS_TMPL.trim_start())?;
            write_file(&src_dir.join("prompt.rs"), PROMPT_RS_TMPL.trim_start())?;
        }

        // Semiring modules for aggregations
        if !parts.semiring_modules.is_empty() {
            let semiring_dir = src_dir.join("semiring");
            ensure_dir(&semiring_dir)?;
            for (rel_path, content) in &parts.semiring_modules {
                write_file(&src_dir.join(rel_path), content)?;
            }
        }

        // UDF module if a --udf-file was provided
        if let Some(udf_path) = config.udf_file() {
            let content = std::fs::read_to_string(udf_path).map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("Failed to read UDF file '{udf_path}': {e}"),
                )
            })?;
            write_file(&src_dir.join("udf.rs"), content.trim_start())?;
        }

        // Profiler logs — write into a `log/` directory alongside the final
        // executable so they survive build-directory cleanup when --save-temps
        // is not set.
        with_profiler_ref(profiler, |profiler| {
            let exe = config.executable_path();
            let log_dir = exe.with_file_name("log");
            ensure_dir(&log_dir)?;
            let ops_path = log_dir.join("ops.json");
            profiler.write_json(&ops_path)
        })?;

        Ok(())
    }
}
