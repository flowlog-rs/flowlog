//! FlowLog compiler — generate a standalone Rust executable from a FlowLog
//! program.
//!
//! The caller is responsible for running the upstream pipeline
//! (parse → stratify → plan). This crate accepts the resulting
//! [`StratumPlanner`] list and drives code generation, scaffolding, and
//! the final `cargo build --release` invocation.
//!
//! Typical use:
//!
//! ```ignore
//! let mut compiler = Compiler::new(config, program);
//! compiler.compile(&strata, &mut profiler)?;
//! ```
//!
//! Internal layout:
//!
//! - [`build`] — the two-stage pipeline behind [`Compiler::compile`]
//!   (`emit_sources`, then shell out to cargo).
//! - [`scaffold`] — write the emitted crate to disk + render Cargo metadata.
//! - [`assembly`], [`io`], [`relation`], [`imports`] — codegen modules that
//!   produce the token streams spliced into the emitted `main.rs` and
//!   `relation.rs`.

mod assembly;
mod build;
mod error;
mod imports;
mod io;
mod relation;
mod scaffold;

pub use error::CompilerError;

use flowlog_build::common::BoxError;
use flowlog_build::common::Config;
use flowlog_build::CodeGen;
use flowlog_build::parser::Program;
use flowlog_build::planner::StratumPlanner;
use flowlog_build::profiler::Profiler;

/// Drives code generation + build for a single FlowLog program.
pub struct Compiler {
    config: Config,
    program: Program,
    codegen: CodeGen,
}

impl Compiler {
    /// Create a compiler bound to `config` + `program`. The [`CodeGen`] is
    /// constructed eagerly; call [`Self::compile`] to actually produce code.
    pub fn new(config: Config, program: Program) -> Self {
        Self {
            codegen: CodeGen::new(config.clone(), program.clone()),
            program,
            config,
        }
    }

    /// Emit the scaffolded Rust crate, run `cargo build --release`, copy the
    /// binary to [`Config::executable_path`], and clean up build artifacts
    /// unless [`Config::save_temps`] is set.
    ///
    /// Returns a [`BoxError`] on failure — user-facing codegen diagnostics
    /// propagate directly, and infrastructure failures (cargo shell-out,
    /// filesystem writes) surface as internal-compiler-error ICEs.
    pub fn compile(
        &mut self,
        strata: &[StratumPlanner],
        profiler: &mut Option<Profiler>,
    ) -> Result<(), BoxError> {
        self.emit_sources(strata, profiler)?;
        self.build().map_err(CompilerError::from)?;
        Ok(())
    }
}
