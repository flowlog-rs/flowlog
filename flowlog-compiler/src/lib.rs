//! FlowLog compiler — generate a standalone Rust executable from a FlowLog
//! program.
//!
//! The caller is responsible for parsing + type-checking the program and
//! building a [`ProgramPlanner`]. This crate consumes the planner and
//! drives code generation, scaffolding, and the final `cargo build
//! --release` invocation.
//!
//! Typical use:
//!
//! ```ignore
//! let mut compiler = Compiler::new(config, program);
//! compiler.compile(&program_planner, &mut profiler)?;
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
mod cli;
mod error;
mod imports;
mod io;
mod options;
mod relation;
mod scaffold;

pub use cli::Cli;
pub use error::CompilerError;
use flowlog_build::CodeGen;
use flowlog_build::planner::ProgramPlanner;
use flowlog_build::profiler::Profiler;
use flowlog_common::BoxError;
use flowlog_common::Config;
use flowlog_parser::Program;
pub use options::CompileOptions;

/// Drives code generation + build for a single FlowLog program.
pub struct Compiler {
    config: Config,
    options: CompileOptions,
    program: Program,
    codegen: CodeGen,
}

impl Compiler {
    /// Create a compiler bound to `config` + `options` + `program`. The
    /// [`CodeGen`] is constructed eagerly; call [`Self::compile`] to actually
    /// produce code.
    pub fn new(config: Config, options: CompileOptions, program: Program) -> Self {
        Self {
            codegen: CodeGen::new(config.clone(), program.clone()),
            program,
            config,
            options,
        }
    }

    /// Emit the scaffolded Rust crate, run `cargo build --release`, copy the
    /// binary to [`CompileOptions::executable_path`], and clean up build artifacts
    /// unless [`CompileOptions::save_temps`] is set.
    ///
    /// Returns a [`BoxError`] on failure — user-facing codegen diagnostics
    /// propagate directly, and infrastructure failures (cargo shell-out,
    /// filesystem writes) surface as internal-compiler-error ICEs.
    pub fn compile(
        &mut self,
        program_planner: &ProgramPlanner,
        profiler: &mut Option<Profiler>,
    ) -> Result<(), BoxError> {
        self.emit_sources(program_planner, profiler)?;
        self.build().map_err(CompilerError::from)?;
        Ok(())
    }
}
