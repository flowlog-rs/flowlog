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
mod imports;
mod io;
mod relation;
mod scaffold;

use common::Config;
use generator::Generator;
use parser::Program;
use planner::StratumPlanner;
use profiler::Profiler;

/// Drives code generation + build for a single FlowLog program.
pub struct Compiler {
    config: Config,
    program: Program,
    generator: Generator,
}

impl Compiler {
    /// Create a compiler bound to `config` + `program`. The [`Generator`] is
    /// constructed eagerly; call [`Self::compile`] to actually produce code.
    pub fn new(config: Config, program: Program) -> Self {
        Self {
            generator: Generator::new(config.clone(), program.clone()),
            program,
            config,
        }
    }

    /// Emit the scaffolded Rust crate, run `cargo build --release`, copy the
    /// binary to [`Config::executable_path`], and clean up build artifacts
    /// unless [`Config::save_temps`] is set.
    pub fn compile(
        &mut self,
        strata: &[StratumPlanner],
        profiler: &mut Option<Profiler>,
    ) -> std::io::Result<()> {
        self.emit_sources(strata, profiler)?;
        self.build()
    }
}
