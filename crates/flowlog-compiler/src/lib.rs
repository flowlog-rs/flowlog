//! FlowLog Compiler
//!
//! Generates an executable Rust crate from a FlowLog program by
//! delegating code generation to the `generator` crate and then
//! scaffolding, building, and collecting the final binary.

// =========================================================================
// Module Declarations
// =========================================================================
mod assembly;
mod build;
mod fs_utils;
mod scaffold;

pub use build::build_and_collect;

// =========================================================================
// Imports
// =========================================================================
use generator::Generator;
use planner::StratumPlanner;
use profiler::Profiler;

use common::Config;
use parser::Program;

// =========================================================================
// Compiler
// =========================================================================

pub struct Compiler {
    config: Config,
    generator: Generator,
}

impl Compiler {
    /// Create a new `Compiler` instance from `Config` and `Program`.
    pub fn new(config: Config, program: Program) -> Self {
        Self {
            generator: Generator::new(config.clone(), program),
            config,
        }
    }

    /// Generate the executable: codegen → assemble → scaffold → build-ready.
    ///
    /// After this returns, call [`build_and_collect`] to compile the
    /// generated crate into a binary.
    pub fn generate_executable_at(
        &mut self,
        strata: &[StratumPlanner],
        profiler: &mut Option<Profiler>,
    ) -> std::io::Result<()> {
        let parts = self.generator.generate(strata, profiler);
        let main_rs = self.assemble_main(&parts);
        self.write_project(&parts, &main_rs, profiler)
    }
}
