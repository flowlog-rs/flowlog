//! FlowLog Compiler Library
//!
//! Generates executable Rust code from planned strata.

// =========================================================================
// Module Declarations
// =========================================================================
mod aggregation;
mod arg;
mod assembly;
mod build;
mod dedup;
mod features;
mod flow;
mod fs_utils;
mod ident;
mod import;
mod io;
mod profile;
mod scaffold;
mod ty;

pub use build::build_and_collect;

// =========================================================================
// Imports
// =========================================================================
use proc_macro2::Ident;
use std::collections::HashMap;

use common::Config;
use parser::{DataType, Program};
use planner::StratumPlanner;
use profiler::{with_profiler, Profiler};

use features::Features;

// =========================================================================
// Compiler
// =========================================================================

pub struct Compiler {
    /// Configuration provided to the compiler.
    config: Config,
    /// The parsed FlowLog program.
    program: Program,

    /// Global map from relation fingerprint to its identifier.
    global_fp_to_ident: HashMap<u64, Ident>,
    /// Global map from relation fingerprint to its key-value data type.
    global_fp_to_type: HashMap<u64, (Vec<DataType>, Vec<DataType>)>,

    /// Codegen feature flags for the current compilation unit.
    features: Features,
}

impl Compiler {
    /// Create a new `Compiler` instance from `Config` and `Program`.
    pub fn new(config: Config, program: Program) -> Self {
        let mut compiler = Self {
            config,
            program,
            global_fp_to_ident: HashMap::new(),
            global_fp_to_type: HashMap::new(),
            features: Features::default(),
        };

        compiler.make_global_ident_map();
        compiler.make_global_data_type_map();
        compiler
    }

    /// Create executable from the strata plan.
    pub fn generate_executable_at(
        &mut self,
        strata: &[StratumPlanner],
        profiler: &mut Option<Profiler>,
    ) -> std::io::Result<()> {
        // Record entering main dataflow scope in profiler if enabled
        with_profiler(profiler, |profiler| {
            profiler.enter_scope();
        });

        let main_rs = self.generate_main(strata, profiler);
        self.write_project(&main_rs, profiler)
    }
}
