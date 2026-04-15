//! FlowLog code generator.
//!
//! Turns a parsed program + stratified execution plan into an
//! [`AssemblyParts`] bundle of token-stream fragments. Downstream crates
//! assemble them into their own Rust source (binary via `flowlog-compiler`,
//! library via `flowlog-build`).

// =========================================================================
// Module Declarations
// =========================================================================
mod aggregation;
mod arg;
mod assembly;
mod dedup;
pub mod features;
mod flow;
mod ident;
mod io;
mod profile;
mod semiring;
mod ty;

pub use assembly::AssemblyParts;
pub use io::idb_buffers::{field_accessor, gen_drain_block};
pub use ty::data::data_type_tokens;

// =========================================================================
// Imports
// =========================================================================
use proc_macro2::Ident;
use std::collections::HashMap;

use common::Config;
use parser::{DataType, Program};
use planner::StratumPlanner;
use profiler::Profiler;

use features::Features;

// =========================================================================
// Generator
// =========================================================================

pub struct Generator {
    /// Configuration provided to the generator.
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

impl Generator {
    /// Create a new `Generator` instance from `Config` and `Program`.
    pub fn new(config: Config, program: Program) -> Self {
        let mut gen = Self {
            config,
            program,
            global_fp_to_ident: HashMap::new(),
            global_fp_to_type: HashMap::new(),
            features: Features::default(),
        };

        gen.make_global_ident_map();
        gen.make_global_data_type_map();
        gen
    }

    /// Feature flags populated during [`Self::generate`].
    pub fn features(&self) -> &features::Features {
        &self.features
    }

    /// Run all code-generation passes and return the complete
    /// [`AssemblyParts`] containing every fragment the downstream
    /// compiler or build tool needs.
    pub fn generate(
        &mut self,
        strata: &[StratumPlanner],
        profiler: &mut Option<Profiler>,
    ) -> AssemblyParts {
        self.features.reset();
        self.collect_parts(strata, profiler)
    }
}
