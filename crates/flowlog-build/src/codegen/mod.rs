//! The shared codegen core. Turns a parsed FlowLog program plus a
//! stratified execution plan into a [`CodeParts`] bundle that each frontend
//! (library mode here, binary mode in `flowlog-compiler`) assembles into
//! its own final Rust source.

pub(crate) mod aggregation;
pub(crate) mod arg;
pub(crate) mod code_parts;
pub(crate) mod dedup;
pub(crate) mod edb_handles;
pub(crate) mod features;
pub(crate) mod flow;
pub(crate) mod ident;
pub(crate) mod idb_buffers;
pub(crate) mod profile;
pub(crate) mod semiring;
pub(crate) mod ty;

use std::collections::HashMap;

use proc_macro2::Ident;

use common::Config;
use parser::{DataType, Program};
use planner::StratumPlanner;
use profiler::Profiler;

use self::code_parts::CodeParts;
use self::features::Features;

pub struct CodeGen {
    pub(crate) config: Config,
    pub(crate) program: Program,

    /// Fingerprint → identifier map, stable across strata — local recursion
    /// strata may introduce new identifiers that refer back to these.
    pub(crate) global_fp_to_ident: HashMap<u64, Ident>,
    /// Fingerprint → `(key_types, value_types)`. Seeded in `new` from the
    /// parsed program; extended in `generate` with inferred output types.
    pub(crate) global_fp_to_type: HashMap<u64, (Vec<DataType>, Vec<DataType>)>,

    /// Populated during `generate`; drives the frontend's import and derive
    /// emission.
    pub(crate) features: Features,
}

impl CodeGen {
    pub fn new(config: Config, program: Program) -> Self {
        let mut cg = Self {
            config,
            program,
            global_fp_to_ident: HashMap::new(),
            global_fp_to_type: HashMap::new(),
            features: Features::default(),
        };
        cg.make_global_ident_map();
        cg.make_global_data_type_map();
        cg
    }

    pub fn features(&self) -> &Features {
        &self.features
    }

    /// Run every code-generation pass and return the resulting [`CodeParts`].
    pub fn generate(
        &mut self,
        strata: &[StratumPlanner],
        profiler: &mut Option<Profiler>,
    ) -> CodeParts {
        self.features.reset();
        self.collect_parts(strata, profiler)
    }
}
