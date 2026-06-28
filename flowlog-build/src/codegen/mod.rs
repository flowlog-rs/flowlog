//! The shared codegen core. Turns a parsed FlowLog program plus a
//! stratified execution plan into a [`CodeParts`] bundle that each frontend
//! (library mode here, binary mode in `flowlog-compiler`) assembles into
//! its own final Rust source.

mod aggregation;
mod arg;
mod code_parts;
mod dedup;
mod edb_handles;
mod error;
mod features;
mod flow;
mod idb_buffers;
mod ident;
mod profile;
mod semiring;
mod ty;

// External API — used by flowlog-compiler via lib.rs re-exports.
// `AggSemiringNeeds` leaks through `Features::agg_semirings()`.
pub use arg::const_to_token;
pub use code_parts::CodeParts;
pub use error::CodegenError;
pub use features::{AggSemiringNeeds, Features};
pub use idb_buffers::{field_accessor, gen_drain_block};
pub use ty::data::data_type_tokens;

// Intra-crate shortcuts used by build/ (library mode).
pub(crate) use ty::data::{tuple_tokens, user_tuple_tokens};

use std::collections::HashMap;

use proc_macro2::Ident;

use crate::parser::{DataType, Program};
use crate::planner::ProgramPlanner;
use crate::profiler::Profiler;
use flowlog_common::Config;

pub struct CodeGen {
    pub(crate) config: Config,
    pub(crate) program: Program,

    /// Fingerprint → binding-ident map, stable across strata — local
    /// recursion strata may introduce new identifiers that refer back to
    /// these. Idents are synthetic; see [`ident`] for the scheme.
    pub(crate) global_fp_to_ident: HashMap<u64, Ident>,
    /// Fingerprint → `(key_types, value_types)`. Seeded in `new` from the
    /// parsed program; extended in `generate` with inferred output types.
    pub(crate) global_fp_to_type: HashMap<u64, (Vec<DataType>, Vec<DataType>)>,

    /// Populated during `generate`; drives the frontend's import and derive
    /// emission.
    pub(crate) features: Features,

    /// Outer-scope arrangement cache: fingerprint → `*_arr` ident. Persists
    /// across strata so a later stratum can reuse an arrangement built by an
    /// earlier stratum's prelude. Reset at the start of every `generate`.
    pub(crate) outer_arranged: HashMap<u64, Ident>,
}

impl CodeGen {
    pub fn new(config: Config, program: Program) -> Self {
        let mut cg = Self {
            config,
            program,
            global_fp_to_ident: HashMap::new(),
            global_fp_to_type: HashMap::new(),
            features: Features::default(),
            outer_arranged: HashMap::new(),
        };
        cg.make_global_data_type_map();
        cg
    }

    pub fn features(&self) -> &Features {
        &self.features
    }

    /// Run every code-generation pass and return the resulting [`CodeParts`].
    pub fn generate(
        &mut self,
        program_planner: &ProgramPlanner,
        profiler: &mut Option<Profiler>,
    ) -> Result<CodeParts, CodegenError> {
        self.make_global_ident_map();
        self.features.reset();
        self.outer_arranged.clear();
        self.collect_parts(program_planner.strata(), profiler)
    }
}
