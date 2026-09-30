//! The shared codegen core. Turns a parsed FlowLog program plus a
//! stratified execution plan into a [`Skeleton`] that each frontend
//! (library mode here, binary mode in `flowlog-compiler`) assembles into
//! its own final Rust source.
//!
//! The modules follow where their output lands in the generated engine:
//!
//! - `skeleton`: the [`Skeleton`] itself, and the order it is filled in.
//! - `io`: the program's boundary, from relation declarations and input
//!   collections to the emitters its outputs publish through.
//! - `stratum`: the dataflow's strata in order, each non-recursive or
//!   recursive.
//! - `rule`: a rule's head and body steps, shared by both kinds of stratum.
//! - `expr`: the expressions inside operator closures.
//! - `ty`: the `(Data, Diff, Time)` types every collection carries.
//! - `ident`, `features`, `error`, `profile`: binding idents, the features a
//!   program needs, internal errors, and the profiler's side of the engine.

mod error;
mod expr;
mod features;
mod ident;
mod io;
mod profile;
mod rule;
mod skeleton;
mod stratum;
mod ty;

// External API -- used by flowlog-compiler via lib.rs re-exports.
use std::collections::HashMap;

pub use error::CodegenError;
pub use expr::term::constant::const_to_token;
pub use features::Features;
use flowlog_common::Config;
use flowlog_parser::DataType;
use flowlog_parser::Mutability;
use flowlog_parser::Program;
use flowlog_planner::planner::ProgramPlanner;
use flowlog_profiler::PlanGraph;
pub use ident::input_field_ident;
pub use ident::input_handle_ident;
pub use ident::output_emitter_ident;
pub use ident::relation_marker_ident;
pub use io::relation::gen_relations;
use proc_macro2::Ident;
pub use skeleton::Skeleton;
pub use ty::data::data_type_tokens;
pub(crate) use ty::data::row_is_copy;
// Intra-crate shortcuts used by build/ (library mode).
pub(crate) use ty::data::{tuple_tokens, user_tuple_tokens};
pub(crate) use ty::diff::weight_tokens;

pub struct CodeGen {
    pub(crate) config: Config,
    pub(crate) program: Program,

    /// Fingerprint -> binding-ident map, stable across strata: local
    /// recursion strata may introduce new identifiers that refer back to
    /// these. Idents are synthetic; see [`ident`] for the scheme.
    pub(crate) global_fp_to_ident: HashMap<u64, Ident>,
    /// Fingerprint -> `(key_types, value_types)`. Seeded in `new` from the
    /// parsed program; extended in `generate` with inferred output types.
    pub(crate) global_fp_to_type: HashMap<u64, (Vec<DataType>, Vec<DataType>)>,

    /// Populated during `generate`; drives the frontend's import and derive
    /// emission.
    pub(crate) features: Features,

    /// Outer-scope arrangement cache: fingerprint -> `*_arr` ident. Persists
    /// across strata so a later stratum can reuse an arrangement built by an
    /// earlier stratum's prelude. Reset at the start of every `generate`.
    pub(crate) outer_arranged: HashMap<u64, Ident>,

    /// Fingerprint -> mutability of each emitted collection and of each
    /// relation's current binding; a later stratum that rebinds a relation
    /// overwrites its entry. An entry only rises: a collection's never
    /// changes, and a rebinding unions the earlier binding in. Seeded from
    /// the inputs' declarations in `generate`.
    pub(crate) global_fp_to_mutability: HashMap<u64, Mutability>,
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
            global_fp_to_mutability: HashMap::new(),
        };
        cg.make_global_data_type_map();
        cg
    }

    pub fn features(&self) -> &Features {
        &self.features
    }

    /// Run every code-generation pass and return the resulting [`Skeleton`].
    pub fn generate(
        &mut self,
        program_planner: &ProgramPlanner,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Skeleton, CodegenError> {
        self.make_global_ident_map();
        self.features.reset();
        self.outer_arranged.clear();
        self.global_fp_to_mutability = self
            .program
            .edbs()
            .into_iter()
            .map(|rel| (rel.fingerprint(), rel.input_mutability()))
            .collect();
        self.gen_skeleton(program_planner.strata(), plan_graph)
    }
}
