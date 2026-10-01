//! The shared codegen core. Turns a parsed FlowLog program plus a
//! stratified execution plan into a [`Skeleton`] that each frontend
//! (library mode in `flowlog-build`, binary mode in `flowlog-compiler`)
//! assembles into its own final Rust source.
//!
//! [`Codegen`] itself, the state every pass reads and extends, lives here.
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
//! - `ident`, `error`, `profiling`: binding idents, internal errors, and the
//!   profiler's side of the engine.

mod error;
mod expr;
mod ident;
mod io;
mod profiling;
mod rule;
mod skeleton;
mod stratum;
#[cfg(test)]
mod test_harness;
mod ty;

use std::collections::HashMap;

pub use error::CodegenError;
pub(crate) use expr::term::constant::const_to_token;
use flowlog_common::Config;
use flowlog_parser::DataType;
use flowlog_parser::Mutability;
use flowlog_parser::Program;
use flowlog_planner::planner::ProgramPlanner;
use flowlog_profiler::PlanGraph;
pub use ident::input_field_ident;
pub use ident::input_handle_ident;
pub(crate) use ident::local_emitter_ident;
pub use ident::output_emitter_ident;
pub use ident::relation_marker_ident;
pub use io::gen_relations;
use proc_macro2::Ident;
pub use skeleton::Skeleton;
pub(crate) use ty::data::internal_tuple_tokens;
pub(crate) use ty::data::row_is_copy;
pub(crate) use ty::data::tuple_tokens;
pub use ty::data::user_tuple_tokens;
pub use ty::diff::weight_tokens;

pub struct Codegen {
    pub(crate) config: Config,
    pub(crate) program: Program,

    /// Fingerprint -> binding-ident map, stable across strata: local
    /// recursion strata may introduce new identifiers that refer back to
    /// these. Idents are synthetic; see [`crate::ident`] for the scheme.
    pub(crate) global_fp_to_ident: HashMap<u64, Ident>,
    /// Fingerprint -> `(key_types, value_types)`. Seeded in `generate` from
    /// the parsed program and extended there with inferred output types.
    pub(crate) global_fp_to_type: HashMap<u64, (Vec<DataType>, Vec<DataType>)>,

    /// Outer-scope arrangement cache: fingerprint -> `*_arr` ident. Persists
    /// across strata so a later stratum can reuse an arrangement built by an
    /// earlier stratum's prelude. Reset at the start of every `generate`.
    pub(crate) outer_fp_to_arrangement: HashMap<u64, Ident>,

    /// Fingerprint -> mutability of each emitted collection and of each
    /// relation's current binding; a later stratum that rebinds a relation
    /// overwrites its entry. An entry only rises: a collection's never
    /// changes, and a rebinding unions the earlier binding in. Seeded from
    /// the inputs' declarations in `generate`.
    pub(crate) global_fp_to_mutability: HashMap<u64, Mutability>,
}

impl Codegen {
    pub fn new(config: Config, program: Program) -> Self {
        Self {
            config,
            program,
            global_fp_to_ident: HashMap::new(),
            global_fp_to_type: HashMap::new(),
            outer_fp_to_arrangement: HashMap::new(),
            global_fp_to_mutability: HashMap::new(),
        }
    }

    /// Run every code-generation pass and return the resulting [`Skeleton`].
    ///
    /// Every table is reseeded from the program first, so running twice
    /// over the same planner yields the same skeleton.
    pub fn generate(
        &mut self,
        program_planner: &ProgramPlanner,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Skeleton, CodegenError> {
        self.seed_global_types();
        self.seed_global_idents();
        self.outer_fp_to_arrangement.clear();
        self.global_fp_to_mutability = self
            .program
            .edbs()
            .into_iter()
            .map(|rel| (rel.fingerprint(), rel.input_mutability()))
            .collect();
        self.gen_skeleton(program_planner.strata(), plan_graph)
    }
}
