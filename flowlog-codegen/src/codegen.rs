//! [`Codegen`]: the state every pass reads and extends while a program is
//! lowered, and the entry point that runs the passes.

use std::collections::HashMap;

use flowlog_common::Config;
use flowlog_parser::DataType;
use flowlog_parser::Mutability;
use flowlog_parser::Program;
use flowlog_planner::planner::ProgramPlanner;
use flowlog_profiler::PlanGraph;
use proc_macro2::Ident;

use crate::CodegenError;
use crate::Skeleton;

pub struct Codegen {
    pub(crate) config: Config,
    pub(crate) program: Program,

    /// Fingerprint -> binding-ident map, stable across strata: local
    /// recursion strata may introduce new identifiers that refer back to
    /// these. Idents are synthetic; see [`crate::ident`] for the scheme.
    pub(crate) global_fp_to_ident: HashMap<u64, Ident>,
    /// Fingerprint -> `(key_types, value_types)`. Seeded in `new` from the
    /// parsed program; extended in `generate` with inferred output types.
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
        let mut cg = Self {
            config,
            program,
            global_fp_to_ident: HashMap::new(),
            global_fp_to_type: HashMap::new(),
            outer_fp_to_arrangement: HashMap::new(),
            global_fp_to_mutability: HashMap::new(),
        };
        cg.seed_global_types();
        cg
    }

    /// Run every code-generation pass and return the resulting [`Skeleton`].
    pub fn generate(
        &mut self,
        program_planner: &ProgramPlanner,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Skeleton, CodegenError> {
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
