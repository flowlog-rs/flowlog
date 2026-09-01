//! The shared codegen core. Turns a parsed FlowLog program plus a
//! stratified execution plan into a [`CodeParts`] bundle that each frontend
//! (library mode here, binary mode in `flowlog-compiler`) assembles into
//! its own final Rust source.

mod aggregation;
mod arg;
mod code_parts;
mod edb_handles;
mod error;
mod features;
mod flow;
mod idb_buffers;
mod ident;
mod profile;
mod ty;

// External API -- used by flowlog-compiler via lib.rs re-exports.
use std::collections::HashMap;

pub use arg::const_to_token;
pub use code_parts::CodeParts;
pub use error::CodegenError;
pub use features::Features;
use flowlog_common::Config;
use flowlog_parser::DataType;
use flowlog_parser::Program;
use flowlog_planner::planner::ProgramPlanner;
use flowlog_profiler::PlanGraph;
pub use idb_buffers::field_accessor;
pub use idb_buffers::gen_drain_block;
use proc_macro2::Ident;
pub use ty::data::data_type_tokens;
pub(crate) use ty::data::row_is_copy;
// Intra-crate shortcuts used by build/ (library mode).
pub(crate) use ty::data::{tuple_tokens, user_tuple_tokens};

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
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<CodeParts, CodegenError> {
        self.make_global_ident_map();
        self.features.reset();
        self.outer_arranged.clear();
        self.collect_parts(program_planner.strata(), plan_graph)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flowlog_common::SourceMap;
    use flowlog_planner::planner::ProgramPlanner;
    use tempfile::NamedTempFile;

    use super::*;

    fn generated_flows(src: &str) -> String {
        let mut tmp = NamedTempFile::new().expect("tempfile");
        tmp.write_all(src.as_bytes()).expect("write");
        let mut source_map = SourceMap::new();
        let config = Config::default();
        let mut parse_config = config.clone();
        let program = flowlog_parser::parse(
            &tmp.path().to_string_lossy(),
            &[],
            &mut source_map,
            &mut parse_config,
        )
        .expect("parse");
        let planner = ProgramPlanner::from_program(&config, &program, &mut None).expect("plan");
        let mut codegen = CodeGen::new(config, program);
        codegen
            .generate(&planner, &mut None)
            .expect("codegen")
            .flows
            .into_iter()
            .map(|tokens| tokens.to_string())
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn generated_flow_order_is_deterministic() {
        let source = "\
            .decl A(x: symbol, a: symbol, b: symbol, c: symbol)\n\
            .decl B(x: symbol, y: symbol, z: symbol)\n\
            .decl P(x: symbol)\n\
            .decl Q(x: symbol)\n\
            .decl R(x: symbol)\n\
            .decl S(x: symbol)\n\
            .input A\n\
            .input B\n\
            .output P\n\
            .output Q\n\
            .output R\n\
            .output S\n\
            P(x) :- A(x, \"one\", \"two\", _), B(x, y, z).\n\
            Q(x) :- A(x, \"one\", \"two\", _), B(x, y, z).\n\
            R(x) :- A(x, \"one\", \"two\", _).\n\
            R(x) :- S(x).\n\
            S(x) :- R(x).\n";
        let expected = generated_flows(source);
        for _ in 0..32 {
            assert_eq!(generated_flows(source), expected);
        }
    }
}
