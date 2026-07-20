//! Library-mode compilation pipeline.
//!
//! ```text
//! parse → stratify → plan → codegen → library-mode relation module
//! ```
//!
//! The caller owns the [`SourceMap`] so any [`BoxError`] can be rendered
//! against the parsed source on both success and failure.

use std::io;
use std::path::Path;
use std::path::PathBuf;

use flowlog_common::BoxError;
use flowlog_common::Config;
use flowlog_common::SourceMap;
use flowlog_parser::Program;
use flowlog_profiler::PlanGraph;
use proc_macro2::TokenStream;

use crate::BuildError;
use crate::Builder;
use crate::CodeGen;
use crate::CodeParts;
use crate::build::relation::gen_input_module;
use crate::build::relation::validate_api_surface;
use crate::codegen::Features;
use crate::planner::ProgramPlanner;

/// Artifacts produced by one compilation, consumed by library-mode assembly.
pub(crate) struct Pipeline {
    pub(crate) config: Config,
    pub(crate) parts: CodeParts,
    pub(crate) program: Program,
    /// Library-mode relation module: `{Name}Input` handlers + `Inputs` container.
    pub(crate) relations: TokenStream,
    pub(crate) features: Features,
}

impl Pipeline {
    pub(crate) fn build(
        builder: &Builder,
        program_path: &Path,
        sm: &mut SourceMap,
    ) -> Result<Self, BoxError> {
        let program_str = program_path.to_str().ok_or_else(|| {
            BuildError::from(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("non-UTF-8 program path: {}", program_path.display()),
            ))
        })?;

        let mut config = build_config(builder, program_str);
        // `parse` runs type-check + constant-fold (literals pinned, casts
        // stripped), so the catalog and dataflow never see polymorphic literals
        // or constant sub-expressions.
        let program = parse(&mut config, &builder.include_dirs, sm)?;
        // The generated library API mirrors relation names verbatim; reject
        // the rare names it cannot represent before codegen runs.
        validate_api_surface(&program)?;
        let mut plan_graph = config
            .profiling_enabled()
            .then(|| PlanGraph::new(config.mode()));
        let program_planner = ProgramPlanner::from_program(&config, &program, &mut plan_graph)?;

        let mut cg = CodeGen::new(config.clone(), program.clone());
        let parts = cg.generate(&program_planner, &mut plan_graph)?;
        let features = cg.features().clone();
        let relations = gen_input_module(&program, &features)?;

        Ok(Self {
            config,
            parts,
            program,
            relations,
            features,
        })
    }
}

fn parse(
    config: &mut Config,
    include_dirs: &[PathBuf],
    sm: &mut SourceMap,
) -> Result<Program, BoxError> {
    let include_refs: Vec<&Path> = include_dirs.iter().map(PathBuf::as_path).collect();
    let program_path = config.program().to_owned();
    flowlog_parser::parse(&program_path, &include_refs, sm, config).map_err(Into::into)
}

/// Project a [`Builder`] onto the shared pipeline [`Config`].
///
/// Library mode never drains to stdout (`output_to_stdout = false`) — outputs
/// flow through `BatchResults` rather than stdout or a file.
fn build_config(builder: &Builder, program: &str) -> Config {
    Config {
        program: program.to_string(),
        mode: builder.mode,
        profile: builder.profile,
        sip: builder.sip,
        str_intern: builder.string_intern,
        udf_file: builder
            .udf_file
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned()),
        include_dirs: builder
            .include_dirs
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect(),
        output_to_stdout: false,
        serialize_load: false,
    }
}
