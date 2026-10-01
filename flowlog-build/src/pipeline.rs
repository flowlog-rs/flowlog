//! Library-mode compilation pipeline.
//!
//! ```text
//! parse -> stratify -> plan -> codegen -> library-mode relation module
//! ```
//!
//! The caller owns the [`SourceMap`] so any [`BoxError`] can be rendered
//! against the parsed source on both success and failure.

use std::io;
use std::path::Path;
use std::path::PathBuf;

use flowlog_codegen::Codegen;
use flowlog_codegen::Skeleton;
use flowlog_codegen::gen_relations;
use flowlog_common::BoxError;
use flowlog_common::Config;
use flowlog_common::SourceMap;
use flowlog_parser::Program;
use flowlog_planner::planner::ProgramPlanner;
use flowlog_profiler::PlanGraph;
use proc_macro2::TokenStream;

use crate::BuildError;
use crate::Builder;
use crate::bindings::validate_api_surface;

/// Artifacts produced by one compilation, consumed by library-mode assembly.
pub(crate) struct Pipeline {
    pub(crate) config: Config,
    pub(crate) skeleton: Skeleton,
    pub(crate) program: Program,
    /// Relation declarations and worker-local input ownership.
    pub(crate) relations: TokenStream,
}

impl Pipeline {
    /// Runs the pipeline over the program at `program_path` with `builder`'s
    /// options, recording its sources in `sm`.
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
            .then(|| PlanGraph::new(program.is_incremental()));
        let program_planner = ProgramPlanner::from_program(&program, &mut plan_graph)?;

        let mut cg = Codegen::new(config.clone(), program.clone());
        let skeleton = cg.generate(&program_planner, &mut plan_graph)?;
        let relations = gen_relations(&program, config.str_intern_enabled())?;

        Ok(Self {
            config,
            skeleton,
            program,
            relations,
        })
    }
}

/// Returns the program `config` names, parsed and typechecked.
fn parse(
    config: &mut Config,
    include_dirs: &[PathBuf],
    sm: &mut SourceMap,
) -> Result<Program, BoxError> {
    let include_refs: Vec<&Path> = include_dirs.iter().map(PathBuf::as_path).collect();
    let program_path = config.program().to_owned();
    flowlog_parser::parse(&program_path, &include_refs, sm, config).map_err(Into::into)
}

/// Returns the shared pipeline [`Config`] a [`Builder`] projects to.
///
/// Library mode never drains to stdout (`output_to_stdout = false`): outputs
/// reach the caller through the engine's results rather than stdout or a
/// file.
fn build_config(builder: &Builder, program: &str) -> Config {
    Config {
        program: program.to_string(),
        profile: builder.profile,
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
        metrics_flush_interval_ms: builder.metrics_flush_interval_ms,
    }
}
