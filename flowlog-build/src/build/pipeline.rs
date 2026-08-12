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
use flowlog_common::ExecutionMode;
use flowlog_common::SourceMap;
use flowlog_parser::Program;
use flowlog_planner::planner::ProgramPlanner;
use flowlog_profiler::PlanGraph;
use proc_macro2::TokenStream;

use crate::BuildError;
use crate::Builder;
use crate::CodeGen;
use crate::CodeParts;
use crate::build::relation::gen_input_module;
use crate::build::relation::validate_api_surface;
use crate::codegen::Features;

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
        validate_options(&config)?;
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

/// Reject builder option combinations no generated engine can honor, so a
/// caller learns at build time instead of receiving a silently downgraded
/// engine. Only [`Builder::inline_single_worker`] can conflict:
/// [`Builder::trusted_set_inputs`] is a promise the other modes simply do
/// not act on.
fn validate_options(config: &Config) -> Result<(), BuildError> {
    if !config.inline_single_worker {
        return Ok(());
    }

    let conflict = if config.mode() != ExecutionMode::DatalogInc {
        format!(
            "it is only supported for ExecutionMode::DatalogInc, but this \
             build selected {:?}",
            config.mode()
        )
    } else if config.profile {
        "it cannot be combined with Builder::profile(true): the loggers and \
         per-commit metric tables are registered inside the spawned worker \
         closure the inline engine does not have"
            .to_string()
    } else {
        return Ok(());
    };

    Err(BuildError::from(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("Builder::inline_single_worker(true) was requested but {conflict}"),
    )))
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
        metrics_flush_interval_ms: builder.metrics_flush_interval_ms,
        inline_single_worker: builder.inline_single_worker,
        trusted_set_inputs: builder.trusted_set_inputs,
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    /// Both opt-in options must be off in a builder nobody configured, and
    /// the rest of the projection must be unaffected by their arrival.
    #[test]
    fn default_builder_projects_both_opt_in_options_off() {
        let config = build_config(&Builder::default(), "policy.dl");
        assert!(!config.inline_single_worker);
        assert!(!config.trusted_set_inputs);
        assert!(!config.sip);
        assert!(!config.str_intern);
        assert!(!config.profile);
        assert_eq!(config.mode, ExecutionMode::default());
        assert_eq!(config.metrics_flush_interval_ms, 0);
        assert!(config.udf_file.is_none());
        assert!(config.include_dirs.is_empty());
    }

    #[test]
    fn inline_single_worker_reaches_the_config() {
        let enabled = Builder::default().inline_single_worker(true);
        assert!(build_config(&enabled, "policy.dl").inline_single_worker);

        let disabled = Builder::default()
            .inline_single_worker(true)
            .inline_single_worker(false);
        assert!(!build_config(&disabled, "policy.dl").inline_single_worker);
    }

    #[test]
    fn trusted_set_inputs_reaches_the_config() {
        let enabled = Builder::default().trusted_set_inputs(true);
        assert!(build_config(&enabled, "policy.dl").trusted_set_inputs);

        let disabled = Builder::default()
            .trusted_set_inputs(true)
            .trusted_set_inputs(false);
        assert!(!build_config(&disabled, "policy.dl").trusted_set_inputs);
    }

    fn validate(builder: Builder) -> Result<(), BuildError> {
        validate_options(&build_config(&builder, "policy.dl"))
    }

    #[test]
    fn a_builder_that_asks_for_nothing_extra_validates() {
        assert!(validate(Builder::default()).is_ok());
        assert!(validate(Builder::default().trusted_set_inputs(true)).is_ok());
        assert!(
            validate(
                Builder::default()
                    .mode(ExecutionMode::DatalogInc)
                    .inline_single_worker(true)
            )
            .is_ok()
        );
    }

    /// Only `DatalogInc` gets an inline engine, so every other mode has to
    /// say so rather than hand back a spawned-thread engine the caller did
    /// not ask for.
    #[rstest]
    #[case::datalog_batch(ExecutionMode::DatalogBatch)]
    #[case::extend_batch(ExecutionMode::ExtendBatch)]
    #[case::extend_inc(ExecutionMode::ExtendInc)]
    fn inline_single_worker_is_rejected_outside_datalog_inc(#[case] mode: ExecutionMode) {
        let err = validate(Builder::default().mode(mode).inline_single_worker(true))
            .expect_err("the mode cannot carry an inline engine");
        assert!(
            err.to_string().contains("DatalogInc"),
            "the error must name the supported mode, got: {err}"
        );
    }

    #[test]
    fn inline_single_worker_is_rejected_alongside_profiling() {
        let err = validate(
            Builder::default()
                .mode(ExecutionMode::DatalogInc)
                .inline_single_worker(true)
                .profile(true),
        )
        .expect_err("profiling and the inline engine cannot both be honored");
        assert!(
            err.to_string().contains("profile"),
            "the error must name the conflicting option, got: {err}"
        );
    }
}
