//! Drive the FlowLog compilation pipeline end-to-end for library mode.
//!
//! The pipeline is shared with the standalone compiler:
//!
//! ```text
//! parse → stratify → plan → generate (+ library-mode relation module)
//! ```
//!
//! [`run`] executes each stage and returns a [`Pipeline`] bundling the
//! artifacts library-mode assembly consumes in [`crate::assembly`].

use std::io;
use std::path::Path;

use common::{Config, ExecutionMode};
use generator::features::Features;
use generator::{AssemblyParts, Generator};
use optimizer::Optimizer;
use parser::Program;
use planner::StratumPlanner;
use proc_macro2::TokenStream;
use stratifier::Stratifier;

use crate::Builder;

/// Artifacts produced by one compilation, consumed by library-mode assembly.
pub(crate) struct Pipeline {
    /// Mode-agnostic codegen fragments (type decls, dataflow, drain, …).
    pub parts: AssemblyParts,
    /// Parsed program — used by assembly for per-relation codegen.
    pub program: Program,
    /// Library-mode relation module: `{Name}Input` handlers + `Inputs` container.
    pub relations: TokenStream,
    /// Features detected during codegen — drives imports and derives.
    pub features: Features,
}

/// Compile `program_path` through every pipeline stage.
pub(crate) fn run(builder: &Builder, program_path: &Path) -> io::Result<Pipeline> {
    let program_str = program_path.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("non-UTF-8 program path: {}", program_path.display()),
        )
    })?;

    let config = build_config(builder, program_str);
    let program = parse(&config, &builder.include_dirs);
    let strata = plan(&config, &program);

    let mut profiler = None;
    let mut generator = Generator::new(config, program.clone());
    let parts = generator.generate(&strata, &mut profiler);
    let features = generator.features().clone();
    let relations = crate::relation::gen_input_module(&program, &features);

    Ok(Pipeline {
        parts,
        program,
        relations,
        features,
    })
}

fn parse(config: &Config, include_dirs: &[std::path::PathBuf]) -> Program {
    let include_refs: Vec<&Path> = include_dirs.iter().map(|p| p.as_path()).collect();
    Program::parse_with_includes(config.program(), config.is_extended(), &include_refs)
}

/// Stratify the program and plan each stratum independently.
fn plan(config: &Config, program: &Program) -> Vec<StratumPlanner> {
    let stratifier = Stratifier::from_program(program, config.is_extended());
    let mut optimizer = Optimizer::new();
    let mut profiler = None;
    stratifier
        .stratum()
        .iter()
        .enumerate()
        .map(|(idx, rule_refs)| {
            let rules: Vec<_> = rule_refs.iter().map(|r| (*r).clone()).collect();
            StratumPlanner::from_rules(
                config,
                &rules,
                &mut optimizer,
                &mut profiler,
                &stratifier,
                idx,
            )
        })
        .collect()
}

/// Project the library-mode [`Builder`] onto the shared compiler [`Config`].
///
/// `output_dir: Some("-")` is a placeholder the generator requires in batch
/// mode. Library mode ignores the binary-mode `merge_stmts` it triggers and
/// drains outputs into a typed `BatchResults` struct instead.
fn build_config(builder: &Builder, program: &str) -> Config {
    Config {
        program: program.to_string(),
        fact_dir: None,
        executable_path: None,
        output_dir: Some("-".to_string()),
        mode: ExecutionMode::DatalogBatch,
        profile: false,
        sip: builder.sip,
        str_intern: builder.string_intern,
        udf_file: builder
            .udf_file
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned()),
        save_temps: false,
        include_dirs: builder
            .include_dirs
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect(),
    }
}
