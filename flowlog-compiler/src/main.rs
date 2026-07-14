//! FlowLog compiler entry point.
//!
//! Parses a single Datalog program, runs the pipeline, and writes out the resulting executable.

use clap::Parser;
use flowlog_build::planner::ProgramPlanner;
use flowlog_common::SourceMap;
use flowlog_common::emit_and_exit;
use flowlog_compiler::Cli;
use flowlog_compiler::Compiler;
use flowlog_profiler::Profiler;
use tracing_subscriber::EnvFilter;

fn main() {
    // Tracing: silent on success; errors surface via codespan diagnostics.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt()
        .without_time()
        .with_env_filter(filter)
        .init();

    let cli = Cli::parse();
    let mut config = cli.to_config();
    let options = cli.to_compile_options();

    // Parse, type-check, and constant-fold the source into a validated AST.
    let mut sm = SourceMap::new();
    // `parse` runs type-check + constant-fold internally. Snapshot the
    // config-derived path and include dirs first, so `config` is free to pass
    // as `&mut` (type-check records `ord`'s serial-load requirement on it).
    let program_path = config.program().to_owned();
    let include_dirs: Vec<std::path::PathBuf> = config
        .include_dirs()
        .iter()
        .map(|p| p.to_path_buf())
        .collect();
    let include_refs: Vec<&std::path::Path> = include_dirs.iter().map(|p| p.as_path()).collect();
    let program = flowlog_parser::parse(&program_path, &include_refs, &mut sm, &mut config)
        .unwrap_or_else(|err| emit_and_exit(err, &sm));

    // Plan into the relational intermediate representation.
    let mut profiler = config
        .profiling_enabled()
        .then(|| Profiler::new(config.mode()));
    let program_planner = ProgramPlanner::from_program(&config, &program, &mut profiler)
        .unwrap_or_else(|err| emit_and_exit(err, &sm));

    // Compile into a Rust executable.
    let mut compiler = Compiler::new(config, options, program);
    compiler
        .compile(&program_planner, &mut profiler)
        .unwrap_or_else(|err| emit_and_exit(err, &sm));
}
