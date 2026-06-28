//! FlowLog compiler entry point.
//!
//! Parses a single Datalog program, runs the pipeline, and writes out the resulting executable.

use clap::Parser;
use tracing_subscriber::EnvFilter;

use flowlog_build::parser::Program;
use flowlog_build::planner::ProgramPlanner;
use flowlog_build::profiler::Profiler;
use flowlog_common::{SourceMap, emit_and_exit};
use flowlog_compiler::{Cli, Compiler};

fn main() {
    // Tracing: silent on success; errors surface via codespan diagnostics.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt()
        .without_time()
        .with_env_filter(filter)
        .init();

    let cli = Cli::parse();
    let config = cli.to_config();
    let options = cli.to_compile_options();

    // Parse the source into an AST.
    let mut sm = SourceMap::new();
    let mut program = Program::parse_with_includes(
        config.program(),
        config.is_extended(),
        &config.include_dirs(),
        &mut sm,
    )
    .unwrap_or_else(|err| emit_and_exit(err, &sm));

    // Type-check the program.
    flowlog_build::typechecker::check_program(&mut program, &config)
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
