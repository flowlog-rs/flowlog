//! FlowLog compiler CLI entry point.
//!
//! Parses a single `.dl` program (or all bundled examples via `--program all`),
//! runs the parse → stratify → plan → compile pipeline, and writes the
//! resulting executable to disk.

use std::process;

use clap::Parser;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use flowlog_build::common::{Config, SECTION_BAR, SourceMap, emit_and_exit, get_example_files};
use flowlog_build::parser::Program;
use flowlog_build::planner::ProgramPlanner;
use flowlog_build::profiler::Profiler;
use flowlog_compiler::Compiler;

fn main() {
    let config = Config::parse();

    // `--program all` is a batch mode that dry-runs the pipeline over every
    // bundled example for regression checking; otherwise compile the single
    // file the user pointed at.
    // Batch mode wants per-file progress (info); single-file wants silent on
    // success and errors via codespan (warn).
    let default_level = if config.should_process_all() {
        "info"
    } else {
        "warn"
    };
    init_tracing(default_level);

    if config.should_process_all() {
        run_all_examples(&config);
        return;
    }

    compile_single(&config);
}

/// Compile the program specified by `config.program()` into an executable.
fn compile_single(config: &Config) {
    let (mut program, sm) = parse_program(config);
    flowlog_build::typechecker::check_program(&mut program)
        .unwrap_or_else(|err| emit_and_exit(err, &sm));
    let mut profiler = new_profiler(config);
    let program_planner = ProgramPlanner::from_program(config, &program, &mut profiler)
        .unwrap_or_else(|err| emit_and_exit(err, &sm));

    let mut compiler = Compiler::new(config.clone(), program);
    compiler
        .compile(&program_planner, &mut profiler)
        .unwrap_or_else(|err| emit_and_exit(err, &sm));
}

/// Dry-run the parse → stratify → plan pipeline on every bundled example.
///
/// Does not invoke the compiler; the goal is a smoke test that catches
/// regressions in upstream passes (parser, stratifier, planner). Each file
/// runs inside `catch_unwind` so one bad example doesn't abort the rest.
fn run_all_examples(config: &Config) {
    let files = get_example_files();
    info!("Running compiler on {} example files...", files.len());

    let mut success = 0;
    let mut failure = 0;

    for file_path in &files {
        let file_name = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("<unnamed>");

        let Some(path_str) = file_path.to_str() else {
            failure += 1;
            error!("FAILED: non-UTF-8 path {}", file_path.display());
            continue;
        };

        let mut sm = SourceMap::new();
        let program = match Program::parse_with_includes(
            path_str,
            config.is_extended(),
            &config.include_dirs(),
            &mut sm,
        ) {
            Ok(p) => p,
            Err(err) => {
                failure += 1;
                error!("FAILED: {} ({err})", file_name);
                continue;
            }
        };
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut profiler = new_profiler(config);
            ProgramPlanner::from_program(config, &program, &mut profiler)
                .map(|pp| (program.rules().len(), pp.strata().len()))
        }));

        match outcome {
            Ok(Ok((rules, strata))) => {
                success += 1;
                info!(
                    "SUCCESS: {} (rules={}, strata={})",
                    file_name, rules, strata
                );
            }
            Ok(Err(err)) => {
                failure += 1;
                error!("FAILED: {} ({err})", file_name);
            }
            Err(_) => {
                failure += 1;
                error!("FAILED: {}", file_name);
            }
        }
    }

    println!();
    println!("{}", SECTION_BAR);
    println!("SUMMARY:");
    println!("  Total files: {}", files.len());
    println!("  Successful: {success}");
    println!("  Failed:     {failure}");

    if failure > 0 {
        println!();
        println!("Some files failed to generate. Check the errors above for details.");
        process::exit(1);
    }
    println!();
    println!("All example files generated successfully!");
}

// =========================================================================
// Pipeline helpers
// =========================================================================

fn parse_program(config: &Config) -> (Program, SourceMap) {
    let mut sm = SourceMap::new();
    let program = Program::parse_with_includes(
        config.program(),
        config.is_extended(),
        &config.include_dirs(),
        &mut sm,
    )
    .unwrap_or_else(|err| emit_and_exit(err, &sm));
    (program, sm)
}

fn new_profiler(config: &Config) -> Option<Profiler> {
    config
        .profiling_enabled()
        .then(|| Profiler::new(config.mode()))
}

// =========================================================================
// Tracing
// =========================================================================

fn init_tracing(default_level: &str) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
