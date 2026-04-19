//! FlowLog compiler CLI entry point.
//!
//! Parses a single `.dl` program (or all bundled examples via `--program all`),
//! runs the parse → stratify → plan → compile pipeline, and writes the
//! resulting executable to disk.

use std::process;

use clap::Parser;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use common::{emit_and_exit, get_example_files, Config, SourceMap, SECTION_BAR};
use flowlog_compiler::Compiler;
use optimizer::Optimizer;
use parser::Program;
use planner::StratumPlanner;
use profiler::Profiler;
use stratifier::Stratifier;

fn main() {
    init_tracing();
    let config = Config::parse();

    // `--program all` is a batch mode that dry-runs the pipeline over every
    // bundled example for regression checking; otherwise compile the single
    // file the user pointed at.
    if config.should_process_all() {
        run_all_examples(&config);
        return;
    }

    compile_single(&config);
}

/// Compile the program specified by `config.program()` into an executable.
fn compile_single(config: &Config) {
    let (program, sm) = parse_program(config);
    let stratifier = Stratifier::from_program(&program, config.is_extended())
        .unwrap_or_else(|err| emit_and_exit(err, &sm));
    let mut profiler = new_profiler(config);
    let mut optimizer = Optimizer::new();
    let strata = plan_strata(config, &mut optimizer, &mut profiler, &stratifier)
        .unwrap_or_else(|err| emit_and_exit(err, &sm));

    let mut compiler = Compiler::new(config.clone(), program);
    if let Err(e) = compiler.compile(&strata, &mut profiler) {
        error!("{}", e);
        process::exit(1);
    }
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

        let mut sm = SourceMap::new();
        let program = match Program::parse_with_includes(
            file_path.to_str().expect("non-UTF-8 path"),
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
        let stratifier = match Stratifier::from_program(&program, config.is_extended()) {
            Ok(s) => s,
            Err(err) => {
                failure += 1;
                error!("FAILED: {} ({err})", file_name);
                continue;
            }
        };
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut optimizer = Optimizer::new();
            let mut profiler = new_profiler(config);
            plan_strata(config, &mut optimizer, &mut profiler, &stratifier)
                .map(|strata| (program.rules().len(), strata.len()))
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

fn plan_strata(
    config: &Config,
    optimizer: &mut Optimizer,
    profiler: &mut Option<Profiler>,
    stratifier: &Stratifier,
) -> Result<Vec<StratumPlanner>, common::diag::BoxError> {
    stratifier
        .stratum()
        .iter()
        .enumerate()
        .map(|(idx, rule_refs)| {
            let rules: Vec<_> = rule_refs.iter().map(|r| (*r).clone()).collect();
            StratumPlanner::from_rules(config, &rules, optimizer, profiler, stratifier, idx)
                .map_err(common::diag::BoxError::from)
        })
        .collect()
}

// =========================================================================
// Tracing
// =========================================================================

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
