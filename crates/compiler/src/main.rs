use clap::Parser;
use std::process;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use common::{get_example_files, Config};
use compiler::{build_and_collect, Compiler};
use optimizer::Optimizer;
use parser::Program;
use planner::StratumPlanner;
use profiler::Profiler;
use stratifier::Stratifier;

fn main() {
    // Initialize simple tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Parse command line arguments with clap
    let config = Config::parse();

    // Backward-compat convenience: allow `--program all` or `--program --all`
    if config.should_process_all() {
        run_all_examples(&config);
        return;
    }

    // Parse and process single file
    let program = Program::parse(config.program(), config.is_extended());

    // Stratify the program
    let stratifier = Stratifier::from_program(&program, config.is_extended());

    // Profiler to collect profiling data
    let mut profiler = if config.profiling_enabled() {
        Some(Profiler::new(config.mode()))
    } else {
        None
    };

    // Plan each stratum using StratumPlanner then hand transformations to compiler
    let mut optimizer = Optimizer::new();
    generate_program(
        &config,
        &mut optimizer,
        &mut profiler,
        &stratifier,
        &program,
    );
}

/// Plan each stratum into a list of `StratumPlanner`s.
fn plan_strata(
    config: &Config,
    optimizer: &mut Optimizer,
    profiler: &mut Option<Profiler>,
    stratifier: &Stratifier,
) -> Vec<StratumPlanner> {
    stratifier
        .stratum()
        .iter()
        .enumerate()
        .map(|(stratum_idx, rule_refs)| {
            let rules: Vec<_> = rule_refs.iter().map(|r| (*r).clone()).collect();
            StratumPlanner::from_rules(config, &rules, optimizer, profiler, stratifier, stratum_idx)
        })
        .collect()
}

fn generate_program(
    config: &Config,
    optimizer: &mut Optimizer,
    profiler: &mut Option<Profiler>,
    stratifier: &Stratifier,
    program: &Program,
) {
    let strata = plan_strata(config, optimizer, profiler, stratifier);

    let mut compiler = Compiler::new(config.clone(), program.clone());
    if let Err(e) = compiler.generate_executable_at(&strata, profiler) {
        error!("Failed to generate project: {}", e);
        process::exit(1);
    }

    if let Err(e) = build_and_collect(
        &config.build_dir(),
        &config.crate_name(),
        &config.executable_path(),
        config.save_temps(),
    ) {
        error!("{}", e);
        process::exit(1);
    }
}

/// Run compiler on all example files in the example directory
fn run_all_examples(config: &Config) {
    let files = get_example_files();

    info!("Running compiler on {} example files...", files.len());
    let mut success_count = 0;
    let mut failure_count = 0;

    for file_path in &files {
        let file_name = file_path.file_name().unwrap().to_str().unwrap();

        match std::panic::catch_unwind(|| {
            let program = Program::parse(file_path.to_str().unwrap(), config.is_extended());
            let stratifier = Stratifier::from_program(&program, config.is_extended());
            let mut optimizer = Optimizer::new();
            let mut profiler = if config.profiling_enabled() {
                Some(Profiler::new(config.mode()))
            } else {
                None
            };
            let strata = plan_strata(config, &mut optimizer, &mut profiler, &stratifier);
            (program.rules().len(), strata.len())
        }) {
            Ok((rule_count, strata_count)) => {
                success_count += 1;
                info!(
                    "SUCCESS: {} (rules={}, strata={})",
                    file_name, rule_count, strata_count
                );
            }
            Err(_) => {
                failure_count += 1;
                error!("FAILED: {}", file_name);
            }
        }
    }

    println!("\n{}", "=".repeat(80));
    println!("SUMMARY:");
    println!("  Total files: {}", files.len());
    println!("  Successful: {}", success_count);
    println!("  Failed: {}", failure_count);

    if failure_count > 0 {
        println!("\nSome files failed to generate. Check the errors above for details.");
        process::exit(1);
    } else {
        println!("\nAll example files generated successfully!");
    }
}
