use clap::Parser;
use std::{fs, path::Path, process};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use common::Config;
use compiler::Compiler;
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
    let program = Program::parse(config.program());

    // Stratify the program
    let stratifier = Stratifier::from_program(&program);

    // Profiler to collect profiling data
    let mut profiler = if config.profiling_enabled() {
        Some(Profiler::default())
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

fn generate_program(
    config: &Config,
    optimizer: &mut Optimizer,
    profiler: &mut Option<Profiler>,
    stratifier: &Stratifier,
    program: &Program,
) {
    let mut strata = Vec::new();
    for (stratum_idx, rule_refs) in stratifier.stratum().iter().enumerate() {
        let is_recursive = stratifier.is_recursive_stratum(stratum_idx);

        // Clone rules into a local Vec to satisfy StratumPlanner signature
        let rules: Vec<_> = rule_refs.iter().map(|r| (*r).clone()).collect();
        let sp = StratumPlanner::from_rules(
            &rules,
            optimizer,
            profiler,
            is_recursive,
            stratifier.stratum_iterative_relation(stratum_idx),
            stratifier.stratum_leave_relation(stratum_idx),
            stratifier.stratum_available_relations(stratum_idx),
        );
        strata.push(sp);
    }

    let mut compiler = Compiler::new(config.clone(), program.clone());

    // Generate code for the deduplicated transformation list for this stratum
    if let Err(e) = compiler.generate_executable_at(&strata, profiler) {
        error!("Failed to generate project: {}", e);
        process::exit(1);
    }
    info!(
        "Compile project at '{}'",
        config.executable_path().to_string_lossy()
    );
}

/// Run compiler on all example files in the example directory
fn run_all_examples(config: &Config) {
    let example_dir = "example";

    // Check if example directory exists
    if !Path::new(example_dir).exists() {
        error!("Directory '{}' not found", example_dir);
        process::exit(1);
    }

    // Read and collect .dl files
    let entries = match fs::read_dir(example_dir) {
        Ok(entries) => entries,
        Err(e) => {
            error!("Error reading example dir: {}", e);
            process::exit(1);
        }
    };

    let mut files = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("dl") {
            files.push(path);
        }
    }

    files.sort();

    if files.is_empty() {
        error!("No .dl files found in {}", example_dir);
        process::exit(1);
    }

    // Process all files
    info!("Running compiler on {} example files...", files.len());
    let mut success_count = 0;
    let mut failure_count = 0;

    for file_path in &files {
        let file_name = file_path.file_name().unwrap().to_str().unwrap();

        match std::panic::catch_unwind(|| {
            let program = Program::parse(file_path.to_str().unwrap());
            let stratifier = Stratifier::from_program(&program);
            let mut optimizer = Optimizer::new();
            let mut profiler = if config.profiling_enabled() {
                Some(Profiler::default())
            } else {
                None
            };

            for (stratum_idx, rule_refs) in stratifier.stratum().iter().enumerate() {
                let is_recursive = stratifier.is_recursive_stratum(stratum_idx);
                let rules: Vec<_> = rule_refs.iter().map(|r| (*r).clone()).collect();
                let _sp = StratumPlanner::from_rules(
                    &rules,
                    &mut optimizer,
                    &mut profiler,
                    is_recursive,
                    stratifier.stratum_iterative_relation(stratum_idx),
                    stratifier.stratum_leave_relation(stratum_idx),
                    stratifier.stratum_available_relations(stratum_idx),
                );
                // In batch mode, skip file generation to avoid overwriting.
            }

            (program.rules().len(), stratifier.stratum().len())
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

    // Print summary
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
