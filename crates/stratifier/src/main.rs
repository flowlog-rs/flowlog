use std::process;

use args::{get_example_files, Args};
use clap::Parser;
use parser::Program;
use stratifier::{DependencyGraph, Stratifier};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

fn main() {
    // Initialize tracing similar to parser main
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .init();

    let args = Args::parse();

    if args.should_process_all() {
        run_all_examples();
        return;
    }

    let program = Program::parse(args.program());
    info!("Success parse program (rules={})", program.rules().len());

    // Show dependency graph then stratification summary
    info!(
        "Dependency Graph:{}",
        DependencyGraph::from_program(&program)
    );
    let stratifier = Stratifier::from_program(&program);
    let recursive_cnt = stratifier
        .is_recursive_stratum_bitmap()
        .iter()
        .filter(|b| **b)
        .count();
    info!(
        "Stratification ({} strata, {} recursive):{}",
        stratifier.is_recursive_stratum_bitmap().len(),
        recursive_cnt,
        stratifier
    );
}

fn run_all_examples() {
    let example_files = get_example_files();

    info!("Running stratifier on {} example files...", example_files.len());
    info!("{}", "=".repeat(80));

    let mut successful = 0usize;
    let mut failed = 0usize;

    for (i, file_path) in example_files.iter().enumerate() {
        let file_name = file_path.file_name().unwrap().to_str().unwrap();
        info!("[{}/{}] Processing: {}", i + 1, example_files.len(), file_name);
        info!("{}", "-".repeat(40));

        match std::panic::catch_unwind(|| Program::parse(file_path.to_str().unwrap())) {
            Ok(program) => {
                successful += 1;
                info!("  SUCCESS: {}", file_name);
                info!("    Statistics:");
                info!("     Rules: {}", program.rules().len());
                let stratifier = Stratifier::from_program(&program);
                let recursive_cnt = stratifier
                    .is_recursive_stratum_bitmap()
                    .iter()
                    .filter(|b| **b)
                    .count();
                info!(
                    "     Strata: {}",
                    stratifier.is_recursive_stratum_bitmap().len()
                );
                info!("     Recursive strata: {}", recursive_cnt);
            }
            Err(panic_info) => {
                failed += 1;
                info!("  FAILED: {}", file_name);
                if let Some(s) = panic_info.downcast_ref::<String>() {
                    info!("  Error: {}", s);
                } else if let Some(s) = panic_info.downcast_ref::<&str>() {
                    info!("  Error: {}", s);
                } else {
                    info!("  Error: Unknown panic occurred");
                }
            }
        }
    }

    info!("{}", "=".repeat(80));
    info!("SUMMARY:");
    info!("  Total files: {}", example_files.len());
    info!("  Successful: {}", successful);
    info!("  Failed: {}", failed);

    if failed > 0 {
        error!("Some files failed. See errors above.");
        process::exit(1);
    } else {
        info!("All example files stratified successfully!");
    }
}
