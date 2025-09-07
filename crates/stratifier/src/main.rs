use parser::Program;
use std::env;
use std::fs;
use std::path::Path;
use std::process;
use stratifier::{DependencyGraph, Stratifier};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

fn main() {
    // Initialize tracing similar to parser main
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .init();

    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <program_file>", args[0]);
        eprintln!("       {} --all", args[0]);
        eprintln!("Examples:");
        eprintln!("  {} ./example/reach.dl", args[0]);
        eprintln!("  {} --all", args[0]);
        process::exit(1);
    }

    let argument = &args[1];
    if argument == "--all" {
        run_all_examples();
        return;
    }

    let program = Program::parse(argument);
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
    let example_dir = "example";
    if !Path::new(example_dir).exists() {
        error!("Error: example directory '{}' not found", example_dir);
        process::exit(1);
    }

    let entries = match fs::read_dir(example_dir) {
        Ok(e) => e,
        Err(err) => {
            error!("Error reading example directory: {err}");
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
        error!("No .dl files found in {example_dir} directory");
        process::exit(1);
    }

    info!("Running stratifier on {} example files...", files.len());
    info!("{}", "=".repeat(80));

    let mut successful = 0usize;
    let mut failed = 0usize;

    for (i, file_path) in files.iter().enumerate() {
        let file_name = file_path.file_name().unwrap().to_str().unwrap();
        info!("[{}/{}] Processing: {}", i + 1, files.len(), file_name);
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
    info!("  Total files: {}", files.len());
    info!("  Successful: {}", successful);
    info!("  Failed: {}", failed);

    if failed > 0 {
        error!("Some files failed. See errors above.");
        process::exit(1);
    } else {
        info!("All example files stratified successfully!");
    }
}
