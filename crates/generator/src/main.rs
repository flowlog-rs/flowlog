use std::{fs, path::Path, process};

use args::Args;
use clap::Parser;
use generator::Generator;
use optimizer::Optimizer;
use parser::Program;
use planner::StratumPlanner;
use stratifier::Stratifier;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

fn main() {
    // Initialize simple tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Parse command line arguments with clap
    let args = Args::parse();

    // Backward-compat convenience: allow `--program all` or `--program --all`
    if args.should_process_all() {
        run_all_examples();
        return;
    }

    // Parse and process single file
    let program = Program::parse(args.program());
    info!("Parsed program: {} rules", program.rules().len());

    // Stratify the program
    let stratifier = Stratifier::from_program(&program);
    info!(
        "Stratified program into {} strata",
        stratifier.stratum().len()
    );

    // Plan each stratum using StratumPlanner then hand transformations to generator
    let mut optimizer = Optimizer::new();
    let generator = Generator::new();
    generate_program(&generator, &mut optimizer, &stratifier);
}

fn generate_program(generator: &Generator, optimizer: &mut Optimizer, stratifier: &Stratifier) {
    for (stratum_idx, rule_refs) in stratifier.stratum().iter().enumerate() {
        let is_recursive = stratifier.is_recursive_stratum(stratum_idx);

        // Clone rules into a local Vec to satisfy StratumPlanner signature
        let rules: Vec<_> = rule_refs.iter().map(|r| (*r).clone()).collect();
        let sp = StratumPlanner::from_rules(&rules, optimizer);

        info!("{}", "=".repeat(80));
        info!(
            "[Stratum {}] {} rules (recursive: {})",
            stratum_idx,
            rule_refs.len(),
            is_recursive
        );
        // Generate code for the deduplicated transformation list for this stratum
        generator.generate(sp.transformations());
    }
}

/// Run generator on all example files in the example directory
fn run_all_examples() {
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
    info!("Running generator on {} example files...", files.len());
    let mut success_count = 0;
    let mut failure_count = 0;

    for file_path in &files {
        let file_name = file_path.file_name().unwrap().to_str().unwrap();

        match std::panic::catch_unwind(|| {
            let program = Program::parse(file_path.to_str().unwrap());
            let stratifier = Stratifier::from_program(&program);
            let mut optimizer = Optimizer::new();
            let generator = Generator::new();

            for rule_refs in stratifier.stratum().iter() {
                let rules: Vec<_> = rule_refs.iter().map(|r| (*r).clone()).collect();
                let sp = StratumPlanner::from_rules(&rules, &mut optimizer);
                generator.generate(sp.transformations());
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
