use std::{env, fs, path::Path, process};

use catalog::rule::Catalog;
use optimizer::{Optimizer, PlanTree};
use parser::Program;
use stratifier::Stratifier;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .init();

    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        print_usage(&args[0]);
        process::exit(1);
    }

    let argument = &args[1];
    if argument == "--all" {
        run_all_examples();
        return;
    }

    // Parse and process single file
    let program = Program::parse(argument);
    info!("Parsed program: {} rules", program.rules().len());

    // Stratify the program
    let stratifier = Stratifier::from_program(&program);
    info!(
        "Stratified program into {} strata",
        stratifier.stratum().len()
    );

    // Optimize each stratum
    let mut optimizer = Optimizer::new();
    optimize_and_print(&mut optimizer, &stratifier);
}

/// Print usage information
fn print_usage(program_name: &str) {
    eprintln!("Usage: {} <program_file>", program_name);
    eprintln!("       {} --all", program_name);
    eprintln!(
        "Examples:\n  {} ./example/reach.dl\n  {} --all",
        program_name, program_name
    );
}

/// Optimize and print results for each stratum in the stratified program
fn optimize_and_print(optimizer: &mut Optimizer, stratifier: &Stratifier) {
    for (stratum_idx, rule_refs) in stratifier.stratum().iter().enumerate() {
        let is_recursive = stratifier.is_recursive_stratum(stratum_idx);

        // Create catalogs for all rules in this stratum
        let catalogs: Vec<Catalog> = rule_refs
            .iter()
            .map(|rule| Catalog::from_rule(rule))
            .collect();

        let catalog_refs: Vec<&Catalog> = catalogs.iter().collect();

        // Plan the entire stratum
        let plans: Vec<PlanTree> = optimizer.plan_stratum(catalog_refs);

        info!("{}", "=".repeat(80));
        info!(
            "[Stratum {}] {} rules (recursive: {})",
            stratum_idx,
            rule_refs.len(),
            is_recursive
        );

        for (rule_idx, (rule, plan)) in rule_refs.iter().zip(plans.iter()).enumerate() {
            info!("{}", "-".repeat(40));
            info!("[Rule {}] {}", rule_idx, rule);
            info!("Plan tree:\n{}", plan);
        }
    }
}

/// Run optimizer on all example files in the example directory
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
    info!("Running optimizer on {} example files...", files.len());
    let mut success_count = 0;
    let mut failure_count = 0;

    for file_path in &files {
        let file_name = file_path.file_name().unwrap().to_str().unwrap();

        match std::panic::catch_unwind(|| {
            let program = Program::parse(file_path.to_str().unwrap());
            let stratifier = Stratifier::from_program(&program);
            let optimizer = Optimizer::new();

            // Just run optimization without printing details
            for rule_refs in stratifier.stratum().iter() {
                let catalogs: Vec<Catalog> = rule_refs
                    .iter()
                    .map(|rule| Catalog::from_rule(rule))
                    .collect();
                let catalog_refs: Vec<&Catalog> = catalogs.iter().collect();
                let _plans: Vec<PlanTree> = optimizer.plan_stratum(catalog_refs);
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
        println!("\nSome files failed to optimize. Check the errors above for details.");
        process::exit(1);
    } else {
        println!("\nAll example files optimized successfully!");
    }
}
