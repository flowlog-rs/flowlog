use std::{env, fs, path::Path, process};

use optimizer::Optimizer;
use parser::Program;
use planner::StratumPlanner;
use stratifier::Stratifier;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

fn main() {
    // Initialize simple tracing
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
    if argument == "--all" || argument == "all" {
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

    // Plan each stratum using StratumPlanner
    let mut optimizer = Optimizer::new();
    plan_and_print(&mut optimizer, &stratifier);
}

/// Print usage information
fn print_usage(program_name: &str) {
    info!("Usage: {} <program_file>", program_name);
    info!("       {} --all", program_name);
    info!(
        "Examples:\n  {} ./example/reach.dl\n  {} --all",
        program_name, program_name
    );
}

/// Plan and print results for each stratum in the stratified program
fn plan_and_print(optimizer: &mut Optimizer, stratifier: &Stratifier) {
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

        info!("Per-rule transformations:");
        for (rule_idx, txs) in sp.rule_transformation_infos().iter().enumerate() {
            // Print the original rule text for context
            if let Some(rule) = rules.get(rule_idx) {
                info!("  - Rule {}: {}", rule_idx, rule);
            } else {
                info!("  - Rule {}:", rule_idx);
            }
            info!("    {} transformations", txs.len());
            for (i, t) in txs.iter().enumerate() {
                info!("\n[{:>3}] {}", i, t);
            }
        }
    }
}

/// Run planner on all example files in the example directory
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
    info!("Running planner on {} example files...", files.len());
    let mut success_count = 0;
    let mut failure_count = 0;

    for file_path in &files {
        let file_name = file_path.file_name().unwrap().to_str().unwrap();

        match std::panic::catch_unwind(|| {
            let program = Program::parse(file_path.to_str().unwrap());
            let stratifier = Stratifier::from_program(&program);
            let mut optimizer = Optimizer::new();

            // Run stratum planner without printing details
            for rule_refs in stratifier.stratum().iter() {
                let rules: Vec<_> = rule_refs.iter().map(|r| (*r).clone()).collect();
                let _sp = StratumPlanner::from_rules(&rules, &mut optimizer);
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
    info!("\n{}", "=".repeat(80));
    info!("SUMMARY:");
    info!("  Total files: {}", files.len());
    info!("  Successful: {}", success_count);
    info!("  Failed: {}", failure_count);

    if failure_count > 0 {
        info!("\nSome files failed to plan. Check the errors above for details.");
        process::exit(1);
    } else {
        info!("\nAll example files planned successfully!");
    }
}
