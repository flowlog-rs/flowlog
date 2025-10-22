use std::process;

use args::{get_example_files, Args};
use catalog::rule::Catalog;
use clap::Parser;
use optimizer::Optimizer;
use parser::Program;
use stratifier::Stratifier;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

fn main() {
    // Initialize simple tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("trace")),
        )
        .init();

    // Parse command line arguments
    let args = Args::parse();

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

    // Optimize each stratum
    let mut optimizer = Optimizer::new();
    optimize_and_print(&mut optimizer, &stratifier);
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
        let is_planned = vec![false; catalogs.len()];
        // Plan the entire stratum
        let _ = optimizer.plan_stratum(&catalogs, is_planned);

        info!("{}", "=".repeat(80));
        info!(
            "[Stratum {}] {} rules (recursive: {})",
            stratum_idx,
            rule_refs.len(),
            is_recursive
        );
    }
}

/// Run optimizer on all example files in the example directory
fn run_all_examples() {
    let example_files = get_example_files();

    // Process all files
    info!("Running optimizer on {} example files...", example_files.len());
    let mut success_count = 0;
    let mut failure_count = 0;

    for file_path in &example_files {
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
                let is_planned = vec![false; catalogs.len()];
                let _ = optimizer.plan_stratum(&catalogs, is_planned);
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
    println!("  Total files: {}", example_files.len());
    println!("  Successful: {}", success_count);
    println!("  Failed: {}", failure_count);

    if failure_count > 0 {
        println!("\nSome files failed to optimize. Check the errors above for details.");
        process::exit(1);
    } else {
        println!("\nAll example files optimized successfully!");
    }
}
