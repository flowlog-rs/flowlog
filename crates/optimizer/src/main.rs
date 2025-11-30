use catalog::rule::Catalog;
use clap::Parser;
use common::{get_example_files, Args, TestResult};
use optimizer::Optimizer;
use parser::Program;
use stratifier::Stratifier;
use tracing_subscriber::EnvFilter;

fn main() {
    let args = Args::parse();

    if args.should_process_all() {
        tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
            )
            .init();
        run_all_examples();
        return;
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug")),
        )
        .init();

    // Parse and process single file
    let program = Program::parse(args.program());

    // Stratify the program
    let stratifier = Stratifier::from_program(&program);

    // Optimize each stratum
    let mut optimizer = Optimizer::new();
    optimize_and_print(&mut optimizer, &stratifier);
}

/// Optimize and print results for each stratum in the stratified program
fn optimize_and_print(optimizer: &mut Optimizer, stratifier: &Stratifier) {
    for rules in stratifier.stratum().iter() {
        // Create catalogs for all rules in this stratum
        let catalogs: Vec<Catalog> = rules.iter().map(|rule| Catalog::from_rule(rule)).collect();
        // Plan the entire stratum
        let _ = optimizer.plan_stratum(&catalogs);
    }
}

/// Run optimizer on all example files in the example directory
fn run_all_examples() {
    let example_files = get_example_files();
    let mut formatter = TestResult::new("optimizer", example_files.len());

    for file_path in &example_files {
        let file_name = file_path.file_stem().unwrap().to_str().unwrap();

        match std::panic::catch_unwind(|| {
            let program = Program::parse(file_path.to_str().unwrap());
            let stratifier = Stratifier::from_program(&program);
            let optimizer = Optimizer::new();

            // Just run optimization without printing details
            for rules in stratifier.stratum().iter() {
                let catalogs: Vec<Catalog> =
                    rules.iter().map(|rule| Catalog::from_rule(rule)).collect();
                let _ = optimizer.plan_stratum(&catalogs);
            }

            (program.rules().len(), stratifier.stratum().len())
        }) {
            Ok((rule_count, strata_count)) => {
                let stats = format!("rules={}, strata={}", rule_count, strata_count);
                formatter.report_success(file_name, Some(&stats));
            }
            Err(_) => {
                formatter.report_failure(file_name, Some("Optimization failed"));
            }
        }
    }

    formatter.finish();
}
