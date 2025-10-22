use catalog::rule::Catalog;
use clap::Parser;
use common::{get_example_files, AllResultsFormatter, Args};
use optimizer::Optimizer;
use parser::Program;
use stratifier::Stratifier;
use tracing::info;
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
    let mut formatter = AllResultsFormatter::new("optimizer", example_files.len());

    for file_path in &example_files {
        let file_name = file_path.file_stem().unwrap().to_str().unwrap();

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
