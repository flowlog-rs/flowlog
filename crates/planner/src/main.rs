use clap::Parser;
use common::{get_example_files, Args, TestResult};
use optimizer::Optimizer;
use parser::Program;
use planner::StratumPlanner;
use stratifier::Stratifier;
use tracing_subscriber::EnvFilter;

fn main() {
    // Parse command line arguments
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

    // Plan each stratum using StratumPlanner
    let mut optimizer = Optimizer::new();
    plan_and_print(&mut optimizer, &stratifier);
}

/// Plan and print results for each stratum in the stratified program
fn plan_and_print(optimizer: &mut Optimizer, stratifier: &Stratifier) {
    for (stratum_idx, rule_refs) in stratifier.stratum().iter().enumerate() {
        let is_recursive = stratifier.is_recursive_stratum(stratum_idx);

        // Clone rules into a local Vec to satisfy StratumPlanner signature
        let rules: Vec<_> = rule_refs.iter().map(|r| (*r).clone()).collect();

        let _stratum_planner = StratumPlanner::from_rules(
            &rules,
            optimizer,
            is_recursive,
            stratifier.stratum_iterative_relation(stratum_idx),
            stratifier.stratum_leave_relation(stratum_idx),
        );
    }
}

/// Run planner on all example files in the example directory
fn run_all_examples() {
    let example_files = get_example_files();
    let mut formatter = TestResult::new("planner", example_files.len());

    for file_path in &example_files {
        let file_name = file_path.file_name().unwrap().to_str().unwrap();

        match std::panic::catch_unwind(|| {
            let program = Program::parse(file_path.to_str().unwrap());
            let stratifier = Stratifier::from_program(&program);
            let mut optimizer = Optimizer::new();

            // Run stratum planner without printing details
            for (stratum_idx, rule_refs) in stratifier.stratum().iter().enumerate() {
                let is_recursive = stratifier.is_recursive_stratum(stratum_idx);
                let rules: Vec<_> = rule_refs.iter().map(|r| (*r).clone()).collect();
                let _sp = StratumPlanner::from_rules(
                    &rules,
                    &mut optimizer,
                    is_recursive,
                    stratifier.stratum_iterative_relation(stratum_idx),
                    stratifier.stratum_leave_relation(stratum_idx),
                );
            }

            (program.rules().len(), stratifier.stratum().len())
        }) {
            Ok((rule_count, strata_count)) => {
                let stats = format!("rules={}, strata={}", rule_count, strata_count);
                formatter.report_success(file_name, Some(&stats));
            }
            Err(_) => {
                formatter.report_failure(file_name, None);
            }
        }
    }

    formatter.finish();
}
