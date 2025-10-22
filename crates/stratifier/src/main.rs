use clap::Parser;
use common::{get_example_files, AllResultsFormatter, Args};
use parser::Program;
use stratifier::{DependencyGraph, Stratifier};
use tracing::info;
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
    let mut formatter = AllResultsFormatter::new("stratifier", example_files.len());

    for file_path in example_files.iter() {
        let file_name = file_path.file_name().unwrap().to_str().unwrap();

        match std::panic::catch_unwind(|| Program::parse(file_path.to_str().unwrap())) {
            Ok(program) => {
                let stratifier = Stratifier::from_program(&program);
                let recursive_cnt = stratifier
                    .is_recursive_stratum_bitmap()
                    .iter()
                    .filter(|b| **b)
                    .count();
                let stats = format!(
                    "rules={}, strata={}, recursive={}",
                    program.rules().len(),
                    stratifier.is_recursive_stratum_bitmap().len(),
                    recursive_cnt
                );
                formatter.report_success(file_name, Some(&stats));
            }
            Err(panic_info) => {
                let error_msg = if let Some(s) = panic_info.downcast_ref::<String>() {
                    s.clone()
                } else if let Some(s) = panic_info.downcast_ref::<&str>() {
                    s.to_string()
                } else {
                    "Unknown panic occurred".to_string()
                };
                formatter.report_failure(file_name, Some(&error_msg));
            }
        }
    }

    formatter.finish();
}
