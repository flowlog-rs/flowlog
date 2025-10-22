use clap::Parser;
use common::{get_example_files, Args, TestResult};
use parser::program::Program;
use tracing::info;
use tracing_subscriber::EnvFilter;

fn main() {
    // Initialize simple tracing
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .init();

    // Parse command line arguments
    let args = Args::parse();

    if args.should_process_all() {
        // Run parser on all example files
        run_all_examples();
    } else {
        // Parse single program file
        let program = Program::parse(args.program());
        info!("Success parse program\n{program}");
    }
}

fn run_all_examples() {
    let example_files = get_example_files();
    let mut formatter = TestResult::new("parser", example_files.len());

    for file_path in example_files.iter() {
        let file_name = file_path.file_name().unwrap().to_str().unwrap();

        match std::panic::catch_unwind(|| Program::parse(file_path.to_str().unwrap())) {
            Ok(program) => {
                let edbs = program.edbs();
                let idbs = program.idbs();
                let rules = program.rules();
                let stats = format!(
                    "rules={}, edbs={}, idbs={}",
                    rules.len(),
                    edbs.len(),
                    idbs.len()
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
