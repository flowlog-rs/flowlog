use catalog::rule::Catalog;
use clap::Parser;
use common::{get_example_files, Args, TestResult};
use parser::Program;
use tracing::info;
use tracing_subscriber::EnvFilter;

fn main() {
    // Initialize tracing similar to parser/stratifier mains
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .init();

    let args = Args::parse();

    if args.should_process_all() {
        run_all_examples();
        return;
    }

    let program = Program::parse(args.program());
    info!(
        "Parsed program: {} rules ({} EDBs, {} IDBs)",
        program.rules().len(),
        program.edbs().len(),
        program.idbs().len()
    );
    print_catalogs(&program);
}

fn print_catalogs(program: &Program) {
    for (i, rule) in program.rules().iter().enumerate() {
        info!("{}", "-".repeat(80));
        info!("[Rule {}] {}", i, rule.head().name());
        let catalog = Catalog::from_rule(rule);
        info!("{}", catalog);
    }
}

fn run_all_examples() {
    let example_files = get_example_files();
    let mut formatter = TestResult::new("catalog", example_files.len());

    for file_path in example_files.iter() {
        let file_name = file_path.file_name().unwrap().to_str().unwrap();

        match std::panic::catch_unwind(|| Program::parse(file_path.to_str().unwrap())) {
            Ok(program) => {
                let stats = format!(
                    "rules={}, edbs={}, idbs={}",
                    program.rules().len(),
                    program.edbs().len(),
                    program.idbs().len()
                );
                formatter.report_success(file_name, Some(&stats));
            }
            Err(_panic_info) => {
                formatter.report_failure(file_name, None);
            }
        }
    }

    formatter.finish();
}
