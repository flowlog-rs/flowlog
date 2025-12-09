use catalog::rule::Catalog;
use clap::Parser;
use common::{get_example_files, Config, TestResult};
use parser::Program;
use tracing_subscriber::EnvFilter;

fn main() {
    let config = Config::parse();

    if config.should_process_all() {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::new("info"))
            .init();
        run_all_examples();
        return;
    }

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("debug"))
        .init();

    let program = Program::parse(config.program());
    print_catalogs(&program);
}

fn print_catalogs(program: &Program) {
    for rule in program.rules().iter() {
        let _catalog = Catalog::from_rule(rule);
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
