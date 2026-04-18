use catalog::rule::Catalog;
use clap::Parser;
use common::{emit_and_exit, get_example_files, Config, SourceMap, TestResult};
use parser::Program;
use tracing_subscriber::EnvFilter;

fn main() {
    let config = Config::parse();

    if config.should_process_all() {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::new("info"))
            .init();
        run_all_examples(&config);
        return;
    }

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("debug"))
        .init();

    let mut sm = SourceMap::new();
    let program = Program::parse(config.program(), config.is_extended(), &mut sm)
        .unwrap_or_else(|err| emit_and_exit(err, &sm));
    print_catalogs(&program);
}

fn print_catalogs(program: &Program) {
    for rule in program.rules().iter() {
        let _catalog = Catalog::from_rule(rule);
    }
}

fn run_all_examples(config: &Config) {
    let example_files = get_example_files();
    let mut formatter = TestResult::new("catalog", example_files.len());

    for file_path in example_files.iter() {
        let file_name = file_path.file_name().unwrap().to_str().unwrap();

        let mut sm = SourceMap::new();
        match Program::parse(file_path.to_str().unwrap(), config.is_extended(), &mut sm) {
            Ok(program) => {
                let stats = format!(
                    "rules={}, relations={}",
                    program.rules().len(),
                    program.relations().len()
                );
                formatter.report_success(file_name, Some(&stats));
            }
            Err(err) => {
                formatter.report_failure(file_name, Some(&err.to_string()));
            }
        }
    }

    formatter.finish();
}
