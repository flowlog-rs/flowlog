use clap::Parser;
use common::{emit_and_exit, get_example_files, Config, SourceMap, TestResult};
use parser::Program;
use stratifier::Stratifier;
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
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .init();

    let mut sm = SourceMap::new();
    let program = Program::parse(config.program(), config.is_extended(), &mut sm)
        .unwrap_or_else(|err| emit_and_exit(err, &sm));
    let _stratifier = Stratifier::from_program(&program, config.is_extended())
        .unwrap_or_else(|err| emit_and_exit(err, &sm));
}

fn run_all_examples(config: &Config) {
    let example_files = get_example_files();
    let mut formatter = TestResult::new("stratifier", example_files.len());

    for file_path in example_files.iter() {
        let file_name = file_path.file_name().unwrap().to_str().unwrap();

        let mut sm = SourceMap::new();
        let program =
            match Program::parse(file_path.to_str().unwrap(), config.is_extended(), &mut sm) {
                Ok(p) => p,
                Err(err) => {
                    formatter.report_failure(file_name, Some(&err.to_string()));
                    continue;
                }
            };
        match Stratifier::from_program(&program, config.is_extended()) {
            Ok(stratifier) => {
                let recursive_cnt = stratifier
                    .is_recursive_stratum_bitmap()
                    .iter()
                    .filter(|b| **b)
                    .count();
                let stats = format!(
                    "rules={}, strata={}, recursive={}",
                    program.rules().len(),
                    stratifier.is_recursive_stratum_bitmap().len(),
                    recursive_cnt,
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
