use clap::Parser;

use common::{emit_and_exit, get_example_files, Config, SourceMap, TestResult};
use optimizer::Optimizer;
use parser::Program;
use planner::{PlanError, StratumPlanner};
use profiler::Profiler;
use stratifier::Stratifier;
use tracing_subscriber::EnvFilter;

fn main() {
    // Parse command line arguments
    let config = Config::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .init();

    if config.should_process_all() {
        run_all_examples(&config);
        return;
    }

    let mut sm = SourceMap::new();
    let program = Program::parse(config.program(), config.is_extended(), &mut sm)
        .unwrap_or_else(|err| emit_and_exit(err, &sm));

    // Stratify the program
    let stratifier = Stratifier::from_program(&program, config.is_extended())
        .unwrap_or_else(|err| emit_and_exit(err, &sm));

    // Plan each stratum using StratumPlanner
    let mut optimizer = Optimizer::new();

    // Profiler to collect profiling data
    let mut profiler = if config.profiling_enabled() {
        Some(Profiler::default())
    } else {
        None
    };

    if let Err(err) = plan_and_print(&config, &mut optimizer, &stratifier, &mut profiler) {
        emit_and_exit(err, &sm);
    }
}

/// Plan and print results for each stratum in the stratified program
fn plan_and_print(
    config: &Config,
    optimizer: &mut Optimizer,
    stratifier: &Stratifier,
    profiler: &mut Option<Profiler>,
) -> Result<(), PlanError> {
    for (stratum_idx, rule_refs) in stratifier.stratum().iter().enumerate() {
        let rules: Vec<_> = rule_refs.iter().map(|r| (*r).clone()).collect();

        let _stratum_planner = StratumPlanner::from_rules(
            config,
            &rules,
            optimizer,
            profiler,
            stratifier,
            stratum_idx,
        )?;
    }
    Ok(())
}

/// Run planner on all example files in the example directory
fn run_all_examples(config: &Config) {
    let example_files = get_example_files();
    let mut formatter = TestResult::new("planner", example_files.len());

    for file_path in &example_files {
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
        let stratifier = match Stratifier::from_program(&program, config.is_extended()) {
            Ok(s) => s,
            Err(err) => {
                formatter.report_failure(file_name, Some(&err.to_string()));
                continue;
            }
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut optimizer = Optimizer::new();
            let mut profiler = if config.profiling_enabled() {
                Some(Profiler::default())
            } else {
                None
            };

            for (stratum_idx, rule_refs) in stratifier.stratum().iter().enumerate() {
                let rules: Vec<_> = rule_refs.iter().map(|r| (*r).clone()).collect();
                StratumPlanner::from_rules(
                    config,
                    &rules,
                    &mut optimizer,
                    &mut profiler,
                    &stratifier,
                    stratum_idx,
                )?;
            }

            Ok::<_, PlanError>((program.rules().len(), stratifier.stratum().len()))
        }));
        match result {
            Ok(Ok((rule_count, strata_count))) => {
                let stats = format!("rules={}, strata={}", rule_count, strata_count);
                formatter.report_success(file_name, Some(&stats));
            }
            Ok(Err(err)) => {
                formatter.report_failure(file_name, Some(&err.to_string()));
            }
            Err(_) => {
                formatter.report_failure(file_name, None);
            }
        }
    }

    formatter.finish();
}
