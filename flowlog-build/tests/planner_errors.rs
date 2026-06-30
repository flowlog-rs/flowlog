mod errors;

use errors::fixture;
use errors::render;
use flowlog_build::optimizer::Optimizer;
use flowlog_build::planner::PlanError;
use flowlog_build::planner::StratumPlanner;
use flowlog_build::stratifier::Stratifier;
use flowlog_common::Config;
use flowlog_common::SourceMap;
use flowlog_parser::Program;

fn plan_fixture(name: &str) -> (Result<(), PlanError>, SourceMap) {
    let mut sm = SourceMap::new();
    let path = fixture("planner", name);
    let program = Program::parse(&path, false, &[], &mut sm).expect("fixture should parse cleanly");
    let stratifier =
        Stratifier::from_program(&program, false).expect("fixture should stratify cleanly");

    let config = Config {
        program: path,
        output_to_stdout: true,
        ..Default::default()
    };
    let mut optimizer = Optimizer::new();
    let mut profiler = None;

    for (idx, rule_refs) in stratifier.stratum().iter().enumerate() {
        let rules: Vec<_> = rule_refs.iter().map(|&r| r.clone()).collect();
        if let Err(e) = StratumPlanner::from_rules(
            &config,
            &rules,
            &mut optimizer,
            &mut profiler,
            &stratifier,
            idx,
        ) {
            return (Err(e), sm);
        }
    }
    (Ok(()), sm)
}

#[test]
fn unknown_head_variable() {
    assert_err!(
        plan_fixture("unknown_head_variable.dl"),
        PlanError::UnknownHeadVariable { var, .. } if var == "salutation",
        ["unknown head variable", "salutation", "never bound"]
    );
}

#[test]
fn multiple_aggregations_in_head() {
    assert_err!(
        plan_fixture("multiple_aggregations_in_head.dl"),
        PlanError::MultipleAggregationsInHead { rel, count: 2, .. } if rel == "Totals",
        ["contains 2 aggregations", "at most one is allowed"]
    );
}

#[test]
fn inconsistent_aggregation() {
    assert_err!(
        plan_fixture("inconsistent_aggregation.dl"),
        PlanError::InconsistentAggregation { .. },
        [
            "inconsistent aggregation",
            "Totals",
            "conflicting aggregation"
        ]
    );
}
