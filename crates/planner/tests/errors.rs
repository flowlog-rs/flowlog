//! Integration tests for every user-reachable `PlanError` variant.
//!
//! Each fixture under `tests/errors/*.dl` exercises one variant. We
//! (a) parse and stratify successfully, (b) run the stratum planner and
//! assert the right variant is returned, and (c) render via
//! `common::diag::emit` and check the output cites the offending program
//! element.

use std::path::PathBuf;

use common::diag::{emit, BoxError};
use common::{Config, ExecutionMode, SourceMap};
use optimizer::Optimizer;
use parser::Program;
use planner::{PlanError, StratumPlanner};
use stratifier::Stratifier;

fn test_config(program: &str) -> Config {
    Config {
        program: program.to_string(),
        fact_dir: None,
        executable_path: None,
        output_dir: Some("-".to_string()),
        mode: ExecutionMode::DatalogBatch,
        profile: false,
        sip: false,
        str_intern: false,
        udf_file: None,
        save_temps: false,
        include_dirs: Vec::new(),
    }
}

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("errors")
        .join(name)
}

fn plan_fixture(name: &str) -> (Result<(), PlanError>, SourceMap) {
    let mut sm = SourceMap::new();
    let path = fixture(name);
    let program = Program::parse(path.to_str().unwrap(), false, &mut sm)
        .expect("fixture should parse cleanly");
    let stratifier =
        Stratifier::from_program(&program, false).expect("fixture should stratify cleanly");

    let config = test_config(path.to_str().unwrap());
    let mut optimizer = Optimizer::new();
    let mut profiler = None;

    for (idx, rule_refs) in stratifier.stratum().iter().enumerate() {
        let rules: Vec<_> = rule_refs.iter().map(|r| (*r).clone()).collect();
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

fn render(err: PlanError, sm: &SourceMap) -> String {
    let err: BoxError = err.into();
    let mut buf: Vec<u8> = Vec::new();
    emit(&err, sm, &mut buf).unwrap();
    String::from_utf8(buf).unwrap()
}

#[test]
fn unknown_head_variable_variant_and_render() {
    let (res, sm) = plan_fixture("unknown_head_variable.dl");
    let err = res.unwrap_err();
    let PlanError::UnknownHeadVariable { ref var, .. } = err else {
        panic!("expected UnknownHeadVariable, got {err:?}");
    };
    assert_eq!(var, "salutation");
    let out = render(err, &sm);
    assert!(out.contains("unknown head variable"), "got: {out}");
    assert!(out.contains("salutation"), "got: {out}");
    assert!(out.contains("never bound"), "got: {out}");
}

#[test]
fn multiple_aggregations_in_head_variant_and_render() {
    let (res, sm) = plan_fixture("multiple_aggregations_in_head.dl");
    let err = res.unwrap_err();
    let PlanError::MultipleAggregationsInHead { ref rel, count, .. } = err else {
        panic!("expected MultipleAggregationsInHead, got {err:?}");
    };
    assert_eq!(rel, "totals");
    assert_eq!(count, 2);
    let out = render(err, &sm);
    assert!(out.contains("contains 2 aggregations"), "got: {out}");
    assert!(out.contains("at most one is allowed"), "got: {out}");
}

#[test]
fn inconsistent_aggregation_variant_and_render() {
    let (res, sm) = plan_fixture("inconsistent_aggregation.dl");
    let err = res.unwrap_err();
    let PlanError::InconsistentAggregation { ref rel, .. } = err else {
        panic!("expected InconsistentAggregation, got {err:?}");
    };
    assert_eq!(rel, "totals");
    let out = render(err, &sm);
    assert!(out.contains("inconsistent aggregation"), "got: {out}");
    assert!(out.contains("totals"), "got: {out}");
    assert!(out.contains("conflicting aggregation"), "got: {out}");
}
