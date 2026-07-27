mod errors;

use errors::fixture;
use errors::render;
use flowlog_common::Config;
use flowlog_common::ExecutionMode;
use flowlog_common::SourceMap;
use flowlog_planner::stratifier::Stratifier;
use flowlog_planner::stratifier::StratifyError;

/// Parse + stratify a fixture. `extended` selects Datalog mode. Parsing runs
/// the full pipeline (typecheck/fold/prune), matching what the compiler feeds
/// the stratifier.
fn stratify(name: &str, extended: bool) -> (Result<Stratifier, StratifyError>, SourceMap) {
    let mut sm = SourceMap::new();
    let mut config = Config {
        mode: if extended {
            ExecutionMode::ExtendBatch
        } else {
            ExecutionMode::DatalogBatch
        },
        ..Default::default()
    };
    let program = flowlog_parser::parse(&fixture("stratifier", name), &[], &mut sm, &mut config)
        .expect("fixture should parse cleanly");
    let res = Stratifier::from_program(&program, extended);
    (res, sm)
}

#[test]
fn recursion_outside_loop() {
    assert_err!(
        stratify("recursion_outside_loop.dl", true),
        StratifyError::RecursionOutsideLoop { .. },
        ["recursive rules must be inside", "fixpoint"]
    );
}

#[test]
fn forward_reference() {
    assert_err!(
        stratify("forward_reference.dl", true),
        StratifyError::ForwardReference { rel, .. } if rel == "B",
        ["not yet defined", "later segment"]
    );
}

#[test]
fn recursive_stratum_empty() {
    assert_err!(
        stratify("recursive_stratum_empty.dl", true),
        StratifyError::RecursiveStratumEmpty { stratum: 1, .. },
        ["no recursive relations", "body atom"]
    );
}

#[test]
fn iterative_not_in_loop_head() {
    assert_err!(
        stratify("iterative_not_in_loop_head.dl", true),
        StratifyError::IterativeNotInLoopHead { rel, .. } if rel == "ghost",
        ["`iterative` relation `ghost`", "head"]
    );
}

#[test]
fn iterative_not_recursive() {
    assert_err!(
        stratify("iterative_not_recursive.dl", true),
        StratifyError::IterativeNotRecursive { rel, .. } if rel == "sink",
        ["not recursive in this loop", "iterative"]
    );
}

#[test]
fn loop_condition_not_recursive() {
    assert_err!(
        stratify("loop_condition_invalid.dl", true),
        StratifyError::LoopConditionNotRecursive { rel, .. } if rel == "done",
        [
            "loop until condition",
            "done",
            "does not depend on any recursive relation",
        ]
    );
}
