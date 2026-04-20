//! Integration tests for every user-reachable `StratifyError` variant.
//!
//! Each fixture under `tests/errors/*.dl` exercises one variant. We
//! (a) parse successfully, (b) assert the stratifier returns the right
//! variant, and (c) render via `common::diag::emit` and check the output
//! cites the offending program element.

use std::path::PathBuf;

use flowlog_build::common::diag::{emit, BoxError};
use flowlog_build::common::source::SourceMap;
use flowlog_build::parser::Program;
use flowlog_build::stratifier::{Stratifier, StratifyError};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures").join("stratifier_errors")
        .join(name)
}

/// Parse + stratify a fixture. `extended` selects Datalog mode.
fn stratify(name: &str, extended: bool) -> (Result<Stratifier, StratifyError>, SourceMap) {
    let mut sm = SourceMap::new();
    let path = fixture(name);
    let program = Program::parse(path.to_str().unwrap(), extended, &mut sm)
        .expect("fixture should parse cleanly");
    let res = Stratifier::from_program(&program, extended);
    (res, sm)
}

fn render(err: StratifyError, sm: &SourceMap) -> String {
    let err: BoxError = err.into();
    let mut buf: Vec<u8> = Vec::new();
    emit(&err, sm, &mut buf).unwrap();
    String::from_utf8(buf).unwrap()
}

#[test]
fn recursion_outside_loop_variant_and_render() {
    let (res, sm) = stratify("recursion_outside_loop.dl", true);
    let err = res.unwrap_err();
    assert!(
        matches!(err, StratifyError::RecursionOutsideLoop { .. }),
        "got {err:?}"
    );
    let out = render(err, &sm);
    assert!(out.contains("recursive rules must be inside"), "got: {out}");
    assert!(out.contains("fixpoint"), "got: {out}");
}

#[test]
fn forward_reference_variant_and_render() {
    let (res, sm) = stratify("forward_reference.dl", true);
    let err = res.unwrap_err();
    let StratifyError::ForwardReference { ref rel, .. } = err else {
        panic!("expected ForwardReference, got {err:?}");
    };
    assert_eq!(rel, "b");
    let out = render(err, &sm);
    assert!(out.contains("not yet defined"), "got: {out}");
    assert!(out.contains("later segment"), "got: {out}");
}

#[test]
fn recursive_stratum_empty_variant_and_render() {
    let (res, sm) = stratify("recursive_stratum_empty.dl", true);
    let err = res.unwrap_err();
    assert!(
        matches!(err, StratifyError::RecursiveStratumEmpty { stratum: 1, .. }),
        "got {err:?}"
    );
    let out = render(err, &sm);
    assert!(out.contains("no recursive relations"), "got: {out}");
    assert!(out.contains("body atom"), "got: {out}");
}

#[test]
fn iterative_not_in_loop_head_variant_and_render() {
    let (res, sm) = stratify("iterative_not_in_loop_head.dl", true);
    let err = res.unwrap_err();
    let StratifyError::IterativeNotInLoopHead { ref rel, .. } = err else {
        panic!("expected IterativeNotInLoopHead, got {err:?}");
    };
    assert_eq!(rel, "ghost");
    let out = render(err, &sm);
    assert!(out.contains("`iterative` relation `ghost`"), "got: {out}");
    assert!(out.contains("head"), "got: {out}");
}

#[test]
fn iterative_not_recursive_variant_and_render() {
    let (res, sm) = stratify("iterative_not_recursive.dl", true);
    let err = res.unwrap_err();
    let StratifyError::IterativeNotRecursive { ref rel, .. } = err else {
        panic!("expected IterativeNotRecursive, got {err:?}");
    };
    assert_eq!(rel, "sink");
    let out = render(err, &sm);
    assert!(out.contains("not recursive in this loop"), "got: {out}");
    assert!(out.contains("iterative"), "got: {out}");
}

#[test]
fn loop_condition_not_recursive_variant_and_render() {
    let (res, sm) = stratify("loop_condition_invalid.dl", true);
    let err = res.unwrap_err();
    let StratifyError::LoopConditionNotRecursive { ref rel, .. } = err else {
        panic!("expected LoopConditionNotRecursive, got {err:?}");
    };
    assert_eq!(rel, "done");
    let out = render(err, &sm);
    assert!(out.contains("loop until condition"), "got: {out}");
    assert!(out.contains("done"), "got: {out}");
    assert!(
        out.contains("does not depend on any recursive relation"),
        "got: {out}"
    );
}
