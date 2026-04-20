//! Integration tests for every user-reachable `CatalogError` variant.
//!
//! Each fixture under `tests/errors/*.dl` exercises one variant by
//! building a [`Catalog`] per rule and rendering the error via
//! [`common::diag::emit`].

use std::path::PathBuf;

use flowlog_build::catalog::{Catalog, CatalogError};
use flowlog_build::common::diag::{emit, BoxError};
use flowlog_build::common::source::SourceMap;
use flowlog_build::parser::Program;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures").join("catalog_errors")
        .join(name)
}

/// Parse `name` and build a catalog for each rule. Returns the first
/// catalog error encountered (or `Ok(())` if none).
fn catalog_for(name: &str) -> (Result<(), CatalogError>, SourceMap) {
    let mut sm = SourceMap::new();
    let path = fixture(name);
    let program = Program::parse(path.to_str().unwrap(), false, &mut sm)
        .expect("fixture should parse cleanly");
    let mut result = Ok(());
    for rule in program.rules() {
        if let Err(e) = Catalog::from_rule(rule) {
            result = Err(e);
            break;
        }
    }
    (result, sm)
}

fn render(err: CatalogError, sm: &SourceMap) -> String {
    let err: BoxError = err.into();
    let mut buf: Vec<u8> = Vec::new();
    emit(&err, sm, &mut buf).unwrap();
    String::from_utf8(buf).unwrap()
}

fn assert_unsafe(
    fixture: &str,
    expected_var: &str,
    predicate_substring: &str,
    kind_substring: &str,
) {
    let (res, sm) = catalog_for(fixture);
    let err = res.expect_err("expected CatalogError");
    let CatalogError::UnsafeVariable {
        ref var,
        ref predicate,
        ..
    } = err
    else {
        panic!("expected UnsafeVariable, got {err:?}");
    };
    assert_eq!(var, expected_var);
    assert!(
        predicate.contains(predicate_substring),
        "predicate `{predicate}` missing `{predicate_substring}`"
    );

    let out = render(err, &sm);
    assert!(out.contains("unsafe variable"), "got: {out}");
    assert!(out.contains(&format!("`{expected_var}`")), "got: {out}");
    assert!(out.contains(kind_substring), "got: {out}");
    assert!(out.contains(fixture), "got: {out}");
}

#[test]
fn unsafe_variable_in_negation() {
    assert_unsafe(
        "unsafe_variable_in_negation.dl",
        "other",
        "blocked",
        "negated atom",
    );
}

#[test]
fn unsafe_variable_in_comparison() {
    assert_unsafe("unsafe_variable_in_comparison.dl", "z", "z", "comparison");
}

#[test]
fn unsafe_variable_in_fn_call() {
    assert_unsafe(
        "unsafe_variable_in_fn_call.dl",
        "z",
        "is_positive",
        "function call",
    );
}
