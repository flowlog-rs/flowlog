//! Integration tests for every user-reachable `ParseError` variant.
//!
//! Each fixture under `tests/fixtures/bad/*.dl` exercises one variant. We
//! (a) assert the right variant comes back, and (b) run `common::diag::emit`
//! and check the rendered text cites the offending line and message.

use std::path::PathBuf;

use common::diag::{emit, BoxError};
use common::source::SourceMap;
use parser::error::DirectiveKind;
use parser::{ParseError, Program};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("bad")
        .join(name)
}

/// Parse a fixture in extended mode (accepts `fixpoint`/`loop` blocks).
fn parse_extended(name: &str) -> (Result<Program, ParseError>, SourceMap) {
    let mut sm = SourceMap::new();
    let path = fixture(name);
    let res = Program::parse(path.to_str().unwrap(), true, &mut sm);
    (res, sm)
}

/// Parse a fixture in standard (non-extended) mode.
fn parse_standard(name: &str) -> (Result<Program, ParseError>, SourceMap) {
    let mut sm = SourceMap::new();
    let path = fixture(name);
    let res = Program::parse(path.to_str().unwrap(), false, &mut sm);
    (res, sm)
}

fn render(err: ParseError, sm: &SourceMap) -> String {
    let err: BoxError = err.into();
    let mut buf: Vec<u8> = Vec::new();
    emit(&err, sm, &mut buf).unwrap();
    String::from_utf8(buf).unwrap()
}

#[test]
fn duplicate_decl_variant_and_render() {
    let (res, sm) = parse_extended("duplicate_decl.dl");
    let err = res.unwrap_err();
    assert!(
        matches!(err, ParseError::DuplicateDecl { .. }),
        "got {err:?}"
    );
    let out = render(err, &sm);
    assert!(out.contains("duplicate declaration"), "got: {out}");
    assert!(out.contains("redeclared here"), "got: {out}");
    assert!(out.contains("first declared here"), "got: {out}");
    assert!(out.contains("edge"), "got: {out}");
}

#[test]
fn duplicate_attribute_variant_and_render() {
    let (res, sm) = parse_extended("duplicate_attribute.dl");
    let err = res.unwrap_err();
    assert!(
        matches!(err, ParseError::DuplicateAttribute { .. }),
        "got {err:?}"
    );
    let out = render(err, &sm);
    assert!(out.contains("duplicate attribute"), "got: {out}");
    assert!(out.contains("first declared here"), "got: {out}");
    assert!(out.contains("edge"), "got: {out}");
}

#[test]
fn duplicate_directive_variant_and_render() {
    let (res, sm) = parse_extended("duplicate_directive.dl");
    let err = res.unwrap_err();
    let ParseError::DuplicateDirective { kind, .. } = &err else {
        panic!("expected DuplicateDirective, got {err:?}");
    };
    assert_eq!(*kind, DirectiveKind::Output);
    let out = render(err, &sm);
    assert!(out.contains("duplicate .output"), "got: {out}");
    assert!(out.contains("first directive here"), "got: {out}");
}

#[test]
fn undeclared_in_directive_variant_and_render() {
    let (res, sm) = parse_extended("undeclared_in_directive.dl");
    let err = res.unwrap_err();
    assert!(
        matches!(err, ParseError::UndeclaredInDirective { .. }),
        "got {err:?}"
    );
    let out = render(err, &sm);
    assert!(out.contains(".output"), "got: {out}");
    assert!(out.contains("undeclared"), "got: {out}");
    assert!(out.contains("missing_rel"), "got: {out}");
}

#[test]
fn undeclared_iterative_variant_and_render() {
    let (res, sm) = parse_extended("undeclared_iterative.dl");
    let err = res.unwrap_err();
    assert!(
        matches!(err, ParseError::UndeclaredInIterativeList { .. }),
        "got {err:?}"
    );
    let out = render(err, &sm);
    assert!(out.contains("iterative"), "got: {out}");
    assert!(out.contains("ghost_rel"), "got: {out}");
}

#[test]
fn undeclared_loop_condition_variant_and_render() {
    let (res, sm) = parse_extended("undeclared_loop_condition.dl");
    let err = res.unwrap_err();
    assert!(
        matches!(err, ParseError::UndeclaredLoopCondition { .. }),
        "got {err:?}"
    );
    let out = render(err, &sm);
    assert!(out.contains("loop condition"), "got: {out}");
    assert!(out.contains("ghost_flag"), "got: {out}");
}

#[test]
fn non_nullary_loop_condition_variant_and_render() {
    let (res, sm) = parse_extended("non_nullary_loop_condition.dl");
    let err = res.unwrap_err();
    assert!(
        matches!(err, ParseError::NonNullaryLoopCondition { .. }),
        "got {err:?}"
    );
    let out = render(err, &sm);
    assert!(out.contains("must be nullary"), "got: {out}");
    assert!(out.contains("done_flag"), "got: {out}");
}

#[test]
fn placeholder_in_udf_variant_and_render() {
    let (res, sm) = parse_extended("placeholder_in_udf.dl");
    let err = res.unwrap_err();
    assert!(
        matches!(err, ParseError::PlaceholderInUdf { .. }),
        "got {err:?}"
    );
    let out = render(err, &sm);
    assert!(out.contains("`_` placeholder"), "got: {out}");
    assert!(out.contains("my_udf"), "got: {out}");
}

#[test]
fn syntax_error_variant_and_render() {
    let (res, sm) = parse_extended("syntax_error.dl");
    let err = res.unwrap_err();
    assert!(matches!(err, ParseError::Syntax { .. }), "got {err:?}");
    let out = render(err, &sm);
    assert!(out.contains("syntax error"), "got: {out}");
}

#[test]
fn loop_in_standard_mode_variant_and_render() {
    let (res, sm) = parse_standard("loop_in_standard_mode.dl");
    let err = res.unwrap_err();
    assert!(
        matches!(err, ParseError::LoopBlockInStandardMode { .. }),
        "got {err:?}"
    );
    let out = render(err, &sm);
    assert!(
        out.contains("extend-batch") || out.contains("Extended"),
        "got: {out}"
    );
}

#[test]
fn circular_include_variant_and_render() {
    let (res, sm) = parse_extended("circular_include_a.dl");
    let err = res.unwrap_err();
    assert!(
        matches!(err, ParseError::CircularInclude { .. }),
        "got {err:?}"
    );
    let out = render(err, &sm);
    assert!(out.contains("circular include"), "got: {out}");
    assert!(out.contains("circular_include_a.dl"), "got: {out}");
    assert!(out.contains("include chain:"), "got: {out}");
}

#[test]
fn include_io_variant_and_render() {
    let (res, sm) = parse_extended("include_io.dl");
    let err = res.unwrap_err();
    assert!(matches!(err, ParseError::IncludeIo { .. }), "got {err:?}");
    let out = render(err, &sm);
    assert!(out.contains("failed to read"), "got: {out}");
    assert!(out.contains("nonexistent_file.dl"), "got: {out}");
}
