//! Integration tests for every user-reachable `TypeCheckError` variant.
//!
//! Each fixture under `tests/errors/*.dl` exercises one variant via
//! [`typechecker::check_program`] and renders the error through
//! [`common::diag::emit`].

use std::path::PathBuf;

use common::diag::{emit, BoxError};
use common::source::SourceMap;
use parser::Program;
use typechecker::{check_program, TypeCheckError};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("errors")
        .join(name)
}

fn typecheck_fixture(name: &str) -> (Result<(), TypeCheckError>, SourceMap) {
    let mut sm = SourceMap::new();
    let path = fixture(name);
    let program = Program::parse(path.to_str().unwrap(), false, &mut sm)
        .expect("fixture should parse cleanly");
    (check_program(&program), sm)
}

fn render(err: TypeCheckError, sm: &SourceMap) -> String {
    let err: BoxError = err.into();
    let mut buf: Vec<u8> = Vec::new();
    emit(&err, sm, &mut buf).unwrap();
    String::from_utf8(buf).unwrap()
}

/// Asserts that `fixture` produces an error matching `pat` and that its
/// rendered form contains every string in `expected_in_render`.
macro_rules! assert_typecheck {
    ($fixture:expr, $pat:pat, [$($expected_in_render:expr),* $(,)?]) => {{
        let (res, sm) = typecheck_fixture($fixture);
        let err = res.expect_err("expected TypeCheckError");
        assert!(matches!(err, $pat), "got {err:?}");
        let out = render(err, &sm);
        $(
            assert!(
                out.contains($expected_in_render),
                "render missing `{}`\n--- got ---\n{out}",
                $expected_in_render,
            );
        )*
    }};
}

#[test]
fn type_mismatch() {
    assert_typecheck!(
        "type_mismatch.dl",
        TypeCheckError::TypeMismatch { .. },
        ["variable `v`", "Int32", "String"]
    );
}

#[test]
fn arithmetic_type_mismatch() {
    assert_typecheck!(
        "arithmetic_type_mismatch.dl",
        TypeCheckError::ArithmeticTypeMismatch { .. },
        ["mixed types", "Int32", "Float64"]
    );
}

#[test]
fn arithmetic_int_float_mixing() {
    // `pts + 5.0` where `pts: Int32` — exercises LitKind family tracking
    // (concrete int × float-literal family mismatch).
    assert_typecheck!(
        "arithmetic_int_float_mixing.dl",
        TypeCheckError::ArithmeticTypeMismatch { .. },
        ["Int32", "Float32"]
    );
}

#[test]
fn arithmetic_op_not_allowed_on_bool() {
    assert_typecheck!(
        "arithmetic_on_bool.dl",
        TypeCheckError::ArithmeticOpNotAllowed {
            ty: parser::DataType::Bool,
            ..
        },
        ["not allowed on", "Bool"]
    );
}

#[test]
fn comparison_type_mismatch() {
    assert_typecheck!(
        "comparison_type_mismatch.dl",
        TypeCheckError::ComparisonTypeMismatch { .. },
        ["comparison sides disagree", "Int32", "String"]
    );
}

#[test]
fn comparison_op_not_allowed_on_bool() {
    assert_typecheck!(
        "comparison_op_on_bool.dl",
        TypeCheckError::ComparisonOpNotAllowed {
            ty: parser::DataType::Bool,
            ..
        },
        ["not allowed on", "Bool"]
    );
}

#[test]
fn literal_column_mismatch_atom() {
    assert_typecheck!(
        "literal_column_mismatch_atom.dl",
        TypeCheckError::LiteralColumnMismatch { .. },
        ["literal `5`", "String"]
    );
}

#[test]
fn literal_column_mismatch_head() {
    assert_typecheck!(
        "literal_column_mismatch_head.dl",
        TypeCheckError::LiteralColumnMismatch { .. },
        ["literal `5`", "String"]
    );
}

#[test]
fn aggregation_input_not_numeric() {
    assert_typecheck!(
        "aggregation_type_mismatch.dl",
        TypeCheckError::AggregationInputNotNumeric { .. },
        ["requires a numeric input", "String"]
    );
}

#[test]
fn aggregation_output_type() {
    assert_typecheck!(
        "aggregation_output_type.dl",
        TypeCheckError::AggregationOutputType { .. },
        ["cannot produce result of type", "String"]
    );
}

#[test]
fn udf_arity() {
    assert_typecheck!(
        "udf_arity.dl",
        TypeCheckError::UdfArity { .. },
        ["expects 2 argument", "got 1"]
    );
}

#[test]
fn udf_arg_type() {
    assert_typecheck!(
        "udf_arg_type.dl",
        TypeCheckError::UdfArgType { .. },
        ["expects `String`", "got `Int32`"]
    );
}

#[test]
fn undeclared_udf() {
    assert_typecheck!(
        "undeclared_udf.dl",
        TypeCheckError::UndeclaredUdf { .. },
        ["undeclared UDF", "stamp_name"]
    );
}

#[test]
fn head_column_type() {
    assert_typecheck!(
        "head_column_type.dl",
        TypeCheckError::HeadColumnType { .. },
        ["expects `String`", "Int32"]
    );
}

#[test]
fn head_arity() {
    assert_typecheck!(
        "head_arity.dl",
        TypeCheckError::HeadArity { .. },
        ["expects arity 3", "got 2"]
    );
}
