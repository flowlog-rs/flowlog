mod errors;

use flowlog_build::common::{Config, SourceMap};
use flowlog_build::parser::{DataType, Program};
use flowlog_build::typechecker::{TypeCheckError, check_program};

use errors::{fixture, render};

fn typecheck(name: &str) -> (Result<(), TypeCheckError>, SourceMap) {
    let mut sm = SourceMap::new();
    let mut program = Program::parse(&fixture("typechecker", name), false, &mut sm)
        .expect("fixture should parse cleanly");
    (check_program(&mut program, &Config::default()), sm)
}

#[test]
fn type_mismatch() {
    assert_err!(
        typecheck("type_mismatch.dl"),
        TypeCheckError::TypeMismatch { .. },
        ["variable `v`", "Int32", "String"]
    );
}

#[test]
fn arithmetic_type_mismatch() {
    assert_err!(
        typecheck("arithmetic_type_mismatch.dl"),
        TypeCheckError::ArithmeticTypeMismatch { .. },
        ["mixed types", "Int32", "Float64"]
    );
}

#[test]
fn arithmetic_int_float_mixing() {
    // `pts + 5.0` where `pts: Int32` — exercises LitKind family tracking
    // (concrete int × float-literal family mismatch).
    assert_err!(
        typecheck("arithmetic_int_float_mixing.dl"),
        TypeCheckError::ArithmeticTypeMismatch { .. },
        ["Int32", "Float32"]
    );
}

#[test]
fn arithmetic_op_not_allowed_on_bool() {
    assert_err!(
        typecheck("arithmetic_on_bool.dl"),
        TypeCheckError::ArithmeticOpNotAllowed {
            ty: DataType::Bool,
            ..
        },
        ["not allowed on", "Bool"]
    );
}

#[test]
fn comparison_type_mismatch() {
    assert_err!(
        typecheck("comparison_type_mismatch.dl"),
        TypeCheckError::ComparisonTypeMismatch { .. },
        ["comparison sides disagree", "Int32", "String"]
    );
}

#[test]
fn comparison_op_not_allowed_on_bool() {
    assert_err!(
        typecheck("comparison_op_on_bool.dl"),
        TypeCheckError::ComparisonOpNotAllowed {
            ty: DataType::Bool,
            ..
        },
        ["not allowed on", "Bool"]
    );
}

#[test]
fn literal_column_mismatch_atom() {
    assert_err!(
        typecheck("literal_column_mismatch_atom.dl"),
        TypeCheckError::LiteralColumnMismatch { .. },
        ["literal `5`", "String"]
    );
}

#[test]
fn literal_column_mismatch_head() {
    assert_err!(
        typecheck("literal_column_mismatch_head.dl"),
        TypeCheckError::LiteralColumnMismatch { .. },
        ["literal `5`", "String"]
    );
}

#[test]
fn aggregation_input_not_numeric() {
    assert_err!(
        typecheck("aggregation_type_mismatch.dl"),
        TypeCheckError::AggregationInputNotNumeric { .. },
        ["requires a numeric input", "String"]
    );
}

#[test]
fn aggregation_output_type() {
    assert_err!(
        typecheck("aggregation_output_type.dl"),
        TypeCheckError::AggregationOutputType { .. },
        ["cannot produce result of type", "String"]
    );
}

#[test]
fn udf_arity() {
    assert_err!(
        typecheck("udf_arity.dl"),
        TypeCheckError::UdfArity { .. },
        ["expects 2 argument", "got 1"]
    );
}

#[test]
fn udf_arg_type() {
    assert_err!(
        typecheck("udf_arg_type.dl"),
        TypeCheckError::UdfArgType { .. },
        ["expects `String`", "got `Int32`"]
    );
}

#[test]
fn undeclared_udf() {
    assert_err!(
        typecheck("undeclared_udf.dl"),
        TypeCheckError::UndeclaredUdf { .. },
        ["undeclared UDF", "stamp_name"]
    );
}

#[test]
fn head_column_type() {
    assert_err!(
        typecheck("head_column_type.dl"),
        TypeCheckError::HeadColumnType { .. },
        ["expects `String`", "Int32"]
    );
}

#[test]
fn head_arity() {
    assert_err!(
        typecheck("head_arity.dl"),
        TypeCheckError::HeadArity { .. },
        ["expects arity 3", "got 2"]
    );
}
