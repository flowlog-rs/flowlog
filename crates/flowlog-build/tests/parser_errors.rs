mod errors;

use flowlog_build::common::source::SourceMap;
use flowlog_build::parser::error::DirectiveKind;
use flowlog_build::parser::{ParseError, Program};

use errors::{fixture, render};

fn parse(name: &str, extended: bool) -> (Result<Program, ParseError>, SourceMap) {
    let mut sm = SourceMap::new();
    let res = Program::parse(&fixture("parser", name), extended, &mut sm);
    (res, sm)
}

#[test]
fn duplicate_decl() {
    assert_err!(
        parse("duplicate_decl.dl", true),
        ParseError::DuplicateDecl { .. },
        [
            "duplicate declaration",
            "redeclared here",
            "first declared here",
            "edge",
        ]
    );
}

#[test]
fn duplicate_attribute() {
    assert_err!(
        parse("duplicate_attribute.dl", true),
        ParseError::DuplicateAttribute { .. },
        ["duplicate attribute", "first declared here", "edge"]
    );
}

#[test]
fn duplicate_directive() {
    assert_err!(
        parse("duplicate_directive.dl", true),
        ParseError::DuplicateDirective {
            kind: DirectiveKind::Output,
            ..
        },
        ["duplicate .output", "first directive here"]
    );
}

#[test]
fn undeclared_in_directive() {
    assert_err!(
        parse("undeclared_in_directive.dl", true),
        ParseError::UndeclaredInDirective { .. },
        [".output", "undeclared", "missing_rel"]
    );
}

#[test]
fn undeclared_iterative() {
    assert_err!(
        parse("undeclared_iterative.dl", true),
        ParseError::UndeclaredInIterativeList { .. },
        ["iterative", "ghost_rel"]
    );
}

#[test]
fn undeclared_loop_condition() {
    assert_err!(
        parse("undeclared_loop_condition.dl", true),
        ParseError::UndeclaredLoopCondition { .. },
        ["loop condition", "ghost_flag"]
    );
}

#[test]
fn undeclared_in_rule() {
    assert_err!(
        parse("undeclared_in_rule.dl", true),
        ParseError::UndeclaredInRule { .. },
        ["rule references undeclared relation", "ghost_rel"]
    );
}

#[test]
fn undeclared_in_fact() {
    assert_err!(
        parse("undeclared_in_fact.dl", true),
        ParseError::UndeclaredInFact { .. },
        ["fact references undeclared relation", "ghost_rel"]
    );
}

#[test]
fn non_nullary_loop_condition() {
    assert_err!(
        parse("non_nullary_loop_condition.dl", true),
        ParseError::NonNullaryLoopCondition { .. },
        ["must be nullary", "done_flag"]
    );
}

#[test]
fn placeholder_in_udf() {
    assert_err!(
        parse("placeholder_in_udf.dl", true),
        ParseError::PlaceholderInUdf { .. },
        ["`_` placeholder", "my_udf"]
    );
}

#[test]
fn syntax_error() {
    assert_err!(
        parse("syntax_error.dl", true),
        ParseError::Syntax { .. },
        ["syntax error"]
    );
}

#[test]
fn loop_in_standard_mode() {
    let (res, sm) = parse("loop_in_standard_mode.dl", false);
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
fn circular_include() {
    assert_err!(
        parse("circular_include_a.dl", true),
        ParseError::CircularInclude { .. },
        [
            "circular include",
            "circular_include_a.dl",
            "include chain:"
        ]
    );
}

#[test]
fn include_io() {
    assert_err!(
        parse("include_io.dl", true),
        ParseError::IncludeIo { .. },
        ["failed to read", "nonexistent_file.dl"]
    );
}
