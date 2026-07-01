mod errors;

use errors::fixture;
use errors::render;
use flowlog_common::SourceMap;
use flowlog_parser::DirectiveKind;
use flowlog_parser::ParseError;
use flowlog_parser::Program;

fn parse(name: &str, extended: bool) -> (Result<Program, ParseError>, SourceMap) {
    let mut sm = SourceMap::new();
    let res = Program::parse(&fixture("parser", name), extended, &[], &mut sm);
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
fn duplicate_extern_fn() {
    assert_err!(
        parse("duplicate_extern_fn.dl", true),
        ParseError::DuplicateExternFn { .. },
        [
            "duplicate declaration of extern function",
            "redeclared here",
            "first declared here",
            "hash",
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

// A ground fact whose head holds a variable (`k(E).`) names no tuple. It must
// be a clean parse error, not a panic. Regression for the fuzz-found crash in
// https://github.com/flowlog-rs/flowlog/issues/182.
#[test]
fn ground_fact_not_const() {
    assert_err!(
        parse("ground_fact_not_const.dl", true),
        ParseError::GroundRuleNotConst { .. },
        ["not a constant fact"]
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
fn type_decl_duplicate() {
    assert_err!(
        parse("type_decl_duplicate.dl", true),
        ParseError::DuplicateTypeDecl { .. },
        ["duplicate", "X"]
    );
}

#[test]
fn type_decl_shadow_primitive() {
    // `.type number = ...` collides with the built-in primitive.
    assert_err!(
        parse("type_decl_shadow_primitive.dl", true),
        ParseError::DuplicateTypeDecl { .. },
        ["duplicate", "number"]
    );
}

#[test]
fn type_decl_unknown_parent() {
    assert_err!(
        parse("type_decl_unknown_parent.dl", true),
        ParseError::UnknownTypeParent { .. },
        ["unknown type", "NoSuchType"]
    );
}

#[test]
fn tuple_subtype_decl() {
    // `.type T <: ( … )` must error: a tuple is defined with `=`, and the `<:`
    // operator can't be silently dropped (it would register an ordinary tuple).
    assert_err!(
        parse("tuple_subtype_decl.dl", true),
        ParseError::TupleSubtypeDecl { .. },
        ["must be defined with `=`"]
    );
}

#[test]
fn attribute_unknown_type() {
    assert_err!(
        parse("attribute_unknown_type.dl", true),
        ParseError::UnknownAttributeType { .. },
        ["unknown type", "NoSuchType"]
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

#[test]
fn comp_unknown() {
    assert_err!(
        parse("comp_unknown.dl", true),
        ParseError::UnknownComponent { .. },
        ["unknown component", "Container"]
    );
}

#[test]
fn comp_circular_inherit() {
    assert_err!(
        parse("comp_circular_inherit.dl", true),
        ParseError::CircularInheritance { .. },
        ["circular component inheritance"]
    );
}

#[test]
fn comp_arity_mismatch() {
    assert_err!(
        parse("comp_arity_mismatch.dl", true),
        ParseError::ComponentArityMismatch { .. },
        ["Pair", "expects 1 type argument", "got 2"]
    );
}

#[test]
fn comp_unresolved_ref() {
    assert_err!(
        parse("comp_unresolved_ref.dl", true),
        ParseError::UnresolvedQualifiedRef { .. },
        ["unresolved qualified reference", "cfg.X"]
    );
}
