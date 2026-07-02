mod common;

use common::parse_and_check_result;
use common::parse_program;
use flowlog_typechecker::TypeCheckError;

// ── Tuples ─────────────────────────────────────────────────────────

/// Construct (`p = (x, y)`) and destructure (`p = (a, b)`) of a tuple
/// column both type-check against the declared tuple type.
#[test]
fn tuple_construct_and_destructure_typecheck() {
    let src = "\
        .type Pair = ( a: symbol, b: symbol )\n\
        .decl In(x: symbol, y: symbol)\n\
        .decl Out(p: Pair)\n\
        .decl Back(a: symbol, b: symbol)\n\
        .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
        .output Back\n\
        Out(p) :- In(x, y), p = (x, y).\n\
        Back(a, b) :- Out(p), p = (a, b).\n";
    parse_and_check_result(src).expect("tuple construct + destructure must type-check");
}

/// A construct with the wrong number of fields is rejected (here a 3-field
/// literal flowing into an arity-2 tuple column).
#[test]
fn tuple_construct_wrong_arity_rejected() {
    let src = "\
        .type Pair = ( a: symbol, b: symbol )\n\
        .decl In(x: symbol, y: symbol, z: symbol)\n\
        .decl Out(p: Pair)\n\
        .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
        .output Out\n\
        Out(p) :- In(x, y, z), p = (x, y, z).\n";
    assert!(
        parse_and_check_result(src).is_err(),
        "3-field tuple into an arity-2 tuple column must be rejected"
    );
}

/// A field of the wrong type is rejected (a `number` field given a symbol).
#[test]
fn tuple_field_type_mismatch_rejected() {
    let src = "\
        .type Tv = ( t: symbol, v: number )\n\
        .decl In(s: symbol, n: symbol)\n\
        .decl Out(p: Tv)\n\
        .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
        .output Out\n\
        Out(p) :- In(s, n), p = (s, n).\n";
    assert!(
        parse_and_check_result(src).is_err(),
        "a symbol in a `number` tuple field must be rejected"
    );
}

/// A tuple literal flowing into a scalar column (and vice-versa) is
/// rejected — tuples are not interchangeable with their fields.
#[test]
fn tuple_vs_scalar_mismatch_rejected() {
    let src = "\
        .decl In(x: symbol, y: symbol)\n\
        .decl Out(p: symbol)\n\
        .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
        .output Out\n\
        Out(p) :- In(x, y), p = (x, y).\n";
    assert!(
        parse_and_check_result(src).is_err(),
        "a tuple literal into a scalar column must be rejected"
    );
}

/// A polymorphic numeric literal in a tuple field whose declared width is
/// not the family default (here `int64`) must be accepted and pinned — the
/// same leniency a scalar `int64` column gets. (Regression: an earlier
/// version collapsed the literal to `Int32` and rejected it.)
#[test]
fn tuple_field_non_default_width_literal_accepted() {
    let src = "\
        .type Tv = ( t: symbol, v: int64 )\n\
        .decl In(s: symbol)\n\
        .decl Out(p: Tv)\n\
        .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
        .output Out\n\
        Out(p) :- In(s), p = (s, 5).\n";
    parse_and_check_result(src).expect("an int literal in an int64 tuple field must be accepted");
}

/// Arithmetic on a tuple operand is rejected at type-check (a clean
/// diagnostic, not a generated-Rust `Add`-not-satisfied error).
#[test]
fn tuple_arithmetic_rejected() {
    let src = "\
        .type Pair = ( a: number, b: number )\n\
        .decl In(x: number, y: number)\n\
        .decl Out(q: Pair)\n\
        .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
        .output Out\n\
        Out(q) :- In(x, y), p = (x, y), q = p + p.\n";
    assert!(
        parse_and_check_result(src).is_err(),
        "arithmetic on a tuple operand must be rejected"
    );
}

/// Destructuring a non-tuple bound variable is a clean user error, not an
/// internal compiler panic.
#[test]
fn destructure_of_non_tuple_is_clean_error() {
    let src = "\
        .decl In(x: symbol)\n\
        .decl Out(a: symbol)\n\
        .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
        .output Out\n\
        Out(a) :- In(x), x = (a, b).\n";
    match parse_and_check_result(src) {
        Err(TypeCheckError::TupleDestructure { .. }) => {}
        other => panic!("expected a clean TupleDestructure error, got {other:?}"),
    }
}

/// `.input` on a relation with a tuple column is rejected (tuples are
/// constructed by rules, never read from facts) — a clean parse error, not
/// a codegen panic.
#[test]
fn tuple_edb_input_rejected() {
    let src = "\
        .type Pair = ( a: symbol, b: symbol )\n\
        .decl In(p: Pair)\n\
        .decl Out(p: Pair)\n\
        .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
        .output Out\n\
        Out(p) :- In(p).\n";
    // The `.input` rejection is a ParseError, so it surfaces from parsing.
    assert!(
        parse_program(src).is_err(),
        "`.input` on a tuple-column relation must be rejected"
    );
}

/// A destructure with only placeholders against a non-tuple is rejected
/// (the placeholder still witnesses tuple-ness/arity), not silently
/// accepted.
#[test]
fn placeholder_only_destructure_of_non_tuple_rejected() {
    let src = "\
        .decl In(x: symbol)\n\
        .decl Out(x: symbol)\n\
        .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
        .output Out\n\
        Out(x) :- In(x), x = (_,).\n";
    assert!(
        parse_and_check_result(src).is_err(),
        "`x = (_,)` on a non-tuple `x` must be rejected"
    );
}

/// A trailing placeholder past the tuple's arity is rejected, not ignored.
#[test]
fn extra_placeholder_past_arity_rejected() {
    let src = "\
        .type Pair = ( a: symbol, b: symbol )\n\
        .decl In(x: symbol, y: symbol)\n\
        .decl P(p: Pair)\n\
        .decl Out(a: symbol)\n\
        .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
        .output Out\n\
        P(p)   :- In(x, y), p = (x, y).\n\
        Out(a) :- P(p), p = (a, b, _).\n";
    assert!(
        parse_and_check_result(src).is_err(),
        "a trailing `_` past the tuple's arity must be rejected"
    );
}

/// A destructure pattern wider than the tuple is a clean error, not a panic.
#[test]
fn over_arity_destructure_is_clean_error() {
    let src = "\
        .type Pair = ( a: symbol, b: symbol )\n\
        .decl In(x: symbol, y: symbol)\n\
        .decl P(p: Pair)\n\
        .decl Out(c: symbol)\n\
        .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
        .output Out\n\
        P(p)   :- In(x, y), p = (x, y).\n\
        Out(c) :- P(p), p = (a, b, c).\n";
    match parse_and_check_result(src) {
        Err(TypeCheckError::TupleDestructure { .. }) => {}
        other => panic!("expected a clean TupleDestructure error, got {other:?}"),
    }
}
