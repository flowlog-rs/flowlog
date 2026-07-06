mod common;

use common::parse_and_check_result;
use flowlog_parser::AtomArg;
use flowlog_parser::ConstType;
use flowlog_parser::Factor;
use flowlog_parser::HeadArg;
use flowlog_parser::Predicate;
use flowlog_parser::Program;

/// Parse + type-check `src`, panicking on any error. Returns the pinned program.
fn parse_and_check(src: &str) -> Program {
    parse_and_check_result(src).expect("type-check should succeed")
}

/// Body-positive atom literal: `Flag(5)` with `.decl Flag(x: int8)` must
/// pin `5` to `Int8(5)` via `pin_atom_consts`. If that pass becomes a
/// no-op, catalog calls `data_type()` on a polymorphic `Int` and panics.
#[test]
fn body_atom_const_pinned_to_declared_column_width() {
    let src = "\
        .decl Item(x: int8)\n\
        .decl Flag(x: int8)\n\
        .decl Out(x: int8)\n\
        .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
        .input Flag(IO=\"file\", filename=\"Flag.csv\", delimiter=\",\")\n\
        .output Out\n\
        Out(x) :- Item(x), Flag(5).\n";
    let program = parse_and_check(src);
    let rule = program.rules()[0];
    let flag_atom = match &rule.rhs()[1] {
        Predicate::PositiveAtom(a) => a,
        other => panic!("expected Flag atom, got {other:?}"),
    };
    match &flag_atom.arguments()[0] {
        AtomArg::Const(c) => assert_eq!(c, &ConstType::Int8(5)),
        other => panic!("expected Const, got {other:?}"),
    }
}

/// Comparison operand literal: `x > 100` with `x: int16` must pin `100`
/// to `Int16(100)` via `pin_arith_literals` inside `check_comparison`.
/// Guards the pin-target selection after `merge_lit` unifies left/right.
#[test]
fn comparison_literal_pinned_to_variable_type() {
    let src = "\
        .decl Item(x: int16)\n\
        .decl Big(x: int16)\n\
        .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
        .output Big\n\
        Big(x) :- Item(x), x > 100.\n";
    let program = parse_and_check(src);
    let rule = program.rules()[0];
    let cmp = match &rule.rhs()[1] {
        Predicate::Compare(c) => c,
        other => panic!("expected comparison, got {other:?}"),
    };
    match cmp.right().init() {
        Factor::Const(c) => assert_eq!(c, &ConstType::Int16(100)),
        other => panic!("expected Const, got {other:?}"),
    }
}

/// Nested UDF call: in `f(1) + x` where `x: int64` and `f: int8 -> int64`,
/// the `1` must be pinned to the UDF's parameter width (`Int8`), NOT the
/// enclosing expression's target (`Int64`). A regression using outer
/// context inside `pin_fn_call_args` would silently widen the literal.
#[test]
fn nested_udf_arg_pinned_to_param_type_not_outer_target() {
    let src = "\
        .decl Item(x: int64)\n\
        .decl Flag(x: int64)\n\
        .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
        .output Flag\n\
        .extern fn f(a: int8) -> int64\n\
        Flag(f(1) + x) :- Item(x).\n";
    let program = parse_and_check(src);
    let rule = program.rules()[0];
    let head_arith = match &rule.head().head_arguments()[0] {
        HeadArg::Arith(a) => a,
        other => panic!("expected Arith head arg, got {other:?}"),
    };
    let fc = match head_arith.init() {
        Factor::FnCall(fc) => fc,
        other => panic!("expected FnCall factor, got {other:?}"),
    };
    match fc.args()[0].init() {
        Factor::Const(c) => assert_eq!(
            c,
            &ConstType::Int8(1),
            "UDF arg must pin to param type (Int8), not outer target (Int64)"
        ),
        other => panic!("expected Const, got {other:?}"),
    }
}

/// Fact tuple literal: `P(5)` with `.decl P(x: uint64)` must pin via
/// `check_and_pin_facts`. This is a separate code path from rule-body
/// pinning — a regression here would leak polymorphic literals into
/// `program.facts()` even though all rule literals are concrete.
#[test]
fn fact_tuple_const_pinned_to_declared_column_width() {
    let src = "\
        .decl P(x: uint64)\n\
        .decl Out(x: uint64)\n\
        .output Out\n\
        P(5).\n\
        Out(x) :- P(x).\n";
    let program = parse_and_check(src);
    let p_facts = program.facts().get("p").expect("p facts");
    assert_eq!(p_facts[0].columns[0], ConstType::UInt64(5));
}
