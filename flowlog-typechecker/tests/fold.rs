mod common;

use common::parse_and_check_result;
use flowlog_parser::ConstType;
use flowlog_parser::Factor;
use flowlog_parser::HeadArg;
use flowlog_parser::Predicate;
use flowlog_parser::Program;
use flowlog_typechecker::fold_constants;

/// Parse + type-check (so literals are pinned) + constant-fold.
fn checked_and_folded(src: &str) -> Program {
    let mut program = parse_and_check_result(src).expect("type-check should succeed");
    fold_constants(&mut program);
    program
}

/// A constant comparison operand `x > 2 + 3` collapses to the single pinned
/// literal `5` — the core "pre-compute constant predicate operand" win.
#[test]
fn comparison_operand_folds_to_single_literal() {
    let src = "\
        .decl Item(x: int32)\n\
        .decl Out(x: int32)\n\
        .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
        .output Out\n\
        Out(x) :- Item(x), x > 2 + 3.\n";
    let program = checked_and_folded(src);
    let rule = program.rules()[0];
    let cmp = match &rule.rhs()[1] {
        Predicate::Compare(c) => c,
        other => panic!("expected comparison, got {other:?}"),
    };
    assert!(
        cmp.right().rest().is_empty(),
        "`2 + 3` should collapse to a lone constant"
    );
    match cmp.right().init() {
        Factor::Const(c) => assert_eq!(c, &ConstType::Int32(5)),
        other => panic!("expected Const(5), got {other:?}"),
    }
}

/// A fully-constant head argument `10 * 2` folds to `20` at the declared width.
#[test]
fn constant_head_arg_folds() {
    let src = "\
        .decl Item(x: int32)\n\
        .decl Out(v: int32)\n\
        .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
        .output Out\n\
        Out(10 * 2) :- Item(x).\n";
    let program = checked_and_folded(src);
    let rule = program.rules()[0];
    let head = match &rule.head().head_arguments()[0] {
        HeadArg::Arith(a) => a,
        other => panic!("expected arith head arg, got {other:?}"),
    };
    assert!(head.rest().is_empty(), "head arg should be a lone const");
    match head.init() {
        Factor::Const(c) => assert_eq!(c, &ConstType::Int32(20)),
        other => panic!("expected Const(20), got {other:?}"),
    }
}

/// An expression that overflows its pinned width is left unfolded so the
/// generated (wrapping) release code computes the identical value.
#[test]
fn overflowing_expression_is_left_unfolded() {
    let src = "\
        .decl Item(x: int8)\n\
        .decl Out(x: int8)\n\
        .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
        .output Out\n\
        Out(x) :- Item(x), x > 100 + 100.\n";
    let program = checked_and_folded(src);
    let rule = program.rules()[0];
    let cmp = match &rule.rhs()[1] {
        Predicate::Compare(c) => c,
        other => panic!("expected comparison, got {other:?}"),
    };
    assert!(
        !cmp.right().rest().is_empty(),
        "100 + 100 overflows int8 and must be left unfolded"
    );
}

/// An always-true constant comparison is dropped when a positive atom
/// remains, so the generated dataflow doesn't run a per-row no-op filter.
#[test]
fn always_true_compare_is_dropped() {
    let src = "\
        .decl Item(x: int32)\n\
        .decl Out(x: int32)\n\
        .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
        .output Out\n\
        Out(x) :- Item(x), 1 < 2.\n";
    let program = checked_and_folded(src);
    let rule = program.rules()[0];
    assert_eq!(
        rule.rhs().len(),
        1,
        "the always-true `1 < 2` should be dropped"
    );
    assert!(
        matches!(&rule.rhs()[0], Predicate::PositiveAtom(_)),
        "the surviving predicate should be the Item atom"
    );
}

/// An always-FALSE rule is eliminated: its body can never hold, so it
/// contributes nothing and is dropped entirely.
#[test]
fn always_false_rule_is_removed() {
    let src = "\
        .decl Item(x: int32)\n\
        .decl Out(x: int32)\n\
        .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
        .output Out\n\
        Out(x) :- Item(x).\n\
        Out(x) :- Item(x), 1 > 2.\n";
    let program = checked_and_folded(src);
    assert_eq!(
        program.rules().len(),
        1,
        "the always-false second rule should be eliminated"
    );
    assert_eq!(
        program.rules()[0].rhs().len(),
        1,
        "the surviving rule keeps just its Item atom"
    );
}

/// An always-TRUE sole rule with an all-constant head becomes an inline fact
/// (which also fixes the pre-existing 0-positive-atom planner panic).
#[test]
fn always_true_sole_rule_becomes_fact() {
    let src = "\
        .decl Item(x: int32)\n\
        .decl Out(v: int32)\n\
        .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
        .output Out\n\
        Out(5) :- 1 < 2.\n";
    let program = checked_and_folded(src);
    assert!(
        program.rules().is_empty(),
        "the always-true-sole rule should be converted away"
    );
    let facts = program.facts().get("out").expect("Out should have a fact");
    assert_eq!(facts.len(), 1);
    assert_eq!(facts[0].columns, vec![ConstType::Int32(5)]);
}

/// An expression with a variable is never folded.
#[test]
fn variable_expression_not_folded() {
    let src = "\
        .decl Item(x: int32)\n\
        .decl Out(x: int32)\n\
        .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
        .output Out\n\
        Out(x + 5) :- Item(x).\n";
    let program = checked_and_folded(src);
    let rule = program.rules()[0];
    let head = match &rule.head().head_arguments()[0] {
        HeadArg::Arith(a) => a,
        other => panic!("expected arith head arg, got {other:?}"),
    };
    assert!(!head.rest().is_empty(), "`x + 5` must stay unfolded");
}
