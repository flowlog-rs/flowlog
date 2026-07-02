mod common;

use common::parse_and_check_result;
use flowlog_parser::Factor;
use flowlog_parser::HeadArg;

/// Two aliases of `number` join freely — aliases are transparent.
#[test]
fn alias_join_allowed() {
    let src = "\
        .type A = number\n\
        .type B = number\n\
        .decl R(x: A)\n\
        .decl S(x: B)\n\
        .decl Out(x: number)\n\
        .input R(IO=\"file\", filename=\"R.csv\", delimiter=\",\")\n\
        .input S(IO=\"file\", filename=\"S.csv\", delimiter=\",\")\n\
        .output Out\n\
        Out(x) :- R(x), S(x).\n";
    parse_and_check_result(src).expect("alias join must be allowed");
}

/// Head narrowing with explicit `as()` is accepted.
#[test]
fn head_narrowing_with_cast_accepted() {
    let src = "\
        .type UserId <: number\n\
        .decl Plain(x: number)\n\
        .decl OnlyUsers(u: UserId)\n\
        .input Plain(IO=\"file\", filename=\"Plain.csv\", delimiter=\",\")\n\
        .output OnlyUsers\n\
        OnlyUsers(as(x, UserId)) :- Plain(x).\n";
    parse_and_check_result(src).expect("explicit narrowing must be allowed");
}

/// Head narrowing without `as()` is rejected — and parentheses around
/// the variable must not bypass the check (`OnlyUsers((x))` is the same
/// pass-through as `OnlyUsers(x)`; the parser collapses the group).
#[test]
fn head_narrowing_without_cast_rejected_even_parenthesized() {
    for head in ["OnlyUsers(x)", "OnlyUsers((x))"] {
        let src = format!(
            ".type UserId <: number\n\
             .decl Plain(x: number)\n\
             .decl OnlyUsers(u: UserId)\n\
             .input Plain(IO=\"file\", filename=\"Plain.csv\", delimiter=\",\")\n\
             .output OnlyUsers\n\
             {head} :- Plain(x).\n"
        );
        assert!(
            parse_and_check_result(&src).is_err(),
            "implicit narrowing must be rejected for {head}"
        );
    }
}

/// `as()` between two sibling subtypes of the same primitive is
/// allowed — that's the escape hatch the rule exists for.
#[test]
fn sibling_subtype_cast_allowed() {
    let src = "\
        .type UserId    <: number\n\
        .type ProductId <: number\n\
        .decl Friend(a: UserId)\n\
        .decl ProductsForUsers(p: ProductId)\n\
        .input Friend(IO=\"file\", filename=\"Friend.csv\", delimiter=\",\")\n\
        .output ProductsForUsers\n\
        ProductsForUsers(as(x, ProductId)) :- Friend(x).\n";
    parse_and_check_result(src).expect("sibling-subtype cast must be allowed");
}

/// After typechecking, every `Factor::Cast` has been lowered to its
/// inner factor. Downstream stages never see a cast wrapper.
#[test]
fn cast_is_lowered_after_typecheck() {
    let src = "\
        .type UserId <: number\n\
        .decl Plain(x: number)\n\
        .decl OnlyUsers(u: UserId)\n\
        .input Plain(IO=\"file\", filename=\"Plain.csv\", delimiter=\",\")\n\
        .output OnlyUsers\n\
        OnlyUsers(as(x, UserId)) :- Plain(x).\n";
    let program = parse_and_check_result(src).expect("typecheck must succeed");
    let rule = program.rules()[0];
    for arg in rule.head().head_arguments() {
        if let HeadArg::Arith(a) = arg {
            assert!(
                !matches!(a.init(), Factor::Cast(_)),
                "cast should have been lowered after typecheck"
            );
        }
    }
}
