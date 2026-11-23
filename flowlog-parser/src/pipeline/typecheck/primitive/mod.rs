//! Pass 1: primitive `DataType` checking and literal pinning. Subtype-blind;
//! [`crate::pipeline::typecheck::subtype`] is Pass 2.
//!
//! A module per construct, built on the literal-typing algebra on [`DataType`]
//! (`merge`/`fits`/`defaulted`): `expr`, `atom`, `fact`, `compare`, and `rule`
//! (the composer). [`check_and_pin`] runs the pass.
//!
//! Function names read `<verb>_<node>`, where the verb is one of:
//!
//! - **bind**: record a variable's type from the atom column it fills: in a
//!   positive atom, or in a negative atom for a variable an assignment
//!   defines. The only place a variable's type is introduced.
//! - **infer**: compute an expression's `DataType` from its parts.
//! - **check**: verify a site's types agree (variables with their bindings,
//!   constants with their column family), introducing nothing.
//! - **pin**: rewrite each polymorphic literal (an `Int`/`Float` placeholder)
//!   to its concrete `DataType`, in place.
//! - **check_and_pin**: `check` then `pin` in one walk.

mod atom;
mod compare;
mod expr;
mod fact;
mod rule;

use std::collections::HashMap;

use flowlog_common::Config;
use flowlog_common::Span;

use crate::Arithmetic;
use crate::BuiltinOperator;
use crate::DataType;
use crate::Factor;
use crate::HeadArg;
use crate::ParseError;
use crate::Predicate;
use crate::Program;
use crate::Variable;
use crate::pipeline::typecheck::env::PrimitiveEnv;

/// Var -> (first-seen type, first-seen span). Later uses must agree.
/// Private, so it stays confined to the `primitive` subtree; the subtype
/// pass keeps its own `TypeId`-keyed map.
type Bindings = HashMap<Variable, (DataType, Span)>;

/// Run Pass 1 over `program`: check every primitive `DataType`, pin each
/// polymorphic literal to its concrete width, and check `ord` usage (which
/// may set [`Config::serialize_load`]).
pub(crate) fn check_and_pin(
    program: &mut Program,
    env: &PrimitiveEnv,
    config: &mut Config,
) -> Result<(), ParseError> {
    rule::check_and_pin_rules(program, env)?;
    check_ord(program, config)?;
    fact::check_and_pin_facts(program.facts_mut(), env)?;
    Ok(())
}

/// Check `ord(_)` usage across every rule: reject it without `--str-intern`
/// ([`ParseError::OrdRequiresStrIntern`]),
/// otherwise set [`Config::serialize_load`] so the loader interns serially and
/// `ord` stays deterministic across worker counts.
fn check_ord(program: &Program, config: &mut Config) -> Result<(), ParseError> {
    fn arith(a: &Arithmetic) -> Option<Span> {
        a.factors().find_map(factor)
    }
    fn factor(f: &Factor) -> Option<Span> {
        match f {
            Factor::Var(_) | Factor::Const(_) => None,
            Factor::FnCall(fc) => fc.args().iter().find_map(arith),
            Factor::Builtin(bc) if bc.op() == BuiltinOperator::Ord => Some(bc.span()),
            Factor::Builtin(bc) => bc.args().iter().find_map(arith),
            Factor::Cast(c) => factor(c.inner()),
            Factor::Group(a) => arith(a),
            Factor::Tuple(fields) => fields.iter().find_map(arith),
            Factor::TupleProj { tuple, .. } => arith(tuple),
            // Rule construction hoisted every aggregate into a binding and
            // named every placeholder.
            Factor::Placeholder | Factor::Aggregate(_) => None,
        }
    }
    fn body(predicates: &[Predicate]) -> Option<Span> {
        predicates.iter().find_map(|predicate| match predicate {
            Predicate::PositiveAtom(_) | Predicate::NegativeAtom(_) => None,
            Predicate::Compare(cmp) => arith(cmp.left()).or_else(|| arith(cmp.right())),
            Predicate::Aggregate(aggregate) => aggregate
                .target()
                .and_then(arith)
                .or_else(|| body(aggregate.body())),
        })
    }

    let ord_span = program.rules().iter().find_map(|rule| {
        body(rule.rhs()).or_else(|| {
            rule.head()
                .head_arguments()
                .iter()
                .find_map(|head_arg| match head_arg {
                    HeadArg::Var(_) => None,
                    HeadArg::Arith(a) => arith(a),
                })
        })
    });

    if let Some(span) = ord_span {
        if !config.str_intern_enabled() {
            return Err(ParseError::OrdRequiresStrIntern { span });
        }
        config.serialize_load = true;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use flowlog_common::Config;
    use rstest::rstest;

    use crate::DataType;
    use crate::ParseError;
    use crate::Predicate;
    use crate::Program;
    use crate::assert_err;
    use crate::test_harness::assembled;
    use crate::test_harness::checked;

    /// Type-checks `src` as `--str-intern` compiles it, so `ord` is allowed.
    fn checked_interned(src: &str) -> Result<Program, ParseError> {
        let mut program = assembled(src)?;
        let mut config = Config {
            str_intern: true,
            ..Config::default()
        };
        crate::check_program(&mut program, &mut config)?;
        Ok(program)
    }

    /// `ord` accepts a tuple operand whether it is a variable, built in place,
    /// or nested.
    #[rstest]
    #[case::variable("Out(ord(p)) :- In(x, y), p = (x, y).")]
    #[case::construct("Out(ord((x, y))) :- In(x, y).")]
    #[case::nested("Out(ord((x, (y, x)))) :- In(x, y).")]
    fn ord_accepts_a_tuple(#[case] rule: &str) {
        let src = format!(
            ".decl In(x: symbol, y: symbol)\n\
             .decl Out(n: int32)\n\
             .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
             .output Out\n\
             {rule}\n"
        );
        checked_interned(&src).expect("ord takes a tuple");
    }

    /// A tuple has a type of its own, so a count read only as a field of
    /// `ord`'s operand takes the width of the column it fills, not the
    /// `int32` that `ord` returns.
    #[test]
    fn a_count_in_an_ord_operand_takes_the_width_of_its_column() {
        let src = "\
            .decl In(x: int32)\n\
            .decl Out(n: int32, c: int64)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(ord((c,)), c) :- c = count : { In(x) }.\n";
        let program = checked_interned(src).expect("the count fills an int64 column");
        let Some(Predicate::Aggregate(count)) = program.rules()[0].rhs().first() else {
            panic!("the rule's body is its count");
        };
        assert_eq!(count.var_type(), Some(&DataType::Int64));
    }

    /// `ord` refuses a literal that nothing gives a width, wherever it sits
    /// in the operand: written in it, assigned to a variable in it, taken
    /// out of an assigned tuple, or in a tuple under a cast to a tuple type,
    /// which keeps the type its fields give it.
    #[rstest]
    #[case::a_field("Out(ord((x, 1))) :- In(x, y).")]
    #[case::a_float_field("Out(ord((x, 1.5))) :- In(x, y).")]
    #[case::a_nested_field("Out(ord((x, (y, 1)))) :- In(x, y).")]
    #[case::an_expression_of_literals("Out(ord((x, 1 + 2))) :- In(x, y).")]
    #[case::an_assigned_tuple("Out(ord(p)) :- In(x, y), p = (x, 1).")]
    #[case::an_assigned_literal("Out(ord((x, i))) :- In(x, y), i = 1.")]
    #[case::a_destructured_tuple("Out(ord(q)) :- In(x, y), p = ((1,),), p = (q,).")]
    #[case::a_destructured_field("Out(ord((a,))) :- In(x, y), p = (1,), p = (a,).")]
    #[case::under_a_tuple_cast("Out(ord(as(p, T))) :- In(x, y), p = (1,).")]
    fn ord_refuses_a_literal_of_unknown_width(#[case] rule: &str) {
        let src = format!(
            ".type T = ( a: int64 )\n\
             .decl In(x: symbol, y: symbol)\n\
             .decl Out(n: int32)\n\
             .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
             .output Out\n\
             {rule}\n"
        );
        assert_err!(checked_interned(&src), ParseError::OrdLiteralWidth { .. });
    }

    /// A literal in `ord`'s operand has a width when a cast states one, a
    /// typed term of its expression fixes one, or it is a field taken out of
    /// a tuple cast to a tuple type, whose field declares one.
    #[rstest]
    #[case::a_cast("Out(ord((n, as(1, int64)))) :- In(n).")]
    #[case::a_cast_in_an_assigned_tuple("Out(ord(p)) :- In(n), p = (n, as(1, int8)).")]
    #[case::a_typed_term("Out(ord((n + 1,))) :- In(n).")]
    #[case::a_typed_term_of_a_field_destructured_under_a_cast(
        "Out(ord((a,))) :- In(n), p = as((n + 1,), T), p = (a,)."
    )]
    #[case::a_field_destructured_under_a_cast(
        "Out(ord((a,))) :- In(n), p = as((1,), T), p = (a,)."
    )]
    #[case::a_typed_term_of_a_destructured_field(
        "Out(ord((a + n,))) :- In(n), p = (n + 1,), p = (a,)."
    )]
    #[case::a_cast_of_a_destructured_field(
        "Out(ord((as(a, int32),))) :- In(n), p = (1,), p = (a,)."
    )]
    #[case::beside_a_destructured_field_ord_does_not_number(
        "Out(ord((s,))) :- In(n), p = (1, \"x\"), p = (a, s)."
    )]
    fn ord_accepts_a_literal_given_a_width(#[case] rule: &str) {
        let src = format!(
            ".type T = ( v: int64 )\n\
             .decl In(n: int64)\n\
             .decl Out(n: int32)\n\
             .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
             .output Out\n\
             {rule}\n"
        );
        checked_interned(&src).expect("every literal has a width");
    }

    /// Each copy of an assigned value takes the width its own context asks:
    /// beside an `int64` column, a copy in an operand that fixes its own
    /// width, here `substr`'s `int32` index, an `int8` parameter, and a cast
    /// to `int8`, keeps that width.
    #[rstest]
    #[case::builtin_parameter("R(i, substr(\"abcd\", i, 1)) :- Seed(n), i = 1 + 1.")]
    #[case::function_parameter("R(i, to_string(narrow(i))) :- Seed(n), i = 1 + 1.")]
    #[case::cast("R(i, to_string(as(i, int8))) :- Seed(n), i = 1 + 1.")]
    #[case::cast_in_ord("R(i, to_string(ord((as(i, int8),)))) :- Seed(n), i = 1 + 1.")]
    fn an_operand_that_fixes_its_width_ignores_the_column_of_its_copy(#[case] rule: &str) {
        let src = format!(
            ".extern fn narrow(a: int8) -> int8\n\
             .decl Seed(n: int32)\n\
             .decl R(i: int64, s: symbol)\n\
             .input Seed(IO=\"file\", filename=\"Seed.csv\", delimiter=\",\")\n\
             .output R\n\
             {rule}\n"
        );
        checked_interned(&src).expect("each copy takes the width its own context asks");
    }

    #[test]
    fn ord_rejects_a_scalar_other_than_a_symbol() {
        let src = "\
            .decl In(n: int32)\n\
            .decl Out(n: int32)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(ord(n)) :- In(n).\n";
        assert_err!(
            checked_interned(src),
            ParseError::BuiltinArgType { arg_index: 0, .. }
        );
    }

    /// Only `ord` takes a tuple; a string built-in still refuses one.
    #[test]
    fn strlen_rejects_a_tuple() {
        let src = "\
            .decl In(x: symbol, y: symbol)\n\
            .decl Out(n: int32)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(strlen((x, y))) :- In(x, y).\n";
        assert_err!(
            checked_interned(src),
            ParseError::BuiltinArgType { arg_index: 0, .. }
        );
    }

    /// The `--str-intern` gate covers a tuple operand as it does a symbol.
    #[test]
    fn ord_of_a_tuple_requires_str_intern() {
        let src = "\
            .decl In(x: symbol, y: symbol)\n\
            .decl Out(n: int32)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(ord((x, y))) :- In(x, y).\n";
        assert_err!(checked(src), ParseError::OrdRequiresStrIntern { .. });
    }

    /// `ord()` needs `--str-intern`; the gate rejects it in a plain rule.
    #[test]
    fn ord_in_plain_rule_requires_str_intern() {
        let src = "\
            .decl In(s: symbol)\n\
            .decl Out(n: int32)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(ord(s)) :- In(s).\n";
        assert_err!(checked(src), ParseError::OrdRequiresStrIntern { .. });
    }

    /// The `ord` gate looks inside aggregates too.
    #[test]
    fn ord_inside_an_aggregate_requires_str_intern() {
        let src = "\
            .decl In(s: symbol)\n\
            .decl Out(s: symbol, n: int32)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(s, n) :- In(s), n = count : { In(t), ord(t) > 0 }.\n";
        assert_err!(checked(src), ParseError::OrdRequiresStrIntern { .. });
    }
}
