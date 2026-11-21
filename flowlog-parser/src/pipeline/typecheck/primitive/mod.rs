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
    use crate::Factor;
    use crate::HeadArg;
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
    #[case::nested("Out(ord((x, (y, 1)))) :- In(x, y).")]
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

    /// Type-checks `rule` under `.type T = ( a: int64 )` and returns the
    /// width of the literal `ord`'s tuple operand holds in head column
    /// `col`.
    fn ord_operand_width(decl: &str, rule: &str, col: usize) -> DataType {
        let src = format!(
            ".type T = ( a: int64 )\n\
             .decl Seed(n: int32)\n\
             {decl}\n\
             .input Seed(IO=\"file\", filename=\"Seed.csv\", delimiter=\",\")\n\
             .output R\n\
             {rule}\n"
        );
        let program = checked_interned(&src).expect("type-checks");
        let HeadArg::Arith(column) = &program.rules()[0].head().head_arguments()[col] else {
            panic!("column {col} is an expression");
        };
        let Factor::Builtin(ord) = column.init() else {
            panic!("column {col} is a call");
        };
        let Factor::Tuple(fields) = ord.args()[0].init() else {
            panic!("ord's operand is a tuple");
        };
        let Factor::Const(literal) = fields[0].init() else {
            panic!("the tuple's field is a literal");
        };
        literal.ty().clone()
    }

    /// Substitution copies `p`'s value into the column and into `ord`, and
    /// both copies take the column's `int64`: one tuple is numbered in one
    /// table, whichever side of the head `ord` sits on.
    // Cases: declaration of R, rule, column of the ord call.
    #[rstest]
    #[case::ord_after_the_column(
        ".decl R(p: T, o: int32)",
        "R(p, ord(p)) :- Seed(n), p = (1,).",
        1
    )]
    #[case::ord_before_the_column(
        ".decl R(o: int32, p: T)",
        "R(ord(p), p) :- Seed(n), p = (1,).",
        0
    )]
    fn an_assigned_tuple_is_numbered_at_the_width_its_column_gives(
        #[case] decl: &str,
        #[case] rule: &str,
        #[case] col: usize,
    ) {
        assert_eq!(ord_operand_width(decl, rule, col), DataType::Int64);
    }

    /// A tuple written in `ord` itself is no copy of the column's, so its
    /// literal keeps the default width.
    #[test]
    fn a_tuple_written_in_ord_keeps_the_default_width() {
        assert_eq!(
            ord_operand_width(
                ".decl R(p: T, o: int32)",
                "R(p, ord((1,))) :- Seed(n), p = (1,).",
                1
            ),
            DataType::Int32
        );
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
