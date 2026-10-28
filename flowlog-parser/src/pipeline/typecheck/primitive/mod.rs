//! Pass 1: primitive `DataType` checking and literal pinning. Subtype-blind;
//! [`crate::pipeline::typecheck::subtype`] is Pass 2.
//!
//! A module per construct, built on the literal-typing algebra on [`DataType`]
//! (`merge`/`fits`/`defaulted`): `expr`, `atom`, `fact`, `compare`, and `rule`
//! (the composer). [`check_and_pin`] runs the pass.
//!
//! Function names read `<verb>_<node>`, where the verb is one of:
//!
//! - **bind**: record a variable's type from the positive-atom column it
//!   fills. The only place a variable's type is introduced.
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
    use crate::ParseError;
    use crate::assert_err;
    use crate::test_harness::checked;

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
