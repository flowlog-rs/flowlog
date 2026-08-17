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
use flowlog_error::Span;

use crate::Arithmetic;
use crate::BuiltinOperator;
use crate::DataType;
use crate::Factor;
use crate::FlowLogRule;
use crate::HeadArg;
use crate::ParseError;
use crate::Predicate;
use crate::Program;
use crate::Segment;
use crate::pipeline::typecheck::env::PrimitiveEnv;

/// Var -> (first-seen type, first-seen span). Later uses must agree.
/// Private, so it stays confined to the `primitive` subtree; the subtype
/// pass keeps its own `TypeId`-keyed map.
type Bindings = HashMap<String, (DataType, Span)>;

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

/// Check `ord(_)` usage across every segment (plain, loop, and fixpoint):
/// reject it without `--str-intern` ([`ParseError::OrdRequiresStrIntern`]),
/// otherwise set [`Config::serialize_load`] so the loader interns serially and
/// `ord` stays deterministic across worker counts.
fn check_ord(program: &Program, config: &mut Config) -> Result<(), ParseError> {
    fn arith(a: &Arithmetic) -> Option<Span> {
        factor(a.init()).or_else(|| a.rest().iter().find_map(|(_, f)| factor(f)))
    }
    fn factor(f: &Factor) -> Option<Span> {
        match f {
            Factor::Var(_) | Factor::Const(_) => None,
            Factor::FnCall(fc) => fc.args().iter().find_map(arith),
            Factor::Builtin(bc) if bc.op() == BuiltinOperator::Ord => Some(bc.span()),
            Factor::Builtin(bc) => bc.args().iter().find_map(arith),
            Factor::Cast(c) => factor(c.inner()),
            Factor::Group(a) => arith(a),
            Factor::Tuple(r) => r.exprs().find_map(arith),
            Factor::TupleProj { tuple, .. } => arith(tuple),
        }
    }

    let ord_span = program.segments().iter().find_map(|segment| {
        let rules: &[FlowLogRule] = match segment {
            Segment::Plain(rules) => rules,
            Segment::Loop(block) | Segment::Fixpoint(block) => block.rules(),
        };
        rules.iter().find_map(|rule| {
            let body = rule.rhs().iter().find_map(|predicate| match predicate {
                Predicate::PositiveAtom(_) | Predicate::NegativeAtom(_) => None,
                Predicate::Compare(cmp) => arith(cmp.left()).or_else(|| arith(cmp.right())),
            });
            body.or_else(|| {
                rule.head()
                    .head_arguments()
                    .iter()
                    .find_map(|head_arg| match head_arg {
                        HeadArg::Var(_) => None,
                        HeadArg::Arith(a) => arith(a),
                        HeadArg::Aggregation(agg) => arith(agg.arithmetic()),
                    })
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
    use crate::test_util::checked;

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

    /// The gate must reach inside loop/fixpoint blocks too, not only plain
    /// segments (regression: it once scanned `as_rules()`, skipping loop rules).
    #[test]
    fn ord_in_loop_rule_requires_str_intern() {
        let src = "\
            .decl Edge(x: symbol)\n\
            .decl A(n: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output A\n\
            fixpoint {\n\
                A(ord(x)) :- Edge(x).\n\
            }\n";
        assert_err!(checked(src), ParseError::OrdRequiresStrIntern { .. });
    }
}
