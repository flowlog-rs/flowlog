//! Pass 1: primitive `DataType` checking and literal pinning. Subtype-blind —
//! [`crate::subtype`] is Pass 2.
//!
//! Per-construct bricks over `ty` (the `LitKind` lattice): `expr` infers and
//! pins expressions; `atom`, `fact`, `compare`, and `rule` (the composer)
//! check and pin each construct. Names read `<verb>_<node>` — `infer`, `check`,
//! `pin`, or `check_and_pin` for both. `check_program` calls
//! `rule::check_and_pin_rules` and `fact::check_and_pin_facts` directly.

mod atom;
mod compare;
mod expr;
pub(crate) mod fact;
pub(crate) mod rule;
mod ty;

use std::collections::HashMap;

use flowlog_common::Config;
use flowlog_common::Span;
use flowlog_parser::Arithmetic;
use flowlog_parser::BuiltinOperator;
use flowlog_parser::DataType;
use flowlog_parser::Factor;
use flowlog_parser::HeadArg;
use flowlog_parser::Predicate;
use flowlog_parser::Program;

use crate::TypeCheckError;

/// Var -> (first-seen type, first-seen span). Later uses must agree.
/// Private, so it stays confined to the `primitive` subtree — the subtype
/// pass keeps its own `TypeId`-keyed map.
type Bindings = HashMap<String, (DataType, Span)>;

/// Reject built-in calls whose semantics depend on a build flag that
/// isn't enabled — today only `ord(_)`, which needs `--str-intern`.
pub(crate) fn check_builtin_config_requirements(
    program: &Program,
    config: &Config,
) -> Result<(), TypeCheckError> {
    if config.str_intern_enabled() {
        return Ok(());
    }
    fn check_arith(a: &Arithmetic) -> Result<(), TypeCheckError> {
        check_factor(a.init())?;
        for (_, f) in a.rest() {
            check_factor(f)?;
        }
        Ok(())
    }
    fn check_factor(f: &Factor) -> Result<(), TypeCheckError> {
        match f {
            Factor::Var(_) | Factor::Const(_) => Ok(()),
            Factor::FnCall(fc) => fc.args().iter().try_for_each(check_arith),
            Factor::Builtin(bc) => {
                if bc.op() == BuiltinOperator::Ord {
                    return Err(TypeCheckError::OrdRequiresStrIntern { span: bc.span() });
                }
                bc.args().iter().try_for_each(check_arith)
            }
            Factor::Cast(c) => check_factor(c.inner()),
            Factor::Group(a) => check_arith(a),
            Factor::Tuple(r) => r.exprs().try_for_each(check_arith),
            Factor::TupleProj { tuple, .. } => check_arith(tuple),
        }
    }
    for segment in program.segments() {
        for rule in segment.as_rules() {
            for predicate in rule.rhs() {
                match predicate {
                    Predicate::PositiveAtom(_) | Predicate::NegativeAtom(_) => {}
                    Predicate::Compare(cmp) => {
                        check_arith(cmp.left())?;
                        check_arith(cmp.right())?;
                    }
                }
            }
            for head_arg in rule.head().head_arguments() {
                match head_arg {
                    HeadArg::Var(_) => {}
                    HeadArg::Arith(a) => check_arith(a)?,
                    HeadArg::Aggregation(agg) => check_arith(agg.arithmetic())?,
                }
            }
        }
    }
    Ok(())
}
