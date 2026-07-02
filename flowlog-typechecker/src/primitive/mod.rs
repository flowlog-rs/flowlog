//! Pass 1 — primitive `DataType` checking and literal pinning.
//!
//! Binds variable types from positive-atom columns, checks every body/head
//! site, and pins every polymorphic literal to a concrete width. Blind to
//! subtypes (`UserId <: number` looks like `Int32`); [`crate::subtype`] is the
//! second pass that recovers the nominal distinctions.

mod check;
mod expr;
mod lattice;

use std::collections::HashMap;

use flowlog_common::Config;
use flowlog_common::Span;
use flowlog_parser::Arithmetic;
use flowlog_parser::BuiltinOperator;
use flowlog_parser::DataType;
use flowlog_parser::Factor;
use flowlog_parser::HeadArg;
use flowlog_parser::InlineFact;
use flowlog_parser::Predicate;
use flowlog_parser::Program;

use crate::TypeCheckError;
use crate::env::PrimitiveEnv;
use crate::primitive::lattice::LitKind;

/// Var -> (first-seen type, first-seen span). Later uses must agree.
/// Per-rule and primitive-pass-local — the subtype pass keeps its own
/// `TypeId`-keyed map, so this deliberately isn't visible outside `primitive`.
pub(in crate::primitive) type Bindings = HashMap<String, (DataType, Span)>;

/// Check + pin every rule (plain and loop-internal) against the primitive
/// type environment.
pub(crate) fn check_and_pin_rules(
    program: &mut Program,
    env: &PrimitiveEnv,
) -> Result<(), TypeCheckError> {
    for segment in program.segments_mut() {
        for rule in segment.as_rules_mut() {
            check::check_rule(rule, env)?;
        }
        if let Some(block) = segment.as_loop_mut() {
            for rule in block.rules_mut() {
                check::check_rule(rule, env)?;
            }
        }
    }
    Ok(())
}

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

/// Validate each fact tuple's column families against its `.decl` and pin
/// polymorphic literals. Diagnostics cite the fact's head span.
pub(crate) fn check_and_pin_facts(
    facts: &mut HashMap<String, Vec<InlineFact>>,
    env: &PrimitiveEnv,
) -> Result<(), TypeCheckError> {
    for (rel_name, entries) in facts.iter_mut() {
        let Some(col_types) = env.decls.get(rel_name) else {
            return Err(TypeCheckError::internal(format!(
                "fact references undeclared relation `{rel_name}`"
            )));
        };
        for fact in entries.iter_mut() {
            if fact.columns.len() != col_types.len() {
                return Err(TypeCheckError::HeadArity {
                    span: fact.span,
                    rel: fact.raw_name.clone(),
                    expected: col_types.len(),
                    found: fact.columns.len(),
                });
            }
            for (c, col_ty) in fact.columns.iter_mut().zip(col_types.iter()) {
                if !LitKind::of(c)?.fits(col_ty) {
                    return Err(TypeCheckError::LiteralColumnMismatch {
                        span: fact.span,
                        literal: c.to_string(),
                        expected: col_ty.clone(),
                    });
                }
                if c.is_polymorphic() {
                    c.pin(col_ty.clone());
                }
            }
        }
    }
    Ok(())
}
