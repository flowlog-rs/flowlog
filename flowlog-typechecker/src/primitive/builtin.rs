//! Builtin checks.

use flowlog_common::Config;
use flowlog_common::Span;
use flowlog_parser::Arithmetic;
use flowlog_parser::BuiltinOperator;
use flowlog_parser::Factor;
use flowlog_parser::HeadArg;
use flowlog_parser::Predicate;
use flowlog_parser::Program;

use crate::TypeCheckError;

/// Check `ord(_)` usage in one walk over the program (including `loop`/`fixpoint`
/// bodies):
///
/// - reject `ord` without `--str-intern` (`OrdRequiresStrIntern`);
/// - otherwise set [`Config::serialize_load`] so the loader interns serially,
///   keeping `ord` deterministic across worker counts.
pub(crate) fn check_ord(program: &Program, config: &mut Config) -> Result<(), TypeCheckError> {
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
            Factor::Tuple(t) => t.exprs().find_map(arith),
            Factor::TupleProj { tuple, .. } => arith(tuple),
        }
    }

    // Chain `as_loop()`: `as_rules()` alone skips rules nested in
    // `loop`/`fixpoint` blocks, so an `ord` there would otherwise escape both
    // the error and the serial-load decision.
    let ord_span = program.segments().iter().find_map(|seg| {
        let plain = seg.as_rules().iter();
        let looped = seg.as_loop().into_iter().flat_map(|b| b.rules().iter());
        plain.chain(looped).find_map(|rule| {
            let body = rule.rhs().iter().find_map(|p| match p {
                Predicate::PositiveAtom(_) | Predicate::NegativeAtom(_) => None,
                Predicate::Compare(c) => arith(c.left()).or_else(|| arith(c.right())),
            });
            body.or_else(|| {
                rule.head().head_arguments().iter().find_map(|h| match h {
                    HeadArg::Var(_) => None,
                    HeadArg::Arith(a) => arith(a),
                    HeadArg::Aggregation(agg) => arith(agg.arithmetic()),
                })
            })
        })
    });

    if let Some(span) = ord_span {
        if !config.str_intern_enabled() {
            return Err(TypeCheckError::OrdRequiresStrIntern { span });
        }
        config.serialize_load = true;
    }
    Ok(())
}
