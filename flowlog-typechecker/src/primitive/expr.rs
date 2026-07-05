//! Infer and pin an expression (`Arithmetic`/`Factor`); the two stay together
//! because pinning a call's args re-infers them.

use flowlog_common::Span;
use flowlog_parser::Arithmetic;
use flowlog_parser::ArithmeticOperator;
use flowlog_parser::BuiltinCall;
use flowlog_parser::DataType;
use flowlog_parser::Factor;
use flowlog_parser::FnCall;
use flowlog_parser::TupleElem;

use crate::TypeCheckError;
use crate::env::UdfSigs;
use crate::primitive::Bindings;
use crate::primitive::ty::LitKind;

/// Infer an expression's kind, merging factor kinds left to right.
/// `None` iff every variable factor is unbound (reported later by the
/// range-restriction pass).
pub(super) fn infer_expr(
    expr: &Arithmetic,
    bindings: &Bindings,
    udfs: &UdfSigs,
) -> Result<Option<LitKind>, TypeCheckError> {
    let span = expr.span();
    let mut inferred = infer_factor(expr.init(), bindings, udfs)?;

    for (op, factor) in expr.rest() {
        if let Some(k) = infer_factor(factor, bindings, udfs)? {
            inferred = match inferred {
                None => Some(k),
                Some(existing) => Some(existing.merge(&k).ok_or_else(|| {
                    TypeCheckError::ArithmeticTypeMismatch {
                        span,
                        left: existing.report_ty(),
                        right: k.report_ty(),
                    }
                })?),
            };
        }
        if let Some(k) = &inferred {
            check_arith_op(k, op, span)?;
        }
    }
    Ok(inferred)
}

fn infer_factor(
    factor: &Factor,
    bindings: &Bindings,
    udfs: &UdfSigs,
) -> Result<Option<LitKind>, TypeCheckError> {
    Ok(match factor {
        Factor::Var(v) => bindings.get(v).map(|(ty, _)| LitKind::Concrete(ty.clone())),
        Factor::Const(c) => Some(LitKind::of(c)?),
        Factor::FnCall(fc) => Some(LitKind::Concrete(infer_fn_call(fc, bindings, udfs)?)),
        Factor::Builtin(bc) => Some(LitKind::Concrete(infer_builtin_call(bc, bindings, udfs)?)),
        // Primitive layer sees through casts; the `subtype` pass
        // enforces the cast rules separately.
        Factor::Cast(c) => infer_factor(c.inner(), bindings, udfs)?,
        Factor::Group(a) => infer_expr(a, bindings, udfs)?,
        // Tuple construct `(e0, …)`: the type is the fixed tuple of the
        // components' concrete types. (Destructures are desugared to `TupleProj`s,
        // so a surviving placeholder here is a `_` in a construct — invalid.)
        Factor::Tuple(r) => {
            let mut fields = Vec::with_capacity(r.fields().len());
            for elem in r.fields() {
                match elem {
                    TupleElem::Expr(a) => match infer_expr(a, bindings, udfs)? {
                        Some(k) => fields.push(k.report_ty()),
                        // A component is unbound — the range-restriction pass
                        // reports it; we can't determine the tuple type yet.
                        None => return Ok(None),
                    },
                    TupleElem::Placeholder => {
                        return Err(TypeCheckError::TuplePlaceholderInConstruct { span: r.span() });
                    }
                }
            }
            Some(LitKind::Concrete(DataType::FixedTuple(fields)))
        }
        // Projection `tuple.index` (synthesized by the destructure desugar):
        // the type is the indexed field's type. A non-tuple base or an
        // out-of-range index means the user destructured something that isn't a
        // tuple of that shape — a clean user error, not an internal bug.
        Factor::TupleProj { tuple, index } => match infer_expr(tuple, bindings, udfs)? {
            Some(LitKind::Concrete(DataType::FixedTuple(fields))) => match fields.get(*index) {
                Some(ty) => Some(LitKind::Concrete(ty.clone())),
                None => {
                    return Err(TypeCheckError::TupleDestructure {
                        span: tuple.span(),
                        detail: format!(
                            "destructure pattern has more than {} field(s)",
                            fields.len()
                        ),
                    });
                }
            },
            Some(other) => {
                return Err(TypeCheckError::TupleDestructure {
                    span: tuple.span(),
                    detail: format!(
                        "cannot destructure `{}`, which is not a tuple",
                        other.report_ty()
                    ),
                });
            }
            None => None,
        },
    })
}

/// Type-check a built-in call against its [`BuiltinOperator`] signature.
/// Arity is already enforced by the parser, so only per-arg types here.
fn infer_builtin_call(
    bc: &BuiltinCall,
    bindings: &Bindings,
    udfs: &UdfSigs,
) -> Result<DataType, TypeCheckError> {
    let op = bc.op();
    debug_assert_eq!(
        bc.args().len(),
        op.param_allowed_types().len(),
        "parser should enforce builtin arity"
    );

    // An arg is valid if its type is in the parameter's allowed set. A
    // multi-element set is a polymorphic parameter (e.g. `to_string` over any
    // numeric/bool scalar); a tuple operand fits no scalar set and is rejected.
    for (i, (arg, allowed)) in bc
        .args()
        .iter()
        .zip(op.param_allowed_types().iter())
        .enumerate()
    {
        let Some(kind) = infer_expr(arg, bindings, udfs)? else {
            continue;
        };
        if !allowed.iter().any(|t| kind.fits(t)) {
            return Err(TypeCheckError::BuiltinArgType {
                span: arg.span(),
                op,
                arg_index: i,
                expected: allowed.to_vec(),
                found: kind.report_ty(),
            });
        }
    }

    Ok(op.ret_type())
}

fn infer_fn_call(
    fc: &FnCall,
    bindings: &Bindings,
    udfs: &UdfSigs,
) -> Result<DataType, TypeCheckError> {
    let (param_types, ret_ty) =
        udfs.get(fc.name())
            .ok_or_else(|| TypeCheckError::UndeclaredUdf {
                span: fc.span(),
                name: fc.name().to_string(),
            })?;

    if fc.args().len() != param_types.len() {
        return Err(TypeCheckError::UdfArity {
            span: fc.span(),
            name: fc.name().to_string(),
            expected: param_types.len(),
            found: fc.args().len(),
        });
    }

    for (arg, (param_name, expected)) in fc.args().iter().zip(param_types.iter()) {
        let Some(kind) = infer_expr(arg, bindings, udfs)? else {
            continue;
        };
        if !kind.fits(expected) {
            return Err(TypeCheckError::UdfArgType {
                span: arg.span(),
                name: fc.name().to_string(),
                param: param_name.clone(),
                expected: expected.clone(),
                found: kind.report_ty(),
            });
        }
    }

    Ok(ret_ty.clone())
}

/// Numeric ops (`+`, `-`, `*`, `/`, `%`) require numeric factors.
/// String / bool factors can't appear in arithmetic.
fn check_arith_op(
    kind: &LitKind,
    op: &ArithmeticOperator,
    span: Span,
) -> Result<(), TypeCheckError> {
    // Arithmetic requires a numeric operand. This rejects `Bool`/`String` and
    // also tuple operands.
    if kind.is_numeric() {
        Ok(())
    } else {
        Err(TypeCheckError::ArithmeticOpNotAllowed {
            span,
            op: op.clone(),
            ty: kind.report_ty(),
        })
    }
}

/// Pin every polymorphic literal in `a` to `target`. Recurses into UDF
/// argument expressions using the UDF's declared parameter types — those
/// types are independent of the enclosing expression's `target`.
pub(super) fn pin_expr(
    a: &mut Arithmetic,
    target: &DataType,
    bindings: &Bindings,
    udfs: &UdfSigs,
) -> Result<(), TypeCheckError> {
    pin_factor(a.init_mut(), target, bindings, udfs)?;
    for (_, f) in a.rest_mut() {
        pin_factor(f, target, bindings, udfs)?;
    }
    Ok(())
}

fn pin_factor(
    factor: &mut Factor,
    target: &DataType,
    bindings: &Bindings,
    udfs: &UdfSigs,
) -> Result<(), TypeCheckError> {
    match factor {
        Factor::Const(c) => {
            if c.is_polymorphic() {
                c.pin(target.clone());
            }
            Ok(())
        }
        Factor::Var(_) => Ok(()),
        Factor::FnCall(fc) => pin_fn_call(fc, bindings, udfs),
        Factor::Builtin(bc) => pin_builtin_call(bc, bindings, udfs),
        // Cast asserts its inner has the target's primitive — pin
        // polymorphic literals inside accordingly.
        Factor::Cast(c) => pin_factor(c.inner_mut(), target, bindings, udfs),
        Factor::Group(a) => pin_expr(a, target, bindings, udfs),
        // Tuple construct: pin each component against its declared field type.
        Factor::Tuple(r) => {
            if let DataType::FixedTuple(field_types) = target {
                for (elem, fty) in r.fields_mut().iter_mut().zip(field_types.iter()) {
                    if let TupleElem::Expr(a) = elem {
                        pin_expr(a, fty, bindings, udfs)?;
                    }
                }
            }
            Ok(())
        }
        // Projection's base is a synthesized bound variable — no literals.
        Factor::TupleProj { .. } => Ok(()),
    }
}

/// Pin polymorphic literals in a built-in call. Every arg is pinned to its own
/// inferred type: validation already guaranteed the arg fits the parameter's
/// allowed set, so the operand's own type is the right (and only consistent)
/// pin target — for fixed-type params it equals the declared type, and for
/// polymorphic params (`to_string`) it's whatever the operand actually is.
fn pin_builtin_call(
    bc: &mut BuiltinCall,
    bindings: &Bindings,
    udfs: &UdfSigs,
) -> Result<(), TypeCheckError> {
    for arg in bc.args_mut() {
        if let Some(kind) = infer_expr(arg, bindings, udfs)? {
            pin_expr(arg, &kind.report_ty(), bindings, udfs)?;
        }
    }
    Ok(())
}

fn pin_fn_call(fc: &mut FnCall, bindings: &Bindings, udfs: &UdfSigs) -> Result<(), TypeCheckError> {
    // Collected by value so the recursive `pin_expr` below can reborrow `udfs`
    // — holding `&param_types` from `udfs.get(...)` across the recursion would
    // block the reborrow.
    let param_types: Vec<DataType> = udfs
        .get(fc.name())
        .map(|(params, _)| params.iter().map(|(_, ty)| ty.clone()).collect())
        .ok_or_else(|| {
            TypeCheckError::internal(format!("pin_fn_call: UDF `{}` not declared", fc.name()))
        })?;
    for (arg, pty) in fc.args_mut().iter_mut().zip(param_types.iter()) {
        pin_expr(arg, pty, bindings, udfs)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use flowlog_common::Span;
    use flowlog_parser::ArithmeticOperator;
    use flowlog_parser::DataType;

    use super::*;

    /// `check_arith_op` rejects every non-numeric type around an
    /// arithmetic op. String concatenation lives in the `cat(a, b)`
    /// built-in (not exercised here — built-ins go through a separate
    /// signature-check path), so a string factor in an arithmetic
    /// expression is always wrong. A regression in any row would
    /// silently flip acceptance for real programs.
    #[test]
    fn check_arith_op_table() {
        use ArithmeticOperator::*;
        use DataType::*;
        use LitKind::Concrete;

        let span = Span::DUMMY;

        // Positive: numeric with numeric ops is fine.
        assert!(check_arith_op(&Concrete(Int32), &Plus, span).is_ok());
        assert!(check_arith_op(&Concrete(Float64), &Multiply, span).is_ok());

        // Negative: numeric op on strings → error.
        assert!(check_arith_op(&Concrete(String), &Plus, span).is_err());

        // Negative: Bool rejects every arithmetic op.
        assert!(check_arith_op(&Concrete(Bool), &Plus, span).is_err());
        assert!(check_arith_op(&Concrete(Bool), &Multiply, span).is_err());
    }
}
