//! Infer and pin an expression (`Arithmetic`/`Factor`).

use flowlog_error::Span;

use crate::Arithmetic;
use crate::ArithmeticOperator;
use crate::BuiltinCall;
use crate::DataType;
use crate::Factor;
use crate::FnCall;
use crate::ParseError;
use crate::TupleElem;
use crate::error::grammar_bug;
use crate::pipeline::typecheck::env::UdfSigs;
use crate::pipeline::typecheck::primitive::Bindings;

/// Infer an expression's type, merging factor types left to right.
/// `None` iff every variable factor is unbound (reported later by the
/// range-restriction pass).
pub(super) fn infer_expr(
    expr: &Arithmetic,
    bindings: &Bindings,
    udfs: &UdfSigs,
) -> Result<Option<DataType>, ParseError> {
    let span = expr.span();
    let mut inferred = infer_factor(expr.init(), bindings, udfs)?;

    for (op, factor) in expr.rest() {
        if let Some(k) = infer_factor(factor, bindings, udfs)? {
            inferred =
                match inferred {
                    None => Some(k),
                    Some(existing) => Some(existing.merge(&k).ok_or_else(|| {
                        ParseError::ArithmeticTypeMismatch {
                            span,
                            left: existing.defaulted(),
                            right: k.defaulted(),
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
) -> Result<Option<DataType>, ParseError> {
    Ok(match factor {
        Factor::Var(v) => bindings.get(v).map(|(ty, _)| ty.clone()),
        Factor::Const(c) => Some(c.ty().clone()),
        Factor::FnCall(fc) => Some(infer_fn_call(fc, bindings, udfs)?),
        Factor::Builtin(bc) => Some(infer_builtin_call(bc, bindings, udfs)?),
        // Primitive layer sees through casts; the `subtype` pass
        // enforces the cast rules separately.
        Factor::Cast(c) => infer_factor(c.inner(), bindings, udfs)?,
        Factor::Group(a) => infer_expr(a, bindings, udfs)?,
        // Tuple construct `(e0, ...)`: the type is the fixed tuple of the
        // components' concrete types. (Destructures are desugared to `TupleProj`s,
        // so a surviving placeholder here is a `_` in a construct, which is invalid.)
        Factor::Tuple(r) => {
            let mut fields = Vec::with_capacity(r.fields().len());
            for elem in r.fields() {
                match elem {
                    TupleElem::Expr(a) => match infer_expr(a, bindings, udfs)? {
                        Some(k) => fields.push(k.defaulted()),
                        // A component is unbound: the range-restriction pass
                        // reports it; we can't determine the tuple type yet.
                        None => return Ok(None),
                    },
                    TupleElem::Placeholder => {
                        return Err(ParseError::TuplePlaceholderInConstruct { span: r.span() });
                    }
                }
            }
            Some(DataType::FixedTuple(fields))
        }
        // Projection `tuple.index` (synthesized by the destructure desugar):
        // the type is the indexed field's type. A non-tuple base or an
        // out-of-range index means the user destructured something that isn't a
        // tuple of that shape: a clean user error, not an internal bug.
        Factor::TupleProj { tuple, index } => match infer_expr(tuple, bindings, udfs)? {
            Some(DataType::FixedTuple(fields)) => match fields.get(*index) {
                Some(ty) => Some(ty.clone()),
                None => {
                    return Err(ParseError::TupleDestructure {
                        span: tuple.span(),
                        detail: format!(
                            "destructure pattern has more than {} field(s)",
                            fields.len()
                        ),
                    });
                }
            },
            Some(other) => {
                return Err(ParseError::TupleDestructure {
                    span: tuple.span(),
                    detail: format!(
                        "cannot destructure `{}`, which is not a tuple",
                        other.defaulted()
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
) -> Result<DataType, ParseError> {
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
            return Err(ParseError::BuiltinArgType {
                span: arg.span(),
                op,
                arg_index: i,
                expected: allowed.to_vec(),
                found: kind.defaulted(),
            });
        }
    }

    Ok(op.ret_type())
}

fn infer_fn_call(fc: &FnCall, bindings: &Bindings, udfs: &UdfSigs) -> Result<DataType, ParseError> {
    let (param_types, ret_ty) = udfs
        .get(fc.name())
        .ok_or_else(|| ParseError::UndeclaredUdf {
            span: fc.span(),
            name: fc.name().to_string(),
        })?;

    if fc.args().len() != param_types.len() {
        return Err(ParseError::UdfArity {
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
            return Err(ParseError::UdfArgType {
                span: arg.span(),
                name: fc.name().to_string(),
                param: param_name.clone(),
                expected: expected.clone(),
                found: kind.defaulted(),
            });
        }
    }

    Ok(ret_ty.clone())
}

/// Numeric ops (`+`, `-`, `*`, `/`, `%`) require numeric factors.
/// String / bool factors can't appear in arithmetic.
fn check_arith_op(kind: &DataType, op: &ArithmeticOperator, span: Span) -> Result<(), ParseError> {
    // Arithmetic requires a numeric operand. This rejects `Bool`/`String` and
    // also tuple operands.
    if kind.defaulted().is_numeric() {
        Ok(())
    } else {
        Err(ParseError::ArithmeticOpNotAllowed {
            span,
            op: op.clone(),
            ty: kind.defaulted(),
        })
    }
}

/// Pin every polymorphic literal in `a` to `target`. Recurses into UDF
/// argument expressions using the UDF's declared parameter types, which are
/// independent of the enclosing expression's `target`.
pub(super) fn pin_expr(
    a: &mut Arithmetic,
    target: &DataType,
    bindings: &Bindings,
    udfs: &UdfSigs,
) -> Result<(), ParseError> {
    let span = a.span();
    pin_factor(a.init_mut(), target, span, bindings, udfs)?;
    for (_, f) in a.rest_mut() {
        pin_factor(f, target, span, bindings, udfs)?;
    }
    Ok(())
}

fn pin_factor(
    factor: &mut Factor,
    target: &DataType,
    span: Span,
    bindings: &Bindings,
    udfs: &UdfSigs,
) -> Result<(), ParseError> {
    match factor {
        Factor::Const(c) => {
            if c.is_polymorphic() {
                c.pin(target.clone(), span)?;
            }
            Ok(())
        }
        Factor::Var(_) => Ok(()),
        Factor::FnCall(fc) => pin_fn_call(fc, bindings, udfs),
        Factor::Builtin(bc) => pin_builtin_call(bc, bindings, udfs),
        // Cast asserts its inner has the target's primitive, so pin
        // polymorphic literals inside accordingly.
        Factor::Cast(c) => pin_factor(c.inner_mut(), target, span, bindings, udfs),
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
        // Projection's base is a synthesized bound variable: no literals.
        Factor::TupleProj { .. } => Ok(()),
    }
}

/// Pin polymorphic literals in a built-in call. Every arg is pinned to its own
/// inferred type: validation already guaranteed the arg fits the parameter's
/// allowed set, so the operand's own type is the right (and only consistent)
/// pin target: for fixed-type params it equals the declared type, and for
/// polymorphic params (`to_string`) it's whatever the operand actually is.
fn pin_builtin_call(
    bc: &mut BuiltinCall,
    bindings: &Bindings,
    udfs: &UdfSigs,
) -> Result<(), ParseError> {
    for arg in bc.args_mut() {
        if let Some(kind) = infer_expr(arg, bindings, udfs)? {
            pin_expr(arg, &kind.defaulted(), bindings, udfs)?;
        }
    }
    Ok(())
}

fn pin_fn_call(fc: &mut FnCall, bindings: &Bindings, udfs: &UdfSigs) -> Result<(), ParseError> {
    // Collected by value so the recursive `pin_expr` below can reborrow `udfs`;
    // holding `&param_types` from `udfs.get(...)` across the recursion would
    // block it.
    let param_types: Vec<DataType> = udfs
        .get(fc.name())
        .map(|(params, _)| params.iter().map(|(_, ty)| ty.clone()).collect())
        .ok_or_else(|| grammar_bug(format!("pin_fn_call: UDF `{}` not declared", fc.name())))?;
    for (arg, pty) in fc.args_mut().iter_mut().zip(param_types.iter()) {
        pin_expr(arg, pty, bindings, udfs)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use flowlog_error::Span;

    use super::*;
    use crate::ArithmeticOperator;
    use crate::Constant;
    use crate::DataType;
    use crate::Factor;
    use crate::HeadArg;
    use crate::ParseError;
    use crate::assert_err;
    use crate::test_util::checked;

    /// Arithmetic mixing an integer and a float (`int32 + 5.0`) has no meet
    /// and is rejected.
    #[test]
    fn arithmetic_mixing_int_and_float_rejected() {
        let src = "\
            .decl Score(pts: int32)\n\
            .decl Out(v: int32)\n\
            .input Score(IO=\"file\", filename=\"Score.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(pts + 5.0) :- Score(pts).\n";
        assert_err!(checked(src), ParseError::ArithmeticTypeMismatch { .. });
    }

    /// A call to a function with no `.extern fn` declaration is rejected.
    #[test]
    fn undeclared_udf_call_rejected() {
        let src = "\
            .decl In(x: int32)\n\
            .decl Out(v: int32)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(f(x)) :- In(x).\n";
        assert_err!(checked(src), ParseError::UndeclaredUdf { .. });
    }

    /// `check_arith_op` rejects every non-numeric type around an
    /// arithmetic op. String concatenation lives in the `cat(a, b)`
    /// built-in (not exercised here; built-ins go through a separate
    /// signature-check path), so a string factor in an arithmetic
    /// expression is always wrong. A regression in any row would
    /// silently flip acceptance for real programs.
    #[test]
    fn check_arith_op_table() {
        use ArithmeticOperator::*;
        use DataType::*;

        let span = Span::DUMMY;

        // Positive: numeric with numeric ops is fine.
        assert!(check_arith_op(&Int32, &Plus, span).is_ok());
        assert!(check_arith_op(&Float64, &Multiply, span).is_ok());

        // Negative: numeric op on strings is an error.
        assert!(check_arith_op(&String, &Plus, span).is_err());

        // Negative: Bool rejects every arithmetic op.
        assert!(check_arith_op(&Bool, &Plus, span).is_err());
        assert!(check_arith_op(&Bool, &Multiply, span).is_err());
    }

    /// Nested UDF call: in `f(1) + x` where `x: int64` and `f: int8 -> int64`,
    /// the `1` must be pinned to the UDF's parameter width (`Int8`), NOT the
    /// enclosing expression's target (`Int64`). A regression using outer
    /// context inside `pin_fn_call` would silently widen the literal.
    #[test]
    fn nested_udf_arg_pinned_to_param_type_not_outer_target() {
        let src = "\
            .decl Item(x: int64)\n\
            .decl Flag(x: int64)\n\
            .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
            .output Flag\n\
            .extern fn f(a: int8) -> int64\n\
            Flag(f(1) + x) :- Item(x).\n";
        let program = checked(src).expect("type-check should succeed");
        let rule = program.rules()[0];
        let head_arith = match &rule.head().head_arguments()[0] {
            HeadArg::Arith(a) => a,
            other => panic!("expected Arith head arg, got {other:?}"),
        };
        let fc = match head_arith.init() {
            Factor::FnCall(fc) => fc,
            other => panic!("expected FnCall factor, got {other:?}"),
        };
        match fc.args()[0].init() {
            Factor::Const(c) => assert_eq!(
                c,
                &Constant::new(DataType::Int8, "1"),
                "UDF arg must pin to param type (Int8), not outer target (Int64)"
            ),
            other => panic!("expected Const, got {other:?}"),
        }
    }

    /// Arithmetic on a tuple operand is rejected at type-check (a clean
    /// diagnostic, not a generated-Rust `Add`-not-satisfied error).
    #[test]
    fn tuple_arithmetic_rejected() {
        let src = "\
            .type Pair = ( a: number, b: number )\n\
            .decl In(x: number, y: number)\n\
            .decl Out(q: Pair)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(q) :- In(x, y), p = (x, y), q = p + p.\n";
        assert!(
            checked(src).is_err(),
            "arithmetic on a tuple operand must be rejected"
        );
    }

    /// Destructuring a non-tuple bound variable is a clean user error, not an
    /// internal compiler panic.
    #[test]
    fn destructure_of_non_tuple_is_clean_error() {
        let src = "\
            .decl In(x: symbol)\n\
            .decl Out(a: symbol)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(a) :- In(x), x = (a, b).\n";
        match checked(src) {
            Err(ParseError::TupleDestructure { .. }) => {}
            other => panic!("expected a clean TupleDestructure error, got {other:?}"),
        }
    }

    /// A destructure with only placeholders against a non-tuple is rejected
    /// (the placeholder still witnesses tuple-ness/arity), not silently
    /// accepted.
    #[test]
    fn placeholder_only_destructure_of_non_tuple_rejected() {
        let src = "\
            .decl In(x: symbol)\n\
            .decl Out(x: symbol)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- In(x), x = (_,).\n";
        assert!(
            checked(src).is_err(),
            "`x = (_,)` on a non-tuple `x` must be rejected"
        );
    }

    /// A trailing placeholder past the tuple's arity is rejected, not ignored.
    #[test]
    fn extra_placeholder_past_arity_rejected() {
        let src = "\
            .type Pair = ( a: symbol, b: symbol )\n\
            .decl In(x: symbol, y: symbol)\n\
            .decl P(p: Pair)\n\
            .decl Out(a: symbol)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            P(p)   :- In(x, y), p = (x, y).\n\
            Out(a) :- P(p), p = (a, b, _).\n";
        assert!(
            checked(src).is_err(),
            "a trailing `_` past the tuple's arity must be rejected"
        );
    }

    /// A destructure pattern wider than the tuple is a clean error, not a panic.
    #[test]
    fn over_arity_destructure_is_clean_error() {
        let src = "\
            .type Pair = ( a: symbol, b: symbol )\n\
            .decl In(x: symbol, y: symbol)\n\
            .decl P(p: Pair)\n\
            .decl Out(c: symbol)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            P(p)   :- In(x, y), p = (x, y).\n\
            Out(c) :- P(p), p = (a, b, c).\n";
        match checked(src) {
            Err(ParseError::TupleDestructure { .. }) => {}
            other => panic!("expected a clean TupleDestructure error, got {other:?}"),
        }
    }
}
