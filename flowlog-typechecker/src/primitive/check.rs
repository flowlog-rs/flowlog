//! Pass 1 per-rule checking walk: bind variables from positive atoms, then
//! check every body predicate (atoms, comparisons) and the head (columns,
//! tuple constructs, aggregations) against those bindings, pinning literals
//! as it goes.

use std::collections::HashMap;

use flowlog_parser::Aggregation;
use flowlog_parser::AggregationOperator;
use flowlog_parser::Arithmetic;
use flowlog_parser::Atom;
use flowlog_parser::AtomArg;
use flowlog_parser::ComparisonExpr;
use flowlog_parser::ComparisonOperator;
use flowlog_parser::DataType;
use flowlog_parser::Factor;
use flowlog_parser::FlowLogRule;
use flowlog_parser::HeadArg;
use flowlog_parser::Predicate;
use flowlog_parser::TupleElem;
use flowlog_parser::TupleLit;

use crate::TypeCheckError;
use crate::env::DeclTypes;
use crate::env::PrimitiveEnv;
use crate::env::UdfSigs;
use crate::primitive::Bindings;
use crate::primitive::expr::infer_expr_type;
use crate::primitive::expr::pin_arith_literals;
use crate::primitive::lattice::LitKind;

pub(crate) fn check_rule(rule: &mut FlowLogRule, env: &PrimitiveEnv) -> Result<(), TypeCheckError> {
    // Bind vars first so out-of-order body predicates can resolve them.
    let mut bindings: Bindings = HashMap::new();
    for predicate in rule.rhs() {
        if let Predicate::PositiveAtom(atom) = predicate {
            bind_atom_vars(atom, &env.decls, &mut bindings)?;
        }
    }

    for predicate in rule.rhs_mut() {
        match predicate {
            Predicate::PositiveAtom(atom) => pin_atom_consts(atom, &env.decls)?,
            Predicate::NegativeAtom(atom) => {
                check_atom_uses(atom, &env.decls, &bindings)?;
                pin_atom_consts(atom, &env.decls)?;
            }
            Predicate::Compare(cmp) => check_comparison(cmp, &bindings, &env.udfs)?,
        }
    }

    check_head(rule, env, &bindings)
}

/// Record each variable's first-seen type; validate const arg families.
/// Consts are pinned separately by [`pin_atom_consts`] so bindings can
/// be populated for the whole rule before any mutation.
fn bind_atom_vars(
    atom: &Atom,
    decls: &DeclTypes,
    bindings: &mut Bindings,
) -> Result<(), TypeCheckError> {
    for (i, arg) in atom.arguments().iter().enumerate() {
        let col_ty = resolve_atom_column(atom, i, decls)?;
        match arg {
            AtomArg::Var(v) => match bindings.get(v) {
                None => {
                    bindings.insert(v.clone(), (col_ty, atom.span()));
                }
                Some((first_ty, first_span)) if first_ty != &col_ty => {
                    return Err(TypeCheckError::TypeMismatch {
                        var: v.clone(),
                        first_ty: first_ty.clone(),
                        first_span: *first_span,
                        later_ty: col_ty,
                        later_span: atom.span(),
                    });
                }
                Some(_) => {}
            },
            AtomArg::Const(c) => {
                if !LitKind::of(c)?.fits(&col_ty) {
                    return Err(TypeCheckError::LiteralColumnMismatch {
                        span: atom.span(),
                        literal: c.to_string(),
                        expected: col_ty,
                    });
                }
            }
            AtomArg::Placeholder => {}
        }
    }
    Ok(())
}

/// Check each bound variable matches its column type. Unbound vars are
/// reported separately by the range-restriction pass.
fn check_atom_uses(
    atom: &Atom,
    decls: &DeclTypes,
    bindings: &Bindings,
) -> Result<(), TypeCheckError> {
    for (i, arg) in atom.arguments().iter().enumerate() {
        let col_ty = resolve_atom_column(atom, i, decls)?;
        match arg {
            AtomArg::Var(v) => {
                if let Some((bound_ty, bound_span)) = bindings.get(v)
                    && bound_ty != &col_ty
                {
                    return Err(TypeCheckError::TypeMismatch {
                        var: v.clone(),
                        first_ty: bound_ty.clone(),
                        first_span: *bound_span,
                        later_ty: col_ty,
                        later_span: atom.span(),
                    });
                }
            }
            AtomArg::Const(c) => {
                if !LitKind::of(c)?.fits(&col_ty) {
                    return Err(TypeCheckError::LiteralColumnMismatch {
                        span: atom.span(),
                        literal: c.to_string(),
                        expected: col_ty,
                    });
                }
            }
            AtomArg::Placeholder => {}
        }
    }
    Ok(())
}

/// Pin every polymorphic const argument of `atom` to its declared column
/// type. Call after [`bind_atom_vars`] / [`check_atom_uses`] has already
/// validated the family fit.
fn pin_atom_consts(atom: &mut Atom, decls: &DeclTypes) -> Result<(), TypeCheckError> {
    let col_types: Vec<DataType> = {
        let Some(decl) = decls.get(atom.name()) else {
            return Err(TypeCheckError::internal(format!(
                "atom `{}` not declared",
                atom.name()
            )));
        };
        decl.clone()
    };
    for (arg, col_ty) in atom.arguments_mut().iter_mut().zip(col_types.iter()) {
        if let AtomArg::Const(c) = arg
            && c.is_polymorphic()
        {
            c.pin(col_ty.clone());
        }
    }
    Ok(())
}

fn resolve_atom_column(
    atom: &Atom,
    i: usize,
    decls: &DeclTypes,
) -> Result<DataType, TypeCheckError> {
    let decl = decls
        .get(atom.name())
        .ok_or_else(|| TypeCheckError::internal(format!("atom `{}` not declared", atom.name())))?;
    decl.get(i).cloned().ok_or_else(|| {
        TypeCheckError::internal(format!(
            "atom `{}` has {} arguments but `.decl` has {}",
            atom.name(),
            atom.arguments().len(),
            decl.len(),
        ))
    })
}

fn check_comparison(
    cmp: &mut ComparisonExpr,
    bindings: &Bindings,
    udfs: &UdfSigs,
) -> Result<(), TypeCheckError> {
    let left = infer_expr_type(cmp.left(), bindings, udfs)?;
    let right = infer_expr_type(cmp.right(), bindings, udfs)?;
    let op = cmp.operator().clone();
    let span = cmp.span();

    // String constraints (`match`/`contains`) take two string operands and
    // produce a bool — they don't pin operand widths like value comparisons.
    if op.is_string_constraint() {
        for ty in [&left, &right].into_iter().flatten() {
            if !matches!(ty, LitKind::Concrete(DataType::String)) {
                return Err(TypeCheckError::ComparisonOpNotAllowed {
                    span,
                    op,
                    ty: ty.report_ty(),
                });
            }
        }
        return Ok(());
    }

    if let (Some(l), Some(r)) = (&left, &right)
        && l.merge(r).is_none()
    {
        return Err(TypeCheckError::ComparisonTypeMismatch {
            span,
            op,
            left: l.report_ty(),
            right: r.report_ty(),
        });
    }

    // Ordering comparisons additionally require an ordered type.
    if !matches!(op, ComparisonOperator::Equal | ComparisonOperator::NotEqual)
        && let Some(kind) = left.as_ref().or(right.as_ref())
    {
        let is_ordered = kind.is_numeric() || matches!(kind, LitKind::Concrete(DataType::String));
        if !is_ordered {
            return Err(TypeCheckError::ComparisonOpNotAllowed {
                span,
                op,
                ty: kind.report_ty(),
            });
        }
    }

    // Pin: both sides unify to the same concrete type. Fall back to the
    // family's representative width when both sides are polymorphic.
    let target = match (&left, &right) {
        (Some(l), Some(r)) => l.merge(r).map(|k| k.report_ty()),
        (Some(k), None) | (None, Some(k)) => Some(k.report_ty()),
        (None, None) => None,
    };
    if let Some(t) = target {
        pin_arith_literals(cmp.left_mut(), &t, bindings, udfs)?;
        pin_arith_literals(cmp.right_mut(), &t, bindings, udfs)?;
    }
    Ok(())
}

fn check_head(
    rule: &mut FlowLogRule,
    env: &PrimitiveEnv,
    bindings: &Bindings,
) -> Result<(), TypeCheckError> {
    let head = rule.head_mut();
    let (rel_name, arity, head_span) = (head.name().to_string(), head.arity(), head.span());
    let rel_display = head.raw_name().to_string();
    let col_types: Vec<DataType> = {
        let Some(decl) = env.decls.get(&rel_name) else {
            return Err(TypeCheckError::internal(format!(
                "head relation `{rel_name}` not declared"
            )));
        };
        decl.clone()
    };

    if arity != col_types.len() {
        return Err(TypeCheckError::HeadArity {
            span: head_span,
            rel: rel_display,
            expected: col_types.len(),
            found: arity,
        });
    }

    for (col, (arg, expected)) in head
        .head_arguments_mut()
        .iter_mut()
        .zip(col_types.iter().cloned())
        .enumerate()
    {
        match arg {
            HeadArg::Aggregation(agg) => check_aggregation(agg, expected, &env.udfs, bindings)?,
            HeadArg::Var(v) => {
                if let Some((found, _)) = bindings.get(v)
                    && found != &expected
                {
                    return Err(TypeCheckError::HeadColumnType {
                        span: head_span,
                        rel: rel_display,
                        col,
                        found: found.clone(),
                        expected,
                    });
                }
            }
            HeadArg::Arith(a) => {
                // A bare tuple construct against a tuple column is checked
                // field-wise (so a polymorphic literal fits any same-family
                // field width, exactly as it would in a scalar column) and
                // pinned per field — avoiding the eager width-collapse that
                // `infer_factor_type`'s Tuple arm does for the general case.
                if a.rest().is_empty()
                    && matches!(a.init(), Factor::Tuple(_))
                    && let DataType::FixedTuple(field_types) = &expected
                {
                    let field_types = field_types.clone();
                    if let Factor::Tuple(lit) = a.init_mut() {
                        check_tuple_construct(lit, &field_types, &env.udfs, bindings)?;
                    }
                    continue;
                }
                if let Some(kind) = infer_expr_type(a, bindings, &env.udfs)?
                    && !kind.fits(&expected)
                {
                    return Err(head_or_literal_mismatch(
                        a,
                        &rel_display,
                        col,
                        expected,
                        kind,
                    ));
                }
                pin_arith_literals(a, &expected, bindings, &env.udfs)?;
            }
        }
    }
    Ok(())
}

/// Field-wise check + pin of a tuple construct `(e0, …)` against a declared
/// tuple column type. Each field is checked like a scalar column (a
/// polymorphic literal fits any same-family width) and then pinned to its
/// field type — avoiding the eager width-collapse `infer_factor_type` does
/// when it builds a single `Concrete(FixedTuple(..))`. Recurses for nested
/// tuple-literal fields.
fn check_tuple_construct(
    lit: &mut TupleLit,
    expected_fields: &[DataType],
    udfs: &UdfSigs,
    bindings: &Bindings,
) -> Result<(), TypeCheckError> {
    if lit.fields().len() != expected_fields.len() {
        return Err(TypeCheckError::TupleConstruct {
            span: lit.span(),
            detail: format!(
                "tuple has {} field(s) but {} value(s) were given",
                expected_fields.len(),
                lit.fields().len()
            ),
        });
    }
    let span = lit.span();
    for (elem, fty) in lit.fields_mut().iter_mut().zip(expected_fields.iter()) {
        match elem {
            TupleElem::Placeholder => {
                return Err(TypeCheckError::TuplePlaceholderInConstruct { span });
            }
            // Nested tuple literal → recurse so its fields get the same
            // literal-width leniency.
            TupleElem::Expr(a) if a.rest().is_empty() && matches!(a.init(), Factor::Tuple(_)) => {
                let DataType::FixedTuple(sub) = fty else {
                    return Err(TypeCheckError::TupleConstruct {
                        span,
                        detail: format!("a tuple literal does not fit field type `{fty}`"),
                    });
                };
                let Factor::Tuple(inner) = a.init_mut() else {
                    unreachable!("guard matched Factor::Tuple")
                };
                check_tuple_construct(inner, sub, udfs, bindings)?;
            }
            TupleElem::Expr(a) => {
                if let Some(kind) = infer_expr_type(a, bindings, udfs)?
                    && !kind.fits(fty)
                {
                    return Err(TypeCheckError::TupleConstruct {
                        span: a.span(),
                        detail: format!(
                            "field value of type `{}` does not fit field type `{fty}`",
                            kind.report_ty()
                        ),
                    });
                }
                pin_arith_literals(a, fty, bindings, udfs)?;
            }
        }
    }
    Ok(())
}

/// Bare literal → `LiteralColumnMismatch` (cites the source text);
/// anything else → `HeadColumnType` (cites the inferred type).
fn head_or_literal_mismatch(
    a: &Arithmetic,
    rel: &str,
    col: usize,
    expected: DataType,
    kind: LitKind,
) -> TypeCheckError {
    // A bare literal is a single constant factor with no operators.
    if let (true, Factor::Const(c)) = (a.is_const(), a.init()) {
        return TypeCheckError::LiteralColumnMismatch {
            span: a.span(),
            literal: c.to_string(),
            expected,
        };
    }
    TypeCheckError::HeadColumnType {
        span: a.span(),
        rel: rel.to_string(),
        col,
        expected,
        found: kind.report_ty(),
    }
}

fn check_aggregation(
    agg: &mut Aggregation,
    declared: DataType,
    udfs: &UdfSigs,
    bindings: &Bindings,
) -> Result<(), TypeCheckError> {
    let op = *agg.operator();
    let span = agg.span();
    let arg_kind = infer_expr_type(agg.arithmetic(), bindings, udfs)?;

    // `count`'s input type is independent of its declared output.
    if matches!(op, AggregationOperator::Count) {
        if !declared.is_numeric() {
            return Err(TypeCheckError::AggregationOutputType { span, op, declared });
        }
        if let Some(k) = arg_kind {
            pin_arith_literals(agg.arithmetic_mut(), &k.report_ty(), bindings, udfs)?;
        }
        return Ok(());
    }

    // sum / avg / min / max: numeric input, output family matches input.
    if let Some(kind) = arg_kind {
        if !kind.is_numeric() {
            return Err(TypeCheckError::AggregationInputNotNumeric {
                span,
                op,
                ty: kind.report_ty(),
            });
        }
        if !kind.fits(&declared) {
            return Err(TypeCheckError::AggregationOutputType { span, op, declared });
        }
    }
    pin_arith_literals(agg.arithmetic_mut(), &declared, bindings, udfs)?;
    Ok(())
}
