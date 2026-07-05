//! Check and pin every rule — its body predicates and its head (the composer).

use std::collections::HashMap;

use flowlog_parser::Aggregation;
use flowlog_parser::AggregationOperator;
use flowlog_parser::Arithmetic;
use flowlog_parser::DataType;
use flowlog_parser::Factor;
use flowlog_parser::FlowLogRule;
use flowlog_parser::HeadArg;
use flowlog_parser::Predicate;
use flowlog_parser::Program;
use flowlog_parser::TupleElem;
use flowlog_parser::TupleLit;

use crate::TypeCheckError;
use crate::env::PrimitiveEnv;
use crate::env::UdfSigs;
use crate::primitive::Bindings;
use crate::primitive::atom::bind_atom;
use crate::primitive::atom::check_atom;
use crate::primitive::atom::pin_atom;
use crate::primitive::compare::check_and_pin_compare;
use crate::primitive::expr::infer_expr;
use crate::primitive::expr::pin_expr;
use crate::primitive::ty::LitKind;

/// Check + pin every rule (plain and loop-internal) against the primitive
/// type environment.
pub(crate) fn check_and_pin_rules(
    program: &mut Program,
    env: &PrimitiveEnv,
) -> Result<(), TypeCheckError> {
    for segment in program.segments_mut() {
        for rule in segment.as_rules_mut() {
            check_and_pin_rule(rule, env)?;
        }
        if let Some(block) = segment.as_loop_mut() {
            for rule in block.rules_mut() {
                check_and_pin_rule(rule, env)?;
            }
        }
    }
    Ok(())
}

/// Check + pin one rule: bind vars from positive atoms first (so out-of-order
/// body predicates resolve), then check+pin each predicate, then the head.
fn check_and_pin_rule(rule: &mut FlowLogRule, env: &PrimitiveEnv) -> Result<(), TypeCheckError> {
    let mut bindings: Bindings = HashMap::new();
    for predicate in rule.rhs() {
        if let Predicate::PositiveAtom(atom) = predicate {
            bind_atom(atom, &env.decls, &mut bindings)?;
        }
    }

    for predicate in rule.rhs_mut() {
        match predicate {
            Predicate::PositiveAtom(atom) => pin_atom(atom, &env.decls)?,
            Predicate::NegativeAtom(atom) => {
                check_atom(atom, &env.decls, &bindings)?;
                pin_atom(atom, &env.decls)?;
            }
            Predicate::Compare(cmp) => check_and_pin_compare(cmp, &bindings, &env.udfs)?,
        }
    }

    check_and_pin_head(rule, env, &bindings)
}

fn check_and_pin_head(
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
            HeadArg::Aggregation(agg) => {
                check_and_pin_aggregation(agg, expected, &env.udfs, bindings)?
            }
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
                // `infer_factor`'s Tuple arm does for the general case.
                if a.rest().is_empty()
                    && matches!(a.init(), Factor::Tuple(_))
                    && let DataType::FixedTuple(field_types) = &expected
                {
                    let field_types = field_types.clone();
                    if let Factor::Tuple(lit) = a.init_mut() {
                        check_and_pin_tuple(lit, &field_types, &env.udfs, bindings)?;
                    }
                    continue;
                }
                if let Some(kind) = infer_expr(a, bindings, &env.udfs)?
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
                pin_expr(a, &expected, bindings, &env.udfs)?;
            }
        }
    }
    Ok(())
}

/// Field-wise check + pin of a tuple construct `(e0, …)` against a declared
/// tuple column type. Each field is checked like a scalar column (a
/// polymorphic literal fits any same-family width) and then pinned to its
/// field type — avoiding the eager width-collapse `infer_factor` does when it
/// builds a single `Concrete(FixedTuple(..))`. Recurses for nested
/// tuple-literal fields.
fn check_and_pin_tuple(
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
                check_and_pin_tuple(inner, sub, udfs, bindings)?;
            }
            TupleElem::Expr(a) => {
                if let Some(kind) = infer_expr(a, bindings, udfs)?
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
                pin_expr(a, fty, bindings, udfs)?;
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

fn check_and_pin_aggregation(
    agg: &mut Aggregation,
    declared: DataType,
    udfs: &UdfSigs,
    bindings: &Bindings,
) -> Result<(), TypeCheckError> {
    let op = *agg.operator();
    let span = agg.span();
    let arg_kind = infer_expr(agg.arithmetic(), bindings, udfs)?;

    // `count`'s input type is independent of its declared output.
    if matches!(op, AggregationOperator::Count) {
        if !declared.is_numeric() {
            return Err(TypeCheckError::AggregationOutputType { span, op, declared });
        }
        if let Some(k) = arg_kind {
            pin_expr(agg.arithmetic_mut(), &k.report_ty(), bindings, udfs)?;
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
    pin_expr(agg.arithmetic_mut(), &declared, bindings, udfs)?;
    Ok(())
}
