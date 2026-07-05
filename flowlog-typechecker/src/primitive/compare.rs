//! Check and pin the operands of a body comparison.

use flowlog_parser::ComparisonExpr;
use flowlog_parser::ComparisonOperator;
use flowlog_parser::DataType;

use crate::TypeCheckError;
use crate::env::UdfSigs;
use crate::primitive::Bindings;
use crate::primitive::expr::infer_expr;
use crate::primitive::expr::pin_expr;
use crate::primitive::ty::LitKind;

pub(super) fn check_and_pin_compare(
    cmp: &mut ComparisonExpr,
    bindings: &Bindings,
    udfs: &UdfSigs,
) -> Result<(), TypeCheckError> {
    let left = infer_expr(cmp.left(), bindings, udfs)?;
    let right = infer_expr(cmp.right(), bindings, udfs)?;
    let op = cmp.operator().clone();
    let span = cmp.span();

    // String constraints (`match`/`contains`) take two string operands and
    // produce a bool — they don't pin operand widths like value comparisons.
    if op.is_string_constraint() {
        for kind in [&left, &right].into_iter().flatten() {
            if !matches!(kind, LitKind::Concrete(DataType::String)) {
                return Err(TypeCheckError::ComparisonOpNotAllowed {
                    span,
                    op,
                    ty: kind.report_ty(),
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
        pin_expr(cmp.left_mut(), &t, bindings, udfs)?;
        pin_expr(cmp.right_mut(), &t, bindings, udfs)?;
    }
    Ok(())
}
