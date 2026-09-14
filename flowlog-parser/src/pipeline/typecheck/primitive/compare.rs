//! Check and pin the operands of a body comparison.

use crate::ComparisonExpr;
use crate::ComparisonOperator;
use crate::DataType;
use crate::ParseError;
use crate::pipeline::typecheck::env::UdfSigs;
use crate::pipeline::typecheck::primitive::Bindings;
use crate::pipeline::typecheck::primitive::expr::infer_expr;
use crate::pipeline::typecheck::primitive::expr::pin_expr;

/// Check a body comparison's operand types and pin their literals to the
/// shared concrete type. `match` / `contains` require string operands;
/// ordering operators require an ordered type; a value comparison requires the
/// two sides to unify.
pub(super) fn check_and_pin_compare(
    cmp: &mut ComparisonExpr,
    bindings: &Bindings,
    udfs: &UdfSigs,
) -> Result<(), ParseError> {
    let left = infer_expr(cmp.left(), bindings, udfs)?;
    let right = infer_expr(cmp.right(), bindings, udfs)?;
    let op = cmp.operator().clone();
    let span = cmp.span();

    // String constraints (`match`/`contains`) take two string operands and
    // produce a bool; they don't pin operand widths like value comparisons.
    if op.is_string_constraint() {
        for kind in [&left, &right].into_iter().flatten() {
            if !matches!(kind, DataType::String) {
                return Err(ParseError::ComparisonOpNotAllowed {
                    span,
                    op,
                    ty: kind.defaulted(),
                });
            }
        }
        return Ok(());
    }

    if let (Some(l), Some(r)) = (&left, &right)
        && l.merge(r).is_none()
    {
        return Err(ParseError::ComparisonTypeMismatch {
            span,
            op,
            left: l.defaulted(),
            right: r.defaulted(),
        });
    }

    // Ordering comparisons additionally require an ordered type.
    if !matches!(op, ComparisonOperator::Equal | ComparisonOperator::NotEqual)
        && let Some(kind) = left.as_ref().or(right.as_ref())
    {
        let is_ordered = kind.defaulted().is_numeric() || matches!(kind, DataType::String);
        if !is_ordered {
            return Err(ParseError::ComparisonOpNotAllowed {
                span,
                op,
                ty: kind.defaulted(),
            });
        }
    }

    // Pin: both sides unify to the same concrete type. Fall back to the
    // family's representative width when both sides are polymorphic.
    let target = match (&left, &right) {
        (Some(l), Some(r)) => l.merge(r).map(|k| k.defaulted()),
        (Some(k), None) | (None, Some(k)) => Some(k.defaulted()),
        (None, None) => None,
    };
    if let Some(t) = target {
        pin_expr(cmp.left_mut(), &t, bindings, udfs)?;
        pin_expr(cmp.right_mut(), &t, bindings, udfs)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::Constant;
    use crate::DataType;
    use crate::Factor;
    use crate::Predicate;
    use crate::test_util::checked;

    /// Comparison operand literal: `x > 100` with `x: int16` must pin `100` to
    /// `Int16(100)`, exercising the pin-target selection after the two sides
    /// unify.
    #[test]
    fn comparison_literal_pinned_to_variable_type() {
        let src = "\
            .decl Item(x: int16)\n\
            .decl Big(x: int16)\n\
            .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
            .output Big\n\
            Big(x) :- Item(x), x > 100.\n";
        let program = checked(src).expect("type-check should succeed");
        let rule = &program.rules()[0];
        let cmp = match &rule.rhs()[1] {
            Predicate::Compare(c) => c,
            other => panic!("expected comparison, got {other:?}"),
        };
        match cmp.right().init() {
            Factor::Const(c) => assert_eq!(c, &Constant::new(DataType::Int16, "100")),
            other => panic!("expected Const, got {other:?}"),
        }
    }
}
