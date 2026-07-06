//! Pure evaluators for constant folding — the correctness core.
//!
//! Results must match what the generated `--release` code computes, or folding
//! would change program output. Integer ops use `checked_*` and return `None`
//! on overflow or divide/modulo-by-zero: leaving the expression unfolded lets
//! the release build reproduce the wrapping value (overflow) or preserve the
//! runtime panic (div/mod by zero) — we never fold to a wrong value. Float
//! ops use raw `f32`/`f64` semantics (inf/NaN on div-by-zero), matching codegen.

use std::cmp::Ordering;

use flowlog_parser::ArithmeticOperator;
use flowlog_parser::ComparisonOperator;
use flowlog_parser::ConstType;
use ordered_float::OrderedFloat;

/// Evaluate `a op b` when both operands are the same concrete numeric
/// constant. `None` when they differ in type, aren't numeric, are still
/// polymorphic, or the operation overflows / divides by zero.
pub(super) fn eval_arith(
    op: &ArithmeticOperator,
    a: &ConstType,
    b: &ConstType,
) -> Option<ConstType> {
    macro_rules! int_op {
        ($ctor:path, $x:ident, $y:ident) => {
            Some($ctor(match op {
                ArithmeticOperator::Plus => (*$x).checked_add(*$y)?,
                ArithmeticOperator::Minus => (*$x).checked_sub(*$y)?,
                ArithmeticOperator::Multiply => (*$x).checked_mul(*$y)?,
                ArithmeticOperator::Divide => (*$x).checked_div(*$y)?,
                ArithmeticOperator::Modulo => (*$x).checked_rem(*$y)?,
            }))
        };
    }
    macro_rules! float_op {
        ($ctor:path, $x:ident, $y:ident) => {{
            let (x, y) = ($x.into_inner(), $y.into_inner());
            Some($ctor(OrderedFloat(match op {
                ArithmeticOperator::Plus => x + y,
                ArithmeticOperator::Minus => x - y,
                ArithmeticOperator::Multiply => x * y,
                ArithmeticOperator::Divide => x / y,
                ArithmeticOperator::Modulo => x % y,
            })))
        }};
    }

    match (a, b) {
        (ConstType::Int8(x), ConstType::Int8(y)) => int_op!(ConstType::Int8, x, y),
        (ConstType::Int16(x), ConstType::Int16(y)) => int_op!(ConstType::Int16, x, y),
        (ConstType::Int32(x), ConstType::Int32(y)) => int_op!(ConstType::Int32, x, y),
        (ConstType::Int64(x), ConstType::Int64(y)) => int_op!(ConstType::Int64, x, y),
        (ConstType::UInt8(x), ConstType::UInt8(y)) => int_op!(ConstType::UInt8, x, y),
        (ConstType::UInt16(x), ConstType::UInt16(y)) => int_op!(ConstType::UInt16, x, y),
        (ConstType::UInt32(x), ConstType::UInt32(y)) => int_op!(ConstType::UInt32, x, y),
        (ConstType::UInt64(x), ConstType::UInt64(y)) => int_op!(ConstType::UInt64, x, y),
        (ConstType::Float32(x), ConstType::Float32(y)) => float_op!(ConstType::Float32, x, y),
        (ConstType::Float64(x), ConstType::Float64(y)) => float_op!(ConstType::Float64, x, y),
        _ => None,
    }
}

/// Evaluate a value comparison `a op b` over two same-typed concrete
/// constants. `None` for string constraints (`match`/`contains`), string
/// ordering (interning-dependent), a NaN float operand (raw runtime semantics
/// not reproduced here), or mismatched/non-comparable operands.
pub(super) fn eval_compare(op: &ComparisonOperator, a: &ConstType, b: &ConstType) -> Option<bool> {
    macro_rules! ord {
        ($x:ident, $y:ident) => {
            ord_to_bool(op, $x.cmp($y))
        };
    }

    match (a, b) {
        (ConstType::Int8(x), ConstType::Int8(y)) => ord!(x, y),
        (ConstType::Int16(x), ConstType::Int16(y)) => ord!(x, y),
        (ConstType::Int32(x), ConstType::Int32(y)) => ord!(x, y),
        (ConstType::Int64(x), ConstType::Int64(y)) => ord!(x, y),
        (ConstType::UInt8(x), ConstType::UInt8(y)) => ord!(x, y),
        (ConstType::UInt16(x), ConstType::UInt16(y)) => ord!(x, y),
        (ConstType::UInt32(x), ConstType::UInt32(y)) => ord!(x, y),
        (ConstType::UInt64(x), ConstType::UInt64(y)) => ord!(x, y),
        (ConstType::Float32(x), ConstType::Float32(y)) => {
            float_compare(op, f64::from(x.into_inner()), f64::from(y.into_inner()))
        }
        (ConstType::Float64(x), ConstType::Float64(y)) => {
            float_compare(op, x.into_inner(), y.into_inner())
        }
        // Equality on bool/string is unambiguous; ordering on strings is not
        // (interning), and the typechecker forbids ordering on bool.
        (ConstType::Bool(x), ConstType::Bool(y)) => bool_eq(op, x == y),
        (ConstType::Text(x), ConstType::Text(y)) => bool_eq(op, x == y),
        _ => None,
    }
}

/// Map an [`Ordering`] to a comparison result for the six value operators;
/// `None` for the string-constraint operators (which never reach here).
fn ord_to_bool(op: &ComparisonOperator, ord: Ordering) -> Option<bool> {
    Some(match op {
        ComparisonOperator::Equal => ord == Ordering::Equal,
        ComparisonOperator::NotEqual => ord != Ordering::Equal,
        ComparisonOperator::GreaterThan => ord == Ordering::Greater,
        ComparisonOperator::GreaterEqualThan => ord != Ordering::Less,
        ComparisonOperator::LessThan => ord == Ordering::Less,
        ComparisonOperator::LessEqualThan => ord != Ordering::Greater,
        ComparisonOperator::Match { .. } | ComparisonOperator::Contains { .. } => return None,
    })
}

/// Raw float comparison. `partial_cmp` yields `None` on a NaN operand.
fn float_compare(op: &ComparisonOperator, x: f64, y: f64) -> Option<bool> {
    ord_to_bool(op, x.partial_cmp(&y)?)
}

/// `Equal`/`NotEqual` from a precomputed equality; `None` for any other
/// operator (ordering on bool/string is not folded here).
fn bool_eq(op: &ComparisonOperator, eq: bool) -> Option<bool> {
    match op {
        ComparisonOperator::Equal => Some(eq),
        ComparisonOperator::NotEqual => Some(!eq),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    // Same-width numeric ops fold to the concrete result.
    #[case::int_add(
        ArithmeticOperator::Plus,
        ConstType::Int32(2),
        ConstType::Int32(3),
        Some(ConstType::Int32(5))
    )]
    #[case::int_mul(
        ArithmeticOperator::Multiply,
        ConstType::Int32(10),
        ConstType::Int32(2),
        Some(ConstType::Int32(20))
    )]
    #[case::uint_sub(
        ArithmeticOperator::Minus,
        ConstType::UInt8(5),
        ConstType::UInt8(2),
        Some(ConstType::UInt8(3))
    )]
    #[case::int_div(
        ArithmeticOperator::Divide,
        ConstType::Int64(20),
        ConstType::Int64(4),
        Some(ConstType::Int64(5))
    )]
    #[case::uint_rem(
        ArithmeticOperator::Modulo,
        ConstType::UInt32(7),
        ConstType::UInt32(3),
        Some(ConstType::UInt32(1))
    )]
    #[case::float_mul(
        ArithmeticOperator::Multiply,
        ConstType::Float64(OrderedFloat(1.5)),
        ConstType::Float64(OrderedFloat(2.0)),
        Some(ConstType::Float64(OrderedFloat(3.0)))
    )]
    #[case::float_add(
        ArithmeticOperator::Plus,
        ConstType::Float32(OrderedFloat(1.0)),
        ConstType::Float32(OrderedFloat(2.0)),
        Some(ConstType::Float32(OrderedFloat(3.0)))
    )]
    // Overflow and divide/modulo-by-zero bail to None: leaving the expression
    // unfolded lets the release build reproduce the wrapping value or preserve
    // the runtime panic.
    #[case::add_overflow(
        ArithmeticOperator::Plus,
        ConstType::Int8(100),
        ConstType::Int8(100),
        None
    )]
    #[case::mul_overflow(
        ArithmeticOperator::Multiply,
        ConstType::UInt8(200),
        ConstType::UInt8(2),
        None
    )]
    #[case::div_by_zero(
        ArithmeticOperator::Divide,
        ConstType::Int32(5),
        ConstType::Int32(0),
        None
    )]
    #[case::rem_by_zero(
        ArithmeticOperator::Modulo,
        ConstType::Int32(5),
        ConstType::Int32(0),
        None
    )]
    // Width/family mismatch never folds (the typechecker rejects mixing), and
    // polymorphic literals must not appear post-typecheck — be defensive.
    #[case::width_mismatch(
        ArithmeticOperator::Plus,
        ConstType::Int8(1),
        ConstType::Int16(1),
        None
    )]
    #[case::family_mismatch(
        ArithmeticOperator::Plus,
        ConstType::Int32(1),
        ConstType::Float32(OrderedFloat(1.0)),
        None
    )]
    #[case::polymorphic(ArithmeticOperator::Plus, ConstType::Int(1), ConstType::Int(2), None)]
    fn eval_arith_cases(
        #[case] op: ArithmeticOperator,
        #[case] a: ConstType,
        #[case] b: ConstType,
        #[case] expected: Option<ConstType>,
    ) {
        assert_eq!(eval_arith(&op, &a, &b), expected);
    }

    #[rstest]
    // Same-typed operands compare per the operator.
    #[case::gt_true(
        ComparisonOperator::GreaterThan,
        ConstType::Int32(5),
        ConstType::Int32(3),
        Some(true)
    )]
    #[case::lt_false(
        ComparisonOperator::LessThan,
        ConstType::Int32(5),
        ConstType::Int32(3),
        Some(false)
    )]
    #[case::eq_true(
        ComparisonOperator::Equal,
        ConstType::UInt8(4),
        ConstType::UInt8(4),
        Some(true)
    )]
    #[case::ne_false(
        ComparisonOperator::NotEqual,
        ConstType::Int64(4),
        ConstType::Int64(4),
        Some(false)
    )]
    #[case::ge_true(
        ComparisonOperator::GreaterEqualThan,
        ConstType::Int32(3),
        ConstType::Int32(3),
        Some(true)
    )]
    #[case::le_false(
        ComparisonOperator::LessEqualThan,
        ConstType::Int32(4),
        ConstType::Int32(3),
        Some(false)
    )]
    #[case::float_lt(
        ComparisonOperator::LessThan,
        ConstType::Float64(OrderedFloat(1.5)),
        ConstType::Float64(OrderedFloat(2.0)),
        Some(true)
    )]
    #[case::bool_eq(
        ComparisonOperator::Equal,
        ConstType::Bool(true),
        ConstType::Bool(true),
        Some(true)
    )]
    #[case::text_eq(ComparisonOperator::Equal, ConstType::Text("a".into()), ConstType::Text("b".into()), Some(false))]
    // Not foldable → None: width mismatch, interning-dependent string ordering,
    // and a NaN float operand (raw runtime semantics not reproduced here).
    #[case::width_mismatch(
        ComparisonOperator::Equal,
        ConstType::Int8(1),
        ConstType::Int16(1),
        None
    )]
    #[case::text_ordering(ComparisonOperator::LessThan, ConstType::Text("a".into()), ConstType::Text("b".into()), None)]
    #[case::nan_operand(
        ComparisonOperator::Equal,
        ConstType::Float64(OrderedFloat(f64::NAN)),
        ConstType::Float64(OrderedFloat(1.0)),
        None
    )]
    fn eval_compare_cases(
        #[case] op: ComparisonOperator,
        #[case] a: ConstType,
        #[case] b: ConstType,
        #[case] expected: Option<bool>,
    ) {
        assert_eq!(eval_compare(&op, &a, &b), expected);
    }
}
