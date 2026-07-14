//! Pure evaluators for constant folding: the correctness core.
//!
//! [`eval_arith`] and [`eval_compare`] evaluate an operation on concrete
//! constants, or return `None` when it cannot be folded safely. The governing
//! contract: a folded value must equal what the generated `--release` code
//! computes at runtime, so anything that cannot guarantee it is left unfolded
//! for the runtime instead.

use std::cmp::Ordering;

use crate::ArithmeticOperator;
use crate::ComparisonOperator;
use crate::Constant;
use crate::types::DataType;

/// Evaluate `a op b` when both operands are the same concrete numeric
/// constant. `None` when they differ in type, aren't numeric, are still
/// polymorphic, or the operation overflows / divides by zero.
pub(super) fn eval_arith(op: &ArithmeticOperator, a: &Constant, b: &Constant) -> Option<Constant> {
    if a.ty() != b.ty() {
        return None;
    }
    // Fold with the same arithmetic release codegen uses, so a folded value
    // never diverges from runtime: integers use `checked_*` (overflow and
    // divide/modulo-by-zero yield `None`, leaving release to wrap or panic),
    // floats use raw `f32`/`f64` (inf/NaN on divide-by-zero).
    macro_rules! int_op {
        ($t:ty, $dt:expr) => {{
            let x: $t = a.text().parse().ok()?;
            let y: $t = b.text().parse().ok()?;
            let r: $t = match op {
                ArithmeticOperator::Plus => x.checked_add(y)?,
                ArithmeticOperator::Minus => x.checked_sub(y)?,
                ArithmeticOperator::Multiply => x.checked_mul(y)?,
                ArithmeticOperator::Divide => x.checked_div(y)?,
                ArithmeticOperator::Modulo => x.checked_rem(y)?,
            };
            Some(Constant::new($dt, r.to_string()))
        }};
    }
    macro_rules! float_op {
        ($t:ty, $dt:expr) => {{
            let x: $t = a.text().parse().ok()?;
            let y: $t = b.text().parse().ok()?;
            let r: $t = match op {
                ArithmeticOperator::Plus => x + y,
                ArithmeticOperator::Minus => x - y,
                ArithmeticOperator::Multiply => x * y,
                ArithmeticOperator::Divide => x / y,
                ArithmeticOperator::Modulo => x % y,
            };
            Some(Constant::new($dt, r.to_string()))
        }};
    }

    match a.ty() {
        DataType::Int8 => int_op!(i8, DataType::Int8),
        DataType::Int16 => int_op!(i16, DataType::Int16),
        DataType::Int32 => int_op!(i32, DataType::Int32),
        DataType::Int64 => int_op!(i64, DataType::Int64),
        DataType::UInt8 => int_op!(u8, DataType::UInt8),
        DataType::UInt16 => int_op!(u16, DataType::UInt16),
        DataType::UInt32 => int_op!(u32, DataType::UInt32),
        DataType::UInt64 => int_op!(u64, DataType::UInt64),
        DataType::Float32 => float_op!(f32, DataType::Float32),
        DataType::Float64 => float_op!(f64, DataType::Float64),
        _ => None,
    }
}

/// Evaluate a value comparison `a op b` over two same-typed concrete
/// constants. `None` for string constraints (`match`/`contains`), string
/// ordering (interning-dependent), a NaN float operand (raw runtime semantics
/// not reproduced here), or mismatched/non-comparable operands.
pub(super) fn eval_compare(op: &ComparisonOperator, a: &Constant, b: &Constant) -> Option<bool> {
    if a.ty() != b.ty() {
        return None;
    }
    macro_rules! ord {
        ($t:ty) => {{
            let x: $t = a.text().parse().ok()?;
            let y: $t = b.text().parse().ok()?;
            ord_to_bool(op, x.cmp(&y))
        }};
    }

    match a.ty() {
        DataType::Int8 => ord!(i8),
        DataType::Int16 => ord!(i16),
        DataType::Int32 => ord!(i32),
        DataType::Int64 => ord!(i64),
        DataType::UInt8 => ord!(u8),
        DataType::UInt16 => ord!(u16),
        DataType::UInt32 => ord!(u32),
        DataType::UInt64 => ord!(u64),
        DataType::Float32 => float_compare(
            op,
            a.text().parse::<f32>().ok()?.into(),
            b.text().parse::<f32>().ok()?.into(),
        ),
        DataType::Float64 => float_compare(
            op,
            a.text().parse::<f64>().ok()?,
            b.text().parse::<f64>().ok()?,
        ),
        // Equality on bool/string is unambiguous; ordering on strings is not
        // (interning), and the typechecker forbids ordering on bool. Bool
        // spellings are canonical (`True`/`False`) and string spellings are
        // the decoded contents, so spelling equality is value equality.
        DataType::Bool | DataType::String => bool_eq(op, a.text() == b.text()),
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

/// Raw float comparison; `None` if either operand is NaN.
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

    fn c(ty: DataType, text: &str) -> Constant {
        Constant::new(ty, text)
    }

    #[rstest]
    // Same-width numeric ops fold to the concrete result.
    #[case::int_add(
        ArithmeticOperator::Plus,
        c(DataType::Int32, "2"),
        c(DataType::Int32, "3"),
        Some(c(DataType::Int32, "5"))
    )]
    #[case::int_mul(
        ArithmeticOperator::Multiply,
        c(DataType::Int32, "10"),
        c(DataType::Int32, "2"),
        Some(c(DataType::Int32, "20"))
    )]
    #[case::uint_sub(
        ArithmeticOperator::Minus,
        c(DataType::UInt8, "5"),
        c(DataType::UInt8, "2"),
        Some(c(DataType::UInt8, "3"))
    )]
    #[case::int_div(
        ArithmeticOperator::Divide,
        c(DataType::Int64, "20"),
        c(DataType::Int64, "4"),
        Some(c(DataType::Int64, "5"))
    )]
    #[case::uint_rem(
        ArithmeticOperator::Modulo,
        c(DataType::UInt32, "7"),
        c(DataType::UInt32, "3"),
        Some(c(DataType::UInt32, "1"))
    )]
    #[case::float_mul(
        ArithmeticOperator::Multiply,
        c(DataType::Float64, "1.5"),
        c(DataType::Float64, "2.0"),
        Some(c(DataType::Float64, "3"))
    )]
    #[case::float_add(
        ArithmeticOperator::Plus,
        c(DataType::Float32, "1.0"),
        c(DataType::Float32, "2.0"),
        Some(c(DataType::Float32, "3"))
    )]
    // A non-canonical spelling folds by value; the result re-renders
    // canonically.
    #[case::leading_zeros(
        ArithmeticOperator::Plus,
        c(DataType::Int32, "007"),
        c(DataType::Int32, "3"),
        Some(c(DataType::Int32, "10"))
    )]
    // Overflow and divide/modulo-by-zero bail to None: leaving the expression
    // unfolded lets the release build reproduce the wrapping value or preserve
    // the runtime panic.
    #[case::add_overflow(
        ArithmeticOperator::Plus,
        c(DataType::Int8, "100"),
        c(DataType::Int8, "100"),
        None
    )]
    #[case::mul_overflow(
        ArithmeticOperator::Multiply,
        c(DataType::UInt8, "200"),
        c(DataType::UInt8, "2"),
        None
    )]
    #[case::div_by_zero(
        ArithmeticOperator::Divide,
        c(DataType::Int32, "5"),
        c(DataType::Int32, "0"),
        None
    )]
    #[case::rem_by_zero(
        ArithmeticOperator::Modulo,
        c(DataType::Int32, "5"),
        c(DataType::Int32, "0"),
        None
    )]
    // Width/family mismatch never folds (the typechecker rejects mixing), and
    // polymorphic literals must not appear post-typecheck; be defensive.
    #[case::width_mismatch(
        ArithmeticOperator::Plus,
        c(DataType::Int8, "1"),
        c(DataType::Int16, "1"),
        None
    )]
    #[case::family_mismatch(
        ArithmeticOperator::Plus,
        c(DataType::Int32, "1"),
        c(DataType::Float32, "1.0"),
        None
    )]
    #[case::polymorphic(
        ArithmeticOperator::Plus,
        c(DataType::IntLit, "1"),
        c(DataType::IntLit, "2"),
        None
    )]
    fn eval_arith_cases(
        #[case] op: ArithmeticOperator,
        #[case] a: Constant,
        #[case] b: Constant,
        #[case] expected: Option<Constant>,
    ) {
        assert_eq!(eval_arith(&op, &a, &b), expected);
    }

    #[rstest]
    // Same-typed operands compare per the operator.
    #[case::gt_true(
        ComparisonOperator::GreaterThan,
        c(DataType::Int32, "5"),
        c(DataType::Int32, "3"),
        Some(true)
    )]
    #[case::lt_false(
        ComparisonOperator::LessThan,
        c(DataType::Int32, "5"),
        c(DataType::Int32, "3"),
        Some(false)
    )]
    #[case::eq_true(
        ComparisonOperator::Equal,
        c(DataType::UInt8, "4"),
        c(DataType::UInt8, "4"),
        Some(true)
    )]
    #[case::ne_false(
        ComparisonOperator::NotEqual,
        c(DataType::Int64, "4"),
        c(DataType::Int64, "4"),
        Some(false)
    )]
    #[case::ge_true(
        ComparisonOperator::GreaterEqualThan,
        c(DataType::Int32, "3"),
        c(DataType::Int32, "3"),
        Some(true)
    )]
    #[case::le_false(
        ComparisonOperator::LessEqualThan,
        c(DataType::Int32, "4"),
        c(DataType::Int32, "3"),
        Some(false)
    )]
    #[case::float_lt(
        ComparisonOperator::LessThan,
        c(DataType::Float64, "1.5"),
        c(DataType::Float64, "2.0"),
        Some(true)
    )]
    // Numeric comparison is by value, not by spelling.
    #[case::leading_zeros_eq(
        ComparisonOperator::Equal,
        c(DataType::Int32, "01"),
        c(DataType::Int32, "1"),
        Some(true)
    )]
    #[case::bool_eq(
        ComparisonOperator::Equal,
        c(DataType::Bool, "True"),
        c(DataType::Bool, "True"),
        Some(true)
    )]
    #[case::text_eq(
        ComparisonOperator::Equal,
        c(DataType::String, "a"),
        c(DataType::String, "b"),
        Some(false)
    )]
    // Not foldable -> None: width mismatch, interning-dependent string ordering,
    // and a NaN float operand (raw runtime semantics not reproduced here).
    #[case::width_mismatch(
        ComparisonOperator::Equal,
        c(DataType::Int8, "1"),
        c(DataType::Int16, "1"),
        None
    )]
    #[case::text_ordering(
        ComparisonOperator::LessThan,
        c(DataType::String, "a"),
        c(DataType::String, "b"),
        None
    )]
    #[case::nan_operand(
        ComparisonOperator::Equal,
        c(DataType::Float64, "NaN"),
        c(DataType::Float64, "1.0"),
        None
    )]
    fn eval_compare_cases(
        #[case] op: ComparisonOperator,
        #[case] a: Constant,
        #[case] b: Constant,
        #[case] expected: Option<bool>,
    ) {
        assert_eq!(eval_compare(&op, &a, &b), expected);
    }
}
