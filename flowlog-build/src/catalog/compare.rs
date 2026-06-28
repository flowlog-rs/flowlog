//! Comparison expression signatures for FlowLog Datalog programs.

use crate::catalog::{arithmetic::ArithmeticPos, atom::AtomArgumentSignature};
use flowlog_parser::{ComparisonExpr, ComparisonOperator};
use std::fmt;

/// A comparison expression with variables resolved to their concrete positions.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(crate) struct ComparisonExprPos {
    left: ArithmeticPos,
    operator: ComparisonOperator,
    right: ArithmeticPos,
}

impl ComparisonExprPos {
    /// Constructs a positional comparison expression from a parsed expression.
    pub(crate) fn from_comparison_expr(
        compare_expr: &ComparisonExpr,
        left_var_signatures: &[AtomArgumentSignature],
        right_var_signatures: &[AtomArgumentSignature],
    ) -> Self {
        let left = ArithmeticPos::from_arithmetic(compare_expr.left(), left_var_signatures);
        let right = ArithmeticPos::from_arithmetic(compare_expr.right(), right_var_signatures);
        let operator = compare_expr.operator().clone();

        Self {
            left,
            operator,
            right,
        }
    }

    /// Construct a positional comparison expression directly from parts.
    pub(crate) fn from_parts(
        left: ArithmeticPos,
        operator: ComparisonOperator,
        right: ArithmeticPos,
    ) -> Self {
        Self {
            left,
            operator,
            right,
        }
    }

    /// Returns the comparison operator.
    #[inline]
    pub(crate) fn operator(&self) -> &ComparisonOperator {
        &self.operator
    }

    /// Returns the left-hand side arithmetic expression.
    #[inline]
    pub(crate) fn left(&self) -> &ArithmeticPos {
        &self.left
    }

    /// Returns the right-hand side arithmetic expression.
    #[inline]
    pub(crate) fn right(&self) -> &ArithmeticPos {
        &self.right
    }
}

impl fmt::Display for ComparisonExprPos {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[{} {} {}]", self.left, self.operator, self.right)
    }
}
