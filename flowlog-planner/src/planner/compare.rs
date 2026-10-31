//! Comparisons over resolved collection slots.

use std::fmt;

use flowlog_parser::ComparisonOperator;

use crate::catalog::ComparisonExprPos;
use crate::planner::argument::TransformationArgument;
use crate::planner::arithmetic::ArithmeticArgument;

/// A comparison whose arithmetic operands read resolved input slots.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ComparisonExprArgument {
    pub(super) left: ArithmeticArgument,
    operator: ComparisonOperator,
    pub(super) right: ArithmeticArgument,
}

impl ComparisonExprArgument {
    /// Creates a new ComparisonExprArgument from constituent parts.
    pub(crate) fn from_comparison_expr(
        compare_expr: &ComparisonExprPos,
        left_arguments: &[TransformationArgument],
        right_arguments: &[TransformationArgument],
    ) -> Self {
        let left =
            ArithmeticArgument::from_arithmetic_pos_in_order(compare_expr.left(), left_arguments);
        let right =
            ArithmeticArgument::from_arithmetic_pos_in_order(compare_expr.right(), right_arguments);
        let operator = compare_expr.operator().clone();

        Self {
            left,
            operator,
            right,
        }
    }

    /// Returns the comparison operator used in this expression.
    pub fn operator(&self) -> &ComparisonOperator {
        &self.operator
    }

    /// Returns the left-hand side arithmetic expression.
    pub fn left(&self) -> &ArithmeticArgument {
        &self.left
    }

    /// Returns the right-hand side arithmetic expression.
    pub fn right(&self) -> &ArithmeticArgument {
        &self.right
    }
}

impl fmt::Display for ComparisonExprArgument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.operator, self.right)
    }
}
