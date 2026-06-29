//! Comparison expression representation for query planning in FlowLog Datalog programs.

use crate::catalog::ComparisonExprPos;
use crate::planner::{argument::TransformationArgument, arithmetic::ArithmeticArgument};
use flowlog_parser::ComparisonOperator;
use std::fmt;

/// Represents a comparison expression in a query plan.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(crate) struct ComparisonExprArgument {
    /// The left-hand side arithmetic expression
    left: ArithmeticArgument,

    /// The comparison operator (=, !=, <, <=, >, >=)
    operator: ComparisonOperator,

    /// The right-hand side arithmetic expression
    right: ArithmeticArgument,
}

impl ComparisonExprArgument {
    /// Creates a new ComparisonExprArgument from constituent parts.
    pub(crate) fn from_comparison_expr(
        compare_expr: &ComparisonExprPos,
        left_arguments: &[TransformationArgument],
        right_arguments: &[TransformationArgument],
    ) -> Self {
        let left = ArithmeticArgument::from_arithmeticpos(compare_expr.left(), left_arguments);
        let right = ArithmeticArgument::from_arithmeticpos(compare_expr.right(), right_arguments);
        let operator = compare_expr.operator().clone();

        Self {
            left,
            operator,
            right,
        }
    }

    /// Returns the comparison operator used in this expression.
    pub(crate) fn operator(&self) -> &ComparisonOperator {
        &self.operator
    }

    /// Returns the left-hand side arithmetic expression.
    pub(crate) fn left(&self) -> &ArithmeticArgument {
        &self.left
    }

    /// Returns the right-hand side arithmetic expression.
    pub(crate) fn right(&self) -> &ArithmeticArgument {
        &self.right
    }
}

impl fmt::Display for ComparisonExprArgument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.operator, self.right)
    }
}
