//! Comparison expression representation for query planning in FlowLog Datalog programs.

use std::fmt;

use flowlog_parser::ComparisonOperator;

use crate::catalog::ComparisonExprPos;
use crate::planner::argument::TransformationArgument;
use crate::planner::arithmetic::ArithmeticArgument;

/// Represents a comparison expression in a query plan.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ComparisonExprArgument {
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

    /// This comparison with each slot it reads replaced by the one `slot`
    /// gives for it.
    pub(crate) fn map_slots(
        &self,
        slot: &impl Fn(TransformationArgument) -> TransformationArgument,
    ) -> Self {
        Self {
            left: self.left.map_slots(slot),
            operator: self.operator.clone(),
            right: self.right.map_slots(slot),
        }
    }
}

impl fmt::Display for ComparisonExprArgument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.operator, self.right)
    }
}
