//! Comparison expression representation for query planning in Macaron Datalog programs.

use crate::{argument::TransformationArgument, arithmetic::ArithmeticArgument};
use catalog::ComparisonExprPos;
use parser::ComparisonOperator;
use std::fmt;

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
    pub fn from_comparison_expr(
        compare_expr: &ComparisonExprPos,
        left_arguments: &Vec<TransformationArgument>,
        right_arguments: &Vec<TransformationArgument>,
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

    /// Returns all transformation arguments referenced in this comparison expression.
    pub fn transformation_arguments(&self) -> Vec<&TransformationArgument> {
        let mut transformation_arguments = self.left.transformation_arguments();
        transformation_arguments.extend(self.right.transformation_arguments());
        transformation_arguments
    }

    /// Creates a new ComparisonExprArgument with all join arguments flipped.
    pub fn jn_flip(&self) -> Self {
        Self {
            left: self.left.jn_flip(),
            operator: self.operator.clone(),
            right: self.right.jn_flip(),
        }
    }
}

impl fmt::Display for ComparisonExprArgument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.operator, self.right)
    }
}
