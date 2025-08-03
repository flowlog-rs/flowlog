//! Comparison expression types for FlowLog.

use super::Arithmetic;
use crate::{Lexeme, Rule};

use pest::iterators::Pair;
use std::collections::HashSet;
use std::fmt;

/// Comparison operators for expressions.
///
/// These operators are used to create boolean expressions that compare
/// arithmetic expressions or values in FlowLog rules.
///
/// # Examples
///
/// ```rust
/// use parser::logic::ComparisonOperator;
///
/// let op = ComparisonOperator::Equal;
/// assert!(op.is_equal());
/// assert_eq!(op.to_string(), "==");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ComparisonOperator {
    /// Equality comparison (==)
    Equal,
    /// Inequality comparison (≠)
    NotEqual,
    /// Greater than comparison (>)
    GreaterThan,
    /// Greater than or equal comparison (≥)
    GreaterEqualThan,
    /// Less than comparison (<)
    LessThan,
    /// Less than or equal comparison (≤)
    LessEqualThan,
}

impl ComparisonOperator {
    /// Checks if this operator is the equality operator.
    #[must_use]
    pub fn is_equal(&self) -> bool {
        matches!(self, Self::Equal)
    }
}

impl fmt::Display for ComparisonOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let symbol = match self {
            Self::Equal => "==",
            Self::NotEqual => "≠",
            Self::GreaterThan => ">",
            Self::GreaterEqualThan => "≥",
            Self::LessThan => "<",
            Self::LessEqualThan => "≤",
        };
        write!(f, "{symbol}")
    }
}

impl Lexeme for ComparisonOperator {
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let operator = parsed_rule.into_inner().next().unwrap();
        match operator.as_rule() {
            Rule::equal => Self::Equal,
            Rule::not_equal => Self::NotEqual,
            Rule::greater_than => Self::GreaterThan,
            Rule::greater_equal_than => Self::GreaterEqualThan,
            Rule::less_than => Self::LessThan,
            Rule::less_equal_than => Self::LessEqualThan,
            _ => unreachable!("Unknown comparison operator"),
        }
    }
}

/// Represents a comparison expression between two arithmetic expressions.
///
/// Comparison expressions evaluate to boolean values and are used in
/// rule conditions to control when rules apply.
///
/// # Examples
///
/// ```rust
/// use parser::logic::{ComparisonExpr, ComparisonOperator, Arithmetic, Factor};
/// use parser::primitive::ConstType;
///
/// // Create comparison: x == 5
/// let left = Arithmetic::new(Factor::Var("x".to_string()), vec![]);
/// let right = Arithmetic::new(Factor::Const(ConstType::Integer(5)), vec![]);
/// let comp = ComparisonExpr::new(left, ComparisonOperator::Equal, right);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ComparisonExpr {
    left: Arithmetic,
    operator: ComparisonOperator,
    right: Arithmetic,
}

impl ComparisonExpr {
    /// Creates a new comparison expression.
    #[must_use]
    pub fn new(left: Arithmetic, operator: ComparisonOperator, right: Arithmetic) -> Self {
        Self {
            left,
            operator,
            right,
        }
    }

    /// Returns a reference to the left arithmetic expression.
    #[must_use]
    pub fn left(&self) -> &Arithmetic {
        &self.left
    }

    /// Returns a reference to the comparison operator.
    #[must_use]
    pub fn operator(&self) -> &ComparisonOperator {
        &self.operator
    }

    /// Returns a reference to the right arithmetic expression.
    #[must_use]
    pub fn right(&self) -> &Arithmetic {
        &self.right
    }

    /// Returns a HashSet of all variables in this comparison expression
    pub fn vars_set(&self) -> HashSet<&String> {
        self.left
            .vars_set()
            .union(&self.right.vars_set())
            .cloned()
            .collect()
    }

    /// Returns all variables from the left side of the comparison
    pub fn left_vars(&self) -> Vec<&String> {
        self.left.vars()
    }

    /// Returns all variables from the right side of the comparison
    pub fn right_vars(&self) -> Vec<&String> {
        self.right.vars()
    }
}

impl fmt::Display for ComparisonExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.operator, self.right)
    }
}

impl Lexeme for ComparisonExpr {
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let mut inner = parsed_rule.into_inner();
        let left = Arithmetic::from_parsed_rule(inner.next().unwrap());
        let operator = ComparisonOperator::from_parsed_rule(inner.next().unwrap());
        let right = Arithmetic::from_parsed_rule(inner.next().unwrap());

        Self::new(left, operator, right)
    }
}
