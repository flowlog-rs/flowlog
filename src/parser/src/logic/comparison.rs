//! Comparison expression types for FlowLog.
//!
//! This module provides types for representing and working with comparison expressions
//! in FlowLog rules. Comparison expressions are used to create boolean conditions that
//! determine when rules should fire.
//!
//! # Overview
//!
//! The module contains two main types:
//! - [`ComparisonOperator`]: Represents the different comparison operators (==, ≠, >, ≥, <, ≤)
//! - [`ComparisonExpr`]: Represents a complete comparison expression with left operand, operator, and right operand
//!
//! # Examples
//!
//! ```rust
//! use parser::logic::{ComparisonExpr, ComparisonOperator, Arithmetic, Factor};
//! use parser::primitive::ConstType;
//!
//! // Create a simple comparison: age > 18
//! let age_var = Arithmetic::new(Factor::Var("age".to_string()), vec![]);
//! let eighteen = Arithmetic::new(Factor::Const(ConstType::Integer(18)), vec![]);
//! let age_check = ComparisonExpr::new(age_var, ComparisonOperator::GreaterThan, eighteen);
//!
//! assert_eq!(age_check.to_string(), "age > 18");
//! ```

use super::Arithmetic;
use crate::{Lexeme, Rule};

use pest::iterators::Pair;
use std::collections::HashSet;
use std::fmt;

/// Comparison operators for expressions.
///
/// These operators are used to create boolean expressions that compare
/// arithmetic expressions or values in FlowLog rules. Each operator
/// has a specific semantic meaning and corresponding symbol representation.
///
/// # Examples
///
/// ```rust
/// use parser::logic::ComparisonOperator;
///
/// // Create an equality operator
/// let op = ComparisonOperator::Equal;
/// assert!(op.is_equal());
/// assert_eq!(op.to_string(), "==");
///
/// // Create a greater-than operator
/// let gt_op = ComparisonOperator::GreaterThan;
/// assert!(!gt_op.is_equal());
/// assert_eq!(gt_op.to_string(), ">");
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
    ///
    /// This is a convenience method for checking if the operator represents
    /// equality comparison. It's useful for special handling of equality
    /// operations in rule processing or optimization.
    ///
    /// # Returns
    ///
    /// `true` if this operator is `Equal`, `false` otherwise.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::ComparisonOperator;
    ///
    /// assert!(ComparisonOperator::Equal.is_equal());
    /// assert!(!ComparisonOperator::GreaterThan.is_equal());
    /// assert!(!ComparisonOperator::NotEqual.is_equal());
    /// ```
    #[must_use]
    pub fn is_equal(&self) -> bool {
        matches!(self, Self::Equal)
    }
}

impl fmt::Display for ComparisonOperator {
    /// Formats the comparison operator using its Unicode symbol representation.
    ///
    /// This implementation provides human-readable symbols for each operator,
    /// using Unicode characters where appropriate for mathematical notation.
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
    /// Parses a comparison operator from a pest parsing rule.
    ///
    /// This method converts parsed tokens from the grammar into the appropriate
    /// `ComparisonOperator` variant. It expects the parsed rule to contain
    /// exactly one inner rule representing the specific operator type.
    ///
    /// # Arguments
    ///
    /// * `parsed_rule` - A pest `Pair` containing the parsed operator rule
    ///
    /// # Returns
    ///
    /// The corresponding `ComparisonOperator` variant.
    ///
    /// # Panics
    ///
    /// Panics if the parsed rule contains an unknown operator type that doesn't
    /// match any of the expected grammar rules.
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
/// A comparison expression consists of a left operand, a comparison operator,
/// and a right operand. When evaluated, it produces a boolean result indicating
/// whether the comparison condition is satisfied.
///
/// Comparison expressions are fundamental building blocks in FlowLog rule bodies,
/// allowing rules to specify conditions that must be met for the rule to fire.
/// They enable filtering, validation, and constraint checking within rule logic.
///
/// # Structure
///
/// Each `ComparisonExpr` contains:
/// - `left`: The left-hand arithmetic expression
/// - `operator`: The comparison operator (==, ≠, >, ≥, <, ≤)
/// - `right`: The right-hand arithmetic expression
///
/// # Variable Extraction
///
/// The struct provides methods to extract variables from both sides of the
/// comparison, which is useful for dependency analysis and rule optimization.
///
/// # Examples
///
/// ```rust
/// use parser::logic::{ComparisonExpr, ComparisonOperator, Arithmetic, Factor};
/// use parser::primitive::ConstType;
///
/// // Create a simple age check: age >= 18
/// let age_var = Arithmetic::new(Factor::Var("age".to_string()), vec![]);
/// let min_age = Arithmetic::new(Factor::Const(ConstType::Integer(18)), vec![]);
/// let age_check = ComparisonExpr::new(age_var, ComparisonOperator::GreaterEqualThan, min_age);
///
/// assert_eq!(age_check.to_string(), "age ≥ 18");
///
/// // Extract variables for analysis
/// let vars = age_check.vars_set();
/// assert_eq!(vars.len(), 1);
/// assert!(vars.iter().any(|&v| v == "age"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ComparisonExpr {
    left: Arithmetic,
    operator: ComparisonOperator,
    right: Arithmetic,
}

impl ComparisonExpr {
    /// Creates a new comparison expression.
    ///
    /// Constructs a comparison expression with the specified left operand,
    /// comparison operator, and right operand. This is the primary constructor
    /// for building comparison expressions programmatically.
    ///
    /// # Arguments
    ///
    /// * `left` - The left-hand arithmetic expression
    /// * `operator` - The comparison operator to apply
    /// * `right` - The right-hand arithmetic expression
    ///
    /// # Returns
    ///
    /// A new `ComparisonExpr` instance representing the comparison.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{ComparisonExpr, ComparisonOperator, Arithmetic, Factor};
    /// use parser::primitive::ConstType;
    ///
    /// let x = Arithmetic::new(Factor::Var("x".to_string()), vec![]);
    /// let five = Arithmetic::new(Factor::Const(ConstType::Integer(5)), vec![]);
    /// let comparison = ComparisonExpr::new(x, ComparisonOperator::GreaterThan, five);
    ///
    /// assert_eq!(comparison.to_string(), "x > 5");
    /// ```
    #[must_use]
    pub fn new(left: Arithmetic, operator: ComparisonOperator, right: Arithmetic) -> Self {
        Self {
            left,
            operator,
            right,
        }
    }

    /// Returns a reference to the left arithmetic expression.
    ///
    /// Provides access to the left operand of the comparison without taking
    /// ownership. This is useful for inspecting or analyzing the left side
    /// of the comparison expression.
    ///
    /// # Returns
    ///
    /// A reference to the left `Arithmetic` expression.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{ComparisonExpr, ComparisonOperator, Arithmetic, Factor};
    /// use parser::primitive::ConstType;
    ///
    /// let left = Arithmetic::new(Factor::Var("score".to_string()), vec![]);
    /// let right = Arithmetic::new(Factor::Const(ConstType::Integer(100)), vec![]);
    /// let expr = ComparisonExpr::new(left.clone(), ComparisonOperator::LessThan, right);
    ///
    /// assert_eq!(expr.left(), &left);
    /// ```
    #[must_use]
    pub fn left(&self) -> &Arithmetic {
        &self.left
    }

    /// Returns a reference to the comparison operator.
    ///
    /// Provides access to the operator used in this comparison expression.
    /// This is useful for pattern matching or operator-specific logic.
    ///
    /// # Returns
    ///
    /// A reference to the `ComparisonOperator`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{ComparisonExpr, ComparisonOperator, Arithmetic, Factor};
    /// use parser::primitive::ConstType;
    ///
    /// let left = Arithmetic::new(Factor::Var("x".to_string()), vec![]);
    /// let right = Arithmetic::new(Factor::Const(ConstType::Integer(0)), vec![]);
    /// let expr = ComparisonExpr::new(left, ComparisonOperator::Equal, right);
    ///
    /// assert!(expr.operator().is_equal());
    /// ```
    #[must_use]
    pub fn operator(&self) -> &ComparisonOperator {
        &self.operator
    }

    /// Returns a reference to the right arithmetic expression.
    ///
    /// Provides access to the right operand of the comparison without taking
    /// ownership. This is useful for inspecting or analyzing the right side
    /// of the comparison expression.
    ///
    /// # Returns
    ///
    /// A reference to the right `Arithmetic` expression.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{ComparisonExpr, ComparisonOperator, Arithmetic, Factor};
    /// use parser::primitive::ConstType;
    ///
    /// let left = Arithmetic::new(Factor::Var("price".to_string()), vec![]);
    /// let right = Arithmetic::new(Factor::Const(ConstType::Integer(50)), vec![]);
    /// let expr = ComparisonExpr::new(left, ComparisonOperator::LessEqualThan, right.clone());
    ///
    /// assert_eq!(expr.right(), &right);
    /// ```
    #[must_use]
    pub fn right(&self) -> &Arithmetic {
        &self.right
    }

    /// Returns a HashSet of all variables in this comparison expression.
    ///
    /// This method collects all unique variables from both the left and right
    /// operands of the comparison. It uses a `HashSet` to automatically handle
    /// deduplication in cases where the same variable appears on both sides.
    ///
    /// # Returns
    ///
    /// A `HashSet` containing references to all unique variable names in the expression.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{ComparisonExpr, ComparisonOperator, Arithmetic, Factor};
    /// use parser::primitive::ConstType;
    ///
    /// // Comparison with variables on both sides: x > y
    /// let left = Arithmetic::new(Factor::Var("x".to_string()), vec![]);
    /// let right = Arithmetic::new(Factor::Var("y".to_string()), vec![]);
    /// let expr = ComparisonExpr::new(left, ComparisonOperator::GreaterThan, right);
    ///
    /// let vars = expr.vars_set();
    /// assert_eq!(vars.len(), 2);
    /// assert!(vars.iter().any(|&v| v == "x"));
    /// assert!(vars.iter().any(|&v| v == "y"));
    /// ```
    pub fn vars_set(&self) -> HashSet<&String> {
        self.left
            .vars_set()
            .union(&self.right.vars_set())
            .cloned()
            .collect()
    }

    /// Returns all variables from the left side of the comparison.
    ///
    /// This method extracts only the variables present in the left operand
    /// of the comparison expression. Unlike `vars_set()`, this preserves
    /// the order and multiplicity of variables as they appear in the left expression.
    ///
    /// # Returns
    ///
    /// A `Vec` containing references to variable names in the left operand.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{ComparisonExpr, ComparisonOperator, Arithmetic, Factor};
    /// use parser::primitive::ConstType;
    ///
    /// let left = Arithmetic::new(Factor::Var("age".to_string()), vec![]);
    /// let right = Arithmetic::new(Factor::Const(ConstType::Integer(21)), vec![]);
    /// let expr = ComparisonExpr::new(left, ComparisonOperator::GreaterEqualThan, right);
    ///
    /// let left_vars = expr.left_vars();
    /// assert_eq!(left_vars.len(), 1);
    /// assert_eq!(left_vars[0], "age");
    /// ```
    pub fn left_vars(&self) -> Vec<&String> {
        self.left.vars()
    }

    /// Returns all variables from the right side of the comparison.
    ///
    /// This method extracts only the variables present in the right operand
    /// of the comparison expression. Unlike `vars_set()`, this preserves
    /// the order and multiplicity of variables as they appear in the right expression.
    ///
    /// # Returns
    ///
    /// A `Vec` containing references to variable names in the right operand.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{ComparisonExpr, ComparisonOperator, Arithmetic, Factor};
    /// use parser::primitive::ConstType;
    ///
    /// let left = Arithmetic::new(Factor::Const(ConstType::Integer(0)), vec![]);
    /// let right = Arithmetic::new(Factor::Var("balance".to_string()), vec![]);
    /// let expr = ComparisonExpr::new(left, ComparisonOperator::LessThan, right);
    ///
    /// let right_vars = expr.right_vars();
    /// assert_eq!(right_vars.len(), 1);
    /// assert_eq!(right_vars[0], "balance");
    /// ```
    pub fn right_vars(&self) -> Vec<&String> {
        self.right.vars()
    }
}

impl fmt::Display for ComparisonExpr {
    /// Formats the comparison expression as a human-readable string.
    ///
    /// The format follows the pattern: `left operator right`, where each
    /// component is formatted according to its own `Display` implementation.
    /// This produces output suitable for debugging, logging, or user display.
    ///
    /// # Format
    ///
    /// The output format is: `{left} {operator} {right}`
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{ComparisonExpr, ComparisonOperator, Arithmetic, Factor};
    /// use parser::primitive::ConstType;
    ///
    /// let age = Arithmetic::new(Factor::Var("age".to_string()), vec![]);
    /// let limit = Arithmetic::new(Factor::Const(ConstType::Integer(65)), vec![]);
    /// let expr = ComparisonExpr::new(age, ComparisonOperator::LessEqualThan, limit);
    ///
    /// assert_eq!(expr.to_string(), "age ≤ 65");
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.operator, self.right)
    }
}

impl Lexeme for ComparisonExpr {
    /// Parses a comparison expression from a pest parsing rule.
    ///
    /// This method constructs a `ComparisonExpr` from parsed grammar tokens.
    /// It expects the parsed rule to contain exactly three inner elements:
    /// a left arithmetic expression, a comparison operator, and a right
    /// arithmetic expression, in that order.
    ///
    /// # Arguments
    ///
    /// * `parsed_rule` - A pest `Pair` containing the parsed comparison expression rule
    ///
    /// # Returns
    ///
    /// A new `ComparisonExpr` instance representing the parsed comparison.
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let mut inner = parsed_rule.into_inner();
        let left = Arithmetic::from_parsed_rule(inner.next().unwrap());
        let operator = ComparisonOperator::from_parsed_rule(inner.next().unwrap());
        let right = Arithmetic::from_parsed_rule(inner.next().unwrap());

        Self::new(left, operator, right)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logic::{Arithmetic, Factor};
    use crate::primitive::ConstType;

    // Helper functions for creating test expressions
    fn var_factor(name: &str) -> Factor {
        Factor::Var(name.to_string())
    }

    fn int_factor(value: i32) -> Factor {
        Factor::Const(ConstType::Integer(value))
    }

    fn simple_arithmetic(factor: Factor) -> Arithmetic {
        Arithmetic::new(factor, vec![])
    }

    fn var_arithmetic(name: &str) -> Arithmetic {
        simple_arithmetic(var_factor(name))
    }

    fn int_arithmetic(value: i32) -> Arithmetic {
        simple_arithmetic(int_factor(value))
    }

    #[test]
    fn test_comparison_operator_equal() {
        let op = ComparisonOperator::Equal;
        assert!(op.is_equal());
        assert_eq!(op.to_string(), "==");
    }

    #[test]
    fn test_comparison_operator_not_equal() {
        let op = ComparisonOperator::NotEqual;
        assert!(!op.is_equal());
        assert_eq!(op.to_string(), "≠");
    }

    #[test]
    fn test_comparison_operator_greater_than() {
        let op = ComparisonOperator::GreaterThan;
        assert!(!op.is_equal());
        assert_eq!(op.to_string(), ">");
    }

    #[test]
    fn test_comparison_operator_greater_equal_than() {
        let op = ComparisonOperator::GreaterEqualThan;
        assert!(!op.is_equal());
        assert_eq!(op.to_string(), "≥");
    }

    #[test]
    fn test_comparison_operator_less_than() {
        let op = ComparisonOperator::LessThan;
        assert!(!op.is_equal());
        assert_eq!(op.to_string(), "<");
    }

    #[test]
    fn test_comparison_operator_less_equal_than() {
        let op = ComparisonOperator::LessEqualThan;
        assert!(!op.is_equal());
        assert_eq!(op.to_string(), "≤");
    }

    #[test]
    fn test_comparison_operator_clone_hash() {
        let op = ComparisonOperator::Equal;
        let cloned = op.clone();
        assert_eq!(op, cloned);

        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(ComparisonOperator::Equal);
        set.insert(ComparisonOperator::NotEqual);
        set.insert(ComparisonOperator::Equal); // Duplicate
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_comparison_expr_creation() {
        let left = var_arithmetic("x");
        let right = int_arithmetic(42);
        let op = ComparisonOperator::Equal;
        let comp = ComparisonExpr::new(left, op, right);

        assert_eq!(comp.operator(), &ComparisonOperator::Equal);
    }

    #[test]
    fn test_comparison_expr_accessors() {
        let left = var_arithmetic("age");
        let right = int_arithmetic(18);
        let op = ComparisonOperator::GreaterEqualThan;
        let comp = ComparisonExpr::new(left.clone(), op.clone(), right.clone());

        assert_eq!(comp.left(), &left);
        assert_eq!(comp.operator(), &op);
        assert_eq!(comp.right(), &right);
    }

    #[test]
    fn test_comparison_expr_vars_set() {
        // Test with variables on both sides: x == y
        let left = var_arithmetic("x");
        let right = var_arithmetic("y");
        let comp = ComparisonExpr::new(left, ComparisonOperator::Equal, right);

        let vars = comp.vars_set();
        assert_eq!(vars.len(), 2);
        assert!(vars.iter().any(|&s| s == "x"));
        assert!(vars.iter().any(|&s| s == "y"));
    }

    #[test]
    fn test_comparison_expr_vars_set_with_constants() {
        // Test with constant on one side: x > 5
        let left = var_arithmetic("x");
        let right = int_arithmetic(5);
        let comp = ComparisonExpr::new(left, ComparisonOperator::GreaterThan, right);

        let vars = comp.vars_set();
        assert_eq!(vars.len(), 1);
        assert!(vars.iter().any(|&s| s == "x"));
    }

    #[test]
    fn test_comparison_expr_vars_set_duplicate_variables() {
        // Test with same variable on both sides: x == x
        let left = var_arithmetic("x");
        let right = var_arithmetic("x");
        let comp = ComparisonExpr::new(left, ComparisonOperator::Equal, right);

        let vars = comp.vars_set();
        assert_eq!(vars.len(), 1);
        assert!(vars.iter().any(|&s| s == "x"));
    }

    #[test]
    fn test_comparison_expr_left_vars() {
        let left = var_arithmetic("x");
        let right = int_arithmetic(10);
        let comp = ComparisonExpr::new(left, ComparisonOperator::LessThan, right);

        let left_vars = comp.left_vars();
        assert_eq!(left_vars.len(), 1);
        assert_eq!(left_vars[0], "x");
    }

    #[test]
    fn test_comparison_expr_right_vars() {
        let left = int_arithmetic(5);
        let right = var_arithmetic("y");
        let comp = ComparisonExpr::new(left, ComparisonOperator::NotEqual, right);

        let right_vars = comp.right_vars();
        assert_eq!(right_vars.len(), 1);
        assert_eq!(right_vars[0], "y");
    }

    #[test]
    fn test_comparison_expr_display() {
        // Simple variable comparison
        let comp1 = ComparisonExpr::new(
            var_arithmetic("x"),
            ComparisonOperator::Equal,
            var_arithmetic("y"),
        );
        assert_eq!(comp1.to_string(), "x == y");

        // Variable vs constant
        let comp2 = ComparisonExpr::new(
            var_arithmetic("age"),
            ComparisonOperator::GreaterEqualThan,
            int_arithmetic(18),
        );
        assert_eq!(comp2.to_string(), "age ≥ 18");

        // Constant vs variable
        let comp3 = ComparisonExpr::new(
            int_arithmetic(0),
            ComparisonOperator::LessThan,
            var_arithmetic("score"),
        );
        assert_eq!(comp3.to_string(), "0 < score");

        // Not equal comparison
        let comp4 = ComparisonExpr::new(
            var_arithmetic("status"),
            ComparisonOperator::NotEqual,
            var_arithmetic("inactive"),
        );
        assert_eq!(comp4.to_string(), "status ≠ inactive");
    }

    #[test]
    fn test_comparison_expr_clone_hash() {
        let comp = ComparisonExpr::new(
            var_arithmetic("test"),
            ComparisonOperator::GreaterThan,
            int_arithmetic(100),
        );
        let cloned = comp.clone();
        assert_eq!(comp, cloned);

        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(comp.clone());
        set.insert(cloned);
        assert_eq!(set.len(), 1); // Should be deduplicated
    }

    #[test]
    fn test_comparison_expr_complex_example() {
        // Test a realistic comparison: salary >= 50000
        let comp = ComparisonExpr::new(
            var_arithmetic("salary"),
            ComparisonOperator::GreaterEqualThan,
            int_arithmetic(50000),
        );

        assert_eq!(comp.to_string(), "salary ≥ 50000");

        let vars = comp.vars_set();
        assert_eq!(vars.len(), 1);
        assert!(vars.iter().any(|&s| s == "salary"));

        let left_vars = comp.left_vars();
        assert_eq!(left_vars, vec!["salary"]);

        let right_vars = comp.right_vars();
        assert!(right_vars.is_empty());
    }

    #[test]
    fn test_all_comparison_operators() {
        let operators = [
            ComparisonOperator::Equal,
            ComparisonOperator::NotEqual,
            ComparisonOperator::GreaterThan,
            ComparisonOperator::GreaterEqualThan,
            ComparisonOperator::LessThan,
            ComparisonOperator::LessEqualThan,
        ];

        let expected_symbols = ["==", "≠", ">", "≥", "<", "≤"];

        for (op, expected) in operators.iter().zip(expected_symbols.iter()) {
            assert_eq!(op.to_string(), *expected);
        }

        // Only Equal should return true for is_equal()
        assert!(operators[0].is_equal());
        for op in &operators[1..] {
            assert!(!op.is_equal());
        }
    }
}
