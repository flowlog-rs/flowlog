//! Aggregation expressions types for FlowLog.
//!
//! This module provides types for representing aggregation operations in FlowLog rules.
//! Aggregations allow rules to compute summary values from collections of data,
//! such as counting items, summing values, or finding minimum/maximum values.
//!
//! # Overview
//!
//! The module defines two main types:
//! - [`AggregationOperator`]: Statistical operators (min, max, count, sum)
//! - [`Aggregation`]: Complete aggregation expressions with operators
//!
//! # Aggregation Types
//!
//! ## Statistical Aggregations
//! - **Count**: Number of matching items
//! - **Sum**: Total value across all items
//!
//! ## Range Aggregations  
//! - **Min**: Smallest value in the collection
//! - **Max**: Largest value in the collection
//!
//! # Examples
//!
//! ```rust
//! use parser::logic::{Aggregation, AggregationOperator, Arithmetic, Factor};
//! use parser::primitive::ConstType;
//!
//! // Count aggregation: count(*)
//! let count_expr = Arithmetic::new(Factor::Const(ConstType::Integer(1)), vec![]);
//! let count_agg = Aggregation::new(AggregationOperator::Count, count_expr);
//! assert_eq!(count_agg.to_string(), "count(1)");
//!
//! // Sum aggregation: sum(price * quantity)
//! let price_factor = Factor::Var("price".to_string());
//! let qty_factor = Factor::Var("quantity".to_string());
//! let sum_expr = Arithmetic::new(price_factor, vec![
//!     (parser::logic::ArithmeticOperator::Multiply, qty_factor)
//! ]);
//! let sum_agg = Aggregation::new(AggregationOperator::Sum, sum_expr);
//! assert_eq!(sum_agg.to_string(), "sum(price * quantity)");
//!
//! // Max aggregation: max(salary)
//! let salary_expr = Arithmetic::new(Factor::Var("salary".to_string()), vec![]);
//! let max_agg = Aggregation::new(AggregationOperator::Max, salary_expr);
//! assert_eq!(max_agg.to_string(), "max(salary)");
//! ```

use super::Arithmetic;
use crate::{Lexeme, Rule};
use pest::iterators::Pair;
use std::fmt;

/// Represents the different types of aggregation operations available in FlowLog.
///
/// Aggregation operators define mathematical operations that compute summary
/// values from collections of data. Each operator has specific semantics for
/// how it processes input values and what type of result it produces.
///
/// # Operator Characteristics
///
/// ## Count Operations
/// - **Count**: Returns the number of items in a collection, regardless of their values
///
/// ## Arithmetic Operations  
/// - **Sum**: Computes the total of all numeric values in a collection
///
/// ## Range Operations
/// - **Min**: Finds the smallest value in a collection
/// - **Max**: Finds the largest value in a collection  
///
/// # Examples
///
/// ```rust
/// use parser::logic::AggregationOperator;
///
/// // Count operations - universal
/// let counter = AggregationOperator::Count;
/// assert_eq!(counter.to_string(), "count");
///
/// // Arithmetic operations - numeric only
/// let summer = AggregationOperator::Sum;
/// assert_eq!(summer.to_string(), "sum");
///
/// // Range operations - comparable types
/// let minimizer = AggregationOperator::Min;
/// let maximizer = AggregationOperator::Max;
/// assert_eq!(minimizer.to_string(), "min");
/// assert_eq!(maximizer.to_string(), "max");
/// ```
#[derive(Debug, Clone, Eq, Hash, PartialEq, Copy)]
pub enum AggregationOperator {
    /// Finds the minimum value in a dataset.
    ///
    /// Returns the smallest value among all input values according to the
    /// natural ordering of the data type. For numeric types, this is
    /// mathematical ordering; for text, this is lexicographic ordering.
    Min,

    /// Finds the maximum value in a dataset.
    ///
    /// Returns the largest value among all input values according to the
    /// natural ordering of the data type. Complement of the Min operation.
    Max,

    /// Counts the number of items in a dataset.
    ///
    /// Returns the total number of input items, regardless of their values.
    /// This is the only aggregation that works universally with any data type.
    /// Typically used for cardinality queries and existence checking.
    Count,

    /// Calculates the sum of all values in a dataset.
    ///
    /// Returns the arithmetic total of all numeric input values. Only applicable
    /// to numeric data types (integers, decimals). For non-numeric types,
    /// the behavior is undefined and may result in runtime errors.
    Sum,
}

impl fmt::Display for AggregationOperator {
    /// Formats the aggregation operator as a lowercase string for display.
    ///
    /// This implementation provides consistent string representation that matches
    /// FlowLog syntax and SQL-like conventions. The lowercase format is used
    /// for grammar parsing, code generation, and user-friendly output.
    ///
    /// # Format Rules
    ///
    /// All operators are formatted as lowercase identifiers:
    /// - `Min` → `"min"`
    /// - `Max` → `"max"`  
    /// - `Count` → `"count"`
    /// - `Sum` → `"sum"`
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::AggregationOperator;
    ///
    /// assert_eq!(format!("{}", AggregationOperator::Min), "min");
    /// assert_eq!(format!("{}", AggregationOperator::Max), "max");
    /// assert_eq!(format!("{}", AggregationOperator::Count), "count");
    /// assert_eq!(format!("{}", AggregationOperator::Sum), "sum");
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AggregationOperator::Min => write!(f, "min"),
            AggregationOperator::Max => write!(f, "max"),
            AggregationOperator::Count => write!(f, "count"),
            AggregationOperator::Sum => write!(f, "sum"),
        }
    }
}

impl Lexeme for AggregationOperator {
    /// Converts a Pest parse rule into an AggregationOperator enum variant.
    ///
    /// This implementation handles the parsing of aggregation operator tokens
    /// from the FlowLog grammar into strongly-typed enum variants. The parser
    /// expects specific grammar rules for each operator type.
    ///
    /// # Grammar Rule Mapping
    ///
    /// The method maps Pest grammar rules to enum variants:
    /// - `Rule::min` → `AggregationOperator::Min`
    /// - `Rule::max` → `AggregationOperator::Max`
    /// - `Rule::count` → `AggregationOperator::Count`
    /// - `Rule::sum` → `AggregationOperator::Sum`
    ///
    /// # Arguments
    ///
    /// * `parsed_rule` - A Pest [`Pair<Rule>`] containing the parsed aggregation operator
    ///
    /// # Returns
    ///
    /// The corresponding [`AggregationOperator`] enum variant
    ///
    /// # Panics
    ///
    /// Panics if the parsed rule doesn't match any expected aggregation operator rules.
    /// This indicates a grammar parsing error or unsupported operator type.
    ///
    /// # Examples
    ///
    /// ```rust
    /// // Note: This example shows the conceptual usage.
    /// // In practice, this method is called by the parser internally.
    /// use parser::{FlowLogParser, Rule, Lexeme, AggregationOperator};
    /// use pest::Parser;
    ///
    /// // This would be called internally during parsing:
    /// // let operator = AggregationOperator::from_parsed_rule(parsed_min_rule);
    /// // assert_eq!(operator, AggregationOperator::Min);
    /// ```
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        // Extract the inner rule that contains the specific operator type
        let operator = parsed_rule.into_inner().next().unwrap();

        // Match the rule type to the corresponding enum variant
        match operator.as_rule() {
            Rule::min => AggregationOperator::Min,
            Rule::max => AggregationOperator::Max,
            Rule::count => AggregationOperator::Count,
            Rule::sum => AggregationOperator::Sum,
            _ => unreachable!(), // Should never reach here if grammar is correct
        }
    }
}

/// Represents a complete aggregation expression in FlowLog rules.
///
/// An aggregation expression combines an aggregation operator with an arithmetic
/// expression that defines what values to aggregate. This structure enables
/// sophisticated data analysis operations within FlowLog rules.
///
/// # Structure
///
/// Each aggregation consists of:
/// - **Operator**: The type of aggregation to perform (min, max, count, sum)
/// - **Expression**: An arithmetic expression defining the values to aggregate
///
/// # Syntax
///
/// FlowLog aggregations follow SQL-like syntax:
/// ```text
/// operator(arithmetic_expression)
/// ```
///
/// # Examples
///
/// ```rust
/// use parser::logic::{Aggregation, AggregationOperator, Arithmetic, Factor};
/// use parser::logic::ArithmeticOperator;
/// use parser::primitive::ConstType;
///
/// // Simple count: count(1) - counts all rows
/// let count_expr = Arithmetic::new(Factor::Const(ConstType::Integer(1)), vec![]);
/// let count_agg = Aggregation::new(AggregationOperator::Count, count_expr);
/// assert_eq!(count_agg.to_string(), "count(1)");
///
/// // Variable sum: sum(salary)
/// let salary_expr = Arithmetic::new(Factor::Var("salary".to_string()), vec![]);
/// let salary_sum = Aggregation::new(AggregationOperator::Sum, salary_expr);
/// assert_eq!(salary_sum.to_string(), "sum(salary)");
///
/// // Complex expression: max(base + bonus * rate)
/// let complex_expr = Arithmetic::new(
///     Factor::Var("base".to_string()),
///     vec![
///         (ArithmeticOperator::Plus, Factor::Var("bonus".to_string())),
///         (ArithmeticOperator::Multiply, Factor::Var("rate".to_string()))
///     ]
/// );
/// let max_total = Aggregation::new(AggregationOperator::Max, complex_expr);
/// assert_eq!(max_total.to_string(), "max(base + bonus * rate)");
///
/// // Accessing components
/// assert_eq!(*max_total.operator(), AggregationOperator::Max);
/// assert_eq!(max_total.vars().len(), 3); // base, bonus, rate
/// ```
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct Aggregation {
    /// The aggregation operation to perform (min, max, count, sum)
    operator: AggregationOperator,
    /// The arithmetic expression to aggregate over
    arithmetic: Arithmetic,
}

impl fmt::Display for Aggregation {
    /// Formats the aggregation as "operator(arithmetic_expression)".
    ///
    /// Produces FlowLog-compatible syntax that can be parsed back into
    /// an aggregation expression. The format follows SQL-like conventions
    /// with the operator name followed by the expression in parentheses.
    ///
    /// # Format Structure
    ///
    /// ```text
    /// operator(expression)
    /// ```
    ///
    /// Where:
    /// - `operator` is the lowercase aggregation operator name
    /// - `expression` is the formatted arithmetic expression
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Aggregation, AggregationOperator, Arithmetic, Factor};
    /// use parser::logic::ArithmeticOperator;
    /// use parser::primitive::ConstType;
    ///
    /// // Simple aggregations
    /// let count = Aggregation::new(
    ///     AggregationOperator::Count,
    ///     Arithmetic::new(Factor::Const(ConstType::Integer(1)), vec![])
    /// );
    /// assert_eq!(format!("{}", count), "count(1)");
    ///
    /// // Variable aggregations  
    /// let sum = Aggregation::new(
    ///     AggregationOperator::Sum,
    ///     Arithmetic::new(Factor::Var("amount".to_string()), vec![])
    /// );
    /// assert_eq!(format!("{}", sum), "sum(amount)");
    ///
    /// // Complex expression aggregations
    /// let max_calc = Aggregation::new(
    ///     AggregationOperator::Max,
    ///     Arithmetic::new(
    ///         Factor::Var("price".to_string()),
    ///         vec![(ArithmeticOperator::Multiply, Factor::Var("qty".to_string()))]
    ///     )
    /// );
    /// assert_eq!(format!("{}", max_calc), "max(price * qty)");
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}({})", self.operator, self.arithmetic)
    }
}

impl Aggregation {
    /// Creates a new aggregation expression with the specified operator and arithmetic expression.
    ///
    /// This constructor builds a complete aggregation from its component parts,
    /// enabling programmatic construction of aggregation expressions for code
    /// generation, query building, and testing scenarios.
    ///
    /// # Arguments
    ///
    /// * `operator` - The [`AggregationOperator`] defining the type of aggregation
    /// * `arithmetic` - The [`Arithmetic`] expression defining what values to aggregate
    ///
    /// # Returns
    ///
    /// A new [`Aggregation`] instance ready for use in FlowLog rules
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Aggregation, AggregationOperator, Arithmetic, Factor};
    /// use parser::logic::ArithmeticOperator;
    /// use parser::primitive::ConstType;
    ///
    /// // Create a count aggregation
    /// let count_all = Aggregation::new(
    ///     AggregationOperator::Count,
    ///     Arithmetic::new(Factor::Const(ConstType::Integer(1)), vec![])
    /// );
    ///
    /// // Create a sum of products
    /// let sum_products = Aggregation::new(
    ///     AggregationOperator::Sum,
    ///     Arithmetic::new(
    ///         Factor::Var("price".to_string()),
    ///         vec![(ArithmeticOperator::Multiply, Factor::Var("quantity".to_string()))]
    ///     )
    /// );
    ///
    /// // Create a max salary aggregation
    /// let max_salary = Aggregation::new(
    ///     AggregationOperator::Max,
    ///     Arithmetic::new(Factor::Var("salary".to_string()), vec![])
    /// );
    /// ```
    #[must_use]
    pub fn new(operator: AggregationOperator, arithmetic: Arithmetic) -> Self {
        Self {
            operator,
            arithmetic,
        }
    }

    /// Returns a vector of references to all variable names used in the arithmetic expression.
    ///
    /// This method performs variable dependency analysis on the aggregation's arithmetic
    /// expression, identifying all variables that must be bound in the rule body for
    /// the aggregation to be evaluated. This information is crucial for:
    /// - Query optimization and planning
    /// - Variable binding validation
    /// - Dependency graph construction
    /// - Rule safety analysis
    ///
    /// The method delegates to the underlying arithmetic expression's variable analysis,
    /// ensuring consistent behavior across all expression types.
    ///
    /// # Returns
    ///
    /// A vector containing references to all variable names in the arithmetic expression.
    /// Variables may appear multiple times if used in different parts of the expression.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Aggregation, AggregationOperator, Arithmetic, Factor};
    /// use parser::logic::ArithmeticOperator;
    ///
    /// // Simple variable aggregation
    /// let sum_salary = Aggregation::new(
    ///     AggregationOperator::Sum,
    ///     Arithmetic::new(Factor::Var("salary".to_string()), vec![])
    /// );
    /// let vars = sum_salary.vars();
    /// assert_eq!(vars.len(), 1);
    /// assert_eq!(vars[0], &"salary".to_string());
    ///
    /// // Complex expression with multiple variables
    /// let sum_total = Aggregation::new(
    ///     AggregationOperator::Sum,
    ///     Arithmetic::new(
    ///         Factor::Var("base".to_string()),
    ///         vec![
    ///             (ArithmeticOperator::Plus, Factor::Var("bonus".to_string())),
    ///             (ArithmeticOperator::Multiply, Factor::Var("rate".to_string()))
    ///         ]
    ///     )
    /// );
    /// let vars = sum_total.vars();
    /// assert_eq!(vars.len(), 3);
    /// assert!(vars.contains(&&"base".to_string()));
    /// assert!(vars.contains(&&"bonus".to_string()));
    /// assert!(vars.contains(&&"rate".to_string()));
    /// ```
    pub fn vars(&self) -> Vec<&String> {
        self.arithmetic.vars()
    }

    /// Returns a reference to the arithmetic expression being aggregated.
    ///
    /// This accessor provides access to the underlying arithmetic expression
    /// that defines what values the aggregation operates on. The expression
    /// can range from simple variables to complex mathematical computations.
    ///
    /// # Returns
    ///
    /// A reference to the internal [`Arithmetic`] expression
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Aggregation, AggregationOperator, Arithmetic, Factor};
    /// use parser::primitive::ConstType;
    ///
    /// let agg = Aggregation::new(
    ///     AggregationOperator::Max,
    ///     Arithmetic::new(Factor::Var("temperature".to_string()), vec![])
    /// );
    ///
    /// let expr = agg.arithmetic();
    /// assert!(expr.is_var());
    /// assert_eq!(expr.vars().len(), 1);
    /// ```
    #[must_use]
    pub fn arithmetic(&self) -> &Arithmetic {
        &self.arithmetic
    }

    /// Returns a reference to the aggregation operator.
    ///
    /// This accessor provides access to the aggregation operator that defines
    /// what type of summary operation is performed on the arithmetic expression.
    /// The operator determines the semantics and expected data types for the aggregation.
    ///
    /// # Returns
    ///
    /// A reference to the [`AggregationOperator`] (min, max, count, or sum)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Aggregation, AggregationOperator, Arithmetic, Factor};
    ///
    /// let sum_agg = Aggregation::new(
    ///     AggregationOperator::Sum,
    ///     Arithmetic::new(Factor::Var("amount".to_string()), vec![])
    /// );
    ///
    /// assert_eq!(*sum_agg.operator(), AggregationOperator::Sum);
    ///
    /// let count_agg = Aggregation::new(
    ///     AggregationOperator::Count,
    ///     Arithmetic::new(Factor::Var("id".to_string()), vec![])
    /// );
    ///
    /// assert_eq!(*count_agg.operator(), AggregationOperator::Count);
    /// ```
    #[must_use]
    pub fn operator(&self) -> &AggregationOperator {
        &self.operator
    }
}

impl Lexeme for Aggregation {
    /// Converts a Pest parse rule into a complete Aggregation struct.
    ///
    /// This implementation handles the parsing of complete aggregation expressions
    /// from FlowLog grammar tokens into strongly-typed Rust structures. The parser
    /// expects a specific two-part grammar structure: operator followed by expression.
    ///
    /// # Grammar Structure
    ///
    /// The expected parse rule structure is:
    /// ```text
    /// aggregation_expr = { aggregation_operator ~ "(" ~ arithmetic_expr ~ ")" }
    /// ```
    ///
    /// This corresponds to FlowLog syntax like:
    /// - `sum(price * quantity)`
    /// - `count(customer_id)`
    /// - `max(salary + bonus)`
    ///
    /// # Parsing Process
    ///
    /// 1. **Operator Extraction**: Parses the first inner rule as an aggregation operator
    /// 2. **Expression Extraction**: Parses the second inner rule as an arithmetic expression
    /// 3. **Structure Assembly**: Combines both components into a complete aggregation
    ///
    /// # Arguments
    ///
    /// * `parsed_rule` - A Pest [`Pair<Rule>`] containing the complete aggregation parse tree
    ///
    /// # Returns
    ///
    /// A new [`Aggregation`] instance with the parsed operator and arithmetic expression
    ///
    /// # Panics
    ///
    /// May panic if:
    /// - The parse rule doesn't contain exactly two inner rules
    /// - The operator rule cannot be parsed as a valid [`AggregationOperator`]
    /// - The expression rule cannot be parsed as a valid [`Arithmetic`] expression
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let mut inner_rules = parsed_rule.into_inner();

        // Parse the aggregation operator (first inner rule)
        let operator = AggregationOperator::from_parsed_rule(inner_rules.next().unwrap());

        // Parse the arithmetic expression (second inner rule)
        let arithmetic = Arithmetic::from_parsed_rule(inner_rules.next().unwrap());

        Self {
            operator,
            arithmetic,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logic::{ArithmeticOperator, Factor};
    use crate::primitive::ConstType;

    #[test]
    fn test_aggregation_operator_display() {
        assert_eq!(format!("{}", AggregationOperator::Min), "min");
        assert_eq!(format!("{}", AggregationOperator::Max), "max");
        assert_eq!(format!("{}", AggregationOperator::Count), "count");
        assert_eq!(format!("{}", AggregationOperator::Sum), "sum");
    }

    #[test]
    fn test_aggregation_operator_copy_clone() {
        let op = AggregationOperator::Sum;
        let copied = op;
        let cloned = op.clone();

        assert_eq!(op, copied);
        assert_eq!(op, cloned);
    }

    #[test]
    fn test_aggregation_creation() {
        let expr = Arithmetic::new(Factor::Var("x".to_string()), vec![]);
        let agg = Aggregation::new(AggregationOperator::Sum, expr);

        assert_eq!(*agg.operator(), AggregationOperator::Sum);
        assert_eq!(agg.vars().len(), 1);
        assert_eq!(agg.vars()[0], &"x".to_string());
    }

    #[test]
    fn test_aggregation_display() {
        // Simple variable aggregation
        let expr = Arithmetic::new(Factor::Var("salary".to_string()), vec![]);
        let agg = Aggregation::new(AggregationOperator::Sum, expr);
        assert_eq!(format!("{}", agg), "sum(salary)");

        // Constant aggregation
        let expr = Arithmetic::new(Factor::Const(ConstType::Integer(1)), vec![]);
        let agg = Aggregation::new(AggregationOperator::Count, expr);
        assert_eq!(format!("{}", agg), "count(1)");

        // Complex expression aggregation
        let expr = Arithmetic::new(
            Factor::Var("price".to_string()),
            vec![(ArithmeticOperator::Multiply, Factor::Var("qty".to_string()))],
        );
        let agg = Aggregation::new(AggregationOperator::Max, expr);
        assert_eq!(format!("{}", agg), "max(price * qty)");
    }

    #[test]
    fn test_aggregation_vars() {
        // Single variable
        let expr = Arithmetic::new(Factor::Var("amount".to_string()), vec![]);
        let agg = Aggregation::new(AggregationOperator::Sum, expr);
        let vars = agg.vars();
        assert_eq!(vars.len(), 1);
        assert_eq!(vars[0], &"amount".to_string());

        // Multiple variables
        let expr = Arithmetic::new(
            Factor::Var("base".to_string()),
            vec![
                (ArithmeticOperator::Plus, Factor::Var("bonus".to_string())),
                (
                    ArithmeticOperator::Multiply,
                    Factor::Var("rate".to_string()),
                ),
            ],
        );
        let agg = Aggregation::new(AggregationOperator::Max, expr);
        let vars = agg.vars();
        assert_eq!(vars.len(), 3);
        assert!(vars.contains(&&"base".to_string()));
        assert!(vars.contains(&&"bonus".to_string()));
        assert!(vars.contains(&&"rate".to_string()));

        // No variables (constant expression)
        let expr = Arithmetic::new(Factor::Const(ConstType::Integer(42)), vec![]);
        let agg = Aggregation::new(AggregationOperator::Count, expr);
        assert_eq!(agg.vars().len(), 0);
    }

    #[test]
    fn test_aggregation_accessors() {
        let expr = Arithmetic::new(Factor::Var("value".to_string()), vec![]);
        let agg = Aggregation::new(AggregationOperator::Min, expr.clone());

        assert_eq!(*agg.operator(), AggregationOperator::Min);
        assert_eq!(agg.arithmetic(), &expr);
    }

    #[test]
    fn test_aggregation_clone_hash_eq() {
        let expr = Arithmetic::new(Factor::Var("test".to_string()), vec![]);
        let agg1 = Aggregation::new(AggregationOperator::Count, expr.clone());
        let agg2 = agg1.clone();

        assert_eq!(agg1, agg2);

        // Test with HashSet to verify Hash implementation
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(agg1.clone());
        set.insert(agg2);
        assert_eq!(set.len(), 1); // Should be deduplicated
    }

    #[test]
    fn test_all_aggregation_operators() {
        let expr = Arithmetic::new(Factor::Var("x".to_string()), vec![]);

        let min_agg = Aggregation::new(AggregationOperator::Min, expr.clone());
        assert_eq!(format!("{}", min_agg), "min(x)");

        let max_agg = Aggregation::new(AggregationOperator::Max, expr.clone());
        assert_eq!(format!("{}", max_agg), "max(x)");

        let count_agg = Aggregation::new(AggregationOperator::Count, expr.clone());
        assert_eq!(format!("{}", count_agg), "count(x)");

        let sum_agg = Aggregation::new(AggregationOperator::Sum, expr);
        assert_eq!(format!("{}", sum_agg), "sum(x)");
    }
}
