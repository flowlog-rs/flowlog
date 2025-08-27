//! Arithmetic expression types for Datalog programs (Macaron engine).
//!
//! This module provides types for representing and manipulating arithmetic expressions
//! in Datalog programs parsed by Macaron. Arithmetic expressions can appear in rule heads and predicates,
//! allowing for computed values and mathematical operations.
//!
//! # Overview
//!
//! The module defines three main types:
//! - [`ArithmeticOperator`]: Mathematical operators (+, -, *, /, %)
//! - [`Factor`]: Basic arithmetic operands (variables, constants)  
//! - [`Arithmetic`]: Complex arithmetic expressions with operators
//!
//! # Expression Structure
//!
//! Arithmetic expressions use a left-associative structure where an initial factor
//! is followed by a sequence of (operator, factor) pairs. This design naturally
//! handles operator precedence and allows for efficient parsing and evaluation.
//!
//! For example, the expression `x + y * 2 - z` is represented as:
//! - Initial factor: `x`
//! - Operations: `[(+, y), (*, 2), (-, z)]`
//!
//! # Examples
//!
//! ```rust
//! use parser::logic::{Arithmetic, ArithmeticOperator, Factor};
//! use parser::primitive::ConstType;
//!
//! // Create a simple arithmetic expression: X + 5
//! let expr = Arithmetic::new(
//!     Factor::Var("X".to_string()),
//!     vec![(ArithmeticOperator::Plus, Factor::Const(ConstType::Integer(5)))]
//! );
//!
//! assert_eq!(expr.to_string(), "X + 5");
//!
//! // Create a more complex expression: price * (1 + tax_rate)  
//! let complex_expr = Arithmetic::new(
//!     Factor::Var("price".to_string()),
//!     vec![
//!         (ArithmeticOperator::Multiply, Factor::Var("tax_multiplier".to_string())),
//!     ]
//! );
//! ```

use crate::primitive::ConstType;
use crate::{Lexeme, Rule};
use pest::iterators::Pair;
use std::collections::HashSet;
use std::fmt;

/// Arithmetic operators for mathematical expressions.
///
/// Supports the five basic arithmetic operations commonly used in mathematical
/// and logical expressions within Macaron rules. These operators follow standard
/// mathematical semantics and precedence rules.
///
/// # Examples
///
/// ```rust
/// use parser::logic::ArithmeticOperator;
///
/// // Basic operator creation and display
/// let add_op = ArithmeticOperator::Plus;
/// assert_eq!(add_op.to_string(), "+");
///
/// let mul_op = ArithmeticOperator::Multiply;  
/// assert_eq!(mul_op.to_string(), "*");
///
/// // Operators can be compared for equality
/// assert_eq!(ArithmeticOperator::Plus, ArithmeticOperator::Plus);
/// assert_ne!(ArithmeticOperator::Plus, ArithmeticOperator::Minus);
/// ```
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum ArithmeticOperator {
    /// Addition operator (+)
    Plus,
    /// Subtraction operator (-)
    Minus,
    /// Multiplication operator (*)
    Multiply,
    /// Division operator (/)
    Divide,
    /// Modulo operator (%)
    Modulo,
}

impl fmt::Display for ArithmeticOperator {
    /// Formats the arithmetic operator using its mathematical symbol.
    ///
    /// Each operator is represented by its standard mathematical symbol,
    /// making the output suitable for mathematical notation and user display.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::ArithmeticOperator;
    ///
    /// assert_eq!(ArithmeticOperator::Plus.to_string(), "+");
    /// assert_eq!(ArithmeticOperator::Multiply.to_string(), "*");
    /// assert_eq!(ArithmeticOperator::Modulo.to_string(), "%");
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let symbol = match self {
            Self::Plus => "+",
            Self::Minus => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::Modulo => "%",
        };
        write!(f, "{symbol}")
    }
}

impl Lexeme for ArithmeticOperator {
    /// Parses an arithmetic operator from a pest parsing rule.
    ///
    /// This method converts parsed tokens from the grammar into the appropriate
    /// `ArithmeticOperator` variant. It expects the parsed rule to contain
    /// exactly one inner rule representing the specific operator type.
    ///
    /// # Arguments
    ///
    /// * `parsed_rule` - A pest `Pair` containing the parsed operator rule
    ///
    /// # Returns
    ///
    /// The corresponding `ArithmeticOperator` variant.
    ///
    /// # Panics
    ///
    /// Panics if the parsed rule contains an unknown operator type that doesn't
    /// match any of the expected grammar rules.
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let operator = parsed_rule.into_inner().next().unwrap();
        match operator.as_rule() {
            Rule::plus => Self::Plus,
            Rule::minus => Self::Minus,
            Rule::times => Self::Multiply,
            Rule::divide => Self::Divide,
            Rule::modulo => Self::Modulo,
            _ => unreachable!("Unknown arithmetic operator"),
        }
    }
}

/// A factor in an arithmetic expression.
///
/// Factors are the basic building blocks of arithmetic expressions, representing
/// either variables or constant values that can be combined with operators.
/// They serve as the atomic operands in mathematical computations.
///
/// # Variants
///
/// - `Var(String)`: Represents a variable reference by name. Variables are
///   placeholders for values that will be bound during rule evaluation.
/// - `Const(ConstType)`: Represents a literal constant value such as integers
///   or text strings that are known at compile time.
///
/// # Examples
///
/// ```rust
/// use parser::logic::Factor;
/// use parser::primitive::ConstType;
///
/// // Create a variable factor
/// let var_factor = Factor::Var("x".to_string());
/// assert!(var_factor.is_var());
/// assert!(!var_factor.is_const());
/// assert_eq!(var_factor.vars(), vec![&"x".to_string()]);
///
/// // Create a constant factor
/// let const_factor = Factor::Const(ConstType::Integer(42));
/// assert!(!const_factor.is_var());
/// assert!(const_factor.is_const());
/// assert!(const_factor.vars().is_empty());
///
/// // Display formatting
/// assert_eq!(var_factor.to_string(), "x");
/// assert_eq!(const_factor.to_string(), "42");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Factor {
    /// A variable reference
    Var(String),
    /// A constant value
    Const(ConstType),
}

impl Factor {
    /// Checks if this factor is a variable.
    ///
    /// Returns `true` if this factor represents a variable reference,
    /// `false` if it represents a constant value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::Factor;
    /// use parser::primitive::ConstType;
    ///
    /// let var = Factor::Var("count".to_string());
    /// let const_val = Factor::Const(ConstType::Integer(100));
    ///
    /// assert!(var.is_var());
    /// assert!(!const_val.is_var());
    /// ```
    #[must_use]
    pub fn is_var(&self) -> bool {
        matches!(self, Self::Var(_))
    }

    /// Checks if this factor is a constant.
    ///
    /// Returns `true` if this factor represents a constant value,
    /// `false` if it represents a variable reference.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::Factor;
    /// use parser::primitive::ConstType;
    ///
    /// let var = Factor::Var("price".to_string());
    /// let const_val = Factor::Const(ConstType::Text("USD".to_string()));
    ///
    /// assert!(!var.is_const());
    /// assert!(const_val.is_const());
    /// ```
    #[must_use]
    pub fn is_const(&self) -> bool {
        matches!(self, Self::Const(_))
    }

    /// Returns a HashSet of all variables in this factor.
    ///
    /// For variables, this returns a set containing the variable name.
    /// For constants, this returns an empty set. The HashSet automatically
    /// handles deduplication, though factors contain at most one variable.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::Factor;
    /// use parser::primitive::ConstType;
    ///
    /// let var = Factor::Var("temperature".to_string());
    /// let const_val = Factor::Const(ConstType::Integer(273));
    ///
    /// let var_set = var.vars_set();
    /// assert_eq!(var_set.len(), 1);
    /// assert!(var_set.contains(&"temperature".to_string()));
    ///
    /// let const_set = const_val.vars_set();
    /// assert!(const_set.is_empty());
    /// ```
    #[must_use]
    pub fn vars_set(&self) -> HashSet<&String> {
        self.vars().into_iter().collect()
    }

    /// Returns a vector of references to variables in this factor.
    ///
    /// For variable factors, this returns a vector containing a single reference
    /// to the variable name. For constant factors, this returns an empty vector.
    ///
    /// This method preserves order and multiplicity (though factors have at most
    /// one variable), making it suitable for dependency analysis where the order
    /// of variable references matters.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::Factor;
    /// use parser::primitive::ConstType;
    ///
    /// let var = Factor::Var("user_id".to_string());
    /// let const_val = Factor::Const(ConstType::Integer(12345));
    ///
    /// let var_list = var.vars();
    /// assert_eq!(var_list.len(), 1);
    /// assert_eq!(var_list[0], "user_id");
    ///
    /// let const_list = const_val.vars();
    /// assert!(const_list.is_empty());
    /// ```
    #[must_use]
    pub fn vars(&self) -> Vec<&String> {
        match self {
            Self::Var(var) => vec![var],
            _ => vec![],
        }
    }
}

impl fmt::Display for Factor {
    /// Formats the factor for display.
    ///
    /// Variables are displayed as their name without any special formatting.
    /// Constants are displayed using their own `Display` implementation,
    /// which includes appropriate formatting like quotes for strings.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::Factor;
    /// use parser::primitive::ConstType;
    ///
    /// let var = Factor::Var("total_count".to_string());
    /// let int_const = Factor::Const(ConstType::Integer(42));
    /// let str_const = Factor::Const(ConstType::Text("hello".to_string()));
    ///
    /// assert_eq!(var.to_string(), "total_count");
    /// assert_eq!(int_const.to_string(), "42");
    /// assert_eq!(str_const.to_string(), "\"hello\"");
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Var(var) => write!(f, "{var}"),
            Self::Const(constant) => write!(f, "{constant}"),
        }
    }
}

impl Lexeme for Factor {
    /// Parses a factor from a pest parsing rule.
    ///
    /// This method constructs a `Factor` from parsed grammar tokens. It expects
    /// the parsed rule to contain exactly one inner rule that is either a
    /// variable or a constant.
    ///
    /// # Arguments
    ///
    /// * `parsed_rule` - A pest `Pair` containing the parsed factor rule
    ///
    /// # Returns
    ///
    /// A new `Factor` instance representing the parsed variable or constant.
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - The parsed rule contains no inner rules
    /// - The inner rule is neither a variable nor a constant
    ///
    /// # Grammar Mapping
    ///
    /// - `Rule::variable` → `Factor::Var(String)` - Creates a variable factor with the parsed name
    /// - `Rule::constant` → `Factor::Const(ConstType)` - Creates a constant factor with the parsed value
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let inner = parsed_rule
            .into_inner()
            .next()
            .expect("Expected inner rule for factor");

        match inner.as_rule() {
            Rule::variable => Self::Var(inner.as_str().to_string()),
            Rule::constant => Self::Const(ConstType::from_parsed_rule(inner)),
            _ => unreachable!("Invalid factor type: {:?}", inner.as_rule()),
        }
    }
}

/// Represents an arithmetic expression.
///
/// An arithmetic expression consists of an initial factor followed by a sequence
/// of (operator, factor) pairs. This representation naturally handles left-associative
/// parsing and allows for expressions like `x + y * 2 - z`.
///
/// # Structure and Evaluation
///
/// The structure follows the pattern: `initial_factor (operator factor)*`
///
/// For example, the expression `a + b - c * d` is represented as:
/// - `init`: Factor `a`
/// - `rest`: `[(+, b), (-, c), (*, d)]`
///
/// This left-associative structure means operations are evaluated from left to right
/// within the same precedence level, which is standard for arithmetic expressions.
///
/// # Constant Detection
///
/// Special methods identify when expressions contain only constants, enabling
/// compile-time optimization and constant folding opportunities.
///
/// # Examples
///
/// ```rust
/// use parser::logic::{Arithmetic, Factor, ArithmeticOperator};
/// use parser::primitive::ConstType;
///
/// // Simple variable: x
/// let simple = Arithmetic::new(Factor::Var("x".to_string()), vec![]);
/// assert!(simple.is_var());
/// assert_eq!(simple.to_string(), "x");
///
/// // Constant expression: 42
/// let constant = Arithmetic::new(Factor::Const(ConstType::Integer(42)), vec![]);
/// assert!(constant.is_const());
/// assert_eq!(constant.to_string(), "42");
///
/// // Complex expression: x + 5 * y - 10
/// let complex = Arithmetic::new(
///     Factor::Var("x".to_string()),
///     vec![
///         (ArithmeticOperator::Plus, Factor::Const(ConstType::Integer(5))),
///         (ArithmeticOperator::Multiply, Factor::Var("y".to_string())),
///         (ArithmeticOperator::Minus, Factor::Const(ConstType::Integer(10))),
///     ]
/// );
///
/// assert!(!complex.is_var() && !complex.is_const());
/// assert_eq!(complex.to_string(), "x + 5 * y - 10");
///
/// // Variable analysis
/// let vars = complex.vars();
/// assert_eq!(vars.len(), 2);
/// assert!(vars.contains(&&"x".to_string()));
/// assert!(vars.contains(&&"y".to_string()));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Arithmetic {
    /// Initial term in the arithmetic expression
    init: Factor,
    /// Sequence of (operator, factor) pairs that follow the initial term
    rest: Vec<(ArithmeticOperator, Factor)>,
}

impl Arithmetic {
    /// Creates a new arithmetic expression from an initial factor and sequence of operations.
    ///
    /// This is the primary constructor for building arithmetic expressions programmatically.
    /// The expression represents `init (op1 factor1) (op2 factor2) ...` where operations
    /// are evaluated left-to-right.
    ///
    /// # Arguments
    ///
    /// * `init` - The initial factor that starts the expression
    /// * `rest` - A vector of (operator, factor) pairs representing the remaining operations
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Arithmetic, Factor, ArithmeticOperator};
    /// use parser::primitive::ConstType;
    ///
    /// // Create simple variable: x
    /// let var_expr = Arithmetic::new(Factor::Var("x".to_string()), vec![]);
    ///
    /// // Create binary expression: x + 10
    /// let binary_expr = Arithmetic::new(
    ///     Factor::Var("x".to_string()),
    ///     vec![(ArithmeticOperator::Plus, Factor::Const(ConstType::Integer(10)))]
    /// );
    ///
    /// // Create complex expression: a * b + c
    /// let complex_expr = Arithmetic::new(
    ///     Factor::Var("a".to_string()),
    ///     vec![
    ///         (ArithmeticOperator::Multiply, Factor::Var("b".to_string())),
    ///         (ArithmeticOperator::Plus, Factor::Var("c".to_string())),
    ///     ]
    /// );
    /// ```
    #[must_use]
    pub fn new(init: Factor, rest: Vec<(ArithmeticOperator, Factor)>) -> Self {
        Self { init, rest }
    }

    /// Returns a reference to the initial factor in this arithmetic expression.
    ///
    /// The initial factor is the first operand in the expression, before any
    /// operators are applied. This is useful for analyzing the expression
    /// structure or accessing the primary component.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Arithmetic, Factor, ArithmeticOperator};
    /// use parser::primitive::ConstType;
    ///
    /// let expr = Arithmetic::new(
    ///     Factor::Var("base".to_string()),
    ///     vec![(ArithmeticOperator::Plus, Factor::Const(ConstType::Integer(1)))]
    /// );
    ///
    /// match expr.init() {
    ///     Factor::Var(name) => assert_eq!(name, "base"),
    ///     _ => panic!("Expected variable"),
    /// }
    /// ```
    #[must_use]
    pub fn init(&self) -> &Factor {
        &self.init
    }

    /// Returns a reference to the sequence of (operator, factor) pairs.
    ///
    /// The rest sequence contains all operations that follow the initial factor,
    /// representing the continuation of the arithmetic expression. Each tuple
    /// contains an operator and the factor it operates on.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Arithmetic, Factor, ArithmeticOperator};
    /// use parser::primitive::ConstType;
    ///
    /// let expr = Arithmetic::new(
    ///     Factor::Var("x".to_string()),
    ///     vec![
    ///         (ArithmeticOperator::Multiply, Factor::Const(ConstType::Integer(2))),
    ///         (ArithmeticOperator::Plus, Factor::Var("y".to_string())),
    ///     ]
    /// );
    ///
    /// let rest = expr.rest();
    /// assert_eq!(rest.len(), 2);
    /// assert_eq!(rest[0].0, ArithmeticOperator::Multiply);
    /// assert_eq!(rest[1].0, ArithmeticOperator::Plus);
    /// ```
    #[must_use]
    pub fn rest(&self) -> &Vec<(ArithmeticOperator, Factor)> {
        &self.rest
    }

    /// Returns all variables used in this arithmetic expression.
    ///
    /// This method collects variables from both the initial factor and all
    /// factors in the rest sequence. The order reflects the order of appearance
    /// in the expression, and duplicates are preserved.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Arithmetic, Factor, ArithmeticOperator};
    /// use parser::primitive::ConstType;
    ///
    /// let expr = Arithmetic::new(
    ///     Factor::Var("x".to_string()),
    ///     vec![
    ///         (ArithmeticOperator::Plus, Factor::Var("y".to_string())),
    ///         (ArithmeticOperator::Multiply, Factor::Const(ConstType::Integer(2))),
    ///         (ArithmeticOperator::Minus, Factor::Var("x".to_string())), // x appears again
    ///     ]
    /// );
    ///
    /// let vars = expr.vars();
    /// assert_eq!(vars.len(), 3); // Includes duplicate 'x'
    /// assert_eq!(vars[0], "x");
    /// assert_eq!(vars[1], "y");
    /// assert_eq!(vars[2], "x");
    /// ```
    #[must_use]
    pub fn vars(&self) -> Vec<&String> {
        let mut vars = self.init.vars();
        for (_, factor) in &self.rest {
            vars.extend(factor.vars());
        }
        vars
    }

    /// Returns a HashSet of all variables in this arithmetic expression.
    ///
    /// This method collects all unique variables from the expression, automatically
    /// deduplicating any variables that appear multiple times. This is useful for
    /// dependency analysis where you need to know which variables are used,
    /// but not how many times they appear.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Arithmetic, Factor, ArithmeticOperator};
    /// use parser::primitive::ConstType;
    ///
    /// let expr = Arithmetic::new(
    ///     Factor::Var("x".to_string()),
    ///     vec![
    ///         (ArithmeticOperator::Plus, Factor::Var("y".to_string())),
    ///         (ArithmeticOperator::Multiply, Factor::Var("x".to_string())), // x repeated
    ///     ]
    /// );
    ///
    /// let vars_set = expr.vars_set();
    /// assert_eq!(vars_set.len(), 2); // Only unique variables
    /// assert!(vars_set.contains(&"x".to_string()));
    /// assert!(vars_set.contains(&"y".to_string()));
    /// ```
    #[must_use]
    pub fn vars_set(&self) -> HashSet<&String> {
        self.vars().into_iter().collect()
    }

    /// Checks if this arithmetic expression contains only constants.
    ///
    /// Returns `true` if the expression consists of a single constant factor
    /// with no additional operations. This enables compile-time optimizations
    /// and constant folding opportunities.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Arithmetic, Factor, ArithmeticOperator};
    /// use parser::primitive::ConstType;
    ///
    /// // Pure constant
    /// let const_expr = Arithmetic::new(Factor::Const(ConstType::Integer(42)), vec![]);
    /// assert!(const_expr.is_const());
    ///
    /// // Variable (not constant)
    /// let var_expr = Arithmetic::new(Factor::Var("x".to_string()), vec![]);
    /// assert!(!var_expr.is_const());
    ///
    /// // Expression with operations (not constant)
    /// let expr_with_ops = Arithmetic::new(
    ///     Factor::Const(ConstType::Integer(1)),
    ///     vec![(ArithmeticOperator::Plus, Factor::Const(ConstType::Integer(2)))]
    /// );
    /// assert!(!expr_with_ops.is_const());
    /// ```
    #[must_use]
    pub fn is_const(&self) -> bool {
        self.init.is_const() && self.rest.is_empty()
    }

    /// Checks if this arithmetic expression is a single variable.
    ///
    /// Returns `true` if the expression consists of a single variable factor
    /// with no additional operations. This is useful for identifying simple
    /// variable references that don't require complex evaluation.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Arithmetic, Factor, ArithmeticOperator};
    /// use parser::primitive::ConstType;
    ///
    /// // Pure variable
    /// let var_expr = Arithmetic::new(Factor::Var("count".to_string()), vec![]);
    /// assert!(var_expr.is_var());
    ///
    /// // Constant (not variable)
    /// let const_expr = Arithmetic::new(Factor::Const(ConstType::Integer(10)), vec![]);
    /// assert!(!const_expr.is_var());
    ///
    /// // Variable with operations (not simple variable)
    /// let expr_with_ops = Arithmetic::new(
    ///     Factor::Var("x".to_string()),
    ///     vec![(ArithmeticOperator::Plus, Factor::Const(ConstType::Integer(1)))]
    /// );
    /// assert!(!expr_with_ops.is_var());
    /// ```
    #[must_use]
    pub fn is_var(&self) -> bool {
        self.init.is_var() && self.rest.is_empty()
    }
}

impl fmt::Display for Arithmetic {
    /// Formats the arithmetic expression as a human-readable string.
    ///
    /// The format follows standard mathematical notation: the initial factor
    /// followed by operators and subsequent factors, separated by spaces.
    /// This produces output suitable for debugging, logging, or user display.
    ///
    /// # Format
    ///
    /// The output format is: `{init} {op1} {factor1} {op2} {factor2} ...`
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Arithmetic, Factor, ArithmeticOperator};
    /// use parser::primitive::ConstType;
    ///
    /// // Simple variable
    /// let var = Arithmetic::new(Factor::Var("x".to_string()), vec![]);
    /// assert_eq!(var.to_string(), "x");
    ///
    /// // Binary expression
    /// let binary = Arithmetic::new(
    ///     Factor::Var("price".to_string()),
    ///     vec![(ArithmeticOperator::Multiply, Factor::Const(ConstType::Integer(2)))]
    /// );
    /// assert_eq!(binary.to_string(), "price * 2");
    ///
    /// // Complex expression
    /// let complex = Arithmetic::new(
    ///     Factor::Var("a".to_string()),
    ///     vec![
    ///         (ArithmeticOperator::Plus, Factor::Var("b".to_string())),
    ///         (ArithmeticOperator::Divide, Factor::Const(ConstType::Integer(3))),
    ///     ]
    /// );
    /// assert_eq!(complex.to_string(), "a + b / 3");
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.init)?;
        for (op, factor) in &self.rest {
            write!(f, " {op} {factor}")?;
        }
        Ok(())
    }
}

impl Lexeme for Arithmetic {
    /// Parses an arithmetic expression from a pest parsing rule.
    ///
    /// This method constructs an `Arithmetic` expression from parsed grammar tokens.
    /// It expects the parsed rule to contain an initial factor followed by
    /// alternating operators and factors.
    ///
    /// # Arguments
    ///
    /// * `parsed_rule` - A pest `Pair` containing the parsed arithmetic expression rule
    ///
    /// # Returns
    ///
    /// A new `Arithmetic` instance representing the parsed expression.
    ///
    /// # Grammar Structure
    ///
    /// The expected grammar structure is:
    /// ```text
    /// arithmetic = { factor ~ (operator ~ factor)* }
    /// ```
    ///
    /// # Parsing Process
    ///
    /// 1. Extracts the initial factor from the first rule
    /// 2. Consumes remaining rules in pairs: (operator, factor)
    /// 3. Continues until all rules are consumed
    /// 4. Builds the expression structure with init and rest components
    ///
    /// # Examples
    ///
    /// For input text like `"x + 5 * y"`, this method would:
    /// 1. Parse `x` as the initial factor
    /// 2. Parse `+` as the first operator and `5` as the first additional factor
    /// 3. Parse `*` as the second operator and `y` as the second additional factor
    /// 4. Construct an Arithmetic with init=`x` and rest=`[(+, 5), (*, y)]`
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let mut inner_rules = parsed_rule.into_inner();
        let init = Factor::from_parsed_rule(inner_rules.next().unwrap());

        // consume every two next() calls as a pair (op, factor) until there is no more next()
        let mut rest = Vec::new();
        while let Some(op) = inner_rules.next() {
            let factor = Factor::from_parsed_rule(inner_rules.next().unwrap());
            rest.push((ArithmeticOperator::from_parsed_rule(op), factor));
        }

        Self { init, rest }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to create test constants
    fn int_const(value: i32) -> Factor {
        Factor::Const(ConstType::Integer(value))
    }

    fn str_const(value: &str) -> Factor {
        Factor::Const(ConstType::Text(value.to_string()))
    }

    fn var_factor(name: &str) -> Factor {
        Factor::Var(name.to_string())
    }

    #[test]
    fn test_arithmetic_operator_display() {
        assert_eq!(ArithmeticOperator::Plus.to_string(), "+");
        assert_eq!(ArithmeticOperator::Minus.to_string(), "-");
        assert_eq!(ArithmeticOperator::Multiply.to_string(), "*");
        assert_eq!(ArithmeticOperator::Divide.to_string(), "/");
        assert_eq!(ArithmeticOperator::Modulo.to_string(), "%");
    }

    #[test]
    fn test_factor_variable() {
        let factor = var_factor("x");
        assert!(factor.is_var());
        assert!(!factor.is_const());
        assert_eq!(factor.vars(), vec![&"x".to_string()]);
        assert_eq!(factor.vars_set().len(), 1);
        assert!(factor.vars_set().contains(&"x".to_string()));
    }

    #[test]
    fn test_factor_constant() {
        let factor = int_const(42);
        assert!(!factor.is_var());
        assert!(factor.is_const());
        assert!(factor.vars().is_empty());
        assert!(factor.vars_set().is_empty());
    }

    #[test]
    fn test_factor_display() {
        assert_eq!(var_factor("x").to_string(), "x");
        assert_eq!(int_const(42).to_string(), "42");
        assert_eq!(str_const("hello").to_string(), "\"hello\"");
    }

    #[test]
    fn test_arithmetic_simple_expression() {
        // Test: x
        let expr = Arithmetic::new(var_factor("x"), vec![]);
        assert!(expr.is_var());
        assert!(!expr.is_const());
        assert_eq!(expr.vars(), vec![&"x".to_string()]);
        assert_eq!(expr.to_string(), "x");
    }

    #[test]
    fn test_arithmetic_constant_expression() {
        // Test: 42
        let expr = Arithmetic::new(int_const(42), vec![]);
        assert!(!expr.is_var());
        assert!(expr.is_const());
        assert!(expr.vars().is_empty());
        assert_eq!(expr.to_string(), "42");
    }

    #[test]
    fn test_arithmetic_binary_expression() {
        // Test: x + 5
        let expr = Arithmetic::new(
            var_factor("x"),
            vec![(ArithmeticOperator::Plus, int_const(5))],
        );

        assert!(!expr.is_var());
        assert!(!expr.is_const());
        assert_eq!(expr.vars(), vec![&"x".to_string()]);
        assert_eq!(expr.to_string(), "x + 5");
        assert_eq!(expr.init(), &var_factor("x"));
        assert_eq!(expr.rest().len(), 1);
    }

    #[test]
    fn test_arithmetic_complex_expression() {
        // Test: x + y - 10 * z
        let expr = Arithmetic::new(
            var_factor("x"),
            vec![
                (ArithmeticOperator::Plus, var_factor("y")),
                (ArithmeticOperator::Minus, int_const(10)),
                (ArithmeticOperator::Multiply, var_factor("z")),
            ],
        );

        assert!(!expr.is_var());
        assert!(!expr.is_const());
        let vars = expr.vars();
        assert_eq!(vars.len(), 3);
        assert!(vars.contains(&&"x".to_string()));
        assert!(vars.contains(&&"y".to_string()));
        assert!(vars.contains(&&"z".to_string()));
        assert_eq!(expr.to_string(), "x + y - 10 * z");
    }

    #[test]
    fn test_arithmetic_vars_set() {
        // Test: x + y + x (should deduplicate variables)
        let expr = Arithmetic::new(
            var_factor("x"),
            vec![
                (ArithmeticOperator::Plus, var_factor("y")),
                (ArithmeticOperator::Plus, var_factor("x")),
            ],
        );

        let vars_set = expr.vars_set();
        assert_eq!(vars_set.len(), 2);
        assert!(vars_set.contains(&"x".to_string()));
        assert!(vars_set.contains(&"y".to_string()));
    }

    #[test]
    fn test_arithmetic_all_operators() {
        let expr = Arithmetic::new(
            var_factor("a"),
            vec![
                (ArithmeticOperator::Plus, var_factor("b")),
                (ArithmeticOperator::Minus, var_factor("c")),
                (ArithmeticOperator::Multiply, var_factor("d")),
                (ArithmeticOperator::Divide, var_factor("e")),
                (ArithmeticOperator::Modulo, var_factor("f")),
            ],
        );

        assert_eq!(expr.to_string(), "a + b - c * d / e % f");
        assert_eq!(expr.vars().len(), 6);
    }
}
