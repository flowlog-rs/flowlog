//! Expression types for FlowLog.
//!
//! This module contains all expression-related types for the FlowLog parser, including:
//! - Arithmetic expressions with operators and factors
//! - Comparison expressions for conditional logic  
//! - Atom expressions representing predicates

use pest::iterators::Pair;
use std::collections::HashSet;
use std::fmt;

use super::{primitive::Const, Lexeme, Rule};

// =============================================================================
// ARITHMETIC EXPRESSIONS
// =============================================================================

/// Arithmetic operators for mathematical expressions.
///
/// Supports the basic arithmetic operations commonly used in mathematical
/// and logical expressions within FlowLog rules.
///
/// # Examples
///
/// ```rust
/// use parser::expression::ArithmeticOperator;
///
/// let op = ArithmeticOperator::Plus;
/// assert_eq!(op.to_string(), "+");
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
///
/// # Examples
///
/// ```rust
/// use parser::expression::Factor;
/// use parser::primitive::Const;
///
/// let var_factor = Factor::Var("x".to_string());
/// let const_factor = Factor::Const(Const::Integer(42));
///
/// assert!(var_factor.is_var());
/// assert!(const_factor.is_const());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Factor {
    /// A variable reference
    Var(String),
    /// A constant value
    Const(Const),
}

impl Factor {
    /// Checks if this factor is a variable.
    #[must_use]
    pub fn is_var(&self) -> bool {
        matches!(self, Self::Var(_))
    }

    /// Checks if this factor is a constant.
    #[must_use]
    pub fn is_const(&self) -> bool {
        matches!(self, Self::Const(_))
    }

    /// Returns a HashSet of all variables in this factor.
    #[must_use]
    pub fn vars_set(&self) -> HashSet<&String> {
        self.vars().into_iter().collect()
    }

    /// Returns a vector of references to variables in this factor.
    #[must_use]
    pub fn vars(&self) -> Vec<&String> {
        match self {
            Self::Var(var) => vec![var],
            _ => vec![],
        }
    }
}

impl fmt::Display for Factor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Var(var) => write!(f, "{var}"),
            Self::Const(constant) => write!(f, "{constant}"),
        }
    }
}

impl Lexeme for Factor {
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        match parsed_rule.as_rule() {
            Rule::factor => {
                // factor is a wrapper that contains either variable or constant
                let inner_rule = parsed_rule.into_inner().next().unwrap();
                Self::from_parsed_rule(inner_rule)
            }
            Rule::variable => Self::Var(parsed_rule.as_str().to_string()),
            Rule::constant => Self::Const(Const::from_parsed_rule(parsed_rule)),
            _ => unreachable!("Invalid factor type: {:?}", parsed_rule.as_rule()),
        }
    }
}

/// Represents an arithmetic expression.
///
/// An arithmetic expression consists of an initial factor followed by a sequence
/// of (operator, factor) pairs. This representation naturally handles left-associative
/// parsing and allows for expressions like `x + y * 2 - z`.
///
/// # Examples
///
/// ```rust
/// use parser::expression::{Arithmetic, Factor, ArithmeticOperator};
/// use parser::primitive::Const;
///
/// // Create expression: x + 5
/// let init = Factor::Var("x".to_string());
/// let rest = vec![(ArithmeticOperator::Plus, Factor::Const(Const::Integer(5)))];
/// let expr = Arithmetic::new(init, rest);
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
    #[must_use]
    pub fn new(init: Factor, rest: Vec<(ArithmeticOperator, Factor)>) -> Self {
        Self { init, rest }
    }

    /// Returns a reference to the initial factor in this arithmetic expression.
    #[must_use]
    pub fn init(&self) -> &Factor {
        &self.init
    }

    /// Returns a reference to the sequence of (operator, factor) pairs.
    #[must_use]
    pub fn rest(&self) -> &Vec<(ArithmeticOperator, Factor)> {
        &self.rest
    }

    /// Returns all terms (factors) in this arithmetic expression.
    ///
    /// This method provides compatibility with the old API by collecting
    /// all factors into a single vector.
    #[must_use]
    pub fn terms(&self) -> Vec<&Factor> {
        let mut terms = vec![&self.init];
        terms.extend(self.rest.iter().map(|(_, factor)| factor));
        terms
    }

    /// Returns all operators in this arithmetic expression.
    ///
    /// This method provides compatibility with the old API by collecting
    /// all operators into a single vector.
    #[must_use]
    pub fn operators(&self) -> Vec<&ArithmeticOperator> {
        self.rest.iter().map(|(op, _)| op).collect()
    }

    /// Returns all variables used in this arithmetic expression.
    #[must_use]
    pub fn vars(&self) -> Vec<&String> {
        let mut vars = self.init.vars();
        for (_, factor) in &self.rest {
            vars.extend(factor.vars());
        }
        vars
    }

    /// Returns a HashSet of all variables in this arithmetic expression.
    #[must_use]
    pub fn vars_set(&self) -> HashSet<&String> {
        self.vars().into_iter().collect()
    }

    /// Checks if this arithmetic expression contains only constants.
    #[must_use]
    pub fn is_const(&self) -> bool {
        self.init.is_const() && self.rest.is_empty()
    }

    /// Checks if this arithmetic expression is a single variable.
    #[must_use]
    pub fn is_var(&self) -> bool {
        self.init.is_var() && self.rest.is_empty()
    }
}

impl fmt::Display for Arithmetic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.init)?;
        for (op, factor) in &self.rest {
            write!(f, " {op} {factor}")?;
        }
        Ok(())
    }
}

impl Lexeme for Arithmetic {
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

// =============================================================================
// COMPARISON EXPRESSIONS
// =============================================================================

/// Comparison operators for expressions.
///
/// These operators are used to create boolean expressions that compare
/// arithmetic expressions or values in FlowLog rules.
///
/// # Examples
///
/// ```rust
/// use parser::expression::ComparisonOperator;
///
/// let op = ComparisonOperator::Equals;
/// assert!(op.is_equals());
/// assert_eq!(op.to_string(), "==");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ComparisonOperator {
    /// Equality comparison (==)
    Equals,
    /// Inequality comparison (≠)
    NotEquals,
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
    pub fn is_equals(&self) -> bool {
        matches!(self, Self::Equals)
    }
}

impl fmt::Display for ComparisonOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let symbol = match self {
            Self::Equals => "==",
            Self::NotEquals => "≠",
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
            Rule::equals => Self::Equals,
            Rule::not_equals => Self::NotEquals,
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
/// use parser::expression::{ComparisonExpr, ComparisonOperator, Arithmetic, Factor};
/// use parser::primitive::Const;
///
/// // Create comparison: x == 5
/// let left = Arithmetic::new(Factor::Var("x".to_string()), vec![]);
/// let right = Arithmetic::new(Factor::Const(Const::Integer(5)), vec![]);
/// let comp = ComparisonExpr::new(left, ComparisonOperator::Equals, right);
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

    /// Returns all variables used in this comparison expression.
    #[must_use]
    pub fn vars(&self) -> Vec<&String> {
        let mut vars = self.left.vars();
        vars.extend(self.right.vars());
        vars
    }

    /// Returns a HashSet of all variables in this comparison expression.
    #[must_use]
    pub fn vars_set(&self) -> HashSet<&String> {
        self.vars().into_iter().collect()
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

// =============================================================================
// ATOMS
// =============================================================================

/// Represents an argument to an atom in rule expressions.
///
/// Atom arguments can be variables, constants, or placeholders (_).
/// Placeholders are used for values that are not bound in the current context.
///
/// # Examples
///
/// ```rust
/// use parser::expression::AtomArg;
/// use parser::primitive::Const;
///
/// let var_arg = AtomArg::Var("x".to_string());
/// let const_arg = AtomArg::Const(Const::Integer(42));
/// let placeholder = AtomArg::Placeholder;
///
/// assert!(var_arg.is_var());
/// assert!(const_arg.is_const());
/// assert!(placeholder.is_placeholder());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AtomArg {
    /// A variable argument
    Var(String),
    /// A constant argument
    Const(Const),
    /// A placeholder argument (_)
    Placeholder,
}

impl AtomArg {
    /// Checks if this argument is a variable.
    #[must_use]
    pub fn is_var(&self) -> bool {
        matches!(self, Self::Var(_))
    }

    /// Checks if this argument is a constant.
    #[must_use]
    pub fn is_const(&self) -> bool {
        matches!(self, Self::Const(_))
    }

    /// Checks if this argument is a placeholder.
    #[must_use]
    pub fn is_placeholder(&self) -> bool {
        matches!(self, Self::Placeholder)
    }

    /// Extracts the variable name from this argument.
    ///
    /// # Panics
    ///
    /// Panics if this argument is not a variable.
    #[must_use]
    pub fn as_var(&self) -> &String {
        match self {
            Self::Var(var) => var,
            _ => unreachable!("Expected variable but got: {self:?}"),
        }
    }
}

impl fmt::Display for AtomArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Var(var) => write!(f, "{var}"),
            Self::Const(constant) => write!(f, "{constant}"),
            Self::Placeholder => write!(f, "_"),
        }
    }
}

impl Lexeme for AtomArg {
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        match parsed_rule.as_rule() {
            Rule::atom_arg => {
                // atom_arg is a wrapper that contains variable, constant, or placeholder
                let inner_rule = parsed_rule.into_inner().next().unwrap();
                Self::from_parsed_rule(inner_rule)
            }
            Rule::variable => Self::Var(parsed_rule.as_str().to_string()),
            Rule::constant => Self::Const(Const::from_parsed_rule(parsed_rule)),
            Rule::placeholder => Self::Placeholder,
            _ => unreachable!("Invalid atom argument type: {:?}", parsed_rule.as_rule()),
        }
    }
}

/// Represents an atom (predicate) in FlowLog rules.
///
/// Atoms are the fundamental building blocks of FlowLog rules, representing
/// relations with named arguments. They correspond to predicates in logic programming.
///
/// # Examples
///
/// ```rust
/// use parser::expression::{Atom, AtomArg};
/// use parser::primitive::Const;
///
/// // Create atom: parent(john, mary)
/// let args = vec![
///     AtomArg::Const(Const::Text("john".to_string())),
///     AtomArg::Const(Const::Text("mary".to_string()))
/// ];
/// let atom = Atom::from_str("parent", args);
/// assert_eq!(atom.arity(), 2);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Atom {
    name: String,
    arguments: Vec<AtomArg>,
}

impl fmt::Display for Atom {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({})",
            self.name,
            self.arguments
                .iter()
                .map(|arg| arg.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

impl Atom {
    /// Creates a new atom from a name and a list of arguments.
    #[must_use]
    pub fn from_str(name: &str, arguments: Vec<AtomArg>) -> Self {
        Self {
            name: name.to_string(),
            arguments,
        }
    }

    /// Adds an argument to this atom.
    pub fn push_arg(&mut self, arg: AtomArg) {
        self.arguments.push(arg);
    }

    /// Returns the name of this atom.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the arguments of this atom.
    #[must_use]
    pub fn arguments(&self) -> &Vec<AtomArg> {
        &self.arguments
    }

    /// Returns the arity (number of arguments) of this atom.
    #[must_use]
    pub fn arity(&self) -> usize {
        self.arguments.len()
    }
}

impl Lexeme for Atom {
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let mut inner = parsed_rule.into_inner();
        let name = inner.next().unwrap().as_str().to_string();
        let mut arguments = Vec::new();

        for pair in inner {
            if pair.as_rule() == Rule::atom_arg {
                arguments.push(AtomArg::from_parsed_rule(pair));
            }
        }

        Self { name, arguments }
    }
}
