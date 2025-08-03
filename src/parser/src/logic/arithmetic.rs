//! Arithmetic expression types for FlowLog.

use crate::primitive::ConstType;
use crate::{Lexeme, Rule};

use pest::iterators::Pair;
use std::collections::HashSet;
use std::fmt;

/// Arithmetic operators for mathematical expressions.
///
/// Supports the basic arithmetic operations commonly used in mathematical
/// and logical expressions within FlowLog rules.
///
/// # Examples
///
/// ```rust
/// use parser::logic::ArithmeticOperator;
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
/// use parser::logic::Factor;
/// use parser::primitive::ConstType;
///
/// let var_factor = Factor::Var("x".to_string());
/// let const_factor = Factor::Const(ConstType::Integer(42));
///
/// assert!(var_factor.is_var());
/// assert!(const_factor.is_const());
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
/// # Examples
///
/// ```rust
/// use parser::logic::{Arithmetic, Factor, ArithmeticOperator};
/// use parser::primitive::ConstType;
///
/// // Create expression: x + 5
/// let init = Factor::Var("x".to_string());
/// let rest = vec![(ArithmeticOperator::Plus, Factor::Const(ConstType::Integer(5)))];
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
