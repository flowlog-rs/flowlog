//! Rule head components for FlowLog.

use super::Arithmetic;
use crate::{Lexeme, Rule};
use pest::iterators::Pair;
use std::fmt;

/// Represents an argument in a rule head.
///
/// Rule head arguments define what values are produced by the rule.
/// They can be simple variables, computed arithmetic expressions,
/// or aggregate functions (currently unimplemented).
///
/// # Examples
///
/// ```rust
/// use parser::logic::HeadArg;
/// use parser::logic::{Arithmetic, Factor};
/// use parser::primitive::ConstType;
/// use parser::logic::ArithmeticOperator;
///
/// // Simple variable argument
/// let var_arg = HeadArg::Var("X".to_string());
///
/// // Arithmetic expression argument: X + 1
/// let arith = Arithmetic::new(
///     Factor::Var("X".to_string()),
///     vec![(ArithmeticOperator::Plus, Factor::Const(ConstType::Integer(1)))]
/// );
/// let arith_arg = HeadArg::Arith(arith);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HeadArg {
    /// A variable argument (e.g., X, Y)
    Var(String),
    /// An arithmetic expression (e.g., X + Y, X * 2)
    Arith(Arithmetic),
    /// An aggregate function (e.g., count(X), sum(X + Y))
    ///
    /// # Note
    ///
    /// Currently unimplemented and will panic if used.
    GroupBy(Arithmetic),
}

impl HeadArg {
    /// Returns a vector of references to all variables in this head argument.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::HeadArg;
    ///
    /// let var_arg = HeadArg::Var("X".to_string());
    /// assert_eq!(var_arg.vars(), vec![&"X".to_string()]);
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if called on a `GroupBy` variant (currently unimplemented).
    #[must_use]
    pub fn vars(&self) -> Vec<&String> {
        match self {
            Self::Var(var) => vec![var],
            Self::Arith(arith) => arith.vars(),
            Self::GroupBy(_arith) => todo!("GroupBy unimplemented"),
        }
    }
}

impl fmt::Display for HeadArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Var(var) => write!(f, "{var}"),
            Self::Arith(arith) => write!(f, "{arith}"),
            Self::GroupBy(arith) => write!(f, "{arith}"),
        }
    }
}

impl Lexeme for HeadArg {
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let inner = parsed_rule
            .into_inner()
            .next()
            .expect("Expected inner rule for head_arg");

        // Process the rule based on its type
        match inner.as_rule() {
            Rule::arithmetic_expr => {
                // Parse as arithmetic (which also handles variables)
                let arithmetic = Arithmetic::from_parsed_rule(inner);

                // Check if it's a simple variable
                if arithmetic.is_var() {
                    // Extract the variable name and create a Var variant
                    let var_name = arithmetic.init().vars()[0].to_string();
                    Self::Var(var_name)
                } else {
                    Self::Arith(arithmetic)
                }
            }
            Rule::aggregate_expr => {
                // TODO: Implement aggregate/groupby parsing
                todo!("Aggregate parsing not implemented yet")
            }
            _ => unreachable!("Unexpected rule in HeadArg: {:?}", inner.as_rule()),
        }
    }
}

/// Represents the head of a rule (e.g., "result(X, Y + Z)").
///
/// The head defines what relation and values are produced when the rule fires.
/// It consists of a relation name and a list of arguments that can be variables,
/// arithmetic expressions, or aggregate functions.
///
/// # Examples
///
/// ```rust
/// use parser::logic::{Head, HeadArg};
///
/// // Create head: person(X, Y)
/// let args = vec![
///     HeadArg::Var("X".to_string()),
///     HeadArg::Var("Y".to_string()),
/// ];
/// let head = Head::new("person".to_string(), args);
/// assert_eq!(head.arity(), 2);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Head {
    /// Name of the relation being defined
    name: String,
    /// Arguments in the head
    head_arguments: Vec<HeadArg>,
}

impl Head {
    /// Creates a new rule head.
    #[must_use]
    pub fn new(name: String, head_arguments: Vec<HeadArg>) -> Self {
        Self {
            name,
            head_arguments,
        }
    }

    /// Returns the name of the relation being defined.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the arguments in this rule head.
    #[must_use]
    pub fn head_arguments(&self) -> &[HeadArg] {
        &self.head_arguments
    }

    /// Returns the arity (number of arguments) of this rule head.
    #[must_use]
    pub fn arity(&self) -> usize {
        self.head_arguments.len()
    }
}

impl fmt::Display for Head {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({})",
            self.name,
            self.head_arguments
                .iter()
                .map(|arg| arg.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

impl Lexeme for Head {
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let mut inner = parsed_rule.into_inner();
        let name = inner.next().unwrap().as_str().to_string();

        let mut head_arguments = Vec::new();
        // Process remaining rules as head arguments directly
        for arg_rule in inner {
            head_arguments.push(HeadArg::from_parsed_rule(arg_rule));
        }

        Self::new(name, head_arguments)
    }
}
