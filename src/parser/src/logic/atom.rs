//! Atom and atom argument types for FlowLog.

use crate::primitive::ConstType;
use crate::{Lexeme, Rule};

use pest::iterators::Pair;
use std::fmt;

/// Represents an argument to an atom in rule expressions.
///
/// Atom arguments can be variables, constants, or placeholders (_).
/// Placeholders are used for values that are not bound in the current context.
///
/// # Examples
///
/// ```rust
/// use parser::logic::AtomArg;
/// use parser::primitive::ConstType;
///
/// let var_arg = AtomArg::Var("x".to_string());
/// let const_arg = AtomArg::Const(ConstType::Integer(42));
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
    Const(ConstType),
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
        let inner = parsed_rule
            .into_inner()
            .next()
            .expect("Expected inner rule for head_arg");

        match inner.as_rule() {
            Rule::variable => Self::Var(inner.as_str().to_string()),
            Rule::constant => Self::Const(ConstType::from_parsed_rule(inner)),
            Rule::placeholder => Self::Placeholder,
            _ => unreachable!("Invalid atom argument type: {:?}", inner.as_rule()),
        }
    }
}

/// Represents an atom in FlowLog rules.
///
/// Atoms are the fundamental building blocks of FlowLog rules, representing
/// relations with named arguments. They correspond to predicates in logic programming.
///
/// # Examples
///
/// ```rust
/// use parser::logic::{Atom, AtomArg};
/// use parser::primitive::ConstType;
///
/// // Create atom: parent(john, mary)
/// let args = vec![
///     AtomArg::Const(ConstType::Text("john".to_string())),
///     AtomArg::Const(ConstType::Text("mary".to_string()))
/// ];
/// let atom = Atom::new("parent", args);
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
    pub fn new(name: &str, arguments: Vec<AtomArg>) -> Self {
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
