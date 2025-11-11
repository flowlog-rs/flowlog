//! Atom types for Macaron Datalog programs.
//!
//! - [`AtomArg`]: variable / constant / placeholder (`_`)
//! - [`Atom`]: `name(arg1, ..., argN)`
//!
//! # Example
//! ```rust
//! use parser::logic::{Atom, AtomArg};
//! use parser::primitive::ConstType;
//! let a = Atom::new("person", vec![
//!     AtomArg::Const(ConstType::Text("Alice".into())),
//!     AtomArg::Const(ConstType::Integer(25)),
//!     AtomArg::Placeholder,
//! ], 0);
//! assert!(a.to_string().starts_with("person(\"Alice\", 25, _)"));
//! ```

use crate::primitive::ConstType;
use crate::{Lexeme, Rule};
use pest::iterators::Pair;
use std::collections::HashSet;
use std::fmt;
use std::hash::Hash;

use common::compute_fp;

/// An argument to an atom: variable, constant, or `_`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AtomArg {
    Var(String),
    Const(ConstType),
    Placeholder,
}

impl AtomArg {
    #[must_use]
    pub fn is_var(&self) -> bool {
        matches!(self, Self::Var(_))
    }

    #[must_use]
    pub fn is_const(&self) -> bool {
        matches!(self, Self::Const(_))
    }

    #[must_use]
    pub fn is_placeholder(&self) -> bool {
        matches!(self, Self::Placeholder)
    }

    /// Return the variable name; panics if not a variable.
    #[must_use]
    pub fn as_var(&self) -> &String {
        match self {
            Self::Var(v) => v,
            _ => panic!("AtomArg::as_var() called on non-variable: {self:?}"),
        }
    }
}

impl fmt::Display for AtomArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Var(v) => write!(f, "{v}"),
            Self::Const(c) => write!(f, "{c}"),
            Self::Placeholder => write!(f, "_"),
        }
    }
}

impl Lexeme for AtomArg {
    /// Parse an atom argument from the grammar.
    ///
    /// # Panics
    /// Panics if the inner token is not `variable|constant|placeholder`.
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let inner = parsed_rule
            .into_inner()
            .next()
            .expect("Parser error: atom_arg missing inner token");

        match inner.as_rule() {
            Rule::variable => Self::Var(inner.as_str().to_string()),
            Rule::constant => Self::Const(ConstType::from_parsed_rule(inner)),
            Rule::placeholder => Self::Placeholder,
            other => panic!("Parser error: invalid atom argument rule: {:?}", other),
        }
    }
}

/// `name(arg1, ..., argN)` predicate.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Atom {
    name: String,
    arguments: Vec<AtomArg>,
    fingerprint: u64,
}

impl Atom {
    /// Create a new atom.
    #[must_use]
    pub fn new(name: &str, arguments: Vec<AtomArg>, fingerprint: u64) -> Self {
        Self {
            name: name.to_string(),
            arguments,
            fingerprint,
        }
    }

    /// Append an argument.
    pub fn push_arg(&mut self, arg: AtomArg) {
        self.arguments.push(arg);
    }

    /// Relation name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Arguments (as a slice).
    #[must_use]
    pub fn arguments(&self) -> &[AtomArg] {
        &self.arguments
    }

    /// Number of arguments.
    #[must_use]
    pub fn arity(&self) -> usize {
        self.arguments.len()
    }

    /// Get the fingerprint.
    #[must_use]
    pub fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    /// Get the set of variable names in this atom's arguments.
    pub fn vars_set(&self) -> HashSet<&String> {
        self.arguments
            .iter()
            .filter_map(|arg| {
                if let AtomArg::Var(v) = arg {
                    Some(v)
                } else {
                    None
                }
            })
            .collect()
    }
}

impl fmt::Display for Atom {
    /// Formats as `name(a, b, _)`, always including parentheses.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name)?;
        for (i, arg) in self.arguments.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{arg}")?;
        }
        write!(f, ") [fp: 0x{:016x}]", self.fingerprint)
    }
}

impl Lexeme for Atom {
    /// Parse `name("(" (atom_arg ("," atom_arg)*)? ")")`.
    ///
    /// # Panics
    /// Panics if the structure is malformed.
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let mut inner = parsed_rule.into_inner();

        let name = inner
            .next()
            .expect("Parser error: atom missing relation name")
            .as_str()
            .to_string();

        let mut arguments = Vec::new();
        for pair in inner {
            if pair.as_rule() == Rule::atom_arg {
                arguments.push(AtomArg::from_parsed_rule(pair));
            }
        }

        // Generate signature
        let fingerprint = compute_fp(("atom", &name));

        Self {
            name,
            arguments,
            fingerprint,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitive::ConstType::{Integer, Text};
    use AtomArg::*;

    fn v(n: &str) -> AtomArg {
        Var(n.into())
    }
    fn i(n: i32) -> AtomArg {
        Const(Integer(n))
    }
    fn s(t: &str) -> AtomArg {
        Const(Text(t.into()))
    }

    #[test]
    fn atomarg_basics_and_display() {
        let va = v("X");
        assert!(va.is_var());
        assert_eq!(va.as_var(), "X");
        assert_eq!(va.to_string(), "X");

        let ca = i(42);
        assert!(ca.is_const());
        assert_eq!(ca.to_string(), "42");

        let pa = Placeholder;
        assert!(pa.is_placeholder());
        assert_eq!(pa.to_string(), "_");
    }

    #[test]
    #[should_panic(expected = "AtomArg::as_var() called on non-variable")]
    fn as_var_panics_on_non_var() {
        let a = i(1);
        let _ = a.as_var();
    }

    #[test]
    fn atom_smoke() {
        // nullary
        let a0 = Atom::new("flag", vec![], 0);
        assert_eq!(a0.arity(), 0);
        assert!(a0.to_string().starts_with("flag()"));

        // unary
        let a1 = Atom::new("student", vec![v("X")], 1);
        assert_eq!(a1.arity(), 1);
        assert!(a1.to_string().starts_with("student(X)"));

        // mixed
        let a = Atom::new("person", vec![s("Alice"), i(25), Placeholder, v("Z")], 2);
        assert_eq!(a.arity(), 4);
        assert_eq!(a.name(), "person");
        assert!(a.to_string().starts_with("person(\"Alice\", 25, _, Z)"));
    }

    #[test]
    fn push_arg() {
        let mut a = Atom::new("r", vec![], 3);
        a.push_arg(v("X"));
        a.push_arg(i(7));
        assert_eq!(a.arity(), 2);
        assert!(a.to_string().starts_with("r(X, 7)"));
    }
}
