//! Arithmetic expressions for Macaron Datalog Programs.
//!
//! - [`ArithmeticOperator`]: `+ | - | * | / | %`
//! - [`Factor`]: variables or constants
//! - [`Arithmetic`]: `factor (op, factor)*`
//!
//! # Example
//! ```rust
//! use parser::logic::{Arithmetic, ArithmeticOperator, Factor};
//! use parser::primitive::ConstType;
//!
//! let expr = Arithmetic::new(
//!     Factor::Var("x".into()),
//!     vec![(ArithmeticOperator::Plus, Factor::Const(ConstType::Integer(5)))],
//! );
//! assert_eq!(expr.to_string(), "x + 5");
//! ```

use crate::primitive::ConstType;
use crate::{Lexeme, Rule};
use pest::iterators::Pair;
use std::collections::HashSet;
use std::fmt;

/// Arithmetic operator.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum ArithmeticOperator {
    Plus,     // +
    Minus,    // -
    Multiply, // *
    Divide,   // /
    Modulo,   // %
}

impl fmt::Display for ArithmeticOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sym = match self {
            Self::Plus => "+",
            Self::Minus => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::Modulo => "%",
        };
        write!(f, "{sym}")
    }
}

impl Lexeme for ArithmeticOperator {
    /// Parse an operator from the grammar.
    ///
    /// # Panics
    /// Panics if the rule is not one of `plus|minus|times|divide|modulo`.
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let op = parsed_rule
            .into_inner()
            .next()
            .expect("Parser error: operator missing inner token");
        match op.as_rule() {
            Rule::plus => Self::Plus,
            Rule::minus => Self::Minus,
            Rule::times => Self::Multiply,
            Rule::divide => Self::Divide,
            Rule::modulo => Self::Modulo,
            other => panic!("Parser error: unknown arithmetic operator: {:?}", other),
        }
    }
}

/// Atomic operand for arithmetic: variable or constant.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Factor {
    Var(String),
    Const(ConstType),
}

impl Factor {
    #[must_use]
    pub fn is_var(&self) -> bool {
        matches!(self, Self::Var(_))
    }

    #[must_use]
    pub fn is_const(&self) -> bool {
        matches!(self, Self::Const(_))
    }

    /// Variables appearing in this factor (0 or 1).
    #[must_use]
    pub fn vars(&self) -> Vec<&String> {
        match self {
            Self::Var(v) => vec![v],
            _ => vec![],
        }
    }

    /// Unique variables in this factor.
    #[must_use]
    pub fn vars_set(&self) -> HashSet<&String> {
        self.vars().into_iter().collect()
    }
}

impl fmt::Display for Factor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Var(v) => write!(f, "{v}"),
            Self::Const(c) => write!(f, "{c}"),
        }
    }
}

impl Lexeme for Factor {
    /// Parse a factor (variable or constant).
    ///
    /// # Panics
    /// Panics if the inner token is neither `variable` nor `constant`.
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let inner = parsed_rule
            .into_inner()
            .next()
            .expect("Parser error: factor missing inner token");
        match inner.as_rule() {
            Rule::variable => Self::Var(inner.as_str().to_string()),
            Rule::constant => Self::Const(ConstType::from_parsed_rule(inner)),
            other => panic!("Parser error: invalid factor rule: {:?}", other),
        }
    }
}

/// `factor (op, factor)*` expression (left-associative pretty print).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Arithmetic {
    init: Factor,
    rest: Vec<(ArithmeticOperator, Factor)>,
}

impl Arithmetic {
    #[must_use]
    pub fn new(init: Factor, rest: Vec<(ArithmeticOperator, Factor)>) -> Self {
        Self { init, rest }
    }

    /// First term.
    #[must_use]
    pub fn init(&self) -> &Factor {
        &self.init
    }

    /// Remaining `(op, factor)` pairs.
    #[must_use]
    pub fn rest(&self) -> &[(ArithmeticOperator, Factor)] {
        &self.rest
    }

    /// Variables in order of appearance (duplicates preserved).
    #[must_use]
    pub fn vars(&self) -> Vec<&String> {
        let mut out = self.init.vars();
        for (_, f) in &self.rest {
            out.extend(f.vars());
        }
        out
    }

    /// Unique variables (deduplicated).
    #[must_use]
    pub fn vars_set(&self) -> HashSet<&String> {
        self.vars().into_iter().collect()
    }

    /// `true` if a single constant with no ops.
    #[must_use]
    pub fn is_const(&self) -> bool {
        self.rest.is_empty() && self.init.is_const()
    }

    /// `true` if a single variable with no ops.
    #[must_use]
    pub fn is_var(&self) -> bool {
        self.rest.is_empty() && self.init.is_var()
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
    /// Parse `factor (operator factor)*`.
    ///
    /// # Panics
    /// Panics if the sequence is malformed (e.g., operator without following factor).
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let mut inner = parsed_rule.into_inner();

        let init = Factor::from_parsed_rule(
            inner
                .next()
                .expect("Parser error: arithmetic missing initial factor"),
        );

        let mut rest = Vec::new();
        while let Some(op_pair) = inner.next() {
            let factor_pair = inner
                .next()
                .expect("Parser error: arithmetic expected (operator, factor) pair");
            let op = ArithmeticOperator::from_parsed_rule(op_pair);
            let factor = Factor::from_parsed_rule(factor_pair);
            rest.push((op, factor));
        }

        Self::new(init, rest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitive::ConstType::{Integer, Text};
    use ArithmeticOperator::*;
    use Factor::*;

    fn v(name: &str) -> Factor {
        Var(name.into())
    }
    fn i(n: u64) -> Factor {
        Const(Integer(n))
    }
    fn s(t: &str) -> Factor {
        Const(Text(t.into()))
    }

    #[test]
    fn operator_display() {
        assert_eq!(Plus.to_string(), "+");
        assert_eq!(Minus.to_string(), "-");
        assert_eq!(Multiply.to_string(), "*");
        assert_eq!(Divide.to_string(), "/");
        assert_eq!(Modulo.to_string(), "%");
    }

    #[test]
    fn factor_basics_and_display() {
        assert!(v("x").is_var());
        assert!(!i(1).is_var());
        assert!(i(1).is_const());
        assert_eq!(v("x").to_string(), "x");
        assert_eq!(i(42).to_string(), "42");
        assert_eq!(s("hi").to_string(), "\"hi\"");
    }

    #[test]
    fn arithmetic_smoke() {
        let a = Arithmetic::new(v("x"), vec![]);
        assert!(a.is_var());
        assert_eq!(a.to_string(), "x");
        assert_eq!(a.vars(), vec![&"x".to_string()]);

        let b = Arithmetic::new(i(42), vec![]);
        assert!(b.is_const());
        assert_eq!(b.to_string(), "42");

        let c = Arithmetic::new(
            v("x"),
            vec![(Plus, i(5)), (Multiply, v("y")), (Minus, i(10))],
        );
        assert!(!c.is_var() && !c.is_const());
        assert_eq!(c.to_string(), "x + 5 * y - 10");

        let set = c.vars_set();
        let x_str = "x".to_string();
        let y_str = "y".to_string();
        assert!(set.contains(&x_str));
        assert!(set.contains(&y_str));
    }
}
