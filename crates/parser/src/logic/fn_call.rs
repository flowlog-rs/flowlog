//! Function call expressions for FlowLog rule heads and body predicates.
//!
//! A [`FnCall`] represents a user-defined function applied to arguments
//! in a rule head or as a boolean predicate (e.g., `my_udf(x, y + 1)`).
//!
//! # Example
//! ```rust
//! use parser::logic::{FnCall, Arithmetic, Factor};
//! let arg = Arithmetic::new(Factor::Var("x".into()), vec![]);
//! let fc = FnCall::new("my_udf".into(), vec![arg], false);
//! assert_eq!(fc.to_string(), "my_udf(x)");
//! ```

use super::Arithmetic;
use crate::{Lexeme, Rule};

use pest::iterators::Pair;
use std::fmt;

/// A user-defined function call in a rule head or body predicates.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FnCall {
    /// Function name.
    name: String,
    /// Arguments.
    args: Vec<Arithmetic>,
    /// Whether the result is negated (i.e. `!fn_name(...)`).
    is_negated: bool,
}

impl FnCall {
    /// Create a new function call.
    #[must_use]
    pub fn new(name: String, args: Vec<Arithmetic>, is_negated: bool) -> Self {
        Self {
            name,
            args,
            is_negated,
        }
    }

    /// Function name.
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Arguments.
    #[must_use]
    #[inline]
    pub fn args(&self) -> &[Arithmetic] {
        &self.args
    }

    /// Whether the UDF result is negated.
    #[must_use]
    #[inline]
    pub fn is_negated(&self) -> bool {
        self.is_negated
    }

    /// Variables referenced by this function call.
    #[must_use]
    pub fn vars(&self) -> Vec<&String> {
        self.args.iter().flat_map(|a| a.vars()).collect()
    }
}

impl fmt::Display for FnCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let args = self
            .args
            .iter()
            .map(|a| a.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        if self.is_negated {
            write!(f, "!{}({})", self.name, args)
        } else {
            write!(f, "{}({})", self.name, args)
        }
    }
}

impl Lexeme for FnCall {
    /// Parse a `fn_call_expr` grammar node into a [`FnCall`].
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let mut children = parsed_rule.into_inner();
        let fn_name = children
            .next()
            .expect("Parser error: fn_call_expr missing function name")
            .as_str()
            .to_string();
        let args: Vec<Arithmetic> = children
            .filter(|p| p.as_rule() == Rule::arithmetic_expr)
            .map(Arithmetic::from_parsed_rule)
            .collect();
        Self::new(fn_name, args, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logic::Factor;

    fn arith_var(name: &str) -> Arithmetic {
        Arithmetic::new(Factor::Var(name.into()), vec![])
    }

    #[test]
    fn new_non_negated() {
        let fc = FnCall::new("my_udf".into(), vec![arith_var("x")], false);
        assert_eq!(fc.name(), "my_udf");
        assert_eq!(fc.args().len(), 1);
        assert!(!fc.is_negated());
    }

    #[test]
    fn new_negated() {
        let fc = FnCall::new("is_valid".into(), vec![arith_var("x"), arith_var("y")], true);
        assert_eq!(fc.name(), "is_valid");
        assert_eq!(fc.args().len(), 2);
        assert!(fc.is_negated());
    }

    #[test]
    fn display_non_negated() {
        let fc = FnCall::new("f".into(), vec![arith_var("a"), arith_var("b")], false);
        assert_eq!(fc.to_string(), "f(a, b)");
    }

    #[test]
    fn display_negated() {
        let fc = FnCall::new("f".into(), vec![arith_var("a")], true);
        assert_eq!(fc.to_string(), "!f(a)");
    }

    #[test]
    fn vars_collects_all_arguments() {
        let fc = FnCall::new("g".into(), vec![arith_var("x"), arith_var("y")], false);
        let vars: Vec<&str> = fc.vars().iter().map(|s| s.as_str()).collect();
        assert_eq!(vars, vec!["x", "y"]);
    }

    #[test]
    fn parse_fn_call_expr() {
        use crate::{FlowLogParser, Lexeme, Rule};
        use pest::Parser;

        let input = "my_udf(x, y)";
        let mut pairs = FlowLogParser::parse(Rule::fn_call_expr, input).unwrap();
        let fc = FnCall::from_parsed_rule(pairs.next().unwrap());
        assert_eq!(fc.name(), "my_udf");
        assert_eq!(fc.args().len(), 2);
        assert!(!fc.is_negated());
    }
}
