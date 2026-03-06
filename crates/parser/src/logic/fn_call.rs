//! Function call expressions for FlowLog rule heads and body predicates.
//!
//! A [`FnCall`] represents a user-defined function applied to arguments
//! in a rule head or as a boolean predicate (e.g., `my_udf(x, y + 1)`).
//!
//! # Example
//! ```rust
//! use parser::logic::{FnCall, Arithmetic, Factor};
//! let arg = Arithmetic::new(Factor::Var("x".into()), vec![]);
//! let fc = FnCall::new("my_udf".into(), vec![arg]);
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
}

impl FnCall {
    /// Create a new function call.
    #[must_use]
    pub fn new(name: String, args: Vec<Arithmetic>) -> Self {
        Self { name, args }
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
        write!(f, "{}({})", self.name, args)
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
        Self::new(fn_name, args)
    }
}
